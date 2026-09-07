#!/usr/bin/env python3
"""Optimize inference instructions with GEPA and the existing LLM judge."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any

from gepa import optimize
from gepa.adapters.langchain_adapter import LangChainAdapter, make_reflection_lm
from gepa.utils.stop_condition import MaxCandidateProposalsStopper
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openrouter import ChatOpenRouter
from tqdm import tqdm

from evaluate_predictions import (
    JudgeConfig,
    OpenRouterJudge,
    annotate_rows,
    build_case_result,
    metrics,
    restore_document_indices,
)
from paths import ARTIFACTS_DIR, DATASET_DIR
from run_inference import BASE_INSTRUCTIONS, Findings, build_prompt

MODEL = "openai/gpt-5.6-luna:nitro"
JUDGE_MODEL = "openai/gpt-5.6-luna:nitro"


def micro_f2(counts: list[dict[str, int]]) -> float:
    total = {key: sum(row[key] for row in counts) for key in ("tp", "fp", "fn")}
    return float(metrics(**total)["f2"])


class MicroF2Adapter(LangChainAdapter):
    def evaluate(self, batch, candidate, capture_traces=False):
        result = super().evaluate(batch, candidate, capture_traces)
        counts = [output["state"]["counts"] for output in result.outputs]
        result.scores = [micro_f2(counts)] * len(result.scores)
        return result


class IterationProgress:
    def __init__(self, total: int):
        self.bar = tqdm(total=total, desc="GEPA", unit="mutation")
        self.best_score: float | None = None
        self.minibatch_sizes: dict[int, int] = {}

    def on_minibatch_sampled(self, event: dict[str, Any]) -> None:
        self.minibatch_sizes[event["iteration"]] = len(event["minibatch_ids"])

    def on_valset_evaluated(self, event: dict[str, Any]) -> None:
        score = float(event["average_score"])
        if event["iteration"] == 0:
            self.best_score = score
            tqdm.write(f"Baseline validation micro F2: {score:.4f}")
        else:
            is_new_best = event["is_best_program"] and (
                self.best_score is None or score > self.best_score
            )
            if is_new_best:
                self.best_score = score
            suffix = " (new best)" if is_new_best else ""
            tqdm.write(
                f"Iteration {event['iteration']}: validation micro F2: {score:.4f}{suffix}"
            )
        if self.best_score is not None:
            self.bar.set_postfix(best=f"{self.best_score:.4f}")

    def on_candidate_rejected(self, event: dict[str, Any]) -> None:
        size = self.minibatch_sizes.get(event["iteration"], 1)
        score = float(event["new_score"]) / size
        tqdm.write(
            f"Iteration {event['iteration']}: minibatch micro F2: {score:.4f} (rejected)"
        )

    def on_iteration_end(self, event: dict[str, Any]) -> None:
        self.bar.update()

    def on_optimization_end(self, event: dict[str, Any]) -> None:
        self.bar.close()


class QuietLogger:
    def log(self, message: str) -> None:
        pass


def load_split(name: str) -> list[dict[str, Any]]:
    examples = []
    for path in sorted((DATASET_DIR / name).glob("*.md")):
        metadata = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
        examples.append({"input": path.read_text(encoding="utf-8"), "errors": metadata["errors"], "id": path.stem})
    return examples


def model_options(model: str, reasoning_effort: str) -> dict[str, Any]:
    return {
        "model": model,
        "api_key": os.environ["OPENROUTER_API_KEY"],
        "max_tokens": 32768,
        "max_retries": 0,
        "reasoning": {"effort": reasoning_effort, "exclude": True},
        "openrouter_provider": {"require_parameters": True},
    }


def make_adapter(
    model: str, reasoning_effort: str, concurrency: int, retries: int
) -> tuple[LangChainAdapter, ChatOpenRouter]:
    chat = ChatOpenRouter(**model_options(model, reasoning_effort))
    task = chat.with_structured_output(Findings, method="json_schema", strict=True, include_raw=True)
    judge = OpenRouterJudge(JudgeConfig(
        api_key=os.environ["OPENROUTER_API_KEY"], model=JUDGE_MODEL, temperature=None,
        timeout_seconds=120, retries=retries, app_url=None,
        app_title="AI Product Hack GEPA", base_url=None,
    ))

    def rollout(candidate: dict[str, str], example: dict[str, Any]) -> dict[str, Any]:
        messages = [
            SystemMessage(build_prompt(candidate["instructions"])),
            HumanMessage(f"# ПРОВЕРЯЕМЫЙ ДОКУМЕНТ\n\n```\n{example['input']}\n```"),
        ]
        response = task.invoke(messages)
        parsed = response.get("parsed")
        if not isinstance(parsed, Findings):
            raise ValueError(response.get("parsing_error") or "модель вернула пустой ответ")
        return {
            "messages": messages + [response["raw"]],
            "predictions": [item.model_dump() for item in parsed.errors],
        }

    def evaluate(example: dict[str, Any], state: dict[str, Any]) -> tuple[float, str]:
        if error := state.get("error"):
            state["counts"] = {"tp": 0, "fp": 0, "fn": len(example["errors"])}
            return 0.0, f"Inference failed: {error}"
        predictions = annotate_rows(state["predictions"], example["input"])
        ground_truth = annotate_rows(example["errors"], example["input"])
        matches, audit = [], {"skipped": True}
        if predictions and ground_truth:
            for attempt in range(retries + 1):
                try:
                    candidate_matches, audit = judge.judge(predictions, ground_truth)
                    matches = restore_document_indices(candidate_matches, predictions, ground_truth)
                    break
                except Exception:
                    if attempt == retries:
                        raise
                    time.sleep(2**attempt)
        result = build_case_result(example["id"], predictions, ground_truth, matches, audit, False)
        state["counts"] = {key: result[key] for key in ("tp", "fp", "fn")}
        feedback = {
            "f1": result["f1"],
            "f2": result["f2"],
            "false_positives": result["false_positives"],
            "false_negatives": result["false_negatives"],
        }
        return float(result["f2"]), json.dumps(feedback, ensure_ascii=False)

    def reflection_record(
        example: dict[str, Any], state: dict[str, Any], score: float, feedback: str
    ) -> dict[str, Any]:
        return {
            "Inputs": {"document": example["input"]},
            "Generated Outputs": state.get("predictions", str(state.get("error"))),
            "Feedback": feedback,
        }

    return MicroF2Adapter(
        rollout,
        evaluate,
        num_threads=concurrency,
        reflective_record_fn=reflection_record,
        show_progress=False,
    ), chat


def score(adapter: LangChainAdapter, examples: list[dict[str, Any]], candidate: dict[str, str]) -> float:
    return mean(adapter.evaluate(examples, candidate).scores)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GEPA optimization of inference instructions")
    parser.add_argument("--iterations", type=int, default=10, help="число мутаций prompt")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--model", default=MODEL)
    parser.add_argument(
        "--reasoning-effort",
        choices=("minimal", "low", "medium", "high", "xhigh", "max"),
        default="high",
    )
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.iterations < 1:
        raise SystemExit("--iterations должен быть больше нуля")
    if "OPENROUTER_API_KEY" not in os.environ:
        raise SystemExit("Не задан OPENROUTER_API_KEY")

    output = args.output or ARTIFACTS_DIR / "gepa" / datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    output.mkdir(parents=True, exist_ok=True)
    train = load_split("train")
    validation = load_split("val")
    test = load_split("test")
    adapter, reflection_chat = make_adapter(
        args.model, args.reasoning_effort, args.concurrency, args.retries
    )
    seed = {"instructions": BASE_INSTRUCTIONS}
    print(
        f"Models: inference/reflection={args.model} ({args.reasoning_effort}), "
        f"judge={JUDGE_MODEL}"
    )

    result = optimize(
        seed_candidate=seed,
        trainset=train,
        valset=validation,
        adapter=adapter,
        reflection_lm=make_reflection_lm(reflection_chat),
        reflection_minibatch_size=3,
        candidate_selection_strategy="pareto",
        use_merge=False,
        logger=QuietLogger(),
        stop_callbacks=MaxCandidateProposalsStopper(args.iterations),
        run_dir=str(output / "state"),
        display_progress_bar=False,
        callbacks=[IterationProgress(args.iterations)],
        cache_evaluation=False,
        seed=42,
    )

    best = result.best_candidate
    (output / "best_prompt.md").write_text(best["instructions"].strip() + "\n", encoding="utf-8")
    report = {
        "model": args.model,
        "reasoning_effort": args.reasoning_effort,
        "judge_model": JUDGE_MODEL,
        "iterations": args.iterations,
        "split": {"train": len(train), "validation": len(validation), "test": len(test)},
        "validation_history": result.val_aggregate_scores,
        "optimization_metric": "micro_f2",
        "baseline_test_micro_f2": score(adapter, test, seed),
        "optimized_test_micro_f2": score(adapter, test, best),
    }
    (output / "metrics.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Лучший prompt: {output / 'best_prompt.md'}")
    return 0


if __name__ == "__main__":
    main()
