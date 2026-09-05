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
    restore_document_indices,
)
from paths import ARTIFACTS_DIR, DATASET_DIR
from run_inference import BASE_INSTRUCTIONS, Findings, build_prompt

MODEL = "openai/gpt-5.6-luna:nitro"
JUDGE_MODEL = "openai/gpt-5.6-luna:nitro"


class IterationProgress:
    def __init__(self, total: int):
        self.bar = tqdm(total=total, desc="GEPA", unit="mutation")
        self.best_score: float | None = None

    def on_valset_evaluated(self, event: dict[str, Any]) -> None:
        score = float(event["average_score"])
        if event["iteration"] == 0:
            self.best_score = score
            tqdm.write(f"Baseline validation macro F1: {score:.4f}")
        elif event["is_best_program"] and (self.best_score is None or score > self.best_score):
            self.best_score = score
            tqdm.write(f"Iteration {event['iteration']}: new best validation macro F1: {score:.4f}")
        if self.best_score is not None:
            self.bar.set_postfix(best=f"{self.best_score:.4f}")

    def on_iteration_end(self, event: dict[str, Any]) -> None:
        self.bar.update()

    def on_optimization_end(self, event: dict[str, Any]) -> None:
        self.bar.close()


class QuietLogger:
    def log(self, message: str) -> None:
        pass


def load_split(clean_range: range, case_range: range) -> list[dict[str, Any]]:
    examples = []
    for number in clean_range:
        path = DATASET_DIR / "clean" / f"clean_{number:02d}.md"
        examples.append({"input": path.read_text(encoding="utf-8"), "errors": [], "id": path.stem})
    for number in case_range:
        path = DATASET_DIR / "corrupted" / f"case_{number:03d}.md"
        metadata = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
        examples.append({"input": path.read_text(encoding="utf-8"), "errors": metadata["errors"], "id": path.stem})
    return examples


def model_options(model: str) -> dict[str, Any]:
    return {
        "model": model,
        "api_key": os.environ["OPENROUTER_API_KEY"],
        "max_tokens": 32768,
        "max_retries": 0,
        "reasoning": {"effort": "high", "exclude": True},
        "openrouter_provider": {"require_parameters": True},
    }


def make_adapter(model: str, concurrency: int, retries: int) -> tuple[LangChainAdapter, ChatOpenRouter]:
    chat = ChatOpenRouter(**model_options(model))
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
        feedback = {
            "f1": result["f1"],
            "false_positives": result["false_positives"],
            "false_negatives": result["false_negatives"],
        }
        return float(result["f1"]), json.dumps(feedback, ensure_ascii=False)

    def reflection_record(
        example: dict[str, Any], state: dict[str, Any], score: float, feedback: str
    ) -> dict[str, Any]:
        return {
            "Inputs": {"document": example["input"]},
            "Generated Outputs": state.get("predictions", str(state.get("error"))),
            "Feedback": feedback,
        }

    return LangChainAdapter(
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
    train = load_split(range(1, 7), range(1, 31))
    validation = load_split(range(7, 9), range(31, 41))
    test = load_split(range(9, 11), range(41, 51))
    adapter, reflection_chat = make_adapter(args.model, args.concurrency, args.retries)
    seed = {"instructions": BASE_INSTRUCTIONS}
    print(f"Models: inference/reflection={args.model}, judge={JUDGE_MODEL}")

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
        cache_evaluation=True,
        seed=42,
    )

    best = result.best_candidate
    (output / "best_prompt.md").write_text(best["instructions"].strip() + "\n", encoding="utf-8")
    report = {
        "model": args.model,
        "judge_model": JUDGE_MODEL,
        "iterations": args.iterations,
        "split": {"train": len(train), "validation": len(validation), "test": len(test)},
        "validation_history": result.val_aggregate_scores,
        "baseline_test_macro_f1": score(adapter, test, seed),
        "optimized_test_macro_f1": score(adapter, test, best),
    }
    (output / "metrics.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Лучший prompt: {output / 'best_prompt.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
