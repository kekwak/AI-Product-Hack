"""Canonical repository paths used by CLI modules."""

from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DATASET_DIR = REPOSITORY_ROOT / "dataset"
ARTIFACTS_DIR = REPOSITORY_ROOT / "artifacts"
