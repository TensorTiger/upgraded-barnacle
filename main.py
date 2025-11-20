"""Dataset download and transfer helper.

The script pulls artefacts from Hugging Face, ensures they land in a
project-local directory (not the default Hugging Face cache), expands
any tar archives, and optionally uploads to GCS via ``gcloud storage cp``.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable

from huggingface_hub import HfApi, snapshot_download

os.environ["HF_TOKEN"] = "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"
PARQUET_SUFFIXES = {".parquet"}
TAR_SUFFIXES = {".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download a Hugging Face dataset and optionally upload to GCS",
    )
    parser.add_argument(
        "dataset_id",
        nargs="?",
        default="ai4bharat/Svarah",
        help="Dataset repository on Hugging Face (owner/name)",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Base directory where the dataset folder will be created",
    )
    parser.add_argument(
        "--keep-archives",
        action="store_true",
        help="Keep downloaded tar archives instead of deleting them after extraction",
    )
    parser.add_argument(
        "--gcloud",
        nargs=argparse.REMAINDER,
        help=(
            "Optional: destination GCS bucket to upload extracted files. "
            "Example: --gcloud gs://bucket-name/path"
        ),
    )
    return parser.parse_args()


def classify_files(repo_files: Iterable[str]) -> tuple[list[str], list[str]]:
    parquet_files: list[str] = []
    tar_files: list[str] = []

    for file_path in repo_files:
        suffixes = Path(file_path).suffixes
        if not suffixes:
            continue

        if any(suffix in PARQUET_SUFFIXES for suffix in suffixes):
            parquet_files.append(file_path)
            continue

        merged_suffix = "".join(suffixes[-2:]) if len(suffixes) >= 2 else suffixes[-1]
        if merged_suffix in TAR_SUFFIXES or suffixes[-1] in TAR_SUFFIXES:
            tar_files.append(file_path)

    return parquet_files, tar_files


def ensure_path_is_within(base_dir: Path, target_path: Path) -> None:
    try:
        target_path.resolve().relative_to(base_dir.resolve())
    except ValueError as exc:
        raise RuntimeError(
            f"Archive member would extract outside of {base_dir}: {target_path}"
        ) from exc


def extract_tar_archive(archive_path: Path, keep_archive: bool, extraction_path: Path) -> None:
    if not archive_path.exists():
        print(f"Warning: expected archive not found locally: {archive_path}")
        return

    destination = extraction_path
    with tarfile.open(archive_path, "r:*") as tar:
        members = tar.getmembers()
        for member in members:
            ensure_path_is_within(destination, destination / member.name)
        tar.extractall(path=destination, members=members)

    if not keep_archive:
        archive_path.unlink()


def extract_tar_archives(
    dataset_dir: Path, tar_rel_paths: Iterable[str], keep_archives: bool, extraction_path: Path
) -> None:
    tar_paths = [dataset_dir / rel_path for rel_path in tar_rel_paths]
    if not tar_paths:
        return

    cpu_based = max(1, os.cpu_count() or 4)
    max_workers = min(len(tar_paths), min(cpu_based, 5))
    # Keeping the worker pool small avoids spawning more than five local subprocesses/threads.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_path = {
            executor.submit(extract_tar_archive, tar_path, keep_archives, extraction_path): tar_path
            for tar_path in tar_paths
        }
        for future in as_completed(future_to_path):
            tar_path = future_to_path[future]
            try:
                future.result()
                print(f"Finished extracting {tar_path}")
            except Exception as exc:  # pragma: no cover - surfaces extraction failures
                print(f"Extraction failed for {tar_path}: {exc}", file=sys.stderr)
                raise


def download_assets(dataset_id: str, output_dir: Path, patterns: list[str]) -> Path:
    if not patterns:
        raise RuntimeError(
            "No matching parquet or tar files found in the dataset repository"
        )

    cache_dir = output_dir / ".hf-cache"
    os.environ.setdefault("HF_HUB_CACHE", str(cache_dir))

    # snapshot_download copies the requested artefacts into local_dir and avoids symlinks.
    # return Path( 
    return    snapshot_download(
            repo_id=dataset_id, # type: ignore
            repo_type="dataset",
            allow_patterns=patterns,
            local_dir=str(output_dir),
            local_dir_use_symlinks=False,
            cache_dir=str(cache_dir),
            max_workers = 32,
            dry_run=True,
        )
    # )


def run_gcloud_storage_upload(local_dir: Path, gcs_destination: str | None) -> None:
    if not gcs_destination:
        return

    command = ["gcloud", "storage", "cp", "-r", str(local_dir), gcs_destination]
    print(f"Running: {' '.join(command)}")
    subprocess.run(command, check=True)


def main() -> None:
    args = parse_args()

    base_output_dir = Path(args.output_dir).expanduser().resolve()
    dataset_name = args.dataset_id.split("/")[-1]
    dataset_dir = base_output_dir / dataset_name
    dataset_dir.mkdir(parents=True, exist_ok=True)

    api = HfApi()
    repo_files = api.list_repo_files(args.dataset_id, repo_type="dataset")
    parquet_files, tar_files = classify_files(repo_files)

    if parquet_files:
        print(f"Detected {len(parquet_files)} parquet files")
    if tar_files:
        print(f"Detected {len(tar_files)} tar archives")
    if not parquet_files and not tar_files:
        print("No parquet or tar files detected; exiting", file=sys.stderr)
        sys.exit(1)

    download_root = download_assets(
        args.dataset_id, dataset_dir, sorted({*parquet_files, *tar_files})
    )
    print(f"Downloaded files into {download_root}")
    home_folder = os.path.expanduser("~")
    extraction_path = f"{home_folder}/Dataset/{dataset_name}"
    os.makedirs(extraction_path, exist_ok=True)
    extract_tar_archives(dataset_dir, tar_files, args.keep_archives, Path(extraction_path))

    gcs_dest = args.gcloud[0] if args.gcloud else None
    try:
        run_gcloud_storage_upload(Path(extraction_path), gcs_dest)
    except subprocess.CalledProcessError as exc:
        print(f"gcloud command failed with exit code {exc.returncode}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
