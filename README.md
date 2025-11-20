## Project Setup

- **Python toolchain**: Managed via `uv` (see `.python-version` for CPython 3.12).
- **Virtual environment**: Auto-created at `.venv/` on first `uv` command.

## Common Tasks

- Install dependencies: `uv sync`
- Run Python entrypoint: `uv run python main.py`
- Add a new package: `uv add <package>`

## Cloud & Data Access

- Authenticate Google Cloud: `gcloud auth login`
- Configure default project: `gcloud config set project <project-id>`
- Transfer files: `gsutil cp <src> <dest>`
- Authenticate Hugging Face: `uv run huggingface-cli login`
- Download datasets (Python):

	```python
	from datasets import load_dataset

	ds = load_dataset("owner/dataset-name")
	```

## Dataset Workflow Script

- Run end-to-end flow: `uv run python main.py ai4bharat/Svarah --output-dir data --gcloud jobs create gs://src gs://dest`
- `main.py` will:
	1. Inspect Hugging Face repo assets and fetch parquet/tar files into `<output-dir>/<dataset-name>`.
	2. Extract tar archives (unless `--keep-archives` is supplied).
	3. Invoke `gcloud storage transfer ...` with whatever arguments follow `--gcloud`.

`uv run python main.py ai4bharat/Svarah --output-dir data --gcloud jobs create gs://src gs://dest`