.PHONY: sync
sync:
	@uv sync --all-extras

.PHONY: format
format:
	@ruff format .
	@ruff check --fix .

.PHONY: check
check:
	@ruff check .

.PHONY: sb
sb:
	@uv run python -m src.sb.main

.PHONY: extract-pdfs
extract-pdfs:
	@uv run python -m src.pdf_extract.main
