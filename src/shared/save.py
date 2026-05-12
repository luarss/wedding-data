import csv
import json
from pathlib import Path


def save_json(data: list[dict], filepath: str | Path) -> None:
    """Save a list of dicts as JSON with consistent formatting."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def save_csv(data: list[dict], filepath: str | Path, fieldnames: list[str] | None = None) -> None:
    """Save a list of dicts as CSV.

    If fieldnames is None, all keys across all dicts are collected.
    List/dict values are serialized as JSON strings.
    """
    if not data:
        return

    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)

    if fieldnames is None:
        all_keys = set()
        for item in data:
            all_keys.update(item.keys())
        fieldnames = sorted(all_keys)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for item in data:
            row = {}
            for k in fieldnames:
                v = item.get(k, "")
                if isinstance(v, (list, dict)):
                    v = json.dumps(v)
                row[k] = v
            writer.writerow(row)


def save_json_csv(data: list[dict], output_path: str | Path, fieldnames: list[str] | None = None) -> None:
    """Save data to both JSON and CSV files from a base path (without extension)."""
    if not data:
        return

    path = Path(output_path)
    save_json(data, path.with_suffix(".json"))
    save_csv(data, path.with_suffix(".csv"), fieldnames=fieldnames)
