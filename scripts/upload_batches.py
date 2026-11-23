"""Upload VCT snapshots one at a time to the Unity Catalog landing volume.

Snapshots:
  s01_2021 .. s05_2025 : full yearly folders (vct_2021 .. vct_2025)
  s06_2026_partial     : synthetic 80% row-position slice of vct_2026 (simulates earlier state)
  s07_2026_full        : full vct_2026 + all_ids (simulates monthly refresh)

Usage:
  python scripts/upload_batches.py --data-root "D:/DATABRICKS PROJECT" --only s02_2022 --profile dev
  python scripts/upload_batches.py --data-root "D:/DATABRICKS PROJECT" --only s06_2026_partial --dry-run
"""

from __future__ import annotations

import argparse
import csv
import pathlib
import shutil
import subprocess
import tempfile

VOLUME_ROOT = "dbfs:/Volumes/final_project/vct_landing/raw/vct"

SNAPSHOTS: tuple[str, ...] = (
    "s01_2021",
    "s02_2022",
    "s03_2023",
    "s04_2024",
    "s05_2025",
    "s06_2026_partial",
    "s07_2026_full",
)


def partial_row_count(total_rows: int, ratio: float = 0.8) -> int:
    """Deterministic 80% row count for synthetic partial-year snapshot."""
    return int(total_rows * ratio)


def _count_csv_data_rows(path: pathlib.Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)  # header
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def _build_partial_2026(src_dir: pathlib.Path, dst_dir: pathlib.Path, ratio: float = 0.8) -> list[tuple[str, int, int]]:
    """Write the first `ratio` fraction of rows from every CSV in vct_2026 into dst_dir."""
    summary: list[tuple[str, int, int]] = []
    for src_csv in sorted(src_dir.rglob("*.csv")):
        rel = src_csv.relative_to(src_dir)
        out_csv = dst_dir / rel
        out_csv.parent.mkdir(parents=True, exist_ok=True)

        total = _count_csv_data_rows(src_csv)
        keep = partial_row_count(total, ratio)

        with (
            src_csv.open("r", encoding="utf-8-sig", newline="") as fin,
            out_csv.open("w", encoding="utf-8", newline="") as fout,
        ):
            reader = csv.reader(fin)
            writer = csv.writer(fout)
            header = next(reader, None)
            if header is not None:
                writer.writerow(header)
            for idx, row in enumerate(reader):
                if idx >= keep:
                    break
                writer.writerow(row)

        summary.append((rel.as_posix(), total, keep))
    return summary


def _run_cp(local_path: pathlib.Path, remote_path: str, profile: str, dry_run: bool) -> None:
    cmd = ["databricks", "fs", "cp", "--recursive", str(local_path), remote_path, "--profile", profile]
    print("+", " ".join(cmd))
    if not dry_run:
        subprocess.run(cmd, check=True)


def upload_snapshot(snapshot: str, data_root: pathlib.Path, profile: str, dry_run: bool) -> None:
    if snapshot in ("s01_2021", "s02_2022", "s03_2023", "s04_2024", "s05_2025"):
        year = snapshot.split("_")[1]
        local_dir = data_root / f"vct_{year}"
        remote_dir = f"{VOLUME_ROOT}/{snapshot}/vct_{year}"
        _run_cp(local_dir, remote_dir, profile, dry_run)
        return

    if snapshot == "s06_2026_partial":
        src_dir = data_root / "vct_2026"
        remote_dir = f"{VOLUME_ROOT}/{snapshot}/vct_2026"
        if dry_run:
            for src_csv in sorted(src_dir.rglob("*.csv")):
                total = _count_csv_data_rows(src_csv)
                keep = partial_row_count(total)
                print(f"  [dry-run] {src_csv.relative_to(src_dir).as_posix()}: {keep}/{total} rows")
            _run_cp(pathlib.Path("<temp>/vct_2026"), remote_dir, profile, dry_run=True)
            return

        tmp_root = pathlib.Path(tempfile.mkdtemp(prefix="vct_s06_partial_"))
        try:
            staging = tmp_root / "vct_2026"
            summary = _build_partial_2026(src_dir, staging)
            for rel, total, keep in summary:
                print(f"  staged {rel}: {keep}/{total} rows")
            _run_cp(staging, remote_dir, profile, dry_run=False)
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)
        return

    if snapshot == "s07_2026_full":
        _run_cp(data_root / "vct_2026", f"{VOLUME_ROOT}/{snapshot}/vct_2026", profile, dry_run)
        _run_cp(data_root / "all_ids", f"{VOLUME_ROOT}/{snapshot}/all_ids", profile, dry_run)
        return

    raise ValueError(f"Unknown snapshot: {snapshot}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload VCT snapshots to UC landing volume.")
    parser.add_argument("--data-root", type=pathlib.Path, required=True, help="Local folder with vct_YYYY and all_ids")
    parser.add_argument("--profile", default="dev", help="Databricks CLI profile name")
    parser.add_argument(
        "--only",
        nargs="+",
        choices=SNAPSHOTS,
        default=list(SNAPSHOTS),
        help="Snapshot ID(s) to upload",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print actions without uploading")
    args = parser.parse_args()

    for snap in args.only:
        print(f"\n=== Snapshot: {snap} ===")
        upload_snapshot(snap, args.data_root, args.profile, args.dry_run)


if __name__ == "__main__":
    main()
