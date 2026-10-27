"""Export AIS position reports from parquet files to a gzipped CSV for the replayer.

The parquet files have no extension (``2020-2_group_N``); each holds about a
day of reports. The export is sorted by time, which is the order the
replayer sends them in.

The AIS data is licensed: the export is written under ``data/``, which git
ignores. Never commit it.

Usage, from the repository root:

    ..\\port_ml_env\\Scripts\\python tools\\export_ais.py --parquet-dir <folder> --files 5
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

COLUMNS = ["mmsi", "imo_nr", "length", "date_time_utc", "lon", "lat",
           "sog", "cog", "true_heading", "nav_status", "message_nr"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--parquet-dir", required=True, type=Path,
                        help="folder holding the 2020-2_group_N parquet files")
    parser.add_argument("--files", default="5",
                        help="comma-separated group numbers to export, e.g. 5 or 5,6 (default 5)")
    parser.add_argument("--out", default="data/ais-export.csv.gz", type=Path,
                        help="output file (default data/ais-export.csv.gz)")
    args = parser.parse_args()

    frames = []
    for number in (int(n) for n in args.files.split(",")):
        path = args.parquet_dir / f"2020-2_group_{number}"
        # No file extension: read as parquet explicitly, never by extension.
        frame = pd.read_parquet(path, columns=COLUMNS)
        print(f"{path.name}: {len(frame):,} reports, {frame.mmsi.nunique()} vessels")
        frames.append(frame)

    reports = pd.concat(frames, ignore_index=True)
    reports["date_time_utc"] = pd.to_datetime(reports["date_time_utc"], utc=True)
    reports = reports.sort_values("date_time_utc", kind="stable")
    reports["date_time_utc"] = reports["date_time_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    reports.to_csv(args.out, index=False, compression="gzip")
    first, last = reports["date_time_utc"].iloc[0], reports["date_time_utc"].iloc[-1]
    print(f"Wrote {len(reports):,} reports from {first} to {last} to {args.out}")


if __name__ == "__main__":
    main()
