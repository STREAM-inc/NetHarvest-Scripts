"""
Run STREAMREQ-6259 phone-directory crawlers and create a merged deliverable.

This is a local operation helper for the request. The crawler extraction logic
itself remains in sites/phonebook/*.py and continues to rely on NetHarvest's
BaseCrawler and ItemPipeline for execution and CSV output.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Iterable


SCRIPTS_ROOT = Path(__file__).resolve().parent
NET_HARVEST_ROOT = Path(
    os.environ.get(
        "NETHARVEST_ROOT",
        str(Path.home() / "Desktop" / "NetHarvest\u4f50\u6e21"),
    )
)
OUTPUT_DIR = NET_HARVEST_ROOT / "output"
RUN_LOG_DIR = OUTPUT_DIR / "streamreq_6259"

for import_path in (NET_HARVEST_ROOT, SCRIPTS_ROOT):
    value = str(import_path)
    if value not in sys.path:
        sys.path.insert(0, value)

from src.const.schema import Schema  # noqa: E402
from sites.phonebook.jpnumber import (  # noqa: E402
    COL_FIXED_TEL as JPN_COL_FIXED_TEL,
    COL_MOBILE_TEL as JPN_COL_MOBILE_TEL,
    JpnumberScraper,
)
from sites.phonebook.telnavi import (  # noqa: E402
    COL_FIXED_TEL as TELNAVI_COL_FIXED_TEL,
    COL_MOBILE_TEL as TELNAVI_COL_MOBILE_TEL,
    TelnaviScraper,
)


RUN_TARGETS = [
    {
        "site_id": "jpnumber_new_power_agents",
        "site_name": "jpnumber_new_power_agents",
        "url": "https://www.jpnumber.com/",
        "class": JpnumberScraper,
    },
    {
        "site_id": "telnavi_new_power_agents",
        "site_name": "telnavi_new_power_agents",
        "url": "https://www.telnavi.jp/",
        "class": TelnaviScraper,
    },
]

PHONE_COLUMNS = tuple(
    dict.fromkeys(
        [
            Schema.TEL,
            JPN_COL_FIXED_TEL,
            JPN_COL_MOBILE_TEL,
            TELNAVI_COL_FIXED_TEL,
            TELNAVI_COL_MOBILE_TEL,
        ]
    )
)


def configure_logging(timestamp: str) -> Path:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = RUN_LOG_DIR / f"STREAMREQ-6259_run_{timestamp}.log"
    logging.basicConfig(
        filename=log_path,
        filemode="w",
        encoding="utf-8",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    logging.getLogger().addHandler(console)
    return log_path


def run_crawler(target: dict[str, object]) -> dict[str, object]:
    crawler_class = target["class"]
    crawler = crawler_class()
    crawler.site_id = str(target["site_id"])
    crawler.site_name = str(target["site_name"])

    result = {
        "site_id": target["site_id"],
        "site_name": target["site_name"],
        "status": "running",
        "item_count": 0,
        "output_filepath": None,
        "error_count": 0,
        "error": "",
    }

    logging.info("Starting %s", target["site_id"])
    try:
        crawler.execute(str(target["url"]))
        result.update(
            {
                "status": "completed",
                "item_count": crawler.item_count,
                "output_filepath": crawler.output_filepath,
                "error_count": crawler.error_count,
            }
        )
        logging.info(
            "Completed %s: %s items, output=%s",
            target["site_id"],
            crawler.item_count,
            crawler.output_filepath,
        )
    except Exception as exc:  # Keep the full operation moving for the other site.
        result.update(
            {
                "status": "failed",
                "error": str(exc),
                "error_count": getattr(crawler, "error_count", 0),
            }
        )
        logging.error("Failed %s: %s", target["site_id"], exc)
        logging.error(traceback.format_exc())

    return result


def read_rows(csv_paths: Iterable[Path]) -> tuple[list[dict[str, str]], list[str]]:
    rows: list[dict[str, str]] = []
    fieldnames: list[str] = []
    known_fields: set[str] = set()

    for csv_path in csv_paths:
        if not csv_path or not csv_path.exists():
            continue

        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for field in reader.fieldnames or []:
                if field not in known_fields:
                    fieldnames.append(field)
                    known_fields.add(field)

            for row in reader:
                for field in row.keys():
                    if field not in known_fields:
                        fieldnames.append(field)
                        known_fields.add(field)
                rows.append({key: value or "" for key, value in row.items()})

    return rows, fieldnames


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", "", value or "").strip().lower()


def row_key(row: dict[str, str], fallback_index: int) -> str:
    phone_values: list[str] = []
    for column in PHONE_COLUMNS:
        phone_values.append(row.get(column, ""))

    digits = "".join(re.sub(r"\D", "", value) for value in phone_values)
    if digits:
        return f"tel:{digits}"

    name = normalize_text(row.get(Schema.NAME, ""))
    address = normalize_text(row.get(Schema.ADDR, ""))
    homepage = normalize_text(row.get(Schema.HP, ""))
    if name and (address or homepage):
        return f"name:{name}|addr:{address}|hp:{homepage}"

    return f"row:{fallback_index}"


def dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    deduped: list[dict[str, str]] = []

    for index, row in enumerate(rows):
        key = row_key(row, index)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return deduped


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_path = configure_logging(timestamp)

    results = [run_crawler(target) for target in RUN_TARGETS]
    output_paths = [
        Path(str(result["output_filepath"]))
        for result in results
        if result.get("output_filepath")
    ]

    rows, fieldnames = read_rows(output_paths)
    deduped_rows = dedupe_rows(rows)

    final_path = OUTPUT_DIR / f"STREAMREQ-6259_new_power_agents_final_{timestamp}.csv"
    write_csv(final_path, deduped_rows, fieldnames)

    summary = {
        "request_id": "STREAMREQ-6259",
        "started_at": timestamp,
        "finished_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "status": "completed_with_output",
        "individual_outputs": [str(path) for path in output_paths],
        "final_output": str(final_path),
        "raw_row_count": len(rows),
        "deduped_row_count": len(deduped_rows),
        "log_path": str(log_path),
        "crawler_results": results,
    }

    summary_path = RUN_LOG_DIR / f"STREAMREQ-6259_summary_{timestamp}.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    logging.info("Final output: %s", final_path)
    logging.info("Summary: %s", summary_path)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
