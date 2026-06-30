"""
Run JPNumber's broad "電力" keyword crawl and copy the deliverable to Share.

The crawler logic remains in sites/phonebook/jpnumber.py. This helper only
sets the one-off keyword scope and copies the resulting NetHarvest CSV.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import shutil
import sys
import traceback
from datetime import datetime
from pathlib import Path


SCRIPTS_ROOT = Path(__file__).resolve().parent
NET_HARVEST_ROOT = Path(
    os.environ.get(
        "NETHARVEST_ROOT",
        str(Path.home() / "Desktop" / "NetHarvest\u4f50\u6e21"),
    )
)
today = datetime.now().strftime("%Y%m%d")
SHARE_OUTPUT_DIR = Path(
    os.environ.get(
        "STREAMREQ_6259_SHARE_OUTPUT",
        rf"\\STREAM06\Share\Scraping\取得データ\{today[:6]}\{today}",
    )
)

POWER_SPLIT_KEYWORDS = [
    "電力",
    "電力 北海道",
    "電力 青森県",
    "電力 岩手県",
    "電力 宮城県",
    "電力 秋田県",
    "電力 山形県",
    "電力 福島県",
    "電力 茨城県",
    "電力 栃木県",
    "電力 群馬県",
    "電力 埼玉県",
    "電力 千葉県",
    "電力 東京都",
    "電力 神奈川県",
    "電力 新潟県",
    "電力 富山県",
    "電力 石川県",
    "電力 福井県",
    "電力 山梨県",
    "電力 長野県",
    "電力 岐阜県",
    "電力 静岡県",
    "電力 愛知県",
    "電力 三重県",
    "電力 滋賀県",
    "電力 京都府",
    "電力 大阪府",
    "電力 兵庫県",
    "電力 奈良県",
    "電力 和歌山県",
    "電力 鳥取県",
    "電力 島根県",
    "電力 岡山県",
    "電力 広島県",
    "電力 山口県",
    "電力 徳島県",
    "電力 香川県",
    "電力 愛媛県",
    "電力 高知県",
    "電力 福岡県",
    "電力 佐賀県",
    "電力 長崎県",
    "電力 熊本県",
    "電力 大分県",
    "電力 宮崎県",
    "電力 鹿児島県",
    "電力 沖縄県",
]

for import_path in (NET_HARVEST_ROOT, SCRIPTS_ROOT):
    value = str(import_path)
    if value not in sys.path:
        sys.path.insert(0, value)

import sites.phonebook.jpnumber as jpnumber  # noqa: E402


def configure_logging(timestamp: str) -> Path:
    SHARE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_path = SHARE_OUTPUT_DIR / f"STREAMREQ-6259_jpnumber_power_{timestamp}.log"
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


def csv_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def main() -> int:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = configure_logging(timestamp)
    summary_path = SHARE_OUTPUT_DIR / f"STREAMREQ-6259_jpnumber_power_summary_{timestamp}.json"

    summary: dict[str, object] = {
        "request_id": "STREAMREQ-6259",
        "keywords": POWER_SPLIT_KEYWORDS,
        "started_at": timestamp,
        "status": "running",
        "log_path": str(log_path),
        "share_output_dir": str(SHARE_OUTPUT_DIR),
    }

    try:
        # One-off scope: split the broad "電力" query to work around JPNumber's
        # 1000-result search pagination cap while preserving per-run TEL dedupe.
        jpnumber.SEARCH_KEYWORDS = POWER_SPLIT_KEYWORDS
        jpnumber.MAX_PAGES_PER_KEYWORD = None
        jpnumber.JpnumberScraper.DELAY = float(os.environ.get("JPNUMBER_POWER_DELAY", "2.0"))

        scraper = jpnumber.JpnumberScraper()
        scraper.site_id = "jpnumber_power_split"
        scraper.site_name = "jpnumber_power_split"

        logging.info("Starting JPNumber power keyword crawl")
        scraper.execute(jpnumber.BASE_URL)

        if not scraper.output_filepath:
            raise RuntimeError("JPNumber crawler finished without a CSV output")

        local_output = Path(scraper.output_filepath)
        final_output = SHARE_OUTPUT_DIR / (
            f"STREAMREQ-6259_jpnumber_power_split_{timestamp}_{scraper.item_count}件.csv"
        )
        shutil.copy2(local_output, final_output)

        summary.update(
            {
                "status": "completed_with_output",
                "finished_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
                "item_count": scraper.item_count,
                "csv_row_count": csv_row_count(final_output),
                "error_count": scraper.error_count,
                "local_output": str(local_output),
                "final_output": str(final_output),
            }
        )
        logging.info("Final output copied to %s", final_output)
    except Exception as exc:
        summary.update(
            {
                "status": "failed",
                "finished_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
        logging.error("Run failed: %s", exc)
        logging.error(traceback.format_exc())
        raise
    finally:
        with summary_path.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        print(json.dumps(summary, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
