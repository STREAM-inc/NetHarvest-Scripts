"""Local runner for STREAMREQ-5176/5177 full scrape checks."""

import argparse
import logging
import sys
import time
from pathlib import Path


ROOT_PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_PATH))

from scripts.sites.jobs.gaten_info import GatenInfoScraper
from scripts.sites.jobs.gaten_job import GatenJobScraper


SITES = {
    "gaten_info": {
        "class": GatenInfoScraper,
        "name": "ガテン職",
        "url": "https://gaten.info/",
    },
    "gaten_job": {
        "class": GatenJobScraper,
        "name": "ガテン系仕事ナビ",
        "url": "https://gaten-job.com/",
    },
}


def run(site_id: str) -> None:
    config = SITES[site_id]
    scraper = config["class"]()
    scraper.site_id = site_id
    scraper.site_name = config["name"]

    start = time.monotonic()
    scraper.execute(config["url"])
    elapsed = time.monotonic() - start

    print(
        {
            "site": site_id,
            "output": scraper.output_filepath,
            "items": scraper.item_count,
            "columns": scraper.observed_columns,
            "errors": scraper.error_count,
            "elapsed_sec": round(elapsed, 1),
        },
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("site_id", choices=sorted(SITES))
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run(args.site_id)


if __name__ == "__main__":
    main()
