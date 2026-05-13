import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.utils.tainyu import TainyuMacaronScraper as BaseTainyuMacaronScraper


class TainyuMacaronScraper(BaseTainyuMacaronScraper):
    """体入マカロン 店舗情報スクレイパー"""

    pass


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    TainyuMacaronScraper().execute("https://picsastock.com/sitemap.xml")
