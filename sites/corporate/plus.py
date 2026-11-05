"""
異業種ネットplus (e-gyousyu.com) — 近畿・四国東部 + 九州・沖縄・中国・四国エリアの対談記事掲載企業

取得対象:
    エリア別一覧 /area_category/<地域>/ の下記22エリアのみ (他エリアは取得しない)。
    起点URLのエリアスラッグに依存せず、常に 近畿・四国東部セット(7) → 九州・中国・四国セット(15) の順で全巡回する。
    - 近畿・四国東部: shiga, kyoto, osaka, hyogo, nara, wakayama, tokushima (kochi は 404 のため対象外)
    - 九州・中国・四国: fukuoka, saga, nagasaki, kumamoto, ooita, miyazaki, kagoshima, okinawa,
        totori, shimane, okayama, hiroshima, yamaguti, kagawa, ehime

取得フロー:
    - 一覧は 1 ページ約10件、ページ送りは /page/N/ (「次へ」リンクを辿る)。
    - div.content_sub 内の記事リンク (/new/<id>/ または /especially/<id>/) を詳細取得し即 yield。

ページ構造 (詳細):
    - div.cat_box: business_category リンク = 業種、area_category リンク = エリア
    - h2 = 名称、div.especially_content 先頭 p > span = 役職、残りテキスト = 代表者名
    - div.info_box (見出し「事業者情報」): h4 = 住所、p.client_url a = HP

備考 / 方針:
    - 同一企業が new / especially の両方に掲載される場合は名称(空白除去)で重複排除し、
      先に見つかった記事を採用する (1件に統合)。
    - TEL はサイトに掲載なし。
    - リード文・キャッチコピーは自由記述のため除外。
    - サイト上に利用規約ページは存在せず、スクレイピング禁止の明示なし。
      robots.txt は /wp/wp-admin/ 以外を許可。

実行方法:
    python scripts/sites/corporate/plus.py
"""

import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

_AREAS = [
    "fukuoka", "saga", "nagasaki", "kumamoto", "ooita", "miyazaki", "kagoshima", "okinawa",
    "totori", "shimane", "okayama", "hiroshima", "yamaguti", "kagawa", "ehime",
]
# 近畿・四国東部 (kochi は 404 のため除外)
_KANSAI_AREAS = ["shiga", "kyoto", "osaka", "hyogo", "nara", "wakayama", "tokushima"]
_ARTICLE_RE = re.compile(r"/(?:new|especially)/[^/]+/?$")
_AREA_RE = re.compile(r"/area_category/([^/]+)/?")
_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")
_MAX_PAGES = 200


class PlusScraper(StaticCrawler):
    """異業種ネットplus スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["エリア", "記事URL"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 起点URLのエリアには依存せず、関西セット → 従来15エリアの順に全巡回
        areas = list(_KANSAI_AREAS) + list(_AREAS)

        seen_urls: set[str] = set()
        seen_names: set[str] = set()

        for area in areas:
            page_url = _AREA_RE.sub(f"/area_category/{area}/", url, count=1)
            for _ in range(_MAX_PAGES):
                soup = self.get_soup(page_url)
                if soup is None:
                    break
                for a in soup.select("div.content_sub a[href]"):
                    href = urljoin(page_url, a["href"])
                    if not _ARTICLE_RE.search(href) or href in seen_urls:
                        continue
                    seen_urls.add(href)
                    item = self._parse_detail(href)
                    if not item:
                        continue
                    key = re.sub(r"\s+", "", item[Schema.NAME])
                    if key in seen_names:
                        continue
                    seen_names.add(key)
                    yield item

                nxt = soup.select_one('a.next[href], a[rel="next"][href]') or next(
                    (x for x in soup.select("a[href*='/page/']") if x.get_text(strip=True) == "次へ"),
                    None,
                )
                if not nxt:
                    break
                page_url = urljoin(page_url, nxt["href"])

    def _parse_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None
        article = soup.select_one("article") or soup
        name_el = article.select_one("div.main_info_box h2") or article.select_one("h2")
        name = _text(name_el)
        if not name:
            logger.warning("名称が取得できません: %s", url)
            return None

        cats = [_text(a) for a in article.select("div.cat_box a[href*='/business_category/']")]
        areas = [_text(a) for a in article.select("div.cat_box a[href*='/area_category/']")]

        pos = rep = ""
        rep_p = article.select_one("div.especially_content p span")
        if rep_p:
            pos = _text(rep_p)
            p = rep_p.find_parent("p")
            rep = re.sub(r"\s+", " ", p.get_text(" ", strip=True).replace(pos, "", 1)).strip()

        info = article.select_one("div.info_box") or soup.select_one("div.info_box")
        addr = hp = ""
        if info:
            addr = _text(info.select_one("h4"))
            hp_a = info.select_one("p.client_url a[href]")
            if hp_a:
                hp = hp_a["href"].strip()
        pm = _PREF_RE.match(addr)

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pm.group(1) if pm else "",
            Schema.ADDR: addr,
            Schema.REP_NM: rep,
            Schema.POS_NM: pos,
            Schema.CAT_SITE: " / ".join(c for c in cats if c),
            Schema.HP: hp,
            "エリア": " / ".join(a for a in areas if a),
            "記事URL": url,
        }


def _text(el) -> str:
    if el is None:
        return ""
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = PlusScraper()
    scraper.execute("https://e-gyousyu.com/area_category/fukuoka/")
