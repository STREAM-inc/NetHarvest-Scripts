"""
買取専門店さすがや (sasugaya) — 全国店舗一覧スクレイパー

対象サイト: https://sasugaya.jp/shop/

取得フロー:
    1. ルート URL (店舗一覧) から都道府県別一覧 /shop/pref/{pref}/ のリンクを列挙する。
    2. 各都道府県一覧の h3 > a から店舗詳細 /shop/{スラッグ}/ を取得する。
       同一店舗は URL で重複除去する。
    3. 店舗詳細の店舗情報テーブル (table.table-bordered: 店名/電話番号/住所/
       営業時間/定休日/サービス) から抽出し、1 件ごとに即 yield する。

注意:
    - 詳細ページ下部の求人テーブルにある電話番号 (本部 03-5816-6362) や
      フッター代表番号 0120-991-323 は店舗 TEL として取らない
      (店舗情報テーブルの「電話番号」行のみ使用)。
    - アクセス案内・お知らせ・スタッフの声・口コミ等の文章は著作権リスク回避のため取得しない。
    - ルート URL は引数 `url` を唯一の起点とし、配下 URL は urljoin で派生させる。

実行方法:
    python scripts/sites/service/sasugaya.py
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
_PREF_PATTERN = re.compile("^(" + "|".join(_PREFS) + ")")
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_LINK_PATTERN = re.compile(r"/shop/pref/[^/]+/$")
_SHOP_LINK_PATTERN = re.compile(r"/shop/(?!pref/)[^/]+/$")
_TEL_PATTERN = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")

# 店舗 TEL として採用しない番号 (フッター代表番号・本部求人窓口)
_EXCLUDED_TELS = {"0120991323", "0358166362"}

_COL_SERVICE = "サービス"


class SasugayaScraper(StaticCrawler):
    """買取専門店さすがや 店舗一覧スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [_COL_SERVICE]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        pref_urls = []
        for a in soup.select("a[href]"):
            href = urljoin(url, a["href"])
            if _PREF_LINK_PATTERN.search(href) and href not in pref_urls:
                pref_urls.append(href)

        seen = set()
        for pref_url in pref_urls:
            pref_soup = self.get_soup(pref_url)
            if pref_soup is None:
                continue
            for a in pref_soup.select("h3 a[href]"):
                detail_url = urljoin(pref_url, a["href"])
                if not _SHOP_LINK_PATTERN.search(detail_url) or detail_url in seen:
                    continue
                seen.add(detail_url)
                item = self._parse_detail(detail_url)
                if item:
                    yield item

    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None
        table = soup.select_one("table.table-bordered")
        if table is None:
            self.logger.warning("店舗情報テーブルなし: %s", detail_url)
            return None

        fields = {}
        for tr in table.select("tr"):
            th, td = tr.find("th"), tr.find("td")
            if th is None or td is None:
                continue
            fields[th.get_text(strip=True)] = td

        def text(label: str, sep: str = " ") -> str:
            td = fields.get(label)
            return td.get_text(sep, strip=True) if td is not None else ""

        name = text("店名", "")
        if not name:
            h1 = soup.select_one("h1")
            name = h1.get_text(strip=True) if h1 else ""

        tel = ""
        for m in _TEL_PATTERN.finditer(text("電話番号")):
            if m.group(0).replace("-", "") not in _EXCLUDED_TELS:
                tel = m.group(0)
                break

        addr_raw = text("住所")
        post_code = ""
        m = _POST_PATTERN.search(addr_raw)
        if m:
            post_code = m.group(1)
            addr_raw = addr_raw[m.end():]
        addr = re.sub(r"\s+", " ", addr_raw).strip()
        pref = ""
        m = _PREF_PATTERN.match(addr)
        if m:
            pref = m.group(1)
            addr = addr[m.end():].strip()

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.TIME: text("営業時間"),
            Schema.HOLIDAY: text("定休日"),
            Schema.URL: detail_url,
            _COL_SERVICE: text("サービス", "/"),
        }


if __name__ == "__main__":
    scraper = SasugayaScraper()
    scraper.execute("https://sasugaya.jp/shop/")
