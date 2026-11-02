"""
銀座屋 (ginzaya.co.jp) — 買取専門店 店舗一覧スクレイパー

対象サイト: https://www.ginzaya.co.jp/shop/

取得フロー:
    1. 店舗一覧 (/shop/) のエリアナビから全店舗の詳細URL (/shop/{slug}/) を収集
       ※ /shop/page/N/ は同一内容のため巡回不要。slug は shop-XXXXX 以外 (fujioka 等) もある
    2. 各詳細ページの「店舗案内」テーブル (section#shop-info table) から情報を取得
       ※ 同ページのスタッフ紹介も table.table-main のため、section#shop-info に限定する

備考:
    - TEL は詳細の「電話番号」欄の固定番号 (ヘッダの 0120-510-570 は使わない)
    - 最寄駅は「最寄駅」ラベル、無い店舗は「アクセス」ラベルで代替
    - 住所は「〒〒590-0077」のような重複〒表記がある

実行方法:
    python scripts/sites/service/ginzaya.py
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|.{2,3}県)"
)
_POST_CODE_PATTERN = re.compile(r"^[〒\s]*(\d{3})\s*[-－ー−]\s*(\d{4})")


class GinzayaScraper(StaticCrawler):
    """銀座屋 店舗一覧スクレイパー"""

    DELAY = 3.0

    EXTRA_COLUMNS = [
        "FAX",
        "最寄駅",
        "古物商許可番号",
    ]

    @staticmethod
    def _clean(text: str) -> str:
        return re.sub(r"\s+", " ", text.replace("\xa0", " ").replace("　", " ")).strip()

    def _split_addr(self, raw: str) -> tuple[str, str, str]:
        """住所から 郵便番号・都道府県・住所(都道府県以降) を分離する。"""
        text = self._clean(raw)
        post_code = ""
        m = _POST_CODE_PATTERN.search(text)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
            text = text[m.end():].strip()
        pref = ""
        pm = _PREF_PATTERN.match(text)
        if pm:
            pref = pm.group(1)
        return post_code, pref, text

    def _collect_shop_urls(self, url: str) -> list[str]:
        soup = self.get_soup(url)
        if soup is None:
            return []
        base_path = urlparse(url).path.rstrip("/") + "/"
        pattern = re.compile(rf"^{re.escape(base_path)}([^/]+)/$")
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            full = urljoin(url, a["href"])
            parsed = urlparse(full)
            if parsed.netloc != urlparse(url).netloc:
                continue
            m = pattern.match(parsed.path)
            if not m or m.group(1) == "page":
                continue
            clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if clean not in seen:
                seen.add(clean)
                urls.append(clean)
        return urls

    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None
        table = soup.select_one("section#shop-info table")
        if table is None:
            self.logger.warning(f"店舗案内テーブル無し: {detail_url}")
            return None

        info: dict[str, str] = {}
        for th in table.find_all("th"):
            td = th.find_next_sibling("td")
            label = self._clean(th.get_text())
            if td is not None and label and label not in info:
                info[label] = self._clean(td.get_text(" "))

        name = ""
        title = soup.select_one("section#shop-info h2 .d-ib")
        if title:
            name = self._clean(title.get_text())
        if not name:
            h1 = soup.find("h1")
            name = self._clean(h1.get_text()).replace("買取専門店 ", "") if h1 else ""
        if name and not name.startswith("銀座屋"):
            name = f"銀座屋 {name}"

        post_code, pref, addr = self._split_addr(info.get("住所", ""))

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: info.get("電話番号", ""),
            Schema.TIME: info.get("営業時間", ""),
            Schema.HOLIDAY: info.get("定休日", ""),
            "FAX": info.get("FAX", ""),
            "最寄駅": info.get("最寄駅") or info.get("アクセス", ""),
            "古物商許可番号": info.get("古物商許可番号", ""),
        }

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.logger.info(f"店舗URL数: {len(shop_urls)}")
        for detail_url in shop_urls:
            item = self._parse_detail(detail_url)
            if item and item[Schema.NAME]:
                yield item


if __name__ == "__main__":
    scraper = GinzayaScraper()
    scraper.execute("https://www.ginzaya.co.jp/shop/")

    print("\n" + "=" * 40)
    print("📊 実行結果サマリ")
    print("=" * 40)
    print(f"  出力ファイル:   {scraper.output_filepath}")
    print(f"  取得件数:       {scraper.item_count}")
    print(f"  観測カラム数:   {len(scraper.observed_columns)}")
    print("=" * 40)
