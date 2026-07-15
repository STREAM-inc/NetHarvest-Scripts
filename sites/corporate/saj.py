"""
一般社団法人ソフトウェア協会（SAJ） 会員法人一覧

取得対象:
    - SAJ 会員法人の基本情報 + 会社概要（2ページ分をマージ）
    - 合計 798 社（2026-07 時点）

取得フロー:
    一覧ページ (/M10/corporate_list/corporate_name/asc/{page}) を 1 ページ 20 件で巡回。
    各 <li> の onclick から詳細トークンを抽出し、法人ごとに
      - 基本情報ページ  /M10/corporate_detail/{token}/MzYEAA/AwA
      - 会社概要ページ  /M10/corporate_detail/{token}/MzYGAA/AwA
    の 2 ページを取得・マージして 1 件ずつ即 yield する（Pattern B）。

    詳細ページのデータは <dl class="big_block_sub"><dt>ラベル</dt><dd>値</dd></dl> の
    ラベル駆動で抽出する（法人ごとに出現フィールドが増減するため）。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/saj.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id saj
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

# 詳細ページのタブを指すトークン（全法人で固定）
_TAB_BASIC = "MzYEAA"   # 基本情報
_TAB_GAIYO = "MzYGAA"   # 会社概要

# 一覧 li の onclick から詳細トークンを抜き出す
_TOKEN_RE = re.compile(r"corporate_detail/([^/]+)/")


class SajCrawler(StaticCrawler):
    """SAJ 会員法人一覧 スクレイパー"""

    DELAY = 1.0
    # 事業概要（自由記述プロース）は著作権リスクのため除外
    EXTRA_COLUMNS = [
        "会員区分",
        "法人英語表記名",
        "主なソフトウェア関連業務",
        "貴社で取り扱うその他のソフトウェア関連業務",
        "取り扱うソフトウェア・サービスカテゴリ",
        "代表的なソフトウェア製品・サービス名",
    ]

    # WAF が python-requests のデフォルトUAを弾く（403）ため、ブラウザ相当のヘッダを付与する
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    def _setup(self):
        super()._setup()
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                "Referer": "https://www.saj.or.jp/",
            }
        )

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        while True:
            list_url = self._page_url(url, page)
            soup = self.get_soup(list_url)
            if soup is None:
                break

            tokens = []
            for li in soup.find_all("li"):
                onclick = li.get("onclick", "")
                m = _TOKEN_RE.search(onclick)
                if m:
                    tokens.append(m.group(1))

            if not tokens:
                break

            if page == 1:
                self.total_items = self._total_count(soup)

            for token in tokens:
                try:
                    item = self._scrape_detail(url, token)
                except Exception as e:
                    self.logger.error("詳細取得エラー token=%s: %s", token, e)
                    continue
                if item:
                    yield item

            page += 1

    def _page_url(self, url: str, page: int) -> str:
        """引数 url の末尾ページ番号を page に差し替える。"""
        return re.sub(r"/\d+/?$", f"/{page}", url)

    def _total_count(self, soup) -> int | None:
        el = soup.select_one(".page_navi-result")
        if not el:
            return None
        m = re.search(r"計\s*([\d,]+)\s*件", el.get_text())
        return int(m.group(1).replace(",", "")) if m else None

    def _scrape_detail(self, url: str, token: str) -> dict | None:
        basic_url = urljoin(url, f"/M10/corporate_detail/{token}/{_TAB_BASIC}/AwA")
        gaiyo_url = urljoin(url, f"/M10/corporate_detail/{token}/{_TAB_GAIYO}/AwA")

        fields = {}
        basic_soup = self.get_soup(basic_url)
        if basic_soup is None:
            return None
        fields.update(self._extract_fields(basic_soup))

        gaiyo_soup = self.get_soup(gaiyo_url)
        if gaiyo_soup is not None:
            fields.update(self._extract_fields(gaiyo_soup))

        name = fields.get("法人名", "")
        if not name:
            return None

        return {
            Schema.URL: basic_url,
            Schema.NAME: name,
            Schema.NAME_KANA: fields.get("法人名フリガナ", ""),
            Schema.PREF: fields.get("都道府県", ""),
            Schema.REP_NM: fields.get("代表者名", ""),
            Schema.HP: fields.get("法人URL", ""),
            Schema.CAT_SITE: fields.get("業種", ""),
            Schema.OPEN_DATE: fields.get("設立年月日", ""),
            "会員区分": fields.get("会員区分", ""),
            "法人英語表記名": fields.get("法人英語表記名", ""),
            "主なソフトウェア関連業務": fields.get("主なソフトウェア関連業務", ""),
            "貴社で取り扱うその他のソフトウェア関連業務": fields.get(
                "貴社で取り扱うその他のソフトウェア関連業務", ""
            ),
            "取り扱うソフトウェア・サービスカテゴリ": fields.get(
                "取り扱うソフトウェア・サービスカテゴリ", ""
            ),
            "代表的なソフトウェア製品・サービス名": fields.get(
                "代表的なソフトウェア製品・サービス名", ""
            ),
        }

    def _extract_fields(self, soup) -> dict:
        """詳細ページの <dl class="big_block_sub"> をラベル→値の辞書にする。"""
        result = {}
        container = soup.find("div", class_="block_w_basic_tab") or soup
        for dl in container.find_all("dl", class_="big_block_sub"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt:
                continue
            key = dt.get_text(" ", strip=True)
            val = dd.get_text(" ", strip=True) if dd else ""
            if key:
                result[key] = val
        return result


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = SajCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.saj.or.jp/M10/corporate_list/corporate_name/asc/1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
