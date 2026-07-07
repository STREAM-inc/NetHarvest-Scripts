"""
商工会 (全国商工会連合会 会員HP検索 / 全国各地の商工会WEBサーチ) — 全国の商工会一覧

取得対象:
    - 全国 47 都道府県の商工会（および都道府県商工会連合会）の
      名称・ホームページURL・郵便番号・住所・都道府県・電話番号・FAX・商工会コード

取得フロー:
    - 検索フォーム (zyokensentaku.php) は search.php へ POST して結果を返す。
      パラメータ: mode=QU, loadtype=1, kencd[]=<都道府県コード 01-47>, kensu=ALL
    - 都道府県コード 01〜47 を順に POST し、1 都道府県ぶんの結果 (ul.ul_A > li) を
      取得してそのまま 1 件ずつ yield する（都道府県単位で取得→即 yield）。
    - shift_jis (cp932) ページなので明示的にデコードする。

実行方法:
    # ローカルテスト
    python scripts/sites/government/www12.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id www12
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 検索フォームの都道府県コード (01=北海道 〜 47=沖縄)。00 は全国だがここでは
# 都道府県ごとに巡回して PREF を明確にする。
_PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

# 都道府県抽出用 (住所の先頭から)
_PREF_PATTERN = re.compile(
    r"^(東京都|北海道|(?:京都|大阪)府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|"
    r"熊本|大分|宮崎|鹿児島|沖縄)県)"
)

_MAPGO_RE = re.compile(r"mapGo\('([^']*)',\s*'([^']*)'")


class Www12Scraper(StaticCrawler):
    """商工会検索 (全国商工会連合会) スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["FAX", "商工会コード"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 進捗表示用に全国総件数を preflight で取得 (取得できなくても続行)
        self.total_items = self._fetch_total(url)

        for kencd in _PREF_CODES:
            soup = self._post_search(url, kencd)
            if soup is None:
                continue
            ul = soup.select_one("ul.ul_A")
            if ul is None:
                continue
            for li in ul.find_all("li", recursive=False):
                item = self._parse_row(li, url)
                if item:
                    yield item

    # ------------------------------------------------------------------
    # 内部ヘルパー
    # ------------------------------------------------------------------
    def _post_search(self, url: str, kencd: str, kensu: str = "ALL") -> bs4.BeautifulSoup | None:
        """search.php へ検索条件を POST し、結果ページを BeautifulSoup で返す。"""
        data = [
            ("mode", "QU"),
            ("page", ""),
            ("loadtype", "1"),
            ("kencd[]", kencd),
            ("shokokai", ""),
            ("kensu", kensu),
        ]
        try:
            self.logger.info("検索 POST: kencd=%s kensu=%s", kencd, kensu)
            resp = self.session.post(url, data=data, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:  # ネットワークエラーはスキップして継続
            self.error_count += 1
            self.logger.warning("検索 POST 失敗 (スキップ): kencd=%s — %s", kencd, e)
            return None
        # HTTP ヘッダに charset が無く実体は shift_jis なので明示デコード
        resp.encoding = "cp932"
        return bs4.BeautifulSoup(resp.text, "html.parser")

    def _fetch_total(self, url: str) -> int | None:
        """全国 (kencd=00) の総件数を軽量に取得して total_items に使う。"""
        soup = self._post_search(url, "00", kensu="10")
        if soup is None:
            return None
        m = re.search(r"([0-9,]+)件の検索結果", soup.get_text())
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                return None
        return None

    def _parse_row(self, li, url: str) -> dict | None:
        # 名称・HP: 会員HPへのリンク (target=_blank)。無ければ img alt を名称に。
        a = li.find("a", attrs={"target": "_blank"})
        if a and a.get_text(strip=True):
            name = a.get_text(strip=True)
            hp = (a.get("href") or "").strip()
        else:
            img = li.find("img")
            name = (img.get("alt") or "").strip() if img else ""
            hp = ""
        if not name:
            return None

        text = li.get_text(" ", strip=True)

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.HP: hp,
        }

        m = re.search(r"〒\s*([0-9\-]+)", text)
        if m:
            item[Schema.POST_CODE] = m.group(1)

        m = re.search(r"住所\s*(.+?)\s*TEL", text)
        addr = m.group(1).strip() if m else ""
        if addr:
            item[Schema.ADDR] = addr
            pm = _PREF_PATTERN.match(addr)
            if pm:
                item[Schema.PREF] = pm.group(1)

        m = re.search(r"TEL\s*([0-9\-]+)", text)
        if m:
            item[Schema.TEL] = m.group(1)

        m = re.search(r"FAX\s*([0-9\-]+)", text)
        if m:
            item["FAX"] = m.group(1)

        # 商工会コード: map ボタンの mapGo('kencd','syocd',...) の syocd
        inp = li.find("input", attrs={"onclick": True})
        if inp:
            mg = _MAPGO_RE.search(inp.get("onclick") or "")
            if mg:
                item["商工会コード"] = mg.group(2)

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Www12Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www12.shokokai.or.jp/hpsearch/top/php/search.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
