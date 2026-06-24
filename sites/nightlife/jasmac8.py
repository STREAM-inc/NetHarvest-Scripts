# -*- coding: utf-8 -*-
"""
ジャスマック八戸館 — 青森県八戸市長横町のテナントビル(スナック/キャバクラ/バー等)の入居店舗一覧

取得対象:
    - ジャスマック八戸館に入居する各テナント店舗の基本情報

取得フロー:
    sitemap.html (フロア別テナント一覧 #menu_1〜#menu_7)
      → 各テナント詳細ページ blogx/data/{id}.php を1件取得するごとに即 yield (Pattern B)
    ※ ページネーションは無し (単一インデックスページ)

実装メモ:
    - サイトは Shift-JIS。HTTPヘッダに charset 指定が無いが、StaticCrawler.get_soup() が
      apparent_encoding (=SHIFT_JIS) にフォールバックするため正しくデコードされる。
    - 詳細ページの店舗情報は gif 画像ラベル (time_bar.gif / human.gif 等) の直後に
      テキストが続く構造。<br> が閉じられず DOM がネストするため、セル内 HTML を
      gif マーカーで正規表現分割して値を取り出す。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/jasmac8.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jasmac8
"""

import re
import sys
import html as _html
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 建物住所 (全テナント共通。出典: access.html フッター)
_BUILDING_PREF = "青森県"
_BUILDING_ADDR = "青森県八戸市長横町８−２"

# 詳細ページ: gif 画像ラベル(basename) → 内部フィールド名
_GIF_FIELDS = {
    "time_bar": "time",        # 営業時間 / 定休日
    "human": "seats",          # 席数・人数
    "tokucho_bar": "feature",  # 特徴 (自由記述 → 除外)
    "bikou_bar": "note",       # 備考
    "toiawase_bar": "contact", # お問い合わせ (TEL を含む)
    "price_bar": "price",      # 料金
    "servise_bar": "service",  # サービス (販促プロース → 除外)
}
_GIF_RE = re.compile(r"<img[^>]*?/([a-z_0-9]+)\.gif[^>]*>", re.I)
_TEL_RE = re.compile(r"0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}")


class Jasmac8Scraper(StaticCrawler):
    """ジャスマック八戸館 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["フロア", "席数", "料金", "備考"]

    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            return

        # フロア別ナビ (#menu_1〜) から (詳細URL, フロア) を収集 (重複除去)
        targets = []
        seen = set()
        for n in range(1, 9):
            ul = soup.select_one(f"#menu_{n}")
            if not ul:
                continue
            floor = f"{n}F"
            for a in ul.select("a[href]"):
                href = a.get("href")
                if not href or ".php" not in href:
                    continue
                detail_url = urljoin(url, href)
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                targets.append((detail_url, floor))

        self.total_items = len(targets)

        for detail_url, floor in targets:
            try:
                item = self._scrape_detail(detail_url, floor)
                if item:
                    yield item
            except Exception as e:  # 個別店舗の失敗はログして継続
                self.error_count += 1
                import logging
                logging.getLogger(__name__).warning(
                    "詳細ページ解析失敗 (スキップ): %s — %s", detail_url, e
                )
                continue

    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str, floor: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        win = soup.select_one(".shopdetail-window")
        if win is None:
            return None

        # 店名
        title_el = win.select_one(".shop_title_font_big")
        name = title_el.get_text(" ", strip=True) if title_el else ""
        if not name:
            return None

        # gif ラベルごとのテキストを抽出
        fields: dict[str, str] = {}
        for cell in win.select("td.fontsize11only"):
            inner = cell.decode_contents()
            parts = _GIF_RE.split(inner)
            # parts: [pre, gif1, chunk1, gif2, chunk2, ...]
            for i in range(1, len(parts), 2):
                key = _GIF_FIELDS.get(parts[i].lower())
                if not key:
                    continue
                val = self._clean(parts[i + 1])
                if val:
                    fields[key] = val

        # 営業時間 / 定休日 の分離 (例: "19:00〜Last / 定休日　無し")
        time_val, holiday_val = self._split_time_holiday(fields.get("time", ""))

        # TEL は問い合わせ欄から抽出
        tel = ""
        m = _TEL_RE.search(fields.get("contact", ""))
        if m:
            tel = m.group(0)

        return {
            Schema.NAME: name,
            Schema.TEL: tel,
            Schema.TIME: time_val,
            Schema.HOLIDAY: holiday_val,
            Schema.PREF: _BUILDING_PREF,
            Schema.ADDR: _BUILDING_ADDR,
            Schema.URL: url,
            "フロア": floor,
            "席数": fields.get("seats", ""),
            "料金": fields.get("price", ""),
            "備考": fields.get("note", ""),
        }

    # ------------------------------------------------------------------
    @staticmethod
    def _clean(html_chunk: str) -> str:
        """gif マーカー後の HTML 断片からテキスト行を抽出して ' / ' 連結する。"""
        txt = re.sub(r"(?is)<[^>]+>", "\n", html_chunk)
        txt = _html.unescape(txt)
        lines = [ln.strip() for ln in txt.split("\n")]
        lines = [ln for ln in lines if ln]
        return " / ".join(lines)

    @staticmethod
    def _split_time_holiday(value: str) -> tuple[str, str]:
        """ "19:00〜Last / 定休日　無し" → ("19:00〜Last", "無し") """
        if not value:
            return "", ""
        if "定休日" in value:
            before, after = value.split("定休日", 1)
            time_val = before.strip(" /　").strip()
            holiday_val = after.strip(" /　:：").strip()
            return time_val, holiday_val
        return value.strip(" /　").strip(), ""


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Jasmac8Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.jasmac8.com/sitemap.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
