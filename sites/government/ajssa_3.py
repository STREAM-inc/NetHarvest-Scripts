"""
一般社団法人 岩手県警備業協会（AJSSA 会員名簿・岩手県）— 会員名簿

取得対象:
    - 岩手県警備業協会の会員企業（全社・約72社）
    - 会社名 / 業種 / 郵便番号 / 住所 / TEL / HP

取得フロー:
    /free/member は静的な単一ページ。ページ本文に会員一覧テーブルが 1 つあり、
    1 行 = 1 会員 (№ / 会社名 / 業種 / 所在地 / 電話)。ページネーション無し。
      - 会社名セル: テキスト＝会社名、<a href> があれば会社 HP
      - 業種セル: アイコン画像 (<img alt="j1.gif"〜"j4.gif">) で業務種別を表す。
                  ページ上部の【業種凡例】に対応する種別名を _GYOUSHU_LEGEND で復元。
      - 所在地セル: "〒NNN-NNNN　岩手県..." 形式。郵便番号 / 都道府県 / 住所に分離。
    ヘッダ行 (№/会社名/...) をスキップし、データ行を 1 件ずつ即 yield する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_3
"""

import logging
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 業種アイコン (alt="jN.gif") → ページ上部の【業種凡例】に基づく業務種別名
_GYOUSHU_LEGEND = {
    "j1": "施設、保安、空港保安、機械警備業務",
    "j2": "雑踏、交通警備業務",
    "j3": "貴重品等運搬警備業務",
    "j4": "身辺警備業務",
}
_ICON_RE = re.compile(r"(j\d+)\.gif", re.IGNORECASE)
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF = "岩手県"


class Ajssa3(StaticCrawler):
    """一般社団法人 岩手県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務種別 → Schema.CAT_SITE。Schema に収まらない固有カラムは無し。
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if not soup:
            return

        table = soup.select_one("#about table") or soup.find("table")
        if not table:
            logger.warning("会員テーブルが見つかりません: %s", url)
            return

        rows = table.find_all("tr")
        data_rows = []
        for tr in rows:
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 5:
                continue
            # ヘッダ行 (№/会社名/業種/所在地/電話) を除外
            head = tds[0].get_text(strip=True)
            name_txt = tds[1].get_text(strip=True)
            if not head.isdigit() or name_txt in ("会社名", ""):
                continue
            data_rows.append(tds)
        self.total_items = len(data_rows)

        for tds in data_rows:
            try:
                item = self._parse_row(tds, url)
                if item:
                    yield item
            except Exception as e:  # 個別行のエラーはスキップして継続
                logger.warning("行の解析に失敗しskip: %s", e)
                continue

    def _parse_row(self, tds, source_url: str) -> dict | None:
        name_td, gyoushu_td, addr_td, tel_td = tds[1], tds[2], tds[3], tds[4]

        name = name_td.get_text(" ", strip=True)
        if not name:
            return None

        a = name_td.find("a", href=True)
        hp = a["href"].strip() if a else ""

        # 業種: アイコン画像の alt から種別名を復元
        gyoushu = []
        for img in gyoushu_td.find_all("img"):
            m = _ICON_RE.search(img.get("alt", "") or img.get("src", ""))
            if m:
                key = m.group(1).lower()
                if key in _GYOUSHU_LEGEND and _GYOUSHU_LEGEND[key] not in gyoushu:
                    gyoushu.append(_GYOUSHU_LEGEND[key])

        post_code, addr = self._split_address(addr_td.get_text(" ", strip=True))

        tel = tel_td.get_text(" ", strip=True)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: _PREF,  # 岩手県警備業協会の会員 = 全て岩手県
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: "/".join(gyoushu),
        }

    @staticmethod
    def _split_address(addr_raw: str) -> tuple[str, str]:
        """所在地文字列から 郵便番号 / 住所(都道府県以降) を分離する。"""
        addr_raw = addr_raw.replace("　", " ").strip()
        post_code = ""
        m = _POST_RE.search(addr_raw)
        if m:
            post_code = m.group(1)
            if "-" not in post_code:
                post_code = post_code[:3] + "-" + post_code[3:]
            addr_raw = _POST_RE.sub("", addr_raw, count=1)

        addr = addr_raw.replace("〒", "").strip()
        if addr.startswith(_PREF):
            addr = addr[len(_PREF):].strip()
        return post_code, addr


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa3()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://iwakeikyo.jp/free/member")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
