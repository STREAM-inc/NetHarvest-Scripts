"""
一般社団法人 東京都警備業協会（東警協 / AJSSA 会員名簿・東京）— 加盟企業検索

取得対象:
    - 東京都警備業協会の加盟企業（賛助会員を除く全社・約892社）
    - 会社名 / カナ / 住所 / TEL / HP / 業務種別 / 災対（災害対策）指定

取得フロー:
    corp_search.html は iframe (/open/corpSearch/main) を埋め込むだけの器。
    実データは以下の POST 検索フォームで取得する:
      1. GET  /open/corpSearch/main   … csrf_token とセッション Cookie を取得
      2. POST /open/corpSearch/search … chiku=1(全て・賛助除く), gyoushu=1(全て) で
         全加盟企業を 1 ページに列挙 (ページネーション無し)
    取得した結果テーブルの行を 1 件ずつ即 yield する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 業務種別アイコン (i_space17.gif は「該当なし」のスペーサー) → 種別名
_GYOUSHU_ICON = {
    "i_sis": "施設警備",
    "i_hoa": "保安警備",
    "i_ku": "空港保安警備",
    "i_ki": "機械警備",
    "i_hom": "ホームセキュリティ",
    "i_ko": "交通警備",
    "i_za": "雑踏警備",
    "i_sin": "身辺警備",
    "i_yu": "輸送警備",
}
_ICON_RE = re.compile(r"/(i_[a-z]+)\.gif")


class Ajssa(StaticCrawler):
    """一般社団法人 東京都警備業協会 加盟企業検索 スクレイパー"""

    DELAY = 1.5
    # 業務種別 → Schema.CAT_SITE。災対(災害対策)指定の有無のみ EXTRA。
    EXTRA_COLUMNS = ["災対"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url / corp_search.html) を唯一のルートとして派生させる
        main_url = urljoin(url, "/open/corpSearch/main")
        search_url = urljoin(url, "/open/corpSearch/search")

        # 1. 検索フォームページを取得し、csrf_token とセッション Cookie を得る
        form_soup = self.get_soup(main_url)
        token_el = form_soup.select_one("input[name='csrf_token']") if form_soup else None
        csrf_token = token_el.get("value", "") if token_el else ""

        # 2. 全加盟企業（賛助会員を除く / 全業務種別）を 1 回の POST で取得
        payload = {
            "csrf_token": csrf_token,
            "corp_name": "",
            "chiku": "1",     # 全て(賛助会員を除く)
            "gyoushu": "1",   # 全て
        }
        resp = self.session.post(search_url, data=payload, timeout=self.TIMEOUT)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding
        soup = BeautifulSoup(resp.text, "html.parser")

        result_table = soup.select_one("div.search table")
        rows = result_table.select("tr") if result_table else []
        # 先頭行はヘッダ (会社名/住所/TEL/業務種別/災対)
        data_rows = [r for r in rows if r.find("td")]
        self.total_items = len(data_rows)

        for row in data_rows:
            try:
                item = self._parse_row(row, search_url)
                if item:
                    yield item
            except Exception as e:  # 個別行のエラーはスキップして継続
                logger.warning("行の解析に失敗しskip: %s", e)
                continue

    def _parse_row(self, row, source_url: str) -> dict | None:
        tds = row.find_all("td", recursive=False)
        if len(tds) < 5:
            return None

        name_td = tds[0]
        name = name_td.get_text(strip=True)
        if not name:
            return None
        kana = (name_td.get("title") or "").strip()
        a = name_td.find("a", href=True)
        hp = a["href"].strip() if a else ""

        # 住所: "<町名>/ <ビル名>" 形式。区切り "/" を除去して連結
        addr_raw = tds[1].get_text(" ", strip=True)
        addr = re.sub(r"\s*/\s*", " ", addr_raw).strip()

        tel = tds[2].get_text(strip=True)

        # 業務種別: アイコン画像から種別を復元 (スペーサーは除外)
        gyoushu = []
        for img in tds[3].find_all("img"):
            m = _ICON_RE.search(img.get("src", ""))
            if m and m.group(1) in _GYOUSHU_ICON:
                gyoushu.append(_GYOUSHU_ICON[m.group(1)])

        saitai = tds[4].get_text(strip=True)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: "東京都",  # 東京都警備業協会の会員 = 全て東京都
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: "/".join(gyoushu),
            "災対": saitai,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.toukeikyo.or.jp/corp_search.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
