"""
パコラ求人情報 (pacola) — 福岡の求人広告 株式会社パコラ

取得対象:
    - recruitment_special 配下の求人情報 (一覧 → 各求人の詳細ページ)

取得フロー:
    1. 一覧ページ (/recruitment_special/) の各カード (div.archive-post) から
       詳細リンク (a.plink) を取得
    2. 詳細ページごとに企業名・住所・TEL・募集条件等を取得し、1件ずつ即 yield
    3. ページネーション (/recruitment_special/page/{N}/) を「次へ」が消えるまで巡回

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/pacola.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id pacola
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

# 都道府県抽出
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}[-－]\d{4})")
_TEL_PATTERN = re.compile(r"0\d{1,3}[-－]\d{1,4}[-－]\d{3,4}")
_PUB_PATTERN = re.compile(r"(\d{4}[\./]\d{1,2}[\./]\d{1,2})\s*公開")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class PacolaScraper(StaticCrawler):
    """パコラ求人情報 スクレイパー (pacola.co.jp/recruitment_special)"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル",
        "公開日",
        "職種",
        "勤務地",
        "給与",
        "雇用形態",
        "資格・経験",
        "学歴",
        "待遇",
        "社会保険",
        "試用期間",
        "喫煙環境",
        "担当者",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート(SSOT)として扱う。
        # 末尾に "/" が無い場合に備えて補正してからページURLを派生させる。
        root = url if url.endswith("/") else url + "/"

        page = 1
        seen: set[str] = set()
        while True:
            page_url = root if page == 1 else urljoin(root, f"page/{page}/")
            try:
                soup = self.get_soup(page_url)
            except Exception as e:  # noqa: BLE001
                self.logger.warning("ページ取得失敗 page=%d: %s", page, e)
                break

            cards = soup.select("div.archive-post")
            if not cards:
                break

            for card in cards:
                link = card.select_one("a.plink")
                detail_url = link.get("href") if link else None
                if not detail_url:
                    continue
                detail_url = urljoin(page_url, detail_url)
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                # 一覧カードで取得できる情報
                h2 = card.select_one("h2")
                title = _clean(h2.get_text()) if h2 else ""
                loc_el = card.select_one("p.rec_location")
                occ_el = card.select_one("p.rec_occupation")
                list_loc = _clean(loc_el.get_text()) if loc_el else ""
                category = _clean(occ_el.get_text()) if occ_el else ""

                try:
                    item = self._scrape_detail(detail_url, title, list_loc, category)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            # 次ページの有無を確認 (「次へ」リンクが無ければ終了)
            has_next = any(
                "次へ" in _clean(a.get_text()) or f"page/{page + 1}/" in (a.get("href") or "")
                for a in soup.select(".wp-pagenavi a, .pagination a, nav a, a")
            )
            if not has_next:
                break
            page += 1

    def _row_value(self, soup, label: str) -> str:
        """詳細ページの div.jobs-row から指定ラベルの値テキストを取得する。"""
        for row in soup.select("div.jobs-row"):
            lab = row.select_one(".jobs-row-label")
            if lab and _clean(lab.get_text()) == label:
                inp = row.select_one(".jobs-row-input")
                return _clean(inp.get_text(" ")) if inp else ""
        return ""

    def _scrape_detail(
        self, url: str, title: str, list_loc: str, category: str
    ) -> dict | None:
        soup = self.get_soup(url)

        # 企業名 / 店名: 本文先頭の h4 (catchcopy ではない方)
        name = ""
        for h4 in soup.select("h4"):
            if "catchcopy" in (h4.get("class") or []):
                continue
            name = _clean(h4.get_text())
            if name:
                break

        # 受付先名: 会社名 + 郵便番号 + 住所 + TEL を含むブロック
        reception = self._row_value(soup, "受付先名")

        # TEL
        tel = ""
        m = _TEL_PATTERN.search(reception)
        if m:
            tel = m.group(0).replace("－", "-")

        # 郵便番号
        post_code = ""
        m = _POST_PATTERN.search(reception)
        if m:
            post_code = m.group(1).replace("－", "-")

        # 住所 / 都道府県: 受付先名の住所部分を優先、無ければ一覧の地域
        addr = ""
        if m:  # 郵便番号が見つかっていれば、その直後を住所とみなす
            tail = reception[m.end():]
            # TEL・括弧閉じ・注意書き以降を除去
            tail = re.split(r"(?:TEL|ＴＥＬ|電話|☎|※|）|\))", tail)[0]
            addr = _clean(tail)
        if not addr and reception:
            pm = _PREF_PATTERN.search(reception)
            if pm:
                tail = re.split(r"(?:TEL|ＴＥＬ|電話|☎|※)", reception[pm.start():])[0]
                addr = _clean(tail)
        if not addr:
            addr = list_loc

        # 都道府県: 住所文字列から、無ければ一覧の地域から
        pref = ""
        pm = _PREF_PATTERN.search(addr) or _PREF_PATTERN.search(list_loc)
        if pm:
            pref = pm.group(1)

        # 公開日 (タイトルから抽出, 無ければ空)
        pub = ""
        pm3 = _PUB_PATTERN.search(title)
        if pm3:
            pub = pm3.group(1).replace("/", ".")

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: category,
            Schema.TIME: self._row_value(soup, "勤務時間"),
            Schema.HOLIDAY: self._row_value(soup, "休日"),
            "求人タイトル": title,
            "公開日": pub,
            "職種": self._row_value(soup, "職種"),
            "勤務地": self._row_value(soup, "勤務地"),
            "給与": self._row_value(soup, "給与"),
            "雇用形態": self._row_value(soup, "雇用形態"),
            "資格・経験": self._row_value(soup, "資格・経験"),
            "学歴": self._row_value(soup, "学歴"),
            "待遇": self._row_value(soup, "待遇"),
            "社会保険": self._row_value(soup, "社会保険"),
            "試用期間": self._row_value(soup, "試用期間"),
            "喫煙環境": self._row_value(soup, "喫煙環境"),
            "担当者": self._row_value(soup, "担当者"),
        }
        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PacolaScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.pacola.co.jp/recruitment_special/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
