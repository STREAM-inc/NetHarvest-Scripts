"""
コワーキングスペース検索 (coworking-search.jp) — 全国コワーキングスペース情報スクレイパー

取得対象:
    - 検索一覧 /search/ に登録された全物件 (約 1,842 件 / 1 ページ 30 件)
    - 各詳細ページ /space/{ID}/ の物件概要テーブル
      (施設名 / 所在地 / 電話番号 / 営業時間 / ホームページ / 運営会社 ほか)

取得フロー:
    1. ルート URL (/zenkoku/) から全件検索一覧 /search/ を導出する。
       /zenkoku/ は都道府県インデックス (47 県合計 1,176 件) で全件を含まないため、
       全件を網羅する /search/ (1,842 件) を列挙の起点にする。
    2. /search/?p=N (30 件/ページ) を順に辿り、ul#result-office-module 内の
       /space/{ID}/ リンクを収集する。サイドバー・注目枠の重複は ID で dedupe する。
    3. 詳細ページを 1 件取得するごとに即 yield する (Pattern B)。

注意:
    - 電話番号は未掲載の物件が多い (リージャス系など)。依頼仕様どおり空欄でも行は残す。
    - 運営会社はチェーン判定キーのため必須取得項目として EXTRA に保持する。
    - 「料金」「初期費用」「おすすめポイント」は運営者が書いた自由記述の長文のため、
      著作権リスクを避けて取得しない。
    - 海外物件 (13 件) も /search/ に含まれる。都道府県は空欄になる。

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/coworking_search.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id coworking_search
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 全件検索一覧 (ルート相対)。parse() で引数 url から urljoin して使用する。
_SEARCH_PATH = "/search/"

# 詳細ページ /space/{ID}/
_SPACE_HREF = re.compile(r"/space/(\d+)/")

# ページ巡回の安全上限 (1,842 件 / 30 件 = 62 ページ。余裕を持たせる)
_MAX_PAGES = 200

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class CoworkingSearchScraper(StaticCrawler):
    """コワーキングスペース検索 (coworking-search.jp) スクレイパー"""

    DELAY = 1.0
    ITEM_DELAY = 0  # 待機は parse() 内のページ取得側に寄せる

    EXTRA_COLUMNS = [
        "運営会社",
        "最寄り駅",
        "座席数",
        "法人登記",
        "個室",
        "付帯サービス",
    ]

    def parse(self, url: str):
        # 🔒 引数 url を唯一のルート(SSOT)として全 URL を派生させる。
        search_url = urljoin(url, _SEARCH_PATH)
        seen_ids: set[str] = set()

        for page in range(1, _MAX_PAGES + 1):
            page_url = search_url if page == 1 else f"{search_url}?p={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("一覧ページ取得に失敗しました: %s", page_url)
                break

            if page == 1:
                self.total_items = self._read_total(soup)

            new_ids = [i for i in self._extract_space_ids(soup) if i not in seen_ids]
            if not new_ids:
                self.logger.info("新規物件が無くなったため終了します (page=%d)", page)
                break
            seen_ids.update(new_ids)

            for space_id in new_ids:
                detail_url = urljoin(url, f"/space/{space_id}/")
                try:
                    record = self._scrape_detail(detail_url)
                except Exception as e:  # 1 件の失敗で全体を止めない
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue
                if record:
                    # 1 件取得ごとに即 yield (全件収集してから一括 yield しない)
                    yield record
                time.sleep(self.DELAY)

    # ===============================================
    # 一覧ページ
    # ===============================================

    def _read_total(self, soup) -> int | None:
        """一覧ヘッダの「物件数 N 件中」から総件数を読む。"""
        node = soup.select_one("span.office-number")
        if not node:
            return None
        digits = re.sub(r"[^\d]", "", node.get_text())
        return int(digits) if digits else None

    def _extract_space_ids(self, soup) -> list[str]:
        """検索結果リスト内の物件 ID を出現順に返す (サイドバー等は対象外)。"""
        container = soup.select_one("ul#result-office-module") or soup
        ids: list[str] = []
        for a in container.select('a[href*="/space/"]'):
            m = _SPACE_HREF.search(a.get("href") or "")
            if m and m.group(1) not in ids:
                ids.append(m.group(1))
        return ids

    # ===============================================
    # 詳細ページ
    # ===============================================

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        name_node = soup.select_one("h2.detail-office-name")
        if not name_node:
            self.logger.warning("施設名が見つかりません: %s", detail_url)
            return None
        name = name_node.get_text(" ", strip=True)
        if not name:
            return None

        fields = self._read_detail_table(soup)
        address = fields.get("所在地", "")
        pref, rest = self._split_pref(address)

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.FAC_NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: rest,
            Schema.TEL: fields.get("電話番号", ""),
            Schema.TIME: fields.get("営業時間", ""),
            Schema.HP: self._pick_homepage(soup, fields),
            "運営会社": fields.get("運営会社", ""),
            "最寄り駅": fields.get("最寄り駅", ""),
            "座席数": fields.get("座席数", ""),
            "法人登記": fields.get("法人登記", ""),
            "個室": fields.get("個室", ""),
            "付帯サービス": self._read_options(soup),
        }
        return item

    def _read_detail_table(self, soup) -> dict[str, str]:
        """table.office-detail-column の th/td をラベル → 値の辞書にする。"""
        fields: dict[str, str] = {}
        for table in soup.select("table.office-detail-column"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                if not label or label in fields:
                    continue
                fields[label] = self._text(td)
        return fields

    def _read_options(self, soup) -> str:
        """付帯サービスのうち「有り」(option-true) のものだけを / 区切りで返す。"""
        names = [em.get_text(strip=True) for em in soup.select("td.detail-office-option em.option-true")]
        return " / ".join(n for n in names if n)

    def _pick_homepage(self, soup, fields: dict[str, str]) -> str:
        """ホームページ行のリンク先 (無ければテキスト) を返す。"""
        for table in soup.select("table.office-detail-column"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td or th.get_text(strip=True) != "ホームページ":
                    continue
                a = td.find("a", href=True)
                if a:
                    return a["href"].strip()
                return self._text(td)
        return fields.get("ホームページ", "")

    # ===============================================
    # ユーティリティ
    # ===============================================

    @staticmethod
    def _text(node) -> str:
        """<br> を空白に潰し、連続空白を 1 つにまとめたテキストを返す。"""
        for br in node.find_all("br"):
            br.replace_with(" ")
        return " ".join(node.get_text(" ", strip=True).split())

    @staticmethod
    def _split_pref(address: str) -> tuple[str, str]:
        """住所を都道府県と市区町村以降に分割する (海外物件は都道府県を空にする)。"""
        if not address:
            return "", ""
        m = _PREF_PATTERN.match(address)
        if not m:
            return "", address
        return m.group(1), address[m.end():].strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = CoworkingSearchScraper()
    scraper.site_id = "coworking_search"
    scraper.site_name = "コワーキングスペース検索"
    scraper.execute("https://coworking-search.jp/zenkoku/")
