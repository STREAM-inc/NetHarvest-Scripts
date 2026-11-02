# scripts/sites/construction/yaneiroha.py
"""
やねいろは (yaneiroha.com) — 屋根工事店専門ポータル

取得対象:
    - 東北・北陸 10 県 (青森/岩手/宮城/秋田/山形/福島/新潟/富山/石川/福井) の都道府県別一覧
      /yane/{pref}/ に掲載された屋根工事店。
    - 一覧には周辺県の店舗も「対応地域」として混ざるため、詳細ページの所在地が
      上記 10 県のものだけを残す (備考指示)。
    - 工事店 ID (/store/{ID}/) で重複除去。

取得フロー:
    ルート url (= /yane/miyagi/) を起点に、urljoin で同階層の他 9 県一覧 URL を派生
      → 各県一覧 (div.shop-card, 20件/ページ, ?page=N は rel=next を辿る)
      → カードから /store/{ID}/ と 更新日 (p.text_right) を取得
      → 詳細ページ table.tbl_shop01 (th/td) を 1 件取得するごとに即 yield

注意:
    - 共通窓口の電話 (0120-920-302) は店舗固有の番号ではないため TEL は取得しない。
    - 「一番の強み」「代表者経歴」「アフターフォロー体制」「工事店の想い」等は
      自由記述の長文のため著作権リスクを避けて取得しない。
    - 利用規約 第6条 (過度な負荷の禁止) に配慮し DELAY=3.0 秒。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/yaneiroha.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id yaneiroha
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


class YaneirohaScraper(StaticCrawler):
    """やねいろは スクレイパー (東北・北陸 10 県)"""

    DELAY = 3.0

    EXTRA_COLUMNS = [
        "建設業許可",
        "保有資格者",
        "対応工事",
        "特徴",
        "対応エリア",
        "工事店番号",
        "更新日",
    ]

    # 備考指定の 10 県 (一覧 URL のスラッグ → 都道府県名)
    TARGET_PREFS = {
        "aomori": "青森県",
        "iwate": "岩手県",
        "miyagi": "宮城県",
        "akita": "秋田県",
        "yamagata": "山形県",
        "fukushima": "福島県",
        "niigata": "新潟県",
        "toyama": "富山県",
        "ishikawa": "石川県",
        "fukui": "福井県",
    }

    # ページ送りの暴走防止
    MAX_PAGES = 50

    _STORE_RE = re.compile(r"/store/(\d+)/?$")
    _ZIP_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        Args:
            url: 都道府県別一覧 URL (例: https://yaneiroha.com/yane/miyagi/)
        """
        root = url if url.endswith("/") else url + "/"
        m = re.search(r"/yane/([a-z]+)/$", root)
        start_slug = m.group(1) if m else ""

        # 起点 url を先頭に、同階層の他県一覧を urljoin で派生
        list_urls = [root]
        for slug in self.TARGET_PREFS:
            if slug != start_slug:
                list_urls.append(urljoin(root, f"../{slug}/"))

        target_names = tuple(self.TARGET_PREFS.values())
        seen_ids: set[str] = set()

        for list_url in list_urls:
            page_url = list_url
            for _ in range(self.MAX_PAGES):
                soup = self.get_soup(page_url)
                if soup is None:
                    self.logger.warning("一覧ページ取得失敗: %s", page_url)
                    break

                for store_id, detail_url, updated in self._extract_cards(soup, page_url):
                    if store_id in seen_ids:
                        continue
                    seen_ids.add(store_id)
                    try:
                        row = self._parse_detail(detail_url, updated)
                    except Exception as e:  # noqa: BLE001
                        self.logger.warning("詳細解析失敗: %s (%s)", detail_url, e)
                        continue
                    if not row:
                        continue
                    # 備考: 所在地が対象 10 県の店舗のみ
                    if row[Schema.PREF] not in target_names:
                        continue
                    yield row

                nxt = soup.select_one('.pager a[rel="next"][href]')
                if nxt is None:
                    break
                next_url = urljoin(page_url, nxt["href"])
                if next_url == page_url:
                    break
                page_url = next_url

    # ------------------------------------------------------------------
    # 一覧
    # ------------------------------------------------------------------
    def _extract_cards(self, soup, page_url: str) -> list[tuple[str, str, str]]:
        """一覧カードから (工事店ID, 詳細URL, 更新日) を取り出す"""
        cards: list[tuple[str, str, str]] = []
        for card in soup.select("div.shop-card"):
            store_id = ""
            detail_url = ""
            for a in card.find_all("a", href=True):
                m = self._STORE_RE.search(a["href"])
                if m:
                    store_id = m.group(1)
                    detail_url = urljoin(page_url, a["href"])
                    break
            if not store_id:
                continue
            updated = ""
            for p in card.select("p.text_right"):
                mm = re.search(r"更新日[:：]\s*([\d.]+)", p.get_text(strip=True))
                if mm:
                    updated = mm.group(1)
                    break
            cards.append((store_id, detail_url, updated))
        return cards

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str, updated: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            self.logger.warning("詳細ページ取得失敗: %s", detail_url)
            return None

        table = soup.select_one("table.tbl_shop01")
        cells = {}
        if table is not None:
            for tr in table.find_all("tr"):
                th, td = tr.find("th"), tr.find("td")
                if th is not None and td is not None:
                    cells.setdefault(th.get_text(strip=True), td)

        name = self._text(cells.get("会社名"))
        if not name:
            h1 = soup.select_one("h1.title_shop")
            name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        # --- 所在地: 〒xxx-xxxx<br>住所 <span class="maplink">... ---
        post_code, pref, addr = "", "", ""
        loc = cells.get("所在地")
        if loc is not None:
            for extra in loc.select(".maplink, #map_canvas"):
                extra.decompose()
            raw = " ".join(loc.get_text(" ", strip=True).split())
            m = self._ZIP_RE.search(raw)
            if m:
                post_code = m.group(1)
                raw = raw[m.end():].strip()
            pm = re.match(r"(東京都|北海道|(?:京都|大阪)府|.{2,3}県)", raw)
            if pm:
                pref = pm.group(1)
                addr = raw[pm.end():].strip()
            else:
                addr = raw

        num = soup.select_one(".shop_num_text")

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.REP_NM: self._text(cells.get("代表者")),
            Schema.EMP_NUM: self._text(cells.get("従業員数")),
            Schema.TIME: self._text(cells.get("営業時間")),
            Schema.HOLIDAY: self._text(cells.get("定休日")),
            "建設業許可": self._lines(cells.get("建設業許可")),
            "保有資格者": self._lines(cells.get("保有資格者")),
            "対応工事": self._works(cells.get("対応工事")),
            "特徴": self._lines(cells.get("特徴")),
            "対応エリア": self._areas(cells.get("対応エリア")),
            "工事店番号": num.get_text(strip=True) if num else "",
            "更新日": updated,
        }

    # ------------------------------------------------------------------
    # セル整形
    # ------------------------------------------------------------------
    @staticmethod
    def _text(td) -> str:
        if td is None:
            return ""
        return " ".join(td.get_text(" ", strip=True).split())

    @staticmethod
    def _lines(td) -> str:
        """<br> 区切りの複数行を ' / ' で連結"""
        if td is None:
            return ""
        lines = [" ".join(s.split()) for s in td.get_text("\n").split("\n")]
        return " / ".join(s for s in lines if s)

    @staticmethod
    def _works(td) -> str:
        """対応工事タグのうち active のものだけを列挙 (passive は非対応)"""
        if td is None:
            return ""
        works: list[str] = []
        for bind in td.select("div.bind"):
            head = bind.find("div", recursive=False)
            head_name = head.get_text(strip=True) if head else ""
            subs = [
                s.get_text(strip=True)
                for s in bind.select("div.skill_tag")
                if "active" in (s.get("class") or [])
            ]
            if subs:
                works.append(f"{head_name}({'、'.join(subs)})")
            elif "active" in (bind.get("class") or []) and head_name:
                works.append(head_name)
        return " / ".join(works)

    @staticmethod
    def _areas(td) -> str:
        """対応エリアを '宮城県(仙台市、石巻市…) / 福島県(…)' 形式に整形"""
        if td is None:
            return ""
        areas: list[str] = []
        for ul in td.select("ul.area_list"):
            pref, cities = "", []
            for child in ul.find_all(["li", "ul"], recursive=False):
                if child.name == "li":
                    if pref:
                        areas.append(f"{pref}({'、'.join(cities)})" if cities else pref)
                    pref, cities = child.get_text(strip=True), []
                elif "municipalities_list" in (child.get("class") or []):
                    cities.extend(li.get_text(strip=True) for li in child.find_all("li"))
            if pref:
                areas.append(f"{pref}({'、'.join(cities)})" if cities else pref)
        return " / ".join(areas)


if __name__ == "__main__":
    scraper = YaneirohaScraper()
    scraper.execute("https://yaneiroha.com/yane/miyagi/")
