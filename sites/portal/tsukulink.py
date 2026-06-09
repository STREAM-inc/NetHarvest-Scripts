# scripts/sites/portal/tsukulink.py
"""
ツクリンク (tsukulink.net) — 建設業者一覧スクレイパー

取得対象:
    全国の建設業者一覧（企業名、住所、業種、代表者名）
    + 各社の詳細ページから 許認可・従業員数・設立年月日・資本金・HP・Instagram

取得フロー:
    /companies?page=N → 一覧ページからデータ取得
        → 各社の詳細ページ (item の取得URL) にアクセスして会社情報を補完
    ※ 電話番号は問い合わせフォーム経由のみで非公開のため取得不可

実行方法:
    # ローカルテスト
    python scripts/sites/portal/tsukulink.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id tsukulink
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県抽出パターン（東京都・北海道・〇〇府・〇〇県）
_PREF_RE = re.compile(r"^(東京都|北海道|(?:.+?[都道府県]))")

# 和暦・西暦の「YYYY年MM月DD日」を抽出するパターン（設立年月日の正規化用）
_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")

# 許認可カラム（Schema 未定義のサイト固有カラム）
LICENSE_COL = "許認可"


class TsukulinkScraper(StaticCrawler):
    """ツクリンク 建設業者スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [LICENSE_COL]
    START_PAGE = 1  # 再開時はここを変更

    def parse(self, url: str) -> Generator[dict, None, None]:
        base_url = url.rstrip("/")
        page = self.START_PAGE
        while True:
            list_url = f"{base_url}/companies?page={page}"
            self.logger.info("一覧ページ取得: page=%d", page)

            try:
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning("一覧ページ取得失敗: %s (%s)", list_url, e)
                break

            if soup is None:
                self.logger.warning("soup取得失敗（スキップ）: page=%d", page)
                page += 1
                time.sleep(self.DELAY)
                continue

            items = soup.select("li.p-companies-list-item")
            if not items:
                break

            for li in items:
                item = self._parse_item(li, base_url)
                if item:
                    # 詳細ページから会社情報（許認可・従業員数等）を補完
                    self._enrich_from_detail(item)
                    yield item

            # 「次へ」リンクがあれば継続
            next_link = None
            for a in soup.select("a"):
                if "次へ" in a.get_text():
                    next_link = a
                    break

            if next_link:
                page += 1
                time.sleep(self.DELAY)
            else:
                break

    def _parse_item(self, li, base_url: str) -> dict | None:
        # 企業名 & 取得URL
        name_a = li.select_one("a.p-companies-list-item__name")
        if not name_a:
            return None

        href = name_a.get("href", "")
        item = {
            Schema.NAME: name_a.get_text(strip=True),
            Schema.URL: base_url + href if href.startswith("/") else href,
        }

        # 住所 → 都道府県と市区町村以降に分割
        addr_div = li.select_one("div.p-companies-list-item__address")
        if addr_div:
            addr_raw = addr_div.get_text(strip=True)
            m = _PREF_RE.match(addr_raw)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr_raw[m.end():]
            else:
                item[Schema.ADDR] = addr_raw

        # 業種（dl > dt="業種" の dd）
        for dl in li.select("dl.p-companies-list-item__job-list-item"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if dt and dd and dt.get_text(strip=True) == "業種":
                cat = re.sub(r"[\s\u3000]+", " ", dd.get_text(strip=True)).strip("、 ")
                item[Schema.CAT_SITE] = cat
                break

        # 代表者名（"代表　廣田　貢" → "廣田　貢"）
        rep_div = li.select_one("div.c-f-medium.c-t-dark.u-margin-l8p.u-text-nowrap")
        if rep_div:
            rep_text = rep_div.get_text(strip=True)
            rep_text = re.sub(r"^代表[\s\u3000]*", "", rep_text).strip()
            if rep_text:
                item[Schema.REP_NM] = rep_text

        return item

    # -------------------------------------------------------------------------
    # 詳細ページ解析
    # -------------------------------------------------------------------------
    def _enrich_from_detail(self, item: dict) -> None:
        """item[Schema.URL] の詳細ページを取得し、会社情報カラムを item に追記する。

        取得項目:
            許認可・従業員数・設立年月日・資本金・HP（ウェブサイト）・Instagram
        詳細ページの会社情報は `h4.p-companies-show-detail__heading--small`
        （見出し）と、その直後の兄弟要素（値）のペアで構成されている。
        """
        detail_url = item.get(Schema.URL)
        if not detail_url:
            return

        time.sleep(self.DELAY)  # 詳細ページ取得分の負荷軽減
        try:
            soup = self.get_soup(detail_url)
        except Exception as e:
            self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
            return
        if soup is None:
            return

        # 許認可（「業種の許認可確認」リストの項目を結合）
        licenses = self._extract_licenses(soup)
        if licenses:
            item[LICENSE_COL] = licenses

        # 従業員数（例: "9名 (施工管理職員数: 3名、…)"）
        emp = self._detail_value(soup, "従業員数")
        if emp:
            item[Schema.EMP_NUM] = emp

        # 設立年月日（"2017年01月11日" → "2017-01-11"）
        founded = self._detail_value(soup, "設立年月日")
        if founded:
            item[Schema.OPEN_DATE] = self._normalize_date(founded)

        # 資本金（例: "300万円"）
        cap = self._detail_value(soup, "資本金")
        if cap:
            item[Schema.CAP] = cap

        # HP（見出しは「ウェブサイト」）
        website = self._detail_value(soup, "ウェブサイト")
        if website:
            item[Schema.HP] = website

        # Instagram（詳細本文内の instagram.com リンク。サイト公式は除外）
        insta = self._extract_instagram(soup)
        if insta:
            item[Schema.INSTA] = insta

    def _detail_value(self, soup, label: str) -> str | None:
        """会社情報見出し(label)に対応する値テキストを返す。見つからなければ None。"""
        for h4 in soup.select("h4.p-companies-show-detail__heading--small"):
            if h4.get_text(strip=True) == label:
                sib = h4.find_next_sibling()
                if sib:
                    text = re.sub(r"[\s　]+", " ", sib.get_text(" ", strip=True)).strip()
                    return text or None
        return None

    def _extract_licenses(self, soup) -> str | None:
        """「業種の許認可確認」リストの許認可種別を「、」で結合して返す。"""
        for h4 in soup.select("h4.p-companies-show-detail__heading--small"):
            if h4.get_text(strip=True) == "業種の許認可確認":
                ul = h4.find_next_sibling("ul")
                if ul:
                    names = [li.get_text(strip=True) for li in ul.select("li")]
                    names = [n for n in names if n]
                    if names:
                        return "、".join(names)
        return None

    def _extract_instagram(self, soup) -> str | None:
        """詳細本文内（main.p-companies-show）の Instagram リンクを返す。

        フッターのサイト公式 (instagram.com/tsukulink) は対象外。
        """
        main = soup.select_one("main.p-companies-show") or soup
        for a in main.find_all("a", href=True):
            href = a["href"]
            low = href.lower()
            if "instagram.com" in low and "instagram.com/tsukulink" not in low:
                return href
        return None

    @staticmethod
    def _normalize_date(text: str) -> str:
        """"YYYY年MM月DD日" を "YYYY-MM-DD" に正規化する。該当なしは原文を返す。"""
        m = _DATE_RE.search(text)
        if m:
            return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return text


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, default=1)
    args = parser.parse_args()

    scraper = TsukulinkScraper()
    scraper.START_PAGE = args.start_page
    scraper.execute("https://tsukulink.net")
