"""
Good Staff (good-staff-recruit.jp) — 株式会社グッドスタッフ 採用情報スクレイパー

取得対象:
    - 求人一覧 (/job/-/info/list) に掲載される全エリア・全求人の掲載情報
      ※ 備考「エリアが複数あるため全エリア取得したい」を反映し、エリア絞り込みを
        一切かけない素の一覧 (全 area / 全 pref) を起点にする。

取得フロー:
    一覧ページ (POST ページング) を巡回 → 各 /job/-/info/detail/{id} 詳細ページへ遷移
    → 1 件取得するごとに即 yield (途中で打ち切っても無駄な通信が起きない / 早期 yield)

サイト構造:
    - ルート (sites.yml の url) はトップページ。一覧は urljoin で /job/-/info/list を導出。
    - 一覧 1 件 = `.result_box`。1 ページ 50 件。総件数は「N件」表記。
    - ページ送りは GET では効かず、listForm の POST (pageInfo.pageNo / pageInfo.displayNum)
      + _csrf トークンが必須。GET の ?pageNo=N は無視され 1 ページ目固定になる。
    - 店名は一覧カードの `.result_pageid_ttl` がクリーン (詳細 h1 は「店名 職種の募集詳細」)。
    - 詳細ページの構造化データは `.infoBox`(`.infoBox_label` + `.infoBox_area`)。

電話番号について (重要):
    - 詳細ページに載る電話番号 (`.tell_number_detail` / infoBox「応募先電話番号」) は
      全求人で同一の本社採用窓口番号 (例: 03-5365-2342, 平日9:00-18:00) であり、
      勤務先の店舗を特定する番号ではない。実調査 (複数詳細ページ) でも店舗固有の電話番号は
      ページ内に一切存在しなかった (人材紹介サイトのため応募は本社窓口に集約される仕様)。
    - そのため Schema.TEL (= 店舗の連絡先) には敢えてこの共通番号を入れない
      (全件同じ番号が入り「店舗を特定できる TEL」と誤認される弊害を避ける)。
      共通の応募窓口番号は情報として失わないよう EXTRA「応募先電話番号(共通)」に保持する。
    - 店舗の特定は 店名 + 都道府県 + 住所 + アクセス(最寄駅) で行う。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/good_staff.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id good_staff
"""

import math
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 一覧パス (ルート url からの相対派生)。全 area / 全 pref を含む素の一覧。
_LIST_PATH = "/job/-/info/list"
_ITEMS_PER_PAGE = 50

# 都道府県抽出 (住所文字列の先頭から)
_PREF_PATTERN = re.compile(r"(北海道|東京都|(?:京都|大阪)府|.{2,3}県)")
# 地図リンク等の末尾装飾を除去
_MAP_SUFFIX = re.compile(r"\s*[▶▷►].*$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class GoodStaffScraper(StaticCrawler):
    """Good Staff (グッドスタッフ) 採用情報スクレイパー"""

    DELAY = 1.2
    EXTRA_COLUMNS = [
        "求人タイトル",
        "応募先電話番号(共通)",
        "雇用形態",
        "給与",
        "アクセス",
        "勤務期間",
        "勤務形態",
        "シフト・勤務時間",
        "勤務できる曜日",
        "試用期間詳細",
        "社会保険",
        "待遇・福利厚生",
        "特徴・メリット",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url (トップページ = sites.yml の url) を唯一のルートとし、一覧 URL を派生させる。
        list_url = urljoin(url, _LIST_PATH)

        # 1 ページ目は GET で取得 (csrf トークンと総件数を把握)
        soup = self.get_soup(list_url)
        if soup is None:
            return

        total = self._detect_total(soup)
        if total:
            self.total_items = total
            self.logger.info("総件数: %d 件 (約 %d ページ)", total, math.ceil(total / _ITEMS_PER_PAGE))
        last_page = math.ceil(total / _ITEMS_PER_PAGE) if total else None

        seen: set[str] = set()
        page = 1
        while True:
            csrf = self._extract_csrf(soup)

            detail_urls = []
            for card in soup.select(".result_box"):
                a = card.find("a", href=re.compile(r"detail/M\d+"))
                if not a:
                    continue
                detail_url = urljoin(url, a.get("href"))
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                name_el = card.select_one(".result_pageid_ttl")
                sub_el = card.select_one(".result_box_ttl_sub")
                detail_urls.append(
                    (
                        detail_url,
                        _clean(name_el.get_text()) if name_el else "",
                        _clean(sub_el.get_text()) if sub_el else "",
                    )
                )

            if not detail_urls:
                break

            # 詳細を 1 件取得するごとに即 yield (早期 yield / 途中打ち切り耐性)
            for detail_url, list_name, sub_title in detail_urls:
                try:
                    item = self._scrape_detail(detail_url, list_name, sub_title)
                except Exception as e:  # 個別ページのエラーはスキップして継続
                    self.logger.warning("詳細取得失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item and item.get(Schema.NAME):
                    yield item

            # 次ページへ。POST (listForm) でないとページ送りが効かない。
            if last_page is not None and page >= last_page:
                break
            page += 1
            next_soup = self._post_page(list_url, csrf, page)
            if next_soup is None:
                break
            soup = next_soup

    # ------------------------------------------------------------------ #
    # ページング (POST)
    # ------------------------------------------------------------------ #
    def _post_page(self, list_url: str, csrf: str, page: int) -> bs4.BeautifulSoup | None:
        """listForm を POST して指定ページの一覧 HTML を取得する。"""
        self.logger.info("取得中 (POST page=%d): %s", page, list_url)
        data = {
            "_csrf": csrf,
            "pageInfo.pageNo": str(page),
            "pageInfo.displayNum": str(_ITEMS_PER_PAGE),
        }
        try:
            resp = self.session.post(list_url, data=data, timeout=self.TIMEOUT)
            resp.raise_for_status()
            ctype = resp.headers.get("Content-Type", "")
            if "charset=" not in ctype.lower():
                resp.encoding = resp.apparent_encoding
            return bs4.BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                self.logger.warning("ページング失敗 (中断): page=%d — %s", page, e)
                return None
            raise

    @staticmethod
    def _extract_csrf(soup) -> str:
        el = soup.select_one('input[name="_csrf"]')
        return el.get("value", "") if el else ""

    @staticmethod
    def _detect_total(soup) -> int | None:
        m = re.search(r"([0-9,]+)\s*件", soup.get_text())
        if m:
            return int(m.group(1).replace(",", ""))
        return None

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, url: str, list_name: str, sub_title: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 名称: 一覧カードのクリーンな店名を優先 (詳細 h1 は「店名 職種の募集詳細」)
        name = list_name
        if not name:
            h1 = soup.select_one("h1.pageid_ttl")
            name = re.sub(r"の募集詳細$", "", _clean(h1.get_text())) if h1 else ""
        data[Schema.NAME] = name

        # 住所 / 都道府県: 詳細 infoBox「勤務地」(都道府県＋市区町村＋番地)
        loc = _MAP_SUFFIX.sub("", self._infobox(soup, "勤務地")).strip()
        if loc:
            m = _PREF_PATTERN.match(loc)
            if m:
                data[Schema.PREF] = m.group(1)
                data[Schema.ADDR] = loc[m.end():].strip()
            else:
                data[Schema.ADDR] = loc

        # TEL: 詳細ページの電話番号は全求人共通の本社採用窓口番号であり、店舗を特定する
        #      番号ではない (ページ内に店舗固有の電話番号は存在しない)。店舗の連絡先として
        #      Schema.TEL に入れると全件同一番号になり誤認を招くため、ここでは空にする。
        #      共通番号自体は EXTRA「応募先電話番号(共通)」に保持する。
        tel_el = soup.select_one(".tell_number_detail")
        common_tel = _clean(tel_el.get_text()) if tel_el else ""
        if not common_tel:
            m = re.search(r"0\d{1,4}-\d{1,4}-\d{3,4}", self._infobox(soup, "応募先電話番号"))
            common_tel = m.group(0) if m else ""
        data[Schema.TEL] = ""

        # --- EXTRA_COLUMNS (構造化された短いラベル / 数値 / キーワードのみ) ---
        data["求人タイトル"] = sub_title
        data["応募先電話番号(共通)"] = common_tel
        status = soup.select_one(".pageid_status")
        data["雇用形態"] = _clean(status.get_text()) if status else ""
        data["給与"] = self._infobox(soup, "給与")
        data["アクセス"] = self._infobox(soup, "アクセス")
        data["勤務期間"] = self._infobox(soup, "勤務期間")
        data["勤務形態"] = self._infobox(soup, "勤務形態")
        data["シフト・勤務時間"] = self._infobox(soup, "シフト・勤務時間")
        data["勤務できる曜日"] = self._infobox(soup, "勤務できる曜日")
        data["試用期間詳細"] = self._infobox(soup, "試用期間詳細")
        data["社会保険"] = self._infobox(soup, "社会保険")
        data["待遇・福利厚生"] = self._infobox(soup, "待遇・福利厚生")
        data["特徴・メリット"] = self._infobox(soup, "特徴・メリット")

        # ※ お仕事内容 / 応募資格 / 応募方法 / 応募後のプロセス は
        #   長文の自由記述 (プロース) のため著作権リスクを避けて取得しない。

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _infobox(soup, label: str) -> str:
        """詳細ページの `.infoBox`(label + area) から指定ラベルの値を取り出す。"""
        for box in soup.select(".infoBox"):
            lb = box.select_one(".infoBox_label")
            if lb and _clean(lb.get_text()) == label:
                ar = box.select_one(".infoBox_area, .result_box_tag")
                return _clean(ar.get_text()) if ar else ""
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GoodStaffScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://good-staff-recruit.jp/-/top/index.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
