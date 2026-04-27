"""
マイナビ転職 — 求人掲載企業情報スクレイパー

取得対象:
    - 求人掲載中の企業情報（会社概要・採用条件）

取得フロー:
    1. https://tenshoku.mynavi.jp/list/pg{N}/ を巡回（最大1036ページ、50件/ページ）
    2. 各ページの求人カード（cassetteRecruit / cassetteRecruitRecommend）から詳細URLを収集
    3. 詳細ページで会社情報テーブル（table.jobOfferTable.thL）と
       募集要項テーブル（table.jobOfferTable）を解析して企業データを取得

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/mynavi_tenshoku.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id mynavi_tenshoku
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://tenshoku.mynavi.jp"
LIST_URL = "https://tenshoku.mynavi.jp/list/pg{}/"
MAX_PAGES = 1036

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class MynaviTenshokuScraper(StaticCrawler):
    """マイナビ転職 求人企業情報スクレイパー（tenshoku.mynavi.jp）"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "初年度年収",
        "給与",
        "勤務地",
        "勤務時間",
        "昇給賞与",
        "諸手当",
        "休日休暇",
        "福利厚生",
        "売上高",
        "事業所",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()

        for page in range(1, MAX_PAGES + 1):
            list_url = LIST_URL.format(page)
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("ページ取得失敗: %s", list_url)
                break

            # 初回ページで総件数を設定
            if page == 1:
                count_m = re.search(r"([\d,]+)件", soup.get_text())
                if count_m:
                    self.total_items = int(count_m.group(1).replace(",", ""))
                else:
                    self.total_items = MAX_PAGES * 50

            # 求人カードから詳細URLを収集
            cards = soup.select("div.cassetteRecruit, div.cassetteRecruitRecommend")
            if not cards:
                self.logger.info("pg%d: カードなし、終了", page)
                break

            page_urls: list[str] = []
            for card in cards:
                a = card.select_one("a[href*='jobinfo-']")
                if not a:
                    continue
                href = a.get("href", "")
                if href.startswith("//"):
                    href = "https:" + href
                elif href.startswith("/"):
                    href = BASE_URL + href
                if href and href not in seen:
                    seen.add(href)
                    page_urls.append(href)

            self.logger.info("pg%d: %d件の詳細URLを収集", page, len(page_urls))

            for detail_url in page_urls:
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s / %s", detail_url, e)
                    continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # 企業名
        cn = soup.select_one(".companyName")
        if cn:
            data[Schema.NAME] = _clean(cn.get_text())

        # 求人タイトル (h1)
        h1 = soup.select_one("h1")
        if h1:
            data["求人タイトル"] = _clean(h1.get_text(" "))

        # DLサマリー（初年度年収・雇用形態）
        for dl in soup.select("dl"):
            dts = dl.select("dt")
            dds = dl.select("dd")
            for dt, dd in zip(dts, dds):
                key = _clean(dt.get_text())
                val = _clean(dd.get_text(" "))
                if key == "初年度年収":
                    data["初年度年収"] = val
                elif key == "雇用形態" and "雇用形態" not in data:
                    data["雇用形態"] = val

        # 募集要項テーブル（table.jobOfferTable で thL クラスを持たない最初のもの）
        for tbl in soup.select("table.jobOfferTable"):
            if "thL" not in tbl.get("class", []):
                for row in tbl.select("tr"):
                    th = row.select_one("th")
                    td = row.select_one("td")
                    if not th or not td:
                        continue
                    key = _clean(th.get_text())
                    val = _clean(td.get_text(" "))
                    if "雇用形態" in key:
                        data["雇用形態"] = val
                    elif "勤務時間" in key:
                        data["勤務時間"] = val
                    elif key == "勤務地":
                        data["勤務地"] = val
                    elif key == "給与":
                        data["給与"] = val
                    elif "昇給" in key or "賞与" in key:
                        data["昇給賞与"] = val
                    elif "諸手当" in key:
                        data["諸手当"] = val
                    elif "休日" in key or "休暇" in key:
                        data["休日休暇"] = val
                    elif "福利厚生" in key:
                        data["福利厚生"] = val
                break  # 最初の非thLテーブルのみ対象

        # 会社情報テーブル（table.jobOfferTable.thL）
        company_tbl = soup.select_one("table.jobOfferTable.thL")
        if company_tbl:
            for row in company_tbl.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text())
                val = _clean(td.get_text(" "))
                if key == "設立":
                    data[Schema.OPEN_DATE] = val
                elif key == "代表者":
                    data[Schema.REP_NM] = val
                elif key == "従業員数":
                    data[Schema.EMP_NUM] = val
                elif key == "資本金":
                    data[Schema.CAP] = val
                elif key == "売上高":
                    data["売上高"] = val
                elif key == "事業内容":
                    data[Schema.LOB] = val
                elif key == "本社所在地":
                    # 郵便番号抽出
                    m_post = re.search(r"〒\s*(\d{3})-?(\d{4})", val)
                    if m_post:
                        data[Schema.POST_CODE] = f"{m_post.group(1)}-{m_post.group(2)}"
                        val = val[val.index(m_post.group(0)) + len(m_post.group(0)):].strip(" ,，、：:　")
                    # 都道府県抽出
                    m_pref = _PREF_PATTERN.search(val)
                    if m_pref:
                        data[Schema.PREF] = m_pref.group(1)
                    data[Schema.ADDR] = val
                elif key == "事業所":
                    data["事業所"] = val
                elif "企業ホームページ" in key or "ホームページ" in key:
                    a_tag = td.select_one("a[href]")
                    if a_tag:
                        href = a_tag.get("href", "")
                        # mynavi 転送URLは除外
                        if "url-forwarder" not in href and "mynavi.jp" not in href:
                            data[Schema.HP] = href
                        else:
                            data[Schema.HP] = val
                    else:
                        data[Schema.HP] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MynaviTenshokuScraper()
    scraper.execute(LIST_URL.format(1))

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
