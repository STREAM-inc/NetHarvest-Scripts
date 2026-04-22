"""
FitMap — 全国フィットネスジム検索ポータル

取得対象:
    - 全47都道府県のフィットネスジム・パーソナルジム・ヨガスタジオ等の施設情報

取得フロー:
    1. FitMap 固有の都道府県コード (1〜47) をループ
    2. /area/{pref_id}/page/{N}/ を 50件/ページで巡回し詳細URLを収集
    3. 各詳細ページ (/gym/{id}/) から全フィールドを抽出

注意:
    - サイトに電話番号・HP・SNS は掲載されていないため、該当カラムは空のままとなる
    - FitMap の pref_id は JIS 都道府県コードと異なる独自採番

実行方法:
    # ローカルテスト
    python scripts/sites/portal/fitmap.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id fitmap
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://fitmap.jp"
ITEMS_PER_PAGE = 50
MAX_PAGES_PER_PREF = 100  # 安全上限(東京 49 ページが現状最大)

# FitMap 固有の都道府県コード (JIS コードとは異なる)
_PREF_MAP = {
    1: "東京都",   2: "神奈川県", 3: "千葉県",   4: "埼玉県",   5: "茨城県",
    6: "栃木県",   7: "群馬県",   8: "愛知県",   9: "岐阜県",  10: "三重県",
    11: "静岡県", 12: "大阪府",  13: "兵庫県",  14: "京都府",  15: "滋賀県",
    16: "奈良県", 17: "和歌山県", 18: "北海道", 19: "青森県",  20: "岩手県",
    21: "宮城県", 22: "秋田県",  23: "山形県",  24: "福島県",  25: "山梨県",
    26: "長野県", 27: "新潟県",  28: "富山県",  29: "石川県",  30: "福井県",
    31: "広島県", 32: "岡山県",  33: "鳥取県",  34: "島根県",  35: "山口県",
    36: "香川県", 37: "徳島県",  38: "愛媛県",  39: "高知県",  40: "福岡県",
    41: "佐賀県", 42: "長崎県",  43: "熊本県",  44: "大分県",  45: "宮崎県",
    46: "鹿児島県", 47: "沖縄県",
}

_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_DETAIL_URL_PATTERN = re.compile(r"^https?://fitmap\.jp/gym/\d+/?$")
_HOLIDAY_KEYWORDS = ("年中無休", "不定休", "定休日")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _join_tags(tags: list) -> str:
    seen: set[str] = set()
    out: list[str] = []
    for t in tags:
        t = _clean(t)
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return " / ".join(out)


class FitMapScraper(StaticCrawler):
    """FitMap (fitmap.jp) フィットネスジム情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "ジャンル", "駅アクセス", "月会費", "入会金", "キャッシュバック",
        "設備・施設", "利用時間タグ", "レンタル", "体験・入会", "雰囲気",
        "駐車場", "駐輪場", "キャンペーン", "PR特徴",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_details: set[str] = set()

        for pref_id in sorted(_PREF_MAP.keys()):
            pref_name = _PREF_MAP[pref_id]
            self.logger.info("都道府県取得中: %d %s", pref_id, pref_name)

            detail_urls = list(self._collect_detail_urls(pref_id, seen_details))
            if self.total_items is None and detail_urls:
                self.total_items = len(detail_urls) * 47  # 概算

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url, pref_name)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning(
                        "詳細ページ取得失敗 (スキップ): %s — %s", detail_url, e
                    )
                    continue
                time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # 一覧ページ: 詳細URL 収集
    # ------------------------------------------------------------------

    def _collect_detail_urls(
        self, pref_id: int, seen: set[str]
    ) -> Generator[str, None, None]:
        for page_idx in range(1, MAX_PAGES_PER_PREF + 1):
            if page_idx == 1:
                list_url = f"{BASE_URL}/area/{pref_id}/"
            else:
                list_url = f"{BASE_URL}/area/{pref_id}/page/{page_idx}/"

            soup = self.get_soup(list_url)
            if soup is None:
                break

            root = soup.select_one("#left_col.custom_search_results") or soup
            items = root.select("div.caset_div")
            if not items:
                break

            collected_on_page = 0
            for item in items:
                a = item.select_one("div.caset_title h3.title a[href]")
                if not a:
                    continue
                href = a.get("href", "")
                if not href:
                    continue
                full = urljoin(BASE_URL, href)
                if not _DETAIL_URL_PATTERN.match(full):
                    continue
                if full in seen:
                    continue
                seen.add(full)
                collected_on_page += 1
                yield full

            # 次ページの存在確認
            next_link = soup.select_one("nav.pagination a.next.page-numbers")
            if not next_link:
                break
            if collected_on_page == 0:
                break
            time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # 詳細ページ: 全フィールド抽出
    # ------------------------------------------------------------------

    def _scrape_detail(self, url: str, pref_name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {
            Schema.URL: url,
            Schema.PREF: pref_name,
            Schema.HP: "",
            Schema.TEL: "",
            Schema.LINE: "",
            Schema.INSTA: "",
            Schema.X: "",
            Schema.FB: "",
            Schema.TIKTOK: "",
        }

        # --- 店名 ---
        title_el = soup.select_one("h2#post_title strong") or soup.select_one(
            "h2#post_title"
        )
        if title_el:
            data[Schema.NAME] = _clean(title_el.get_text())
        else:
            return None

        # --- 住所 (アクセスセクション or 先頭ヘッダ) ---
        addr_el = soup.select_one("div.post_adress")
        addr_text = _clean(addr_el.get_text()) if addr_el else ""

        station_el = soup.select_one("div.post_station")
        station_text = _clean(station_el.get_text()) if station_el else ""
        data["駅アクセス"] = station_text

        # 郵便番号抽出
        if addr_text:
            m_post = _POST_CODE_PATTERN.search(addr_text)
            if m_post:
                raw_post = m_post.group(1)
                if "-" not in raw_post:
                    raw_post = f"{raw_post[:3]}-{raw_post[3:]}"
                data[Schema.POST_CODE] = raw_post
                addr_text = _POST_CODE_PATTERN.sub("", addr_text).strip()
            # 都道府県プレフィックス除去
            if pref_name and addr_text.startswith(pref_name):
                addr_text = addr_text[len(pref_name):].strip()
            data[Schema.ADDR] = addr_text
        else:
            data[Schema.ADDR] = ""
            data[Schema.POST_CODE] = data.get(Schema.POST_CODE, "")

        # --- カテゴリタグ / ジャンル ---
        cat_tags = [
            _clean(el.get("title") or el.get_text())
            for el in soup.select(
                "h2#post_title + * .cat-gymtag, .facility_title .cat-gymtag"
            )
        ]
        if not cat_tags:
            # フォールバック: ページ内のどこかに必ずある
            cat_tags = [
                _clean(el.get("title") or el.get_text())
                for el in soup.select("ul.metacaset .cat-gymtag")
            ][:5]
        data[Schema.CAT_SITE] = _join_tags(cat_tags)
        data["ジャンル"] = data[Schema.CAT_SITE]

        # --- 基本情報テーブル ---
        hours_text = ""
        parking = ""
        bicycle = ""
        for tr in soup.select("table.facility_infotable tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text())
            # <br> を改行に変換してから読み取る
            for br in td.find_all("br"):
                br.replace_with("\n")
            val = _clean(td.get_text(" "))
            if "営業時間" in key:
                hours_text = val
            elif "駐車場" in key:
                parking = val
            elif "駐輪場" in key:
                bicycle = val
            elif "施設名" in key and not data.get(Schema.NAME):
                data[Schema.NAME] = val

        data[Schema.TIME] = hours_text
        data["駐車場"] = parking
        data["駐輪場"] = bicycle

        # 定休日は営業時間文字列から推測
        holiday = ""
        for kw in _HOLIDAY_KEYWORDS:
            if kw in hours_text:
                holiday = kw
                break
        data[Schema.HOLIDAY] = holiday

        # --- 設備・施設 / 利用時間 / レンタル / 支払い方法 / 体験・入会 / 雰囲気 ---
        facility_block = soup.select_one("div.facillity_infolist div.infolist_back")
        section_tags: dict[str, list[str]] = {}
        if facility_block:
            current_heading: str | None = None
            for child in facility_block.children:
                name = getattr(child, "name", None)
                if name == "h3":
                    current_heading = _clean(child.get_text())
                    section_tags.setdefault(current_heading, [])
                elif name == "ul" and current_heading:
                    for span in child.select("span.info_tags"):
                        section_tags[current_heading].append(_clean(span.get_text()))

        def _get_section(key_substr: str) -> str:
            for k, tags in section_tags.items():
                if key_substr in k:
                    return _join_tags(tags)
            return ""

        data["設備・施設"] = _get_section("設備")
        data["利用時間タグ"] = _get_section("利用時間")
        data["レンタル"] = _get_section("レンタル")
        data[Schema.PAYMENTS] = _get_section("支払")
        data["体験・入会"] = _get_section("体験") or _get_section("入会")
        data["雰囲気"] = _get_section("雰囲気")

        # --- 入会金 (INITIAL セクション) ---
        entry_fee = ""
        initial_header = soup.find(
            "p", class_="shosai_h3_englishtext", string=lambda s: s and "INITIAL" in s
        )
        if initial_header:
            container = initial_header.find_parent("div", class_="h3box_facility")
            if container:
                fee_el = container.select_one("div.plan_panel_box p") or container.select_one(
                    "div.plan_panel_shoki p"
                )
                if fee_el:
                    entry_fee = _clean(fee_el.get_text())
        data["入会金"] = entry_fee

        # --- 月会費 (最安値プラン) ---
        monthly_fee = ""
        ryokin_spans = soup.select(
            "div#ryokin .plan_panel_right span.panel_ryokin, span.panel_ryokin"
        )
        fees_num: list[int] = []
        for span in ryokin_spans:
            t = _clean(span.get_text()).replace(",", "")
            if t.isdigit():
                n = int(t)
                if n > 0:
                    fees_num.append(n)
        if fees_num:
            monthly_fee = f"{min(fees_num):,}円"
        data["月会費"] = monthly_fee

        # --- キャッシュバック ---
        cashback_el = soup.select_one("div.cashback_facility span")
        data["キャッシュバック"] = _clean(cashback_el.get_text()) if cashback_el else ""

        # --- キャンペーン ---
        campaign = ""
        campaign_h4 = soup.select_one("h4.facility_campaign")
        if campaign_h4:
            campaign_title = _clean(campaign_h4.get_text())
            campaign_parent = campaign_h4.parent
            campaign_desc = ""
            if campaign_parent:
                p = campaign_parent.find("p")
                if p:
                    campaign_desc = _clean(p.get_text())
            campaign = (
                f"{campaign_title} / {campaign_desc}"
                if campaign_desc
                else campaign_title
            )
        data["キャンペーン"] = campaign

        # --- PR特徴 (prbox) ---
        pr_items = [
            _clean(el.get_text())
            for el in soup.select("div.prboxs h4.prbox-item")
        ]
        data["PR特徴"] = _join_tags(pr_items[:5])

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = FitMapScraper()
    scraper.execute(f"{BASE_URL}/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
