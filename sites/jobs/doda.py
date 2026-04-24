"""
doda — 転職求人サイト doda（デューダ）スクレイパー

取得対象:
    - doda 全求人一覧 (JobSearchList.action) → 各求人詳細 (-tab__jd/) の会社概要と募集要項

取得フロー:
    1. 一覧ページ (?page=N) を 1 → 末尾まで巡回し、article 単位で求人カードを検出
    2. 各求人の詳細ページ (-tab__jd/) を取得
    3. 企業名 (h1) と会社概要 DL（事業概要/所在地/代表者/従業員数/資本金/設立/企業URL 等）、
       募集要項セクション（仕事内容/勤務地/勤務時間/雇用形態/給与/待遇/休日 等）を抽出

実行方法:
    python scripts/sites/jobs/doda.py
    python bin/run_flow.py --site-id doda
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


BASE_URL = "https://doda.jp"
LIST_URL = "https://doda.jp/DodaFront/View/JobSearchList.action?page={page}"

PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|"
    r"静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    r"熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

POSTAL_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")

# 「代表取締役社長 山田 太郎」→ 役職 + 氏名 に分離
REP_PATTERN = re.compile(
    r"^\s*(代表取締役社長CEO|代表取締役会長|代表取締役社長|代表取締役|"
    r"代表執行役員社長|代表執行役|代表役員|取締役社長|会長兼社長|"
    r"社長|会長|代表者|代表|CEO)\s+(.+?)\s*$"
)

EMPLOYMENT_TAGS = {
    "正社員", "契約社員", "業務委託", "派遣社員", "紹介予定派遣",
    "アルバイト・パート", "アルバイト", "パート",
}


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class DodaScraper(DynamicCrawler):
    """doda（デューダ）求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人ID",
        "求人タイトル",
        "雇用形態",
        "勤務地",
        "勤務時間",
        "給与",
        "仕事内容",
        "対象となる方",
        "待遇・福利厚生",
        "休日・休暇",
        "売上高",
        "平均年齢",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        total_pages = self._detect_total_pages()
        if total_pages:
            self.total_items = total_pages * 50
            self.logger.info(
                "doda 総ページ数: %d / 推定求人件数: 約 %d 件", total_pages, self.total_items
            )
        max_page = total_pages or 10000

        seen_ids: set[str] = set()
        page = 1
        consecutive_empty = 0

        while page <= max_page and consecutive_empty < 5:
            list_url = LIST_URL.format(page=page)
            soup = self.get_soup(list_url, wait_until="domcontentloaded")
            if soup is None:
                consecutive_empty += 1
                page += 1
                continue

            articles = soup.select("article")
            if not articles:
                consecutive_empty += 1
                page += 1
                continue
            consecutive_empty = 0

            for art in articles:
                yield from self._process_article(art, seen_ids)

            page += 1

    def _detect_total_pages(self) -> int | None:
        soup = self.get_soup(LIST_URL.format(page=1), wait_until="domcontentloaded")
        if soup is None:
            return None
        max_p = 0
        for a in soup.select('a[href*="JobSearchList.action"]'):
            m = re.search(r"page=(\d+)", a.get("href", ""))
            if m:
                max_p = max(max_p, int(m.group(1)))
        return max_p or None

    def _process_article(self, article, seen_ids: set[str]) -> Generator[dict, None, None]:
        link = article.select_one('a[href*="JobSearchDetail/j_jid"]')
        if not link:
            return
        href = link.get("href", "")
        m = re.search(r"j_jid__(\d+)", href)
        if not m:
            return
        job_id = m.group(1)
        if job_id in seen_ids:
            return
        seen_ids.add(job_id)

        detail_url = f"{BASE_URL}/DodaFront/View/JobSearchDetail/j_jid__{job_id}/-tab__jd/"

        # 一覧ページのメタデータ（詳細が取れなかったときのフォールバック）
        list_info: dict[str, str] = {"求人ID": job_id}
        name_h2 = article.select_one(".jobCard-header__link h2")
        list_info["_list_name"] = _clean(name_h2.get_text()) if name_h2 else ""
        title_p = article.select_one(".jobCard-header__link p")
        list_info["求人タイトル"] = _clean(title_p.get_text()) if title_p else ""

        # タグから雇用形態を抽出
        tags = [_clean(t.get_text()) for t in article.select(".jobCard-tag")]
        list_info["_employment_list"] = next((t for t in tags if t in EMPLOYMENT_TAGS), "")

        # 一覧の infoList から参考データを拾う（詳細が無い場合のフォールバック）
        for dl in article.select("ul.jobCard-infoList > li dl.jobCard-info"):
            title_el = dl.select_one(".jobCard-info__title")
            content_el = dl.select_one(".jobCard-info__content")
            if not title_el or not content_el:
                continue
            t = _clean(title_el.get_text())
            c = _clean(content_el.get_text())
            if t == "勤務地":
                list_info["_勤務地_list"] = c
            elif t == "給与":
                list_info["_給与_list"] = c
            elif t == "事業":
                list_info["_事業_list"] = c
            elif t == "仕事":
                list_info["_仕事_list"] = c
            elif t == "対象":
                list_info["_対象_list"] = c

        item = self._scrape_detail(detail_url, list_info)
        if item:
            yield item

    def _scrape_detail(self, url: str, list_info: dict[str, str]) -> dict | None:
        soup = self.get_soup(url, wait_until="domcontentloaded")
        if soup is None:
            return None

        data: dict = {Schema.URL: url}
        data["求人ID"] = list_info.get("求人ID", "")
        data["求人タイトル"] = list_info.get("求人タイトル", "")

        # 企業名: 詳細ページ h1 を優先（NEW/締切間近などの付随表示が混ざらない）
        h1 = soup.select_one(
            "h1.jobSearchDetail-heading__title, h1[class*='jobSearchDetail-heading__title']"
        )
        if h1:
            data[Schema.NAME] = _clean(h1.get_text())
        elif list_info.get("_list_name"):
            data[Schema.NAME] = list_info["_list_name"]

        # 募集要項セクション（div.jobSearchDetail-sectionItem 内の h2/h3 見出しで判定）
        section_labels = {
            "仕事内容", "対象となる方", "勤務地", "勤務時間",
            "雇用形態", "給与", "待遇・福利厚生", "休日・休暇",
        }
        found_sections: dict[str, str] = {}
        for section in soup.select(".jobSearchDetail-sectionItem"):
            title_el = section.select_one("h3, h2")
            if not title_el:
                continue
            title = _clean(title_el.get_text())
            if title not in section_labels:
                continue
            body_text = _clean(section.get_text(" "))
            if body_text.startswith(title):
                body_text = body_text[len(title):].strip()
            found_sections[title] = body_text

        for col in (
            "仕事内容", "対象となる方", "勤務地", "勤務時間",
            "給与", "待遇・福利厚生", "休日・休暇",
        ):
            data[col] = found_sections.get(col, "")

        # 雇用形態: 詳細セクション → 一覧タグの順でフォールバック
        data["雇用形態"] = (
            found_sections.get("雇用形態", "")
            or list_info.get("_employment_list", "")
        )

        # 会社概要 DL（DescriptionList-module_...）
        company_info: dict[str, str] = {}
        for dl in soup.select('dl[class*="DescriptionList"]'):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for i, dt in enumerate(dts):
                if i >= len(dds):
                    break
                key = _clean(dt.get_text())
                val = _clean(dds[i].get_text(" "))
                if key:
                    company_info[key] = val

        # 事業内容 (LOB)
        data[Schema.LOB] = (
            company_info.get("事業概要", "")
            or company_info.get("事業内容", "")
            or list_info.get("_事業_list", "")
        )

        # 所在地 → 郵便番号 / 都道府県 / 住所
        address = company_info.get("所在地", "")
        post_code = ""
        pref = ""
        rest = ""
        if address:
            pm = POSTAL_PATTERN.search(address)
            if pm:
                post_code = pm.group(1)
                address = POSTAL_PATTERN.sub("", address, count=1).strip()
            prm = PREF_PATTERN.match(address)
            if prm:
                pref = prm.group(1)
                rest = address[prm.end():].strip()
            else:
                rest = address
        data[Schema.POST_CODE] = post_code
        data[Schema.PREF] = pref
        data[Schema.ADDR] = rest

        # 代表者 → 役職 + 氏名 に分離
        rep = company_info.get("代表者", "")
        pos_nm = ""
        rep_nm = ""
        if rep:
            rm = REP_PATTERN.match(rep)
            if rm:
                pos_nm = rm.group(1)
                rep_nm = _clean(rm.group(2))
            else:
                rep_nm = rep
        data[Schema.POS_NM] = pos_nm
        data[Schema.REP_NM] = rep_nm

        data[Schema.EMP_NUM] = company_info.get("従業員数", "")
        data[Schema.CAP] = company_info.get("資本金", "")
        data[Schema.OPEN_DATE] = company_info.get("設立", "")
        data[Schema.HP] = company_info.get("企業URL", "")

        # EXTRA: 売上高 / 平均年齢 (任意、無ければ空)
        data["売上高"] = company_info.get("売上高", "")
        data["平均年齢"] = company_info.get("平均年齢", "")

        # サイト定義業種（doda は求人カテゴリが検索 URL 側に持たれるため空）
        data[Schema.CAT_SITE] = ""

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DodaScraper()
    scraper.execute("https://doda.jp/DodaFront/View/JobSearchList.action")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
