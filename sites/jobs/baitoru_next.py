# scripts/sites/jobs/baitoru_next.py
"""
バイトルNEXT — 東北エリア正社員・契約社員求人企業情報スクレイパー

取得対象:
    - 詳細ページの企業情報セクション (div.detail-companyInfo)
        社名, 住所, TEL, 代表者名, 事業内容, HP URL
    - 詳細ページの基本情報セクション (div.detail-basicInfo)
        職種, 雇用形態, 給与, 勤務時間, 最寄り駅, 休日・休暇, 福利厚生
    - 詳細ページの募集情報セクション (div.detail-recruitInfo)
        仕事内容, 応募条件

取得フロー:
    一覧ページ (/tohoku/jlist/shain/) → link[rel="next"] でページネーション
    → 各詳細ページから企業・求人情報を取得

実行方法:
    python scripts/sites/jobs/baitoru_next.py
    python bin/run_flow.py --site-id baitoru_next
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.baitoru.com"
LIST_PATH = "/tohoku/jlist/shain/"

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_TYPE_MAP = {
    "正": "正社員",
    "契": "契約社員",
    "派": "派遣社員",
    "業": "業務委託",
    "紹": "紹介予定派遣",
    "無": "無期雇用派遣",
}


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


class BaitoruNextScraper(StaticCrawler):
    """バイトルNEXT 東北エリア 求人企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["職種", "雇用形態", "給与", "最寄り駅", "休日・休暇", "仕事内容", "応募条件", "福利厚生"]

    def parse(self, url: str):
        list_url = BASE_URL + LIST_PATH
        first_page = True

        while list_url:
            soup = self.get_soup(list_url)

            if first_page:
                count_el = soup.select_one("#js-job-count")
                if count_el:
                    try:
                        self.total_items = int(count_el.get_text(strip=True).replace(",", ""))
                    except ValueError:
                        pass
                first_page = False

            articles = soup.select("article.list-jobListDetail")
            if not articles:
                break

            for article in articles:
                a_tag = article.select_one("h3 a[href]")
                if not a_tag:
                    continue
                href = a_tag.get("href", "")
                detail_url = href if href.startswith("http") else BASE_URL + href
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
                    continue

            next_link = soup.find("link", rel="next")
            if next_link and next_link.get("href"):
                href = next_link["href"]
                list_url = href if href.startswith("http") else BASE_URL + href
            else:
                list_url = None

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        data = {Schema.URL: url}

        # ── 企業情報 (div.detail-companyInfo) ──────────────────────────
        company_section = soup.select_one("div.detail-companyInfo")
        if company_section:
            # 会社名
            pt02_p = company_section.select_one(".pt02 dd p")
            if pt02_p:
                data[Schema.NAME] = pt02_p.get_text(strip=True)

            # pt03: ラベル–値ペアの dl 群
            for dl in company_section.select(".pt03 > dl"):
                dt_span = dl.select_one("dt span")
                dd = dl.select_one("dd")
                if not dt_span or not dd:
                    continue
                label = dt_span.get_text(strip=True)

                if label == "所在地":
                    p = dd.select_one("p")
                    if p:
                        text = p.get_text(separator="\n").strip()
                        lines = [l.strip() for l in text.splitlines() if l.strip()]
                        addr_parts = []
                        for line in lines:
                            if "TEL:" in line:
                                tel = re.sub(r".*TEL:", "", line.split("FAX:")[0]).strip()
                                if tel:
                                    data[Schema.TEL] = tel
                            elif "FAX:" not in line:
                                addr_parts.append(line)
                        if addr_parts:
                            addr = " ".join(addr_parts)
                            m = _PREF_RE.match(addr)
                            if m:
                                data[Schema.PREF] = m.group(1)
                                data[Schema.ADDR] = addr[m.end():].strip()
                            else:
                                data[Schema.ADDR] = addr

                elif label == "代表者名":
                    data[Schema.REP_NM] = dd.get_text(strip=True)

                elif label == "事業内容":
                    data[Schema.LOB] = _clean(dd.get_text())

                elif label == "URL":
                    a = dd.select_one("a[href]")
                    if a:
                        data[Schema.HP] = a["href"]

        # ── 基本情報 (div.detail-basicInfo) ────────────────────────────
        basic_section = soup.select_one("div.detail-basicInfo")
        if basic_section:
            # 職種・雇用形態
            dl01 = basic_section.select_one("dl.dl01")
            if dl01:
                em_els = dl01.select("dd ul li p em")
                job_types = [e.get_text(strip=True) for e in em_els if e.get_text(strip=True)]
                if job_types:
                    data["職種"] = "、".join(job_types)
                span01 = dl01.select_one("dd ul li p span.span01")
                if span01:
                    code = span01.get_text(strip=True).strip("[]")
                    data["雇用形態"] = _TYPE_MAP.get(code, code)

            # 給与
            dl02 = basic_section.select_one("dl.dl02")
            if dl02:
                em_els = dl02.select("dd ul li p em")
                salaries = [e.get_text(strip=True) for e in em_els if e.get_text(strip=True)]
                if salaries:
                    data["給与"] = "、".join(salaries)

            # 勤務時間
            dl03 = basic_section.select_one("dl.dl03")
            if dl03:
                em_els = dl03.select("dd ul.ul02 li p em")
                times = [e.get_text(strip=True) for e in em_els if e.get_text(strip=True)]
                if times:
                    data[Schema.TIME] = "、".join(times)

            # 勤務地・面接地 → 最寄り駅・住所 (PREF/ADDR を勤務地で上書き)
            dl04 = basic_section.select_one("dl.dl04")
            if dl04:
                dd04 = dl04.select_one("dd")
                if dd04:
                    for inner_dl in dd04.select("dl"):
                        inner_dt = inner_dl.select_one("dt")
                        inner_dd = inner_dl.select_one("dd")
                        if not inner_dt or not inner_dd:
                            continue
                        inner_label = inner_dt.get_text(strip=True)
                        if "最寄" in inner_label:
                            data["最寄り駅"] = _clean(inner_dd.get_text())
                        elif "住所" in inner_label:
                            li = inner_dd.select_one("ul li")
                            if li:
                                addr = li.get_text(strip=True)
                                m = _PREF_RE.match(addr)
                                if m:
                                    data[Schema.PREF] = m.group(1)
                                    data[Schema.ADDR] = addr[m.end():].strip()
                                else:
                                    data[Schema.ADDR] = addr

            # 休日・休暇
            dl08 = basic_section.select_one("dl.dl08")
            if dl08:
                dd08 = dl08.select_one("dd")
                if dd08:
                    data["休日・休暇"] = _clean(dd08.get_text())

            # 待遇・福利厚生
            dl09 = basic_section.select_one("dl.dl09")
            if dl09:
                dd09 = dl09.select_one("dd")
                if dd09:
                    data["福利厚生"] = _clean(dd09.get_text())[:500]

        # ── 募集情報 (div.detail-recruitInfo) ──────────────────────────
        recruit_section = soup.select_one("div.detail-recruitInfo")
        if recruit_section:
            dl01_r = recruit_section.select_one("dl.dl01")
            if dl01_r:
                dd = dl01_r.select_one("dd")
                if dd:
                    data["仕事内容"] = _clean(dd.get_text())[:500]

            dl05_r = recruit_section.select_one("dl.dl05")
            if dl05_r:
                dd = dl05_r.select_one("dd")
                if dd:
                    data["応募条件"] = _clean(dd.get_text())[:500]

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BaitoruNextScraper()
    scraper.execute(BASE_URL + LIST_PATH)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
