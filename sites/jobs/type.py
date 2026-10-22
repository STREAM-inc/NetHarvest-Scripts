"""
転職type — 求人情報スクレイパー

対象サイト: https://type.jp

取得フロー:
    1. {url}/job/search/?offset=N を 50件刻みで巡回（条件なし検索 = 全職種・全業種、約2,300件）
    2. 各カードの jobId から詳細URL /job-1/{jobId}_detail/ を組み立て（職種番号が違っても正規URLへ302）
    3. 詳細ページの JSON-LD (JobPosting) と 会社概要/連絡先 dl.uq-detail-* を解析して即 yield

備考:
    - 「全業種取得」指定のためフィルタは掛けない
    - 連絡先が type採用事務局（運営元 CDC の住所）の場合は企業住所ではないため
      住所/郵便番号/都道府県には入れず、EXTRA「連絡先住所」にのみ残す
    - 仕事内容・応募資格・福利厚生・選考プロセス本文などの長文は著作権リスクのため取得しない

実行方法:
    python scripts/sites/jobs/type.py
    python bin/run_flow.py --site-id type
"""

import json
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

PAGE_SIZE = 50
MAX_OFFSET = 20000  # 安全上限（実際は約2,300件）

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_JOB_ID_PATTERN = re.compile(r"(?:jobId=|/job-\d+/)(\d+)")
_ZIP_PATTERN = re.compile(r"(\d{3})-?(\d{4})")
_TEL_PATTERN = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")
_COMPANY_ID_PATTERN = re.compile(r"/job-company/(\d+)/")
# type採用事務局（運営元 キャリアデザインセンター）の住所
_AGENCY_ADDR_MARK = "赤坂3-21-20"

_EMPLOYMENT_TYPES = {
    "FULL_TIME": "正社員",
    "PART_TIME": "パート・アルバイト",
    "CONTRACTOR": "契約社員",
    "TEMPORARY": "派遣社員",
    "INTERN": "インターン",
    "VOLUNTEER": "ボランティア",
    "PER_DIEM": "日雇い",
    "OTHER": "その他",
}


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _first_line(el: bs4.Tag | None) -> str:
    """<br> 区切りの先頭行（注記を除いた本体）を返す。"""
    if el is None:
        return ""
    for line in el.get_text("\n").split("\n"):
        line = _clean(line)
        if line:
            return line
    return ""


class TypeScraper(StaticCrawler):
    """転職type スクレイパー（type.jp）"""

    DELAY = 3.0
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    EXTRA_COLUMNS = [
        "求人ID",
        "企業ID",
        "求人タイトル",
        "職種カテゴリ",
        "職種",
        "雇用形態",
        "想定年収下限",
        "想定年収上限",
        "勤務地都道府県",
        "掲載開始日",
        "掲載終了日",
        "主要取引先",
        "選考ステップ",
        "採用担当",
        "連絡先住所",
        "関連リンク",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        search_url = urllib.parse.urljoin(url, "/job/search/")
        seen: set[str] = set()

        for offset in range(0, MAX_OFFSET, PAGE_SIZE):
            list_url = search_url if offset == 0 else f"{search_url}?offset={offset}"
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧取得失敗: %s", list_url)
                break

            if self.total_items is None:
                num = soup.select_one(".whole-num .num")
                if num and num.get_text(strip=True).isdigit():
                    self.total_items = int(num.get_text(strip=True))

            page_ids: list[str] = []
            for art in soup.select("article.mod-job-info"):
                a = art.select_one(".title a[href], a.job-link[href]")
                if not a:
                    continue
                m = _JOB_ID_PATTERN.search(a["href"])
                if not m or m.group(1) in seen:
                    continue
                seen.add(m.group(1))
                page_ids.append(m.group(1))

            self.logger.info("offset=%d: 新規 %d件 (累計 %d件)", offset, len(page_ids), len(seen))
            if not page_ids:
                break

            for job_id in page_ids:
                detail_url = urllib.parse.urljoin(url, f"/job-1/{job_id}_detail/")
                item = self._scrape_detail(detail_url, job_id)
                if item:
                    yield item

            if not soup.select_one(".mod-pagination p.next.active a[href]"):
                break

    def _scrape_detail(self, url: str, job_id: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        ld: dict = {}
        crumbs: list[dict] = []
        for sc in soup.find_all("script", type="application/ld+json"):
            try:
                obj = json.loads(sc.string or "")
            except json.JSONDecodeError:
                continue
            if obj.get("@type") == "JobPosting":
                ld = obj
            elif obj.get("@type") == "BreadcrumbList":
                crumbs = obj.get("itemListElement", [])

        data: dict = {Schema.URL: url, "求人ID": job_id}

        org = ld.get("hiringOrganization") or {}
        data[Schema.NAME] = _clean(org.get("name"))
        data["求人タイトル"] = _clean(ld.get("title")) or _clean(
            soup.select_one("h1.jobname").get_text() if soup.select_one("h1.jobname") else ""
        )
        data[Schema.CAT_SITE] = _clean(ld.get("industry"))
        data["職種"] = _clean(ld.get("occupationalCategory"))
        data["掲載開始日"] = _clean(ld.get("datePosted"))
        data["掲載終了日"] = _clean(ld.get("validThrough"))

        ident = ld.get("identifier") or {}
        data["企業ID"] = _clean(ident.get("value"))
        if len(crumbs) >= 2:
            data["職種カテゴリ"] = _clean((crumbs[1].get("item") or {}).get("name"))
        if not data["企業ID"]:
            for c in crumbs:
                m = _COMPANY_ID_PATTERN.search((c.get("item") or {}).get("@id", ""))
                if m:
                    data["企業ID"] = m.group(1)

        # 雇用形態: ページ表記の先頭行を優先し、無ければ JSON-LD
        data["雇用形態"] = _first_line(soup.select_one(".uq-detail-type ._box_main")) or \
            _EMPLOYMENT_TYPES.get(_clean(ld.get("employmentType")), _clean(ld.get("employmentType")))

        salary = (ld.get("baseSalary") or {})
        if isinstance(salary, dict):
            data["想定年収下限"] = _clean(salary.get("minValue"))
            data["想定年収上限"] = _clean(salary.get("maxValue"))

        regions: list[str] = []
        for loc in ld.get("jobLocation") or []:
            r = _clean(((loc or {}).get("address") or {}).get("addressRegion"))
            if r and r not in regions:
                regions.append(r)
        data["勤務地都道府県"] = " / ".join(regions)

        # 会社概要 / 応募について の dl.uq-detail-*
        def dd(cls: str) -> bs4.Tag | None:
            return soup.select_one(f"dl.uq-detail-{cls} dd")

        def dd_text(cls: str) -> str:
            el = dd(cls)
            return _clean(el.get_text(" / ", strip=True)) if el else ""

        lob = dd("business_description")
        data[Schema.LOB] = lob.get_text("\n", strip=True) if lob else ""
        data[Schema.OPEN_DATE] = dd_text("establishment")
        data[Schema.EMP_NUM] = dd_text("employee")
        data[Schema.CAP] = dd_text("capital")
        data[Schema.SALES] = dd_text("amount_sold")
        data["主要取引先"] = dd_text("client")
        data["関連リンク"] = " / ".join(
            a["href"] for a in (dd("relatedlink").select("a[href^=http]") if dd("relatedlink") else [])
        )

        rep = dd_text("representative")
        if rep:
            parts = re.split(r"[\s　]+", rep, maxsplit=1)
            if len(parts) == 2 and re.search(r"(代表|社長|会長|CEO|理事|取締役|執行役)", parts[0]):
                data[Schema.POS_NM], data[Schema.REP_NM] = parts[0], parts[1]
            else:
                data[Schema.REP_NM] = rep

        hp = dd("homepage")
        hp_a = hp.select_one("a[href^=http]") if hp else None
        data[Schema.HP] = hp_a["href"].strip() if hp_a else ""

        data["選考ステップ"] = " → ".join(
            _clean(p.get_text()) for p in soup.select(".uq-detail-process ol.step .step-inner")
        )

        # 連絡先
        contact = dd("contact")
        if contact:
            staffs = _clean(contact.select_one(".uq-detail-staffs").get_text()) \
                if contact.select_one(".uq-detail-staffs") else ""
            data["採用担当"] = staffs
            addr = _first_line(contact.select_one(".uq-detail-address"))
            zip_el = contact.select_one(".uq-detail-zipcode")
            zm = _ZIP_PATTERN.search(zip_el.get_text()) if zip_el else None
            zipcode = f"{zm.group(1)}-{zm.group(2)}" if zm else ""
            data["連絡先住所"] = _clean(f"〒{zipcode} {addr}") if zipcode else addr

            is_agency = "事務局" in staffs or _AGENCY_ADDR_MARK in addr.replace("－", "-")
            if addr and not is_agency:
                data[Schema.POST_CODE] = zipcode
                pm = _PREF_PATTERN.match(addr)
                if pm:
                    data[Schema.PREF] = pm.group(1)
                    data[Schema.ADDR] = addr[pm.end():].strip()
                else:
                    data[Schema.ADDR] = addr

            tel_el = contact.select_one(".uq-detail-tel")
            if tel_el:
                tm = _TEL_PATTERN.search(tel_el.get_text().translate(
                    str.maketrans("０１２３４５６７８９－", "0123456789-")))
                if tm:
                    data[Schema.TEL] = tm.group(0)

        if not data.get(Schema.NAME):
            self.logger.warning("会社名なし: %s", url)
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TypeScraper()
    scraper.execute("https://type.jp")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
