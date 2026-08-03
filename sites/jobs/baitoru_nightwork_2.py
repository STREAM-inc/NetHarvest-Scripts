"""
バイトル（ナイトワーク系）【ガールズバー・キャバクラ・スナック】 — 全国全件スクレイパー

取得対象:
    ガールズバー・キャバクラ・スナック の全国（関東〜九州）求人を対象とした
    統合一覧ページ（全地域を1つにまとめた jlist/nightwork）から全件を巡回する。

取得フロー (Pattern B: 詳細1件ごとに即 yield):
    1) 統合一覧ページ .../jlist/nightwork/ を pageN/ でページ送り
       - 各ページには「本体の求人リンク」がちょうど 30 件（pname に
         link_job_detail...jlist を含むもの）。おすすめ枠(pname無し)は除外。
    2) 各求人詳細 /jobdetail/{id}/ を取得
       - JSON-LD JobPosting から 店名/HP/都道府県/住所/雇用形態/給与/掲載日等
       - 会社概要ブロック (_itemTitle / _itemValue) から 事業内容/会社所在地/対応エリア
       - お問い合わせ本文から TEL を正規表現抽出

補足:
    - 静的取得可 (Cloudflare 無)。SNS リンクはサイト公式アカウントのみ掲載のため取得しない。
    - 総件数 約5,467件 / 30件per頁 ≒ 183頁 (page400 上限内)。エリア分割不要。

実行方法:
    python scripts/sites/jobs/baitoru_nightwork_2.py
    python bin/run_flow.py --site-id baitoru_nightwork_2
"""

import json
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.baitoru.com"

# サイト内カテゴリ（このクローラーは nightwork = ガールズバー・キャバクラ・スナックに固定）
CAT_SITE_NAME = "ガールズバー・キャバクラ・スナック"

# ページ送り上限（baitoru の一覧は page400 で頭打ち。安全側の上限）
MAX_PAGE = 400

# 本体求人リンク判定: pname に link_job_detail...jlist を含むものだけが一覧本体の 30 件。
# （pname 無しのアンカーはおすすめ・関連求人枠なので除外する）
_MAIN_JOB_RE = re.compile(r"link_job_detail.*jlist")

# 雇用形態コード → 日本語
_EMP_TYPE = {
    "PART_TIME": "アルバイト・パート",
    "FULL_TIME": "正社員",
    "CONTRACTOR": "業務委託",
    "TEMPORARY": "派遣",
    "INTERN": "インターン",
    "OTHER": "その他",
}

# 給与単位コード → 日本語
_SALARY_UNIT = {"HOUR": "時給", "MONTH": "月給", "DAY": "日給", "YEAR": "年収", "WEEK": "週給"}


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\r", " ").replace("\n", " ")).strip()


class BaitoruNightwork2Scraper(StaticCrawler):
    """バイトル（ナイトワーク系）【ガールズバー・キャバクラ・スナック】 全国全件スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "求人タイトル",   # JobPosting.title（見出し）
        "給与",           # baseSalary から構造化（例: 時給5,000〜15,000円）
        "雇用形態",       # employmentType（日本語）
        "対応エリア",     # 会社概要「サービス地域」
        "会社所在地",     # 会社概要「所在地」(本社住所)
        "掲載日",         # datePosted
        "有効期限",       # validThrough
        "求人NO",         # identifier.value
    ]

    def parse(self, url: str):
        # 引数 url を唯一のルートとする。ページ送りはここから派生。
        base = url.split("?", 1)[0].rstrip("/")   # .../jlist/nightwork
        seen: set[str] = set()

        for page in range(1, MAX_PAGE + 1):
            list_url = url if page == 1 else f"{base}/page{page}/"

            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧取得失敗: %s", list_url)
                break

            # 本体求人リンク（30件/頁）を DOM 順で収集・重複除去
            hrefs = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/jobdetail/" not in href or not _MAIN_JOB_RE.search(href):
                    continue
                clean_href = href.split("?", 1)[0]
                detail_url = clean_href if clean_href.startswith("http") else BASE_URL + clean_href
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                hrefs.append(detail_url)

            if not hrefs:
                # 本体求人が無い＝最終ページを越えた
                break

            for detail_url in hrefs:
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # noqa: BLE001 — 1件の失敗で全体を止めない
                    self.logger.warning("詳細解析失敗: %s (%s)", detail_url, e)
                    continue
                if item:
                    yield item

    # ------------------------------------------------------------------ detail

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        ld = self._find_jobposting(soup)
        if not ld:
            return None

        org = ld.get("hiringOrganization") or {}
        name = _clean(org.get("name", ""))
        if not name:
            return None

        # HP（企業サイト）: sameAs（baitoru 自身のURLは除外）
        hp = org.get("sameAs", "") or ""
        if hp and "baitoru.com" in hp:
            hp = ""

        # 住所（勤務地）
        pref, addr = "", ""
        place = ld.get("jobLocation") or {}
        if isinstance(place, list):
            place = place[0] if place else {}
        adr = place.get("address") or {}
        pref = _clean(adr.get("addressRegion", ""))
        locality = _clean(adr.get("addressLocality", ""))
        street = _clean(adr.get("streetAddress", ""))
        addr = " ".join(p for p in (locality, street) if p).strip()

        # 給与
        salary = self._format_salary(ld.get("baseSalary") or {})

        # 雇用形態
        emp = ld.get("employmentType", "")
        if isinstance(emp, list):
            emp = emp[0] if emp else ""
        emp_ja = _EMP_TYPE.get(emp, emp or "")

        # 求人NO
        ident = ld.get("identifier") or {}
        job_no = _clean(ident.get("value", "")) if isinstance(ident, dict) else ""

        # 会社概要ブロック（_itemTitle / _itemValue）
        lob, co_addr, service_area = self._parse_company_block(soup)

        # TEL（お問い合わせ本文から抽出）
        tel = self._extract_tel(soup)

        return {
            Schema.NAME:     name,
            Schema.URL:      url,
            Schema.PREF:     pref,
            Schema.ADDR:     addr,
            Schema.TEL:      tel,
            Schema.HP:       hp,
            Schema.CAT_SITE: CAT_SITE_NAME,
            Schema.LOB:      lob,
            "求人タイトル":   _clean(ld.get("title", "")),
            "給与":           salary,
            "雇用形態":       emp_ja,
            "対応エリア":     service_area,
            "会社所在地":     co_addr,
            "掲載日":         _clean(ld.get("datePosted", "")),
            "有効期限":       _clean(ld.get("validThrough", "")),
            "求人NO":         job_no,
        }

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _find_jobposting(soup) -> dict | None:
        for sc in soup.find_all("script", type="application/ld+json"):
            raw = sc.string or sc.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            candidates = data if isinstance(data, list) else [data]
            for d in candidates:
                if isinstance(d, dict) and d.get("@type") == "JobPosting":
                    return d
        return None

    @staticmethod
    def _format_salary(base_salary: dict) -> str:
        if not isinstance(base_salary, dict):
            return ""
        val = base_salary.get("value") or {}
        if not isinstance(val, dict):
            return ""
        unit = _SALARY_UNIT.get(val.get("unitText", ""), "")
        mn = val.get("minValue")
        mx = val.get("maxValue")
        if mn is None and mx is None:
            return ""
        if mn is not None and mx is not None and mn != mx:
            amount = f"{int(mn):,}〜{int(mx):,}円"
        else:
            one = mn if mn is not None else mx
            amount = f"{int(one):,}円"
        return f"{unit}{amount}" if unit else amount

    def _parse_company_block(self, soup):
        """会社概要の _itemTitle / _itemValue ペアから 事業内容/所在地/サービス地域 を取得。"""
        lob = co_addr = service_area = ""
        for title_el in soup.select('[class*="_itemTitle"]'):
            label = title_el.get_text(" ", strip=True)
            value_el = title_el.find_next(class_=re.compile("_itemValue"))
            if not value_el:
                continue
            value = _clean(value_el.get_text(" ", strip=True))
            if not value:
                continue
            if "事業内容" in label and not lob:
                lob = value
            elif "所在地" in label and not co_addr:
                co_addr = value
            elif ("サービス地域" in label or "対応エリア" in label) and not service_area:
                service_area = value
        return lob, co_addr, service_area

    @staticmethod
    def _extract_tel(soup) -> str:
        """お問い合わせ本文（_text_ ブロック）から店舗TELを正規表現抽出。"""
        tel_re = re.compile(
            r"(?:TEL応募】?|電話応募|店舗)[^0-9]{0,15}(0\d{1,4}[-‐]\d{1,4}[-‐]\d{3,4})"
        )
        for el in soup.select('[class*="_text_"]'):
            m = tel_re.search(el.get_text(" ", strip=True).replace("　", " "))
            if m:
                return m.group(1)
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BaitoruNightwork2Scraper()
    scraper.execute(
        "https://www.baitoru.com/kanto-tokai-kansai-tohoku-koshinetsu-chushikoku-kyushu/jlist/nightwork?pname=link_job_list_pc_all_all"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
