"""
j-Lic 上場企業サーチ — 国内全上場企業の基本情報・財務情報クローラー

取得対象:
    - 日本の全上場企業 (約4,647社 - EDINET有価証券報告書に基づく)

取得フロー:
    1. sitemap.xml.gz をダウンロードし、/companies/{ticker} 形式のURLを列挙
    2. 各詳細ページ (HTML) を取得
    3. 埋め込み JSON-LD (Corporation スキーマ) から住所・HP・従業員数・売上高・平均年収等を抽出
    4. HTML dl/dt/dd から代表者・決算月・会計基準・セグメント等を抽出
    5. #finance-info カードから当期利益・純資産・営業CF を抽出

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/j_lic.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id j_lic
"""

import gzip
import io
import json
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

BASE_URL = "https://j-lic.com"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml.gz"

_COMPANY_URL_RE = re.compile(r"^https?://j-lic\.com/companies/\d+/?$")
_SITEMAP_URL_RE = re.compile(r"<loc>(https://j-lic\.com/companies/\d+)</loc>")
_JSONLD_RE = re.compile(
    r'<script type="application/ld\+json">(.+?)</script>', re.S
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _yen_to_million(yen) -> str:
    """円(int) → 百万円表記文字列 (カンマ区切り)"""
    try:
        n = int(yen)
    except (TypeError, ValueError):
        return ""
    return f"{n // 1_000_000:,}"


class JLicScraper(StaticCrawler):
    """j-Lic 上場企業サーチ (j-lic.com) 上場企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "証券コード", "上場市場", "市区町村", "決算月", "会計基準",
        "コーポレートガバナンス形態", "報告セグメント", "最新有価証券報告書",
        "EDINETページ", "平均年収", "当期利益", "純資産", "営業CF",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        company_urls = self._fetch_sitemap_urls()
        self.total_items = len(company_urls)
        self.logger.info("sitemap から %d 件の企業URLを取得しました", len(company_urls))

        for company_url in company_urls:
            try:
                item = self._scrape_detail(company_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning(
                    "詳細取得失敗 (スキップ): %s — %s", company_url, e
                )
                continue

    # ------------------------------------------------------------------
    # sitemap.xml.gz から全企業URLを収集
    # ------------------------------------------------------------------

    def _fetch_sitemap_urls(self) -> list[str]:
        self.logger.info("sitemap 取得中: %s", SITEMAP_URL)
        resp = self.session.get(SITEMAP_URL, timeout=self.TIMEOUT)
        resp.raise_for_status()
        with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
            xml_text = gz.read().decode("utf-8", errors="replace")

        seen: set[str] = set()
        urls: list[str] = []
        for m in _SITEMAP_URL_RE.finditer(xml_text):
            u = m.group(1)
            if u not in seen:
                seen.add(u)
                urls.append(u)
        return urls

    # ------------------------------------------------------------------
    # 詳細ページ: 全フィールド抽出
    # ------------------------------------------------------------------

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {
            Schema.URL: url,
            Schema.TEL: "",
            Schema.LINE: "",
            Schema.INSTA: "",
            Schema.X: "",
            Schema.FB: "",
            Schema.TIKTOK: "",
            Schema.CO_NUM: "",
            Schema.CAP: "",
        }

        # --- JSON-LD (Corporation) ---
        html_text = str(soup)
        corp_jsonld = self._extract_corporation_jsonld(html_text)
        if corp_jsonld:
            data[Schema.NAME] = _clean(corp_jsonld.get("name"))
            data[Schema.OPEN_DATE] = _clean(corp_jsonld.get("foundingDate"))
            data[Schema.CAT_SITE] = _clean(corp_jsonld.get("industry"))

            num_emp = corp_jsonld.get("numberOfEmployees")
            data[Schema.EMP_NUM] = str(num_emp) if num_emp not in (None, "") else ""

            ticker = _clean(corp_jsonld.get("tickerSymbol"))
            data["証券コード"] = ticker

            addr = corp_jsonld.get("address") or {}
            data[Schema.PREF] = _clean(addr.get("addressRegion"))
            data["市区町村"] = _clean(addr.get("addressLocality"))
            street = _clean(addr.get("streetAddress"))
            pref = data[Schema.PREF]
            if pref and street.startswith(pref):
                street = street[len(pref):].strip()
            data[Schema.ADDR] = street
            data[Schema.POST_CODE] = _clean(addr.get("postalCode"))

            same_as = corp_jsonld.get("sameAs") or []
            hp = ""
            edinet = ""
            for link in same_as:
                if "edinet-fsa.go.jp" in link and not edinet:
                    edinet = link
                elif not hp:
                    hp = link
            data[Schema.HP] = hp
            data["EDINETページ"] = edinet

            member_of = corp_jsonld.get("memberOf") or []
            if member_of and isinstance(member_of, list):
                first = member_of[0]
                if isinstance(first, dict):
                    data["上場市場"] = _clean(first.get("name"))
            data.setdefault("上場市場", "")

            sales_yen = None
            salary_yen = None
            for prop in corp_jsonld.get("additionalProperty") or []:
                if not isinstance(prop, dict):
                    continue
                pname = prop.get("name") or ""
                pval = prop.get("value")
                if "売上高" in pname:
                    sales_yen = pval
                elif "平均年収" in pname:
                    salary_yen = pval
            data[Schema.SALES] = _yen_to_million(sales_yen) if sales_yen else ""
            data["平均年収"] = str(salary_yen) if salary_yen not in (None, "") else ""
        else:
            # JSON-LD がなければ h1 から名称だけ拾う
            h1 = soup.select_one("h1")
            data[Schema.NAME] = _clean(h1.get_text()) if h1 else ""
            for k in (
                Schema.OPEN_DATE, Schema.CAT_SITE, Schema.EMP_NUM,
                Schema.PREF, Schema.ADDR, Schema.POST_CODE, Schema.HP,
                Schema.SALES,
            ):
                data.setdefault(k, "")
            for k in (
                "証券コード", "市区町村", "EDINETページ", "上場市場", "平均年収",
            ):
                data.setdefault(k, "")

        # --- HTML dl/dt/dd (#basic-info) ---
        basic_fields = self._extract_basic_info(soup)
        data[Schema.REP_NM] = basic_fields.get("代表者", "")
        data["決算月"] = basic_fields.get("決算月", "")
        data["会計基準"] = basic_fields.get("会計基準", "")
        data["コーポレートガバナンス形態"] = basic_fields.get(
            "コーポレートガバナンス形態", ""
        )
        data["報告セグメント"] = basic_fields.get("報告セグメント", "")
        data["最新有価証券報告書"] = basic_fields.get("最新の四半期/有価証券報告書", "")
        # 本店所在地から郵便番号/住所フォールバック (JSON-LD が無いケース対策)
        if not data.get(Schema.ADDR):
            data[Schema.ADDR] = basic_fields.get("本店所在地", "")

        # --- HTML #finance-info (当期利益/純資産/営業CF, 売上高は JSON-LD 優先) ---
        finance = self._extract_finance_info(soup)
        data["当期利益"] = finance.get("当期利益", "")
        data["純資産"] = finance.get("純資産", "")
        data["営業CF"] = finance.get("営業CF", "")
        # JSON-LD で取れなかった売上高を HTML からフォールバック
        if not data.get(Schema.SALES):
            data[Schema.SALES] = finance.get("売上高", "")

        return data

    # ------------------------------------------------------------------
    # JSON-LD (Corporation) 抽出
    # ------------------------------------------------------------------

    def _extract_corporation_jsonld(self, html: str) -> dict | None:
        for m in _JSONLD_RE.finditer(html):
            raw = m.group(1).strip()
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict) and obj.get("@type") == "Corporation":
                return obj
        return None

    # ------------------------------------------------------------------
    # #basic-info の dl/dt/dd 抽出
    # ------------------------------------------------------------------

    def _extract_basic_info(self, soup) -> dict:
        out: dict[str, str] = {}
        section = soup.select_one("section#basic-info")
        if not section:
            return out

        for dl in section.select("dl"):
            children = list(dl.children)
            current_dt: str | None = None
            for child in children:
                name = getattr(child, "name", None)
                if name == "dt":
                    current_dt = _clean(child.get_text())
                elif name == "dd" and current_dt:
                    # リンク要素のテキストを使う（ランキングバッジは除外）
                    for badge in child.select("a.badge, a.btn"):
                        badge.extract()
                    # <br> を改行に変換
                    for br in child.find_all("br"):
                        br.replace_with("\n")
                    # 複数 <p> を改行区切りで結合
                    ps = child.find_all("p")
                    if ps:
                        # 「最寄りの連絡場所」行は長い本店所在地には不要
                        lines = [
                            _clean(p.get_text(" "))
                            for p in ps
                            if "最寄りの連絡場所" not in p.get_text()
                        ]
                        val = " / ".join(l for l in lines if l)
                    else:
                        val = _clean(child.get_text(" "))
                    out[current_dt] = val
                    current_dt = None
        return out

    # ------------------------------------------------------------------
    # #finance-info カード抽出
    # ------------------------------------------------------------------

    def _extract_finance_info(self, soup) -> dict:
        out: dict[str, str] = {}
        section = soup.select_one("section#finance-info")
        if not section:
            return out

        for card in section.select("section"):
            h3 = card.find("h3")
            if not h3:
                continue
            label = _clean(h3.get_text())
            # 「売上高（百万円）」→ 「売上高」
            label = re.sub(r"[（(].*?[）)]", "", label).strip()
            p = card.find("p")
            if not p:
                continue
            # バッジを除去してから数値だけ取り出す
            for badge in p.select("a.badge"):
                badge.extract()
            for br in p.find_all("br"):
                br.replace_with(" ")
            raw = _clean(p.get_text(" "))
            m = re.search(r"-?[\d,]+", raw)
            out[label] = m.group(0) if m else ""
        return out


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JLicScraper()
    scraper.execute(f"{BASE_URL}/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
