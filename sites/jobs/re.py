"""
Re就活 — 求人・企業情報スクレイパー

取得対象:
    - 企業情報（設立、代表者、従業員数、資本金、売上高、事業所、事業内容）
    - 募集情報（業種、職種、雇用形態、最終更新日）

取得フロー:
    一覧ページ（/search/sch_result）を全ページ巡回 → 詳細ページを即scrape → 即yield

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/re.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id re
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_POSTAL_PATTERN = re.compile(r"〒?\s*(\d{3}[-－]\d{4})\s*")
# ハイフン類: - (U+002D), － (FF0D), ‐ (U+2010), ‑ (U+2011), – (U+2013)
_TEL_PATTERN = re.compile(r"(?:TEL|Tel|tel)[\/：:\s]*([0-9０-９(（)）\-－‐‑–]+)")
_OFFICE_HEADER = re.compile(r"^[■▶▼●◆□▷▽○◇★☆・【]")


def _filter_bullet_only(text: str) -> str:
    """文章(散文)の場合は空文字を返す。箇条書き・1単語のみ許可。"""
    if not text:
        return ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return ""
    if len(lines) >= 2:
        return text
    single = lines[0]
    if len(single) > 40 or re.search(r"[。をはがにでもため]", single):
        return ""
    return text


class ReScraper(StaticCrawler):
    """Re就活 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["郵便番号", "売上高", "事業所", "職種", "雇用形態", "掲載終了予定日", "最終更新日"]

    def parse(self, url: str):
        base_search = url.rstrip("/") + "/search/sch_result"
        page = 1
        last_page = None
        total_set = False

        while True:
            list_soup = self.get_soup(f"{base_search}?p0=1&pagCnt={page}")
            items = list_soup.select("li.liCompList")
            if not items:
                break

            if not total_set:
                total_el = list_soup.select_one("span.search-number")
                if total_el:
                    m = re.search(r"(\d[\d,]+)", total_el.get_text())
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))
                last_page_el = list_soup.select_one("#hdnLastPage")
                if last_page_el:
                    last_page = int(last_page_el.get("value", "0") or 0)
                total_set = True

            for item in items:
                try:
                    detail_input = item.select_one("input.strRecUrl")
                    if not detail_input:
                        continue
                    detail_path = detail_input.get("value", "")
                    if not detail_path:
                        continue
                    # クエリパラメータを除いてクリーンなURLを構築
                    detail_url = urljoin(
                        "https://re-katsu.jp",
                        detail_path.split("?")[0].rstrip("/") + "/",
                    )
                    record = self._scrape_detail(detail_url)
                    if record:
                        yield record
                except Exception as e:
                    self.logger.warning(f"detail error [page={page}]: {e}")
                    continue

            if last_page and page >= last_page:
                break
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        # キャッチコピーをDOMから除去（名称への混入を防ぐ）
        catchcopy_el = soup.select_one("span#lblWorkTypeCopy")
        if catchcopy_el:
            catchcopy_el.decompose()

        def txt(selector: str) -> str:
            el = soup.select_one(selector)
            return el.get_text(" ", strip=True) if el else ""

        # 会社名
        company_name = txt("span#lblCompanyName")
        job_title = txt("span#lnkComnm") or txt(".company-name") or txt("h1.company")
        if not job_title and not company_name:
            return None
        name = f"{company_name}{job_title}" if company_name else job_title

        # 本社所在地 — 【XX本社】/■XX本社 見出し行を除去し、残る行を結合（郵便番号・住所・建物名が複数行に分かれる）
        addr_el = soup.select_one("span#lblHeadofficelocation")
        address_raw = ""
        if addr_el:
            lines = [l.strip() for l in addr_el.get_text("\n", strip=True).splitlines() if l.strip()]
            clean = [l for l in lines if not _OFFICE_HEADER.match(l)]
            address_raw = " ".join(clean) if clean else ""

        # 郵便番号を住所から切り出す
        postal = ""
        addr = address_raw
        if address_raw:
            pm = _POSTAL_PATTERN.search(address_raw)
            if pm:
                postal = pm.group(1)
                addr = (address_raw[:pm.start()] + address_raw[pm.end():]).strip()

        # 連絡先テキストからTELを抽出（lblContactInfo → lblTel の順で試みる）
        tel = ""
        for tel_src in ["span#lblContactInfo", "span#lblTel", "span#lblPhone"]:
            raw = txt(tel_src)
            if not raw:
                continue
            m = _TEL_PATTERN.search(raw)
            if m:
                tel = m.group(1)
                break
            # "TEL" ラベルなしで数字のみ記載されている場合
            m2 = re.search(r"(0\d{1,4}[\-－‐‑–]\d{1,4}[\-－‐‑–]\d{4})", raw)
            if m2:
                tel = m2.group(1)
                break

        # 企業ホームページ URL
        homepage = ""
        for sel in ["span#lblHomeUrl a", "a#lnkHomeUrl", "span#lblCompanyUrl a",
                    "span#lblHomepage a", "span#lblWebsite a"]:
            el = soup.select_one(sel)
            if el and el.get("href", "").startswith("http"):
                homepage = el["href"].strip()
                break
        if not homepage:
            # テキストに URL が直書きされているケース
            for span_id in ["lblHomeUrl", "lblCompanyUrl", "lblHomepage", "lblWebsite"]:
                raw = txt(f"span#{span_id}")
                m = re.search(r"https?://[^\s　「」【】]+", raw)
                if m:
                    homepage = m.group(0).rstrip("。、")
                    break

        # 雇用形態タグ（data-color="sky"）
        emp_tags = soup.select(".tag-list li[data-color='sky']")
        employment = "、".join(t.get_text(strip=True) for t in emp_tags)

        # 職種 — ヘッダー業種アイコン優先、なければ募集職種テキスト
        job_type = txt("span#lblServIcon") or txt("span#lblWantedJobType")

        return {
            Schema.NAME: name,
            Schema.URL: url,
            Schema.ADDR: addr,
            "郵便番号": postal,
            Schema.TEL: tel,
            Schema.HP: homepage,
            Schema.REP_NM: txt("span#lblRepresentative"),
            Schema.EMP_NUM: txt("span#lblEmployeesCount"),
            Schema.CAP: txt("span#lblCapital"),
            Schema.CAT_SITE: _filter_bullet_only(txt("span#lblIndustryIcon")),
            Schema.OPEN_DATE: txt("span#lblEstablishment"),
            Schema.LOB: _filter_bullet_only(txt("span#lblBusinessContents")),
            "売上高": txt("span#lblAmountSales"),
            "事業所": txt("span#lblOfficePoint"),
            "職種": job_type,
            "雇用形態": employment,
            "掲載終了予定日": txt("span#lblPublishedEnddate"),
            "最終更新日": txt("span#lblPublishedLastdate"),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ReScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://re-katsu.jp/career/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
