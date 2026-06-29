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

# 雇用形態として採用するラベル集合（.tag-list は重複表示・他種タグも混在するため絞り込む）
_EMP_TYPES = {
    "正社員", "契約社員", "派遣社員", "紹介予定派遣", "アルバイト",
    "パート", "業務委託", "嘱託社員", "嘱託", "新卒", "中途",
}


def _id_re(base: str) -> "re.Pattern":
    """ASP.NET の span id 照合用。実機は base に接尾連番が付く（例: lblEstablishment2）。
    旧 `ctl00_..._lblXxx` プレフィックス形式と接尾連番形式の双方に一致させる。"""
    return re.compile(rf"(?:^|_){re.escape(base)}\d*$")


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
        catchcopy_el = soup.find("span", id=_id_re("lblWorkTypeCopy"))
        if catchcopy_el:
            catchcopy_el.decompose()

        def txt(base: str) -> str:
            """span id を接尾連番込みの正規表現で照合してテキスト取得。"""
            el = soup.find("span", id=_id_re(base))
            return el.get_text(" ", strip=True) if el else ""

        # 会社名 — lblCompanyName が無ければ lblFixCompanyName（接尾辞なし）
        company_name = txt("lblCompanyName") or txt("lblFixCompanyName")
        if not company_name:
            return None
        # 末尾の【上場】等の注記を除去
        name = re.sub(r"\s*【[^】]*】\s*$", "", company_name).strip()

        # 本社所在地 — 【本社】【大阪本社】… と複数ブロックが並ぶので最初のブロックのみ採用
        addr_el = soup.find("span", id=_id_re("lblHeadofficelocation"))
        address_raw = ""
        if addr_el:
            lines = [l.strip() for l in addr_el.get_text("\n", strip=True).splitlines() if l.strip()]
            block = []
            started = False
            for l in lines:
                if _OFFICE_HEADER.match(l):
                    if started:  # 2番目の見出し（別事業所）に到達したら打ち切り
                        break
                    started = True
                    continue
                block.append(l)
            address_raw = " ".join(block) if block else " ".join(lines)

        # 郵便番号を住所から切り出す
        postal = ""
        addr = address_raw
        if address_raw:
            pm = _POSTAL_PATTERN.search(address_raw)
            if pm:
                postal = pm.group(1)
                addr = (address_raw[:pm.start()] + address_raw[pm.end():]).strip()

        # TEL/問い合わせ欄・企業HP欄は詳細ページに存在しないため空が正常
        tel = ""
        homepage = ""

        # 雇用形態 — .tag-list は重複表示・他種タグ混在のため既知ラベルのみ抽出
        emp = []
        for li in soup.select(".tag-list li"):
            t = li.get_text(strip=True)
            if t in _EMP_TYPES and t not in emp:
                emp.append(t)
        employment = "、".join(emp)

        return {
            Schema.NAME: name,
            Schema.URL: url,
            Schema.ADDR: addr,
            "郵便番号": postal,
            Schema.TEL: tel,
            Schema.HP: homepage,
            Schema.REP_NM: txt("lblRepresentative"),
            Schema.EMP_NUM: txt("lblEmployeesCount"),
            Schema.CAP: txt("lblCapital"),
            Schema.CAT_SITE: _filter_bullet_only(txt("lblIndustryIcon")),
            Schema.OPEN_DATE: txt("lblEstablishment"),
            Schema.LOB: _filter_bullet_only(txt("lblBusinessContents")),
            "売上高": txt("lblAmountSales"),
            "事業所": txt("lblOfficePoint"),
            "職種": txt("lblWantedJobType"),
            "雇用形態": employment,
            "掲載終了予定日": txt("lblPublishedLastdate"),
            "最終更新日": txt("lblLastUpdate"),
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
