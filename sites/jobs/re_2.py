"""
Re就活（やり直し版） — 求人・企業情報スクレイパー

取得対象:
    - 企業情報（設立、代表者、従業員数、資本金、売上高、事業所、事業内容）
    - 募集情報（業種、職種、雇用形態、掲載終了予定日、最終更新日）

取得フロー:
    検索結果ページ（勤務地を全選択した sch_result）を p0=N でページ送りして
    求人詳細 URL を列挙・重複排除 → 詳細ページを即scrape → 即yield

実装メモ（旧 jobs/re.py からの修正点）:
    - 詳細ページは ASP.NET 製で、ID が `ctl00_ContentPlaceHolder1_lblXxx` のように
      プレフィックス付き。旧コードの `span#lblXxx` は一切ヒットせず会社名も空→全件 None→0件
      だった。本版は後方一致セレクタ `span[id$='_lblXxx']` を使う。
    - 掲載終了予定日／最終更新日のマッピングを修正
      （掲載終了予定日=lblPublishedLastdate, 最終更新日=lblLastUpdate）。
    - 雇用形態タグは data-color='sky'(正社員) と 'mazarine'(契約社員) 等を取りこぼしていたため、
      既知の雇用形態ラベル集合で抽出するように変更。
    - sitemap（recruit_sitemap.xml / end_sitemap.xml）経由では実行時 0 件だったため、
      詳細 URL の列挙を検索結果ページ（sch_result）ベースに変更した。
      検索フォームで勤務地を全選択した状態の URL（p3 に全勤務地コードを指定）を起点に、
      `p0`（ページ番号）を 1 から順に送って一覧の `/company/recruit/{id}/` リンクを収集する。
      勤務地全選択で約 1,094 件が列挙できる（該当数表示で確認）。
      一覧の求人リンクは `?b1=...` 等のクエリ付きで出るため、求人 ID で正規化・重複排除する。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/re_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id re_2
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


_POSTAL_PATTERN = re.compile(r"〒?\s*(\d{3}[-－]\d{4})")
# ハイフン類: - (U+002D), － (FF0D), ‐ (U+2010), ‑ (U+2011), – (U+2013)
_TEL_PATTERN = re.compile(r"(?:TEL|Tel|tel)[\/：:\s]*([0-9０-９(（)）\-－‐‑–]{8,})")
_EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_URL_PATTERN = re.compile(r"https?://[^\s　「」【】]+")
# 事業所見出し（【○○本社】【○○支社】等）の行頭記号
_OFFICE_HEADER = re.compile(r"^[■▶▼●◆□▷▽○◇★☆・]|^【")

# 雇用形態として扱う既知ラベル（タグ list から抽出）
_EMPLOYMENT_LABELS = {
    "正社員", "契約社員", "業務委託", "派遣社員", "派遣", "嘱託社員", "嘱託",
    "アルバイト", "パート", "アルバイト・パート", "紹介予定派遣", "その他",
}

# 一覧ページの求人詳細リンクから求人 ID を取り出す（?b1=... 等のクエリ付きでも拾う）
_RECRUIT_ID_PATTERN = re.compile(r"/company/recruit/(\d+)/")


class Re2Scraper(StaticCrawler):
    """Re就活（やり直し版） スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["事業所", "職種", "雇用形態", "掲載終了予定日", "最終更新日"]

    # 検索フォームで勤務地を全選択した状態の p3（全勤務地コード）。
    # これを指定すると全国の求人が該当し、約 1,094 件が列挙できる。
    ALL_LOCATIONS = (
        "1-0001-2-3-4-0002-5-6-7-8-9-10-11-0014-12-0013-13-0003-0004-0005-0006-0007-"
        "0008-0009-14-0010-0011-0012-15-16-17-18-19-20-21-22-0016-0017-23-0015-24-25-"
        "26-0020-27-0018-0019-28-0021-29-30-31-32-33-0023-34-0022-35-36-37-38-39-40-"
        "0024-0025-41-42-43-0026-44-45-46-47-89-90-99"
    )
    # ページ送りの暴走防止上限（1 ページ約 30 件 → 1,094 件で 40 ページ弱。十分な余裕を持たせる）
    MAX_PAGES = 200

    def parse(self, url: str):
        # 検索結果 URL を引数 url（SSOT = sites.yml の url）から派生させる
        root = url if url.endswith("/") else url + "/"
        search_base = urljoin(root, "search/sch_result")

        # 検索結果ページを p0=1,2,... と送って詳細 URL を収集（求人 ID で重複排除）
        seen_ids: set[str] = set()
        locs: list[str] = []
        for page in range(1, self.MAX_PAGES + 1):
            list_url = (
                f"{search_base}?p0={page}&p3={self.ALL_LOCATIONS}&search_from=top"
            )
            soup = self.get_soup(list_url)
            if not soup:
                self.logger.warning("一覧ページを取得できませんでした: %s", list_url)
                break

            new_count = 0
            for a in soup.select("a[href*='/company/recruit/']"):
                m = _RECRUIT_ID_PATTERN.search(a.get("href", ""))
                if not m:
                    continue
                rid = m.group(1)
                if rid in seen_ids:
                    continue
                seen_ids.add(rid)
                locs.append(urljoin(root, f"company/recruit/{rid}/"))
                new_count += 1

            self.logger.info(
                "p0=%d: 新規 %d 件（累計 %d）", page, new_count, len(locs)
            )
            # 新規リンクが無い＝最終ページを超えたとみなして打ち切る
            if new_count == 0:
                break

        if not locs:
            self.logger.warning("詳細 URL を 1 件も取得できませんでした: %s", search_base)
            return
        self.total_items = len(locs)
        self.logger.info("詳細 URL 件数（重複排除後）: %d", len(locs))

        for detail_url in locs:
            try:
                record = self._scrape_detail(detail_url)
                if record:
                    yield record
            except Exception as e:
                self.logger.warning("detail error [%s]: %s", detail_url, e)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if not soup:
            return None

        def txt(suffix: str) -> str:
            """ASP.NET の id プレフィックスを後方一致で吸収して取得する。"""
            el = soup.select_one(f"span[id$='_{suffix}']")
            return el.get_text(" ", strip=True) if el else ""

        # 会社名（末尾の【東証プライム上場】等の注記は除去）
        company_name = txt("lblCompanyName") or txt("lblFixCompanyName")
        if not company_name:
            return None
        name = re.sub(r"\s*【[^】]*】\s*$", "", company_name).strip() or company_name

        # 本社所在地 — 最初の事業所ブロック（【本社】見出し直後）のみ採用
        addr, postal = self._parse_address(soup)

        # 連絡先テキストから TEL / MAIL を抽出
        contact_raw = txt("lblContactInfo")
        tel = ""
        m = _TEL_PATTERN.search(contact_raw)
        if m:
            tel = m.group(1).strip()
        else:
            m2 = re.search(r"(0\d{1,4}[\-－‐‑–]\d{1,4}[\-－‐‑–]\d{3,4})", contact_raw)
            if m2:
                tel = m2.group(1)
        em = _EMAIL_PATTERN.search(contact_raw)
        email = em.group(0) if em else ""

        # 企業ホームページ URL（連絡先・事業内容のテキストに直書きされていれば拾う）
        homepage = ""
        for raw in (contact_raw, txt("lblBusinessContents")):
            um = _URL_PATTERN.search(raw)
            if um:
                homepage = um.group(0).rstrip("。、）)")
                break

        # 雇用形態タグ（重複表示があるため順序保持で一意化）
        employment_seen: list[str] = []
        for li in soup.select(".tag-list li"):
            label = li.get_text(strip=True)
            if label in _EMPLOYMENT_LABELS and label not in employment_seen:
                employment_seen.append(label)
        employment = "、".join(employment_seen)

        # 職種 — 業種アイコン直下の職種アイコン優先、なければ募集職種テキスト
        job_type = txt("lblServIcon") or txt("lblWantedJobType")

        return {
            Schema.NAME: name,
            Schema.URL: url,
            Schema.POST_CODE: postal,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.EMAIL: email,
            Schema.HP: homepage,
            Schema.REP_NM: txt("lblRepresentative"),
            Schema.EMP_NUM: txt("lblEmployeesCount"),
            Schema.CAP: txt("lblCapital"),
            Schema.SALES: txt("lblAmountSales"),
            Schema.CAT_SITE: txt("lblIndustryIcon"),
            Schema.OPEN_DATE: txt("lblEstablishment"),
            Schema.LOB: txt("lblBusinessContents"),
            "事業所": txt("lblOfficePoint"),
            "職種": job_type,
            "雇用形態": employment,
            "掲載終了予定日": txt("lblPublishedLastdate"),
            "最終更新日": txt("lblLastUpdate"),
        }

    @staticmethod
    def _parse_address(soup) -> tuple[str, str]:
        """本社所在地から「最初の事業所ブロック」の住所と郵便番号を切り出す。"""
        el = soup.select_one("span[id$='_lblHeadofficelocation']")
        if not el:
            return "", ""
        lines = [l.strip() for l in el.get_text("\n", strip=True).splitlines() if l.strip()]

        block: list[str] = []
        started = False
        for l in lines:
            if _OFFICE_HEADER.match(l):
                if started:  # 2 つ目の事業所見出しに到達したら打ち切り
                    break
                continue
            block.append(l)
            started = True
        raw = " ".join(block) if block else " ".join(lines)

        postal = ""
        addr = raw
        pm = _POSTAL_PATTERN.search(raw)
        if pm:
            postal = pm.group(1)
            addr = (raw[:pm.start()] + raw[pm.end():]).strip()
        addr = " ".join(addr.split())  # 全角空白・連続空白を正規化
        return addr, postal


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Re2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://re-katsu.jp/career/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
