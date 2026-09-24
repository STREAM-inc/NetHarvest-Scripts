"""
ロカジョブ (locajob.net) — 全国の求人情報クローラー

取得対象:
    - 雇用形態別アーカイブ (/employment/{正社員|パート|アルバイト|業務委託|契約社員|請負}/) に
      掲載されている求人。全国が対象でエリアフィルタは掛けない。
    - 企業名 / 勤務地 (郵便番号・住所) / アクセス / 職種 / 雇用形態 / 給与 /
      定休日 / 応募条件 / 勤務時間 / 休日・休暇 / 福利厚生 / 事業内容 / 募集人数

取得フロー:
    1. sites.yml の url (トップ) を起点に、雇用形態アーカイブ 6 種の URL を組み立てる
    2. 各アーカイブを /page/N/ で全ページ巡回 (範囲外ページは 404 → 打ち切り)
    3. 一覧カード (article.job-archive-card) から求人詳細 URL を収集
    4. 詳細ページ (/job_post/{slug}/) を 1 件取得するごとに即 yield (Pattern B)
       - 同一求人が複数の雇用形態に掲載されるため、詳細 URL で重複排除する

備考:
    - 「仕事内容」「企業メッセージ」「応募方法」は長文の自由記述プロースのため
      著作権リスクを避けて取得しない。
    - 事業内容 (会社概要欄のうち社名・郵便番号・住所を除く記述) と募集人数は
      掲載されている求人のみ値が入る (大半は空欄)。
    - 電話番号はサイト上に掲載されていない (応募はフォーム経由のため)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/locajob.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id locajob
"""

import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 雇用形態アーカイブ (備考で指定された 6 カテゴリ)。root url からの相対パス。
_EMPLOYMENT_PATHS = [
    "employment/seishain/",
    "employment/part/",
    "employment/baito/",
    "employment/gyomu-itaku/",
    "employment/keiyaku/",
    "employment/ukeoi/",
]

# 詳細ページのバッジに現れる雇用形態ラベル (地方・都道府県バッジと区別するため)
_EMPLOYMENT_LABELS = {
    "正社員", "契約社員", "パート", "アルバイト", "業務委託", "請負",
    "フルタイムパート", "常勤", "嘱託", "その他",
}

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_RECRUIT_NUM_RE = re.compile(r"募集人数[：:\s]*([^\n]{1,30})")
# 1 アーカイブあたりの安全上限 (無限ループ防止。実際は 404 で打ち切られる)
_MAX_PAGE = 100


def _clean(text: str) -> str:
    """空白・改行を 1 スペースに正規化する。"""
    return re.sub(r"\s+", " ", text or "").strip()


def _lines(el) -> list[str]:
    """<br> 区切りの要素を行リストにして返す。"""
    if el is None:
        return []
    raw = el.get_text("\n", strip=True)
    return [_clean(ln) for ln in raw.split("\n") if _clean(ln)]


class LocajobCrawler(StaticCrawler):
    """ロカジョブ (locajob.net) スクレイパー"""

    DELAY = 1.0
    ITEM_DELAY = 0
    EXTRA_COLUMNS = [
        "職種",
        "雇用形態",
        "給与情報",
        "勤務地へのアクセス",
        "応募条件",
        "勤務時間",
        "休日・休暇",
        "福利厚生",
        "募集人数",
    ]

    def parse(self, url: str):
        seen: set[str] = set()

        for emp_path in _EMPLOYMENT_PATHS:
            archive_url = urllib.parse.urljoin(url, emp_path)

            for page in range(1, _MAX_PAGE + 1):
                page_url = archive_url if page == 1 else f"{archive_url}page/{page}/"
                soup = self.get_soup(page_url)
                if soup is None:
                    # 範囲外ページは 404。そのアーカイブは巡回完了とみなす
                    break

                detail_urls = []
                for card in soup.select("article.job-archive-card"):
                    a = card.select_one("a.job-archive-card__thumb[href]") or \
                        card.select_one("h2.job-archive-card__title a[href]")
                    if not a:
                        continue
                    detail_urls.append(urllib.parse.urljoin(page_url, a["href"]))

                if not detail_urls:
                    break

                for detail_url in detail_urls:
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)
                    try:
                        item = self._scrape_detail(detail_url)
                    except Exception as e:
                        self.logger.warning(f"詳細取得失敗 {detail_url}: {e}")
                        continue
                    if item:
                        yield item

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        # ラベル → 行要素 (div.job-row) のマップを作る
        rows: dict[str, object] = {}
        for row in soup.select("div.job-row"):
            label_el = row.select_one(".job-row-label")
            value_el = row.select_one(".job-row-value")
            if not label_el or not value_el:
                continue
            label = _clean(label_el.get_text())
            if label and label not in rows:
                rows[label] = value_el

        def row_lines(label: str) -> list[str]:
            return _lines(rows.get(label))

        def row_text(label: str, sep: str = " / ") -> str:
            return sep.join(row_lines(label))

        item: dict[str, str] = {Schema.URL: detail_url}

        # --- 企業名 (会社概要欄の先頭行。無ければ見出し) ---
        company_lines = row_lines("会社概要")
        name = ""
        if company_lines and not _POST_CODE_RE.match(company_lines[0]):
            name = company_lines[0]
        if not name:
            h1 = soup.select_one("h1.u-job-main-title")
            name = _clean(h1.get_text()) if h1 else ""
        item[Schema.NAME] = name

        # --- 勤務地 (就業地。欠落時は会社概要でフォールバック) ---
        work_lines = row_lines("就業地") or company_lines
        post_code, pref, addr = self._split_address(work_lines)
        if not addr and work_lines is not company_lines:
            post_code2, pref2, addr2 = self._split_address(company_lines)
            post_code = post_code or post_code2
            pref = pref or pref2
            addr = addr or addr2
        item[Schema.POST_CODE] = post_code
        item[Schema.PREF] = pref
        item[Schema.ADDR] = addr

        # --- 事業内容 (会社概要のうち社名・郵便番号・住所を除いた記述) ---
        item[Schema.LOB] = self._extract_lob(company_lines, name)

        # --- 職種 ---
        occupation = soup.select_one(".occupation-name") or soup.select_one("h2.job-hero-title")
        job_type = _clean(occupation.get_text()) if occupation else ""
        item["職種"] = job_type
        item[Schema.CAT_SITE] = job_type

        # --- 雇用形態 (サイドバーのバッジから雇用形態ラベルのみ抽出) ---
        badges = [_clean(b.get_text()) for b in soup.select(".job-badge")]
        employments = [b for b in badges if b in _EMPLOYMENT_LABELS]
        item["雇用形態"] = " / ".join(dict.fromkeys(employments))

        # --- 給与情報 ---
        item["給与情報"] = self._extract_salary(soup)

        # --- アクセス ---
        item["勤務地へのアクセス"] = row_text("アクセス")

        # --- 定休日 / 休日・休暇 ---
        item[Schema.HOLIDAY] = row_text("休日")
        rest = []
        for label in ("年間休日", "有給休暇"):
            val = row_text(label)
            if val:
                rest.append(f"{label}: {val}")
        item["休日・休暇"] = " / ".join(rest)

        # --- 応募条件 ---
        conditions = []
        for label in ("応募資格", "必要経験", "必要な免許・資格", "PCスキル"):
            val = row_text(label)
            if val:
                conditions.append(f"{label}: {val}")
        item["応募条件"] = " / ".join(conditions)

        # --- 勤務時間 ---
        work_time = []
        for label in ("就業時間", "所定労働日数"):
            val = row_text(label)
            if val:
                work_time.append(val if label == "就業時間" else f"{label}: {val}")
        item["勤務時間"] = " / ".join(work_time)

        # --- 福利厚生 ---
        welfare = []
        for label in ("加入保険", "福利厚生・待遇", "通勤手当"):
            val = row_text(label)
            if val:
                welfare.append(f"{label}: {val}")
        item["福利厚生"] = " / ".join(welfare)

        # --- 募集人数 (掲載がある求人のみ) ---
        item["募集人数"] = self._extract_recruit_number(soup, rows)

        return item

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------
    @staticmethod
    def _split_address(lines: list[str]) -> tuple[str, str, str]:
        """行リストから (郵便番号, 都道府県, 市区町村以降の住所) を取り出す。"""
        post_code = ""
        addr_lines: list[str] = []
        for line in lines:
            m = _POST_CODE_RE.search(line)
            if m and not post_code:
                post_code = m.group(1)
                rest = _clean(_POST_CODE_RE.sub("", line, count=1))
                if rest:
                    addr_lines.append(rest)
                continue
            if _PREF_PATTERN.search(line) or addr_lines:
                addr_lines.append(line)

        address = " ".join(addr_lines).strip()
        pref = ""
        m = _PREF_PATTERN.search(address)
        if m:
            pref = m.group(1)
            # 都道府県以降を住所とする (Schema.ADDR は市区町村以降)
            address = address[m.end():].strip()
        return post_code, pref, address

    @staticmethod
    def _extract_lob(company_lines: list[str], name: str) -> str:
        """会社概要欄から社名・郵便番号・住所を除いた記述 (事業内容) を返す。"""
        extras = []
        for line in company_lines:
            if line == name:
                continue
            if _POST_CODE_RE.search(line) or _PREF_PATTERN.search(line):
                continue
            extras.append(line)
        return " / ".join(extras)

    @staticmethod
    def _extract_salary(soup) -> str:
        """給与 (主表示 + 内訳の基本給・手当) を短くまとめて返す。"""
        parts = []
        main = soup.select_one(".job-salary-main")
        if main:
            text = _clean(main.get_text())
            if text:
                parts.append(text)

        for block in soup.select(".job-sub-block"):
            title_el = block.select_one(".job-sub-block-title")
            title = _clean(title_el.get_text()) if title_el else ""
            if title != "内訳":
                continue
            for sub_row in block.select(".job-sub-row"):
                label_el = sub_row.select_one(".job-sub-label")
                value_el = sub_row.select_one(".job-sub-value")
                if not value_el:
                    continue
                label = _clean(label_el.get_text()) if label_el else ""
                value = " ".join(_lines(value_el))
                if not value:
                    continue
                parts.append(f"{label}: {value}" if label else value)

        for row in soup.select("div.job-row"):
            label_el = row.select_one(".job-row-label")
            value_el = row.select_one(".job-row-value")
            if not label_el or not value_el:
                continue
            if _clean(label_el.get_text()) == "昇給・賞与":
                value = " ".join(_lines(value_el))
                if value:
                    parts.append(f"昇給・賞与: {value}")
                break

        return " / ".join(parts)

    @staticmethod
    def _extract_recruit_number(soup, rows: dict) -> str:
        """募集人数 (掲載がある求人のみ) を返す。"""
        for label, value_el in rows.items():
            if "募集人数" in label or label == "人数":
                return " ".join(_lines(value_el))
        m = _RECRUIT_NUM_RE.search(soup.get_text("\n"))
        return _clean(m.group(1)) if m else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    scraper = LocajobCrawler()
    scraper.execute("https://locajob.net/")
