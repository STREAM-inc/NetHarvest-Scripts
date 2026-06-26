"""
求人君 (週刊求人情報誌 求人君) — 求人企業情報スクレイパー (q-jinkun.com)

取得対象:
    - 検索結果一覧 (?mode=ds) に掲載された各求人の掲載企業情報・条件
      (会社名 / 会社住所 / 電話番号 / ホームページ / 事業内容 /
       カテゴリ / 勤務時間 / 休日 などの構造化フィールド)

取得フロー:
    1. 一覧ページ (?mode=ds) から /job/{id}.html への詳細リンクを収集
    2. ページネーション (/page/N/?mode=ds) を最終ページまで巡回
    3. 詳細ページごとに JSON-LD (JobPosting) と
       section.single_job_container 内の job_table_row を解析し即 yield

    ※ 求人内容の自由記述 (内容 / Web応募の定型文 / JSON-LD description) は
      著作権リスク回避のため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/q_jinkun.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id q_jinkun
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POSTCODE_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_PATTERN = re.compile(r"TEL[：:]\s*([\d\-\(\)]+)")
_JOB_HREF_PATTERN = re.compile(r"/job/\d+\.html")
_PAGE_PATTERN = re.compile(r"/page/(\d+)/")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[ \t]+", " ", str(s).replace("　", " ")).strip()


# 詳細ページの job_table_row ラベル → 出力先のマッピング。
# Schema 定数は構造化フィールドへ、文字列キーは EXTRA_COLUMNS へ。
# 「内容」「Web応募」は自由記述 / 定型文のため意図的に含めない。
_LABEL_MAP = {
    "会社名": Schema.NAME,
    "会社住所": Schema.ADDR,
    "会社事業内容": Schema.LOB,
    "ウェブサイト": Schema.HP,
    "カテゴリ": Schema.CAT_SITE,
    "勤務時間": Schema.TIME,
    "休日": Schema.HOLIDAY,
    "雇用形態": "雇用形態",
    "職種": "職種",
    "給与": "給与",
    "勤務地": "勤務地",
    "期間": "期間",
    "待遇": "待遇",
    "掲載期間": "掲載期間",
}


class QJinkunScraper(StaticCrawler):
    """週刊求人情報誌 求人君 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["雇用形態", "職種", "給与", "勤務地", "期間", "待遇", "掲載期間"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # ルートは引数 url (= sites.yml の url) を唯一の起点とする。
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        query = parsed.query  # 例: "mode=ds"

        seen: set[str] = set()
        last_page = 1
        page = 1
        while page <= last_page:
            page_url = url if page == 1 else f"{base}/page/{page}/?{query}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            if page == 1:
                # 総件数 (進捗表示用) と最終ページ番号を確定
                count_el = soup.select_one(".result_num")
                if count_el:
                    m = re.search(r"([\d,]+)", count_el.get_text())
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))
                last_page = self._detect_last_page(soup)

            detail_hrefs = self._collect_detail_links(soup, url)
            if not detail_hrefs:
                break

            for href in detail_hrefs:
                if href in seen:
                    continue
                seen.add(href)
                item = self._scrape_detail(href)
                if item and item.get(Schema.NAME):
                    yield item

            page += 1

    def _detect_last_page(self, soup) -> int:
        last = 1
        pager = soup.select_one(".page_nation")
        if pager:
            for a in pager.find_all("a", href=True):
                m = _PAGE_PATTERN.search(a["href"])
                if m:
                    last = max(last, int(m.group(1)))
        return last

    def _collect_detail_links(self, soup, root_url: str) -> list[str]:
        hrefs: list[str] = []
        seen: set[str] = set()
        for a in soup.select("article.result_articles_item a[href]"):
            href = a["href"]
            if not _JOB_HREF_PATTERN.search(href):
                continue
            full = urljoin(root_url, href)
            if full not in seen:
                seen.add(full)
                hrefs.append(full)
        return hrefs

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # --- JSON-LD (JobPosting) ---
        jsonld = self._parse_jsonld(soup)
        if jsonld:
            org = jsonld.get("hiringOrganization") or {}
            if isinstance(org, dict):
                name = _clean(org.get("name"))
                if name:
                    data[Schema.NAME] = name
                same = _clean(org.get("sameAs"))
                if same:
                    data[Schema.HP] = same
            loc = jsonld.get("jobLocation") or {}
            if isinstance(loc, dict):
                addr = loc.get("address") or {}
                if isinstance(addr, dict):
                    pc = _clean(addr.get("postalCode"))
                    if pc:
                        data[Schema.POST_CODE] = pc
                    region = _clean(addr.get("addressRegion"))
                    if region:
                        data[Schema.PREF] = region

        # --- 詳細テーブル (section.single_job_container 内のみ) ---
        container = soup.select_one("section.single_job_container") or soup
        for row in container.select("div.job_table_row"):
            left = row.select_one(".job_table_col_left")
            right = row.select_one(".job_table_col_right")
            if not left or not right:
                continue
            label = _clean(left.get_text())

            if label == "電話応募":
                m = _TEL_PATTERN.search(right.get_text(" ", strip=True))
                if m:
                    data[Schema.TEL] = _clean(m.group(1))
                continue

            target = _LABEL_MAP.get(label)
            if target is None:
                continue  # 内容 / Web応募 など除外対象

            if target == Schema.HP:
                a = right.find("a", href=True)
                value = a["href"] if a else _clean(right.get_text(" "))
            else:
                value = re.sub(r"\n\s*\n+", "\n", _clean(right.get_text("\n")))

            if value and not data.get(target):
                data[target] = value

        # --- 住所から都道府県・郵便番号を補完 ---
        addr = data.get(Schema.ADDR, "")
        if addr:
            if not data.get(Schema.PREF):
                pm = _PREF_PATTERN.search(addr)
                if pm:
                    data[Schema.PREF] = pm.group(1)
            if not data.get(Schema.POST_CODE):
                pcm = _POSTCODE_PATTERN.search(addr)
                if pcm:
                    data[Schema.POST_CODE] = pcm.group(1)

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _parse_jsonld(soup) -> dict | None:
        for s in soup.find_all("script", type="application/ld+json"):
            txt = s.string or s.get_text() or ""
            if not txt.strip():
                continue
            try:
                obj = json.loads(txt)
            except Exception:
                continue
            candidates = obj if isinstance(obj, list) else [obj]
            for o in candidates:
                if isinstance(o, dict) and o.get("@type") == "JobPosting":
                    return o
        return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = QJinkunScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.q-jinkun.com/?mode=ds")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
