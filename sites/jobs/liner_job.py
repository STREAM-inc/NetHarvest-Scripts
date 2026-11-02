"""
ライナージョブ (liner-job.net) — 求人・企業情報スクレイパー

取得対象:
    旭川・道北のフリーペーパー「ライナー」の求人サイト。掲載中の全求人詳細から
    求人タイトル・雇用形態・勤務エリア・職種・給与・掲載期間・応募電話番号を取得し、
    企業ページ (企業名・所在地) と紐付けて出力する。

取得フロー:
    1. post-sitemap.xml で掲載中の求人詳細URL (/{YYYYMMDD}{post_id}/) を列挙する。
    2. company-sitemap.xml の企業ページ (/company/{id}/) を順に取得し、
       ページ内「求人一覧」のリンクから求人詳細を辿って 1 件ずつ即 yield する。
       (求人詳細側には企業ページへのリンクが無いため、企業→求人の向きで紐付ける)
    3. どの企業ページからも辿れなかった求人は、最後に求人詳細単体で出力する
       (企業名/所在地は求人詳細内の会社データ欄、無ければ見出しで補完)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/liner_job.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id liner_job
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

import warnings

import bs4
from bs4 import XMLParsedAsHTMLWarning

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# sitemap.xml を get_soup (html.parser) で読むための警告抑止
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# 求人詳細URLのパス (例: /202611022668827/) — 公開日8桁 + 投稿ID
_JOB_PATH_RE = re.compile(r"^/(\d{9,})/?$")
_COMPANY_PATH_RE = re.compile(r"^/company/\d+/?$")

_POST_RE = re.compile(r"〒?\s*(\d{3})[-－ー‐](\d{4})")
# 設立 (例: "1984年（昭和59年）6月1日") → YYYY-MM-DD
_DATE_RE = re.compile(r"(\d{4})年.*?(\d{1,2})月(\d{1,2})日")
_PREF_RE = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|"
    r"富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|"
    r"島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|"
    r"鹿児島|沖縄)県)"
)

# 「募集情報」欄の給与ラベル (求人により「給与」「収入」「時給」等で揺れる)
_SALARY_LABELS = ("給与", "収入", "給料", "時給", "日給", "月給", "報酬")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _sitemap_locs(soup: bs4.BeautifulSoup | None) -> list[str]:
    if soup is None:
        return []
    return [_clean(loc.get_text()) for loc in soup.find_all("loc")]


def _job_key(href: str) -> str | None:
    """求人詳細URLから投稿キー (パスの数字部) を返す。?jobclip 等のクエリは無視。"""
    m = _JOB_PATH_RE.match(urlparse(href).path)
    return m.group(1) if m else None


def _table_rows(soup: bs4.BeautifulSoup) -> list[tuple[str, str]]:
    """s-job-article__table の th/td をラベル順に返す (同名ラベルの重複も保持)。"""
    rows = []
    for tr in soup.select("table.s-job-article__table tr"):
        th, td = tr.find("th"), tr.find("td")
        if th is None or td is None:
            continue
        rows.append((_clean(th.get_text()), td))
    return rows


def _company_data(soup: bs4.BeautifulSoup) -> dict:
    """会社データ欄 (企業名/所在地/代表者等) を抽出する。企業ページ・求人詳細の共通処理。"""
    data = {}
    # 会社データ欄は「企業名」行から始まる。それ以前 (募集情報欄) の URL は応募フォーム等
    in_company = False
    for label, td in _table_rows(soup):
        value = _clean(td.get_text(" "))
        if label == "企業名":
            in_company = True
        if label == "企業名" and "name" not in data:
            data["name"] = value
        elif label == "所在地" and "addr" not in data:
            data["addr"] = value
        elif label == "代表者" and "rep" not in data:
            data["rep"] = value
        elif label == "資本金" and "cap" not in data:
            data["cap"] = value
        elif label == "従業員数" and "emp" not in data:
            data["emp"] = value
        elif label == "設立" and "est" not in data:
            m = _DATE_RE.search(value)
            data["est"] = (
                f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}" if m else value
            )
        elif label in ("URL", "HP", "ホームページ") and in_company and "hp" not in data:
            a = td.find("a", href=True)
            data["hp"] = a["href"] if a else value
    return data


def _split_address(addr: str) -> tuple[str, str, str]:
    """所在地文字列 → (郵便番号, 都道府県, 住所)。"""
    post = ""
    m = _POST_RE.search(addr)
    if m:
        post = f"{m.group(1)}-{m.group(2)}"
        addr = _clean(addr[: m.start()] + addr[m.end():])
    addr = addr.lstrip("〒").strip()
    pref = ""
    m = _PREF_RE.match(addr)
    if m:
        pref = m.group(1)
        addr = addr[m.end():].strip()
    elif addr:
        # ライナーは旭川・道北の地域媒体で、所在地は「旭川市…」と市町村から始まる
        # 表記が大半。都道府県表記が無い場合は北海道とみなす。
        pref = "北海道"
    return post, pref, addr


class LinerJobScraper(StaticCrawler):
    """ライナージョブ スクレイパー (企業ページ × 求人詳細)"""

    DELAY = 1.5  # 備考指定: 1〜2秒間隔
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "勤務エリア",
        "職種",
        "給与",
        "掲載期間",
        "求人詳細URL",
        "企業ページURL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url (サイトルート) を唯一の起点として sitemap を派生させる
        job_urls: dict[str, str] = {}
        for loc in _sitemap_locs(self.get_soup(urljoin(url, "/post-sitemap.xml"))):
            key = _job_key(loc)
            if key:
                job_urls.setdefault(key, loc)
        self.logger.info("掲載中の求人: %d 件", len(job_urls))

        company_urls = [
            loc
            for loc in _sitemap_locs(self.get_soup(urljoin(url, "/company-sitemap.xml")))
            if _COMPANY_PATH_RE.match(urlparse(loc).path)
        ]
        self.logger.info("企業ページ: %d 件", len(company_urls))

        done: set[str] = set()
        for company_url in company_urls:
            if job_urls and len(done) >= len(job_urls):
                break  # 掲載中求人をすべて紐付け済み
            soup = self.get_soup(company_url)
            if soup is None:
                continue
            company = _company_data(soup)
            if not company.get("name"):
                h1 = soup.select_one("h1")
                company["name"] = _clean(h1.get_text()) if h1 else ""

            for a in soup.select("div.s-company-jobs a.p-list-article__link[href]"):
                key = _job_key(a["href"])
                if not key or key in done:
                    continue
                # sitemap に無い求人 (掲載終了直後など) もリンクがあれば取得する
                job_url = job_urls.get(key) or urljoin(company_url, f"/{key}/")
                done.add(key)
                item = self._scrape_job(job_url, company, company_url)
                if item:
                    yield item

        # どの企業ページからも辿れなかった求人は求人詳細単体で出力する
        for key, job_url in job_urls.items():
            if key in done:
                continue
            done.add(key)
            item = self._scrape_job(job_url, {}, "")
            if item:
                yield item

    def _scrape_job(self, job_url: str, company: dict, company_url: str) -> dict | None:
        soup = self.get_soup(job_url)
        if soup is None:
            return None

        # 求人詳細内の会社データ欄 (一部求人のみ) で企業ページ側の欠落を補う
        job_company = _company_data(soup)
        merged = {**job_company, **{k: v for k, v in company.items() if v}}
        name = merged.get("name", "")
        if not name:
            h1 = soup.select_one("h1")
            name = _clean(h1.get_text()) if h1 else ""

        post, pref, addr = _split_address(merged.get("addr", ""))

        # 求人タイトル: 職種欄 (td.job) の本文。雇用形態アイコン div は除外
        title = ""
        td_job = soup.select_one("td.job")
        if td_job is not None:
            title = _clean("".join(t for t in td_job.find_all(string=True, recursive=False)))
            if not title:
                title = _clean(td_job.get_text(" "))

        salary = ""
        for label, td in _table_rows(soup):
            if label in _SALARY_LABELS:
                salary = _clean(td.get_text(" "))
                break

        period = ""
        for p in soup.select("p.s-job-article__date"):
            text = _clean(p.get_text())
            if text.startswith("掲載期間"):
                period = re.sub(r"^掲載期間[：:]\s*", "", text)
                break

        relations = {}
        for dt in soup.select("section.s-job-article__relations dt"):
            dd = dt.find_next_sibling("dd")
            if dd is not None:
                values = [_clean(a.get_text()) for a in dd.find_all("a")]
                relations[_clean(dt.get_text())] = "/".join(v for v in values if v)

        tel = ""
        tel_a = soup.select_one("a.s-job-article__entry-telno")
        if tel_a is not None:
            tel = _clean(tel_a.get("href", "").replace("tel:", "")) or _clean(tel_a.get_text())

        return {
            Schema.URL: job_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: merged.get("rep", ""),
            Schema.CAP: merged.get("cap", ""),
            Schema.EMP_NUM: merged.get("emp", ""),
            Schema.OPEN_DATE: merged.get("est", ""),
            Schema.HP: merged.get("hp", ""),
            Schema.CAT_SITE: relations.get("職種", ""),
            "求人タイトル": title,
            "雇用形態": relations.get("雇用形態", ""),
            "勤務エリア": relations.get("勤務エリア", ""),
            "職種": relations.get("職種", ""),
            "給与": salary,
            "掲載期間": period,
            "求人詳細URL": job_url,
            "企業ページURL": company_url,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = LinerJobScraper()
    # 🔒 sites.yml に登録する url と完全一致させる (SSOT = sites.yml)。
    scraper.execute("https://liner-job.net/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
