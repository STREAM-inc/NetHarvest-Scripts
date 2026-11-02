"""
みてネッと (mite-net.com) — 室蘭・登別・伊達圏フリーペーパー掲載事業者スクレイパー

対象サイト: https://mite-net.com/seikatu/

取得フロー:
    サイトは WordPress。投稿タイプごとに WP 標準サイトマップ (wp-sitemap.xml →
    wp-sitemap-posts-{type}-N.xml) があるため、引数 url から派生させたサイトマップ索引を
    取得し、事業者を掲載する投稿タイプの詳細 URL を列挙する。詳細を 1 件取得するごとに即 yield。

    対象投稿タイプ (事業者記事):
        seikatu  生活情報 (広告画像主体。本文に住所/TEL が無いものが多い)
        kyujin   求人情報 (勤務地 div.kinmu / 職種 li.syokusyu / 雇用形態 span)
        ajina    味な店   (店舗情報 table th/td)
        kikou    らーめん紀行 (店舗情報 table th/td)
        tanpost  むろタン (ジャンル/エリアタグ + 店舗情報 table)
        ouenpost 飲食店応援企画 (同上)
    除外 (非事業者記事): mttk(特集) / cook(レシピ) / baby / pet / time(映画上映) /
        seitetsu(健康コラム) / post / page。さらに記事タイトルが特集・プレゼント等のものも除外。

備考:
    - 都道府県は北海道固定 (地域フリーペーパー)。
    - 紹介文・メニュー・クーポン等の長文自由記述は著作権リスクのため取得しない。
    - 掲載日は time.published を優先し、無い投稿タイプ (tanpost/ouenpost) はサイトマップ lastmod。
"""

import re
import sys
import warnings
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import XMLParsedAsHTMLWarning

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# サイトマップ (XML) を get_soup (html.parser) で読むための警告抑止
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# 投稿タイプ → 掲載カテゴリ名 (この順に巡回。生活情報 = 引数 url のカテゴリを先頭に)
_TARGET_TYPES = {
    "seikatu": "生活情報",
    "kyujin": "求人情報",
    "ajina": "味な店",
    "kikou": "らーめん紀行",
    "tanpost": "むろタン",
    "ouenpost": "飲食店応援企画",
}

# 非事業者記事 (特集・プレゼント・占い等) をタイトルで除外
_EXCLUDE_TITLE = re.compile(r"特集|プレゼント|占い|レシピ|ビンゴ|スタンプラリー|抽選")

_SITEMAP_RE = re.compile(r"wp-sitemap-posts-([a-z0-9_]+)-\d+\.xml$")
_TEL_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")
_DASHES = str.maketrans({"ｰ": "-", "ー": "-", "－": "-", "‐": "-", "−": "-", "―": "-"})


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


class MiteNetScraper(StaticCrawler):
    """みてネッと 掲載事業者スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["掲載カテゴリ", "掲載URL", "掲載日", "勤務地", "職種", "雇用形態", "エリア"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        index = self.get_soup(urljoin(url, "/wp-sitemap.xml"))
        if index is None:
            return
        sitemaps: dict[str, list[str]] = {t: [] for t in _TARGET_TYPES}
        for loc in index.find_all("loc"):
            sm_url = loc.get_text(strip=True)
            m = _SITEMAP_RE.search(sm_url)
            if m and m.group(1) in sitemaps:
                sitemaps[m.group(1)].append(sm_url)

        seen: set[str] = set()
        for post_type, category in _TARGET_TYPES.items():
            for sm_url in sitemaps[post_type]:
                sm = self.get_soup(sm_url)
                if sm is None:
                    continue
                for node in sm.find_all("url"):
                    loc = node.find("loc")
                    if loc is None:
                        continue
                    detail_url = loc.get_text(strip=True)
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)
                    lastmod = node.find("lastmod")
                    item = self._parse_detail(
                        detail_url, category, lastmod.get_text(strip=True)[:10] if lastmod else ""
                    )
                    if item:
                        yield item

    def _parse_detail(self, url: str, category: str, lastmod: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None
        article = soup.select_one("article")
        if article is None:
            return None
        h1 = article.select_one("h1")
        name = _norm(h1.get_text()) if h1 else ""
        if not name or _EXCLUDE_TITLE.search(name):
            return None

        published = article.select_one("time.published") or article.select_one("time.entry-date")
        posted = published.get("datetime", "")[:10] if published else lastmod

        # 店舗情報テーブル (th ラベル → td 値)
        info: dict[str, str] = {}
        hp = ""
        fb = ""
        for th in article.select("th"):
            td = th.find_next_sibling("td")
            if td is None:
                continue
            label = _norm(th.get_text())
            if label and label not in info:
                info[label] = _norm(td.get_text(" "))
            link = td.find("a", href=re.compile(r"^https?://"))
            if link and label in ("HP", "ホームページ", "URL") and not hp:
                hp = link["href"]
            if link and label in ("FB", "Facebook") and not fb:
                fb = link["href"]
        if not hp:
            hp = info.get("HP") or info.get("ホームページ") or ""
            if hp and not hp.startswith("http"):
                hp = "http://" + hp if re.match(r"^[\w.-]+\.[a-z]{2,}", hp) else ""

        tel_raw = (info.get("電話") or info.get("TEL") or info.get("電話番号") or "").translate(_DASHES)
        tel_m = _TEL_RE.search(tel_raw)
        if not tel_m:
            tel_link = article.select_one('a[href^="tel:"]')
            tel_m = _TEL_RE.search(tel_link.get_text().translate(_DASHES)) if tel_link else None
        tel = tel_m.group(0) if tel_m else ""
        addr = (info.get("住所") or info.get("所在地") or "").translate(_DASHES)

        # むろタン / 飲食店応援企画: ジャンル・エリアのタグリスト
        genre = area = ""
        for li in article.select("ul.yoko-list li"):
            text = ", ".join(t.strip() for t in li.get_text().split(",") if t.strip())
            if li.select_one(".fa-map-marker"):
                area = text
            else:
                genre = text

        # 求人: 勤務地 / 職種 / 雇用形態
        work_area = jobs = emp = ""
        if article.select_one(".kinmu"):
            work_area = " ".join(s.get_text(strip=True) for s in article.select(".kinmu span"))
            job_list, emp_list = [], []
            for li in article.select("li.syokusyu"):
                head = li.find(string=True, recursive=False)
                if head and head.strip():
                    job_list.append(head.strip())
                for span in li.find_all("span"):
                    t = span.get_text(strip=True)
                    if t and t not in emp_list:
                        emp_list.append(t)
            jobs, emp = " / ".join(job_list), " / ".join(emp_list)

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: "北海道",
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.FB: fb,
            Schema.CAT_SITE: genre or category,
            Schema.TIME: info.get("時間") or info.get("営業時間") or "",
            Schema.HOLIDAY: info.get("定休日", ""),
            "掲載カテゴリ": category,
            "掲載URL": url,
            "掲載日": posted,
            "勤務地": work_area,
            "職種": jobs,
            "雇用形態": emp,
            "エリア": area,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MiteNetScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://mite-net.com/seikatu/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
