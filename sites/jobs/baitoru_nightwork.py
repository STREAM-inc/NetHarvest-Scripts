"""
バイトル ナイトワーク系 — ナイトワーク求人一覧スクレイパー

取得対象:
    - 10カテゴリ × 7地域 の求人一覧ページ
      ガールズバー・キャバクラ・スナック、キャバクラ・クラブ、
      店長・マネージャー候補、フロアレディ・カウンターレディ、
      クラブ・スナック系ホールスタッフ、コスチューム系その他、
      ガールズバー、スナック・パブ・ラウンジ、
      ドライバー・ヘアメイクその他、ラブホテル・ブティックホテル

取得フロー:
    カテゴリ × 地域 の一覧URL → /{region}/jlist/{category}/page{N}/ でページネーション
    一覧ページから店名・勤務地・職種（サイト内業種）・給与・勤務時間を取得

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/baitoru_nightwork.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id baitoru_nightwork
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.baitoru.com"

# jlistパス → カテゴリ表示名
CATEGORIES = {
    "nightwork":          "ガールズバー・キャバクラ・スナック",
    "cabaretclub":        "キャバクラ・クラブ",
    "nightworkleader":    "店長・マネージャー候補(ナイトワーク系)",
    "counterready":       "フロアレディ・カウンターレディ(ナイトワーク系)",
    "nightworkhallstaff": "クラブ・スナック系ホールスタッフ(ナイトワーク系)",
    "costume":            "コスチューム系その他(ナイトワーク系)",
    "bargirls":           "ガールズバー",
    "snackbar":           "スナック・パブ・ラウンジ",
    "nightworkdriver":    "ドライバー・ヘアメイクその他(ナイトワーク系)",
    "lovehotel":          "ラブホテル・ブティックホテル(ナイトワーク系)",
}

REGIONS = [
    "kanto", "kansai", "tokai", "tohoku",
    "kyushu", "koshinetsu", "chushikoku",
]

_PREF_RE = re.compile(
    r"(北海道|(?:東京|大阪|京都)(?:都|府)|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川"
    r"|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良"
    r"|和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀"
    r"|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _strip_cat_prefix(text: str) -> str:
    """[ア・パ]①②③ 等の先頭ノイズを除去する"""
    text = re.sub(r"^\[[\w・\s/ア-ン]+\]\s*", "", text)
    text = re.sub(r"^[①②③④⑤⑥⑦⑧⑨⑩]+\s*", "", text)
    return text.strip()


class BaitoruNightworkScraper(StaticCrawler):
    """バイトル ナイトワーク系 求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["求人タイトル", "給与", "特徴タグ", "検索カテゴリ", "地域"]

    def parse(self, url: str):
        for cat_path, cat_name in CATEGORIES.items():
            for region in REGIONS:
                yield from self._crawl_category(cat_path, cat_name, region)

    def _crawl_category(self, cat_path: str, cat_name: str, region: str):
        page = 1
        while True:
            if page == 1:
                list_url = f"{BASE_URL}/{region}/jlist/{cat_path}/"
            else:
                list_url = f"{BASE_URL}/{region}/jlist/{cat_path}/page{page}/"

            try:
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning("一覧取得失敗: %s (%s)", list_url, e)
                break

            articles = soup.select("article")
            if not articles:
                break

            for article in articles:
                try:
                    item = self._parse_article(article, cat_name, region)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("記事解析失敗: %s / %s p%d (%s)", cat_path, region, page, e)
                    continue

            # 「次へ」ボタンの redirectlink 属性で次ページ有無を判定
            next_btn = soup.find("a", string="次へ")
            if not next_btn or not next_btn.get("redirectlink"):
                break
            page += 1

    def _parse_article(self, article, cat_name: str, region: str) -> dict | None:
        # 店名
        name_el = article.select_one(".pt02b > p")
        name = name_el.get_text(strip=True) if name_el else None
        if not name:
            return None

        # 求人URL
        a_tag = article.select_one("h3 a[href]")
        if not a_tag:
            return None
        href = a_tag.get("href", "")
        job_url = href if href.startswith("http") else BASE_URL + href

        # 求人タイトル
        span = a_tag.select_one("span")
        job_title = span.get_text(strip=True) if span else ""

        # 勤務地 → 都道府県・住所を抽出
        pref, addr = "", ""
        loc_el = article.select_one(".pt02b .ul02 li")
        if loc_el:
            loc_text = _clean(loc_el.get_text())
            loc_text = re.sub(r"^\[?勤務地[・/面接地]*\]?\s*", "", loc_text)
            loc_text = loc_text.split("⁄")[0].strip()
            m = _PREF_RE.search(loc_text)
            if m:
                pref = m.group(1)
                addr = loc_text[m.end():].strip()
            else:
                addr = loc_text

        # 職種・給与・勤務時間（.pt03 内の dl を順番に取得）
        pt03_dls = article.select(".pt03 dl")
        cat_site, salary, work_time = "", "", ""

        if len(pt03_dls) >= 1:
            dd = pt03_dls[0].select_one("dd")
            if dd:
                cat_site = _strip_cat_prefix(_clean(dd.get_text()))

        if len(pt03_dls) >= 2:
            em = pt03_dls[1].select_one("dd em")
            if em:
                salary = em.get_text(strip=True)
            else:
                dd = pt03_dls[1].select_one("dd")
                salary = _clean(dd.get_text()) if dd else ""

        if len(pt03_dls) >= 3:
            dd = pt03_dls[2].select_one("dd")
            if dd:
                work_time = _strip_cat_prefix(_clean(dd.get_text()))

        # 特徴タグ（.pt04 ul li em）
        tags = [em.get_text(strip=True) for em in article.select(".pt04 ul li em") if em.get_text(strip=True)]

        return {
            Schema.NAME:     name,
            Schema.URL:      job_url,
            Schema.PREF:     pref,
            Schema.ADDR:     addr,
            Schema.CAT_SITE: cat_site,
            Schema.TIME:     work_time,
            "求人タイトル":   job_title,
            "給与":           salary,
            "特徴タグ":       "／".join(tags),
            "検索カテゴリ":   cat_name,
            "地域":           region,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BaitoruNightworkScraper()
    # テスト: kanto の nightwork カテゴリ 1ページのみ
    scraper.execute(BASE_URL + "/kanto/jlist/nightwork/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
