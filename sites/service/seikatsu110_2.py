"""
生活110番 (seikatsu110.jp) — 全国・全カテゴリ加盟店(サービス詳細)スクレイパー

取得対象:
    - /category/ に列挙される全小分類 (/service/{大}/{小}/) 約72件
    - 各小分類の47都道府県エリア一覧 (/service/{大}/{小}/area/{県}/) を ?page= で全件巡回
    - 一覧カードのリンク先 サービス詳細ページ (/service/{大}/{小}/{数字ID}/) を1件ずつ取得
    - 1行 = 1加盟店(詳細ページ1件)。同一加盟店が複数小分類に別URLで出るのは仕様として残す
      (完全に同一URLの重複取得だけは避ける)

取得フロー (Pattern B: 詳細1件取得ごとに即 yield):
    1. url から /category/ を導出し、全小分類URL・小分類名を列挙
    2. 各小分類トップから47都道府県のエリアURLを列挙
    3. 各エリアを ?page=1,2,... と辿り、詳細リンク(数字ID)を抽出
    4. 各詳細ページを取得し H1(名称)/ul.c-table(住所/TEL/HP等)/対応サービス/対応エリア を抽出して即 yield

実行方法:
    python scripts/sites/service/seikatsu110_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id seikatsu110_2
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


_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 小分類URL: /service/{大}/{小}/  (末尾に area/ や数字IDが付かないもの)
_SUBCAT_RE = re.compile(r"/service/([^/]+)/([^/]+)/$")
# 空値プレースホルダ (サイトが値なしを "ー" 等で表示する)
_EMPTY_TOKENS = {"", "ー", "-", "―", "−", "なし"}
# ページネーション安全上限 (エリア当たり。1ページ15件なので通常十分)
_MAX_PAGE = 500


class Seikatsu110ServiceScraper(StaticCrawler):
    """生活110番 (seikatsu110.jp) 全国・全カテゴリ 加盟店スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "対応サービス",
        "対応エリア",
        "受付センター番号",
        "営業所",
    ]

    def parse(self, url: str):
        # ルート url (= sites.yml の url) を唯一の起点に /category/ を導出する
        category_url = urljoin(url, "/category/")
        self.logger.info("カテゴリ一覧取得: %s", category_url)
        cat_soup = self.get_soup(category_url)
        if not cat_soup:
            self.logger.error("カテゴリ一覧を取得できませんでした: %s", category_url)
            return

        # 小分類URL → 小分類名 (カテゴリ) を順序保持で列挙
        subcats: list[tuple[str, str]] = []
        seen_subcat: set[str] = set()
        for a in cat_soup.select("a[href]"):
            href = a.get("href", "")
            m = _SUBCAT_RE.search(href)
            if not m:
                continue
            sub_url = urljoin(category_url, href)
            if sub_url in seen_subcat:
                continue
            seen_subcat.add(sub_url)
            subcats.append((sub_url, a.get_text(strip=True)))
        self.logger.info("小分類数: %d", len(subcats))

        seen_detail: set[str] = set()  # 完全同一URLの重複取得防止 (全小分類横断)

        for sub_url, cat_name in subcats:
            m = _SUBCAT_RE.search(sub_url)
            if not m:
                continue
            genre, subcode = m.group(1), m.group(2)
            # この小分類の詳細ページ判定用 (数字ID)
            detail_re = re.compile(
                r"/service/%s/%s/(\d+)/" % (re.escape(genre), re.escape(subcode))
            )

            # 小分類トップから47都道府県エリアURLを列挙 (順序保持)
            area_urls = self._collect_area_urls(sub_url, genre, subcode)
            self.logger.info(
                "小分類 %s (%s): エリア %d 件", cat_name, subcode, len(area_urls)
            )

            for area_url in area_urls:
                yield from self._crawl_area(
                    url, area_url, detail_re, cat_name, seen_detail
                )

    def _collect_area_urls(self, sub_url: str, genre: str, subcode: str) -> list[str]:
        """小分類トップから /service/{大}/{小}/area/{県}/ を順序保持で列挙する。"""
        soup = self.get_soup(sub_url)
        if not soup:
            return []
        area_re = re.compile(
            r"/service/%s/%s/area/[a-z_]+/$" % (re.escape(genre), re.escape(subcode))
        )
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if area_re.search(href):
                abs_url = urljoin(sub_url, href)
                if abs_url not in seen:
                    seen.add(abs_url)
                    urls.append(abs_url)
        return urls

    def _crawl_area(self, root_url, area_url, detail_re, cat_name, seen_detail):
        """1エリアを ?page= で最終ページまで辿り、詳細を1件ずつ yield する。"""
        area_seen: set[str] = set()
        for page in range(1, _MAX_PAGE + 1):
            page_url = area_url if page == 1 else f"{area_url}?page={page}"
            soup = self.get_soup(page_url)
            if not soup:
                break

            # このページの詳細リンク (数字ID) を順序保持で抽出
            page_details: list[str] = []
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if detail_re.search(href):
                    abs_url = urljoin(root_url, href)
                    if abs_url not in page_details:
                        page_details.append(abs_url)

            # 新規リンクが1件も無ければ最終ページ (範囲外ページは詳細0件)
            new_details = [d for d in page_details if d not in area_seen]
            if not new_details:
                break

            for detail_url in new_details:
                area_seen.add(detail_url)
                if detail_url in seen_detail:
                    continue  # 別小分類で取得済みの完全同一URLはスキップ
                seen_detail.add(detail_url)
                try:
                    record = self._scrape_detail(detail_url, cat_name)
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue
                if record:
                    self.total_items = len(seen_detail)
                    yield record

    def _scrape_detail(self, detail_url: str, cat_name: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if not soup:
            return None

        item: dict = {
            Schema.URL: detail_url,
            Schema.CAT_SITE: cat_name,
        }

        # --- 名称: H1 直下テキスト (末尾のサービス名 span を除外) ---
        h1 = soup.find("h1")
        if h1:
            own = "".join(t for t in h1.find_all(string=True, recursive=False)).strip()
            item[Schema.NAME] = own or h1.get_text(strip=True)
            # カテゴリが /category/ から取れなかった場合は H1 span で補完
            if not item.get(Schema.CAT_SITE):
                sp = h1.select_one("span")
                if sp:
                    item[Schema.CAT_SITE] = sp.get_text(strip=True)
        if not item.get(Schema.NAME):
            return None

        # --- 基本情報 (ul.c-table の label/value) ---
        for li in soup.select("ul.c-table li.c-table__list"):
            ttl = li.select_one("p.c-table__ttl")
            txt = li.select_one("p.c-table__txt")
            if not ttl or not txt:
                continue
            label = ttl.get_text(strip=True)
            a = txt.find("a", href=True)
            value = (
                a["href"].strip()
                if (label == "URL" and a)
                else txt.get_text(" ", strip=True)
            ).strip()
            if value in _EMPTY_TOKENS:
                continue

            if label == "住所":
                item[Schema.ADDR] = value
                pm = _PREF_PATTERN.match(value)
                if pm:
                    item[Schema.PREF] = pm.group(1)
            elif label == "電話番号":
                item[Schema.TEL] = value
            elif label == "資本金":
                item[Schema.CAP] = value
            elif label == "従業員数":
                item[Schema.EMP_NUM] = value
            elif label == "営業時間":
                item[Schema.TIME] = value
            elif label == "営業所":
                item["営業所"] = value
            elif label == "定休日":
                item[Schema.HOLIDAY] = value
            elif label == "URL":
                item[Schema.HP] = value
            elif label == "設立年":
                item[Schema.OPEN_DATE] = value

        # --- 受付センター番号 (0120 等、加盟店実番号とは別カラム) ---
        center = soup.select_one("span.c-tel__num")
        if center:
            item["受付センター番号"] = center.get_text(strip=True)

        # --- 対応サービス一覧 (対応サービス節内の h3 名称) ---
        svc_sec = self._section_by_heading(soup, "対応サービス")
        if svc_sec:
            names: list[str] = []
            for h3 in svc_sec.select("h3"):
                name = h3.get_text(strip=True)
                if name and "対応サービス" not in name and name not in names:
                    names.append(name)
            if names:
                item["対応サービス"] = "、".join(names)

        # --- 対応エリア (対応エリア節内のリンク名称) ---
        area_sec = self._section_by_heading(soup, "対応エリア")
        if area_sec:
            areas: list[str] = []
            for a in area_sec.select("a[href]"):
                name = a.get_text(strip=True)
                if name and name not in areas:
                    areas.append(name)
            if areas:
                item["対応エリア"] = "、".join(areas)

        return item

    @staticmethod
    def _section_by_heading(soup, keyword: str):
        """見出し (h2/h3) のテキストで節を特定する (クラス名変更に強い)。"""
        for hd in soup.select("h2, h3"):
            if keyword in hd.get_text():
                return hd.find_parent("section") or hd.parent
        return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Seikatsu110ServiceScraper()
    # 🔒 sites.yml に登録する url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://www.seikatsu110.jp/service/garden/gd_felling/area/tokyo/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
