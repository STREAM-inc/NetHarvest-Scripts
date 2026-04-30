"""
KEPPLE（ケップル） — スタートアップ情報メディアの法人ページから企業情報を取得

取得対象:
    - https://kepple.co.jp/corporates/{法人番号}/page/1 の各企業ページ
    - 名称・代表者・住所・HP・設立年月日・事業概要・タグ
    - 加えて、その企業を発見した記事のタイトル（複数記事から発見されたら | 連結）

取得フロー:
    KEPPLE には法人ページの一覧が存在しないため、記事経由で法人番号を発見する 2 段クロール:
      1. /articles/page/{N} (N=1..136) を巡回し、各記事 URL を収集
      2. 各記事 /articles/{id} を取得し、本文中の <a href="/corporates/{法人番号}/page/1">
         を抽出。法人番号 → 記事タイトル群 の対応表を構築
      3. 収集した法人番号それぞれに対して /corporates/{法人番号}/page/1 を取得し、
         dl/dd から企業情報を抽出

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/kepple.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id kepple
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://kepple.co.jp"
ARTICLES_LIST_URL = f"{BASE_URL}/articles/page/{{page}}"
CORPORATE_URL = f"{BASE_URL}/corporates/{{co_num}}/page/1"

# /articles/page/N と /corporates/{法人番号}/page/N の URL から ID/番号を抽出する正規表現
_ARTICLE_ID_RE = re.compile(r"^/articles/([^/?#]+)$")
_CORPORATE_NUM_RE = re.compile(r"/corporates/(\d{13})")
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class KeppleScraper(StaticCrawler):
    """KEPPLE 法人ページスクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "事業概要",
        "タグ",
        "記事タイトル",
    ]

    def parse(self, url: str):
        # --- Phase A: 記事を巡回して法人番号と記事タイトルを収集 ---
        # corp_titles: {法人番号: [記事タイトル, ...]}
        corp_titles: dict[str, list[str]] = {}

        last_page = self._read_last_page_num()
        self.logger.info("記事一覧の総ページ数: %d", last_page)

        for page_num in range(1, last_page + 1):
            list_url = ARTICLES_LIST_URL.format(page=page_num)
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("記事一覧ページ取得失敗: %s", list_url)
                continue

            article_paths = self._extract_article_paths(soup)
            self.logger.info("[一覧 %d/%d] 記事 %d 件", page_num, last_page, len(article_paths))

            for article_path in article_paths:
                article_url = f"{BASE_URL}{article_path}"
                art_soup = self.get_soup(article_url)
                if art_soup is None:
                    continue

                title_el = art_soup.select_one("h1")
                title = title_el.get_text(strip=True) if title_el else ""

                co_nums = set(_CORPORATE_NUM_RE.findall(art_soup.decode()))
                for co_num in co_nums:
                    corp_titles.setdefault(co_num, []).append(title)

        self.total_items = len(corp_titles)
        self.logger.info(
            "Phase A 完了: 法人 %d 件を発見 (記事走査 %d ページ)",
            len(corp_titles), last_page,
        )

        # --- Phase B: 各法人ページから企業情報を取得 ---
        for i, (co_num, titles) in enumerate(corp_titles.items(), start=1):
            corp_url = CORPORATE_URL.format(co_num=co_num)
            soup = self.get_soup(corp_url)
            if soup is None:
                continue

            item = self._parse_corporate(soup, corp_url, co_num, titles)
            if item:
                yield item
            if i % 50 == 0:
                self.logger.info("[法人 %d/%d] 取得中", i, len(corp_titles))

    def _read_last_page_num(self) -> int:
        """記事一覧の最終ページ番号を取得する。取得失敗時は 1。"""
        soup = self.get_soup(ARTICLES_LIST_URL.format(page=1))
        if soup is None:
            return 1
        # ページネーションの button[data-page] のうち最大値が最終ページ
        nums = []
        for btn in soup.select("button[data-page]"):
            try:
                nums.append(int(btn.get("data-page", "0")))
            except ValueError:
                continue
        return max(nums) if nums else 1

    @staticmethod
    def _extract_article_paths(soup) -> list[str]:
        """記事一覧ページから /articles/{id} 形式のパスを重複排除して返す。"""
        seen: set[str] = set()
        paths: list[str] = []
        for a in soup.select("article a[href^='/articles/']"):
            href = a.get("href", "").split("?")[0].split("#")[0]
            if _ARTICLE_ID_RE.match(href) and href not in seen:
                seen.add(href)
                paths.append(href)
        return paths

    def _parse_corporate(self, soup, url: str, co_num: str, titles: list[str]) -> dict | None:
        """法人ページから企業情報を抽出する。"""
        name_el = soup.select_one("h1")
        if not name_el:
            return None
        name = name_el.get_text(strip=True)

        # dl 構造を {dt: dd} 辞書に変換
        info: dict[str, str] = {}
        for dl in soup.select("dl"):
            dts = dl.select("dt")
            dds = dl.select("dd")
            for dt, dd in zip(dts, dds):
                # dt は「事業概要」「代表者」等。重複文字（モバイル/PC兼用）を取り除く
                key = re.sub(r"\s+", "", dt.get_text(strip=True))
                # 同じラベルが繰り返されているケース ("事業概要事業概要") を半分に
                if len(key) % 2 == 0 and key[: len(key) // 2] == key[len(key) // 2 :]:
                    key = key[: len(key) // 2]
                value = dd.get_text(" ", strip=True)
                if key and value and key not in info:
                    info[key] = value

        # タグは a[href*="/tags/"] のテキストから "#" を除いて取得
        tag_texts: list[str] = []
        for a in soup.select('a[href*="/tags/"]'):
            t = a.get_text(strip=True).lstrip("#")
            if t and t not in tag_texts:
                tag_texts.append(t)

        addr = info.get("住所", "")
        pref_match = _PREF_RE.match(addr) if addr else None

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.CO_NUM: co_num,
        }
        if pref_match:
            item[Schema.PREF] = pref_match.group(1)
        if addr:
            item[Schema.ADDR] = addr
        if "HP" in info:
            item[Schema.HP] = info["HP"]
        if "代表者" in info:
            item[Schema.REP_NM] = info["代表者"]
        if "設立" in info:
            item[Schema.OPEN_DATE] = info["設立"]
        if "事業概要" in info:
            item["事業概要"] = info["事業概要"]
        if tag_texts:
            item["タグ"] = ",".join(tag_texts)
        if titles:
            # 同じ法人を発見した複数記事のタイトルを | 連結
            uniq_titles = list(dict.fromkeys(t for t in titles if t))
            item["記事タイトル"] = "|".join(uniq_titles)

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KeppleScraper()
    scraper.execute(ARTICLES_LIST_URL.format(page=1))

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
