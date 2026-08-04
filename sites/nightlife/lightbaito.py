"""
ライトバイト（コンガルバイト） — 全国のコンカフェ・ガールズバー店舗情報まとめサイト

取得対象:
    - WordPress のまとめ記事型サイト。1 記事（例: concafe-akihabara/ = 秋葉原コンカフェ10選）
      あたり 9〜10 店舗の情報表が記事内に並ぶ。店舗単位で 1 行を出力する。

取得フロー:
    1. 引数 url（店舗紹介カテゴリ）のページネーション（url + page/N/）を巡回し、
       各記事 URL を収集（#main article カードから抽出）。最初の記事から即 yield。
    2. さらに wp-sitemap-posts-post-1.xml（全286記事、url と同一オリジンから派生）を
       走査し、コンカフェ/ガールズバー等 他カテゴリの店舗記事も網羅（記事 URL で重複除去）。
    3. 各記事内の「情報表（住所＋電話/営業時間を含む table）」だけを店舗表として抽出。
       求人ノウハウ等の情報記事（table はあるが住所/電話なし）は自動的にスキップ。

    店舗名は情報表の直前の見出し（h2/h3/h4）から取得する。
    セクキャバ・風俗系の記事はタイトル/URL のキーワードで除外する。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/lightbaito.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id lightbaito
"""

import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 都道府県の抽出（住所文字列の先頭）
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|"
    r"千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|"
    r"愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|"
    r"広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|"
    r"宮崎県|鹿児島県|沖縄県)"
)
# 郵便番号（〒NNN-NNNN / NNNNNNN）
_POST_PATTERN = re.compile(r"〒?\s*(\d{3})[-－ー\s]?(\d{4})")
# 見出しの先頭に付く順位・番号表記を除去（例: "【1位】", "第2位", "3. ", "①"）
_RANK_PREFIX = re.compile(
    r"^[\s　]*(【[^】]*?\d+[^】]*?】|第?\s*\d+\s*位|No\.?\s*\d+|[0-9０-９]+\s*[.．、:：)）]?|[①-⑳])[\s　]*"
)
# 風俗・セクキャバ系の除外キーワード（記事タイトル / URL に含まれる場合）
_EXCLUDE_KEYWORDS = ("風俗", "セクキャバ", "デリヘル", "ソープ", "ヘルス", "fuzoku", "sexcabaret")
# HP 判定から除外するドメイン（地図・SNS・自サイト）
_NON_HP = re.compile(
    r"(maps\.app\.goo|google\.[a-z.]+/maps|goo\.gl/maps|instagram\.com|twitter\.com|"
    r"//x\.com|line\.me|lin\.ee|tiktok\.com|facebook\.com|lightbaito\.com)",
    re.I,
)


class Lightbaito(StaticCrawler):
    """ライトバイト（コンガルバイト） スクレイパー"""

    DELAY = 1.5
    MEDIA_NAME = "ライトバイト"
    EXTRA_COLUMNS = ["掲載ページURL", "アクセス", "掲載媒体名"]

    def parse(self, url: str):
        seen: set[str] = set()

        # --- 1. 店舗紹介カテゴリのページネーション（引数 url を起点に派生） ---
        base = url.rstrip("/")
        page = 1
        while page <= 40:  # 安全上限（実際は約9ページ）
            page_url = url if page == 1 else f"{base}/page/{page}/"
            soup = self.get_soup(page_url)
            if soup is None:
                break
            cards = soup.select("#main article")
            if not cards:
                break
            article_urls = []
            for card in cards:
                a = card.select_one("a[href]")
                if a and a.get("href"):
                    article_urls.append(urljoin(url, a["href"]))
            if not article_urls:
                break
            for article_url in article_urls:
                if article_url in seen:
                    continue
                seen.add(article_url)
                yield from self._scrape_article(article_url)
            page += 1

        # --- 2. サイトマップ走査で他カテゴリ（concafe / girlsbar 等）を網羅 ---
        sitemap_url = urljoin(url, "/wp-sitemap-posts-post-1.xml")
        sm = self.get_soup(sitemap_url)
        if sm is not None:
            locs = [loc.get_text(strip=True) for loc in sm.find_all("loc")]
            self.total_items = len(locs)
            for article_url in locs:
                if not article_url or article_url in seen:
                    continue
                seen.add(article_url)
                yield from self._scrape_article(article_url)

    # ------------------------------------------------------------------ #
    def _scrape_article(self, article_url: str):
        """1 記事内の店舗情報表をすべて抽出し、店舗単位で yield する。"""
        try:
            soup = self.get_soup(article_url)
        except Exception as e:  # noqa: BLE001
            logger.warning("記事取得失敗 %s: %s", article_url, e)
            return
        if soup is None:
            return

        # 風俗・セクキャバ系の記事は除外
        title = (soup.title.get_text(strip=True) if soup.title else "") + " " + article_url
        if any(kw.lower() in title.lower() for kw in _EXCLUDE_KEYWORDS):
            logger.info("除外(風俗系): %s", article_url)
            return

        for table in soup.find_all("table"):
            txt = table.get_text()
            # 店舗情報表の判定: 住所 + (電話 or 営業時間) を持つ table のみ
            if "住所" not in txt:
                continue
            if "電話" not in txt and "営業" not in txt:
                continue

            item = self._parse_store_table(table, article_url)
            if item and item.get(Schema.NAME):
                yield item

    def _parse_store_table(self, table, article_url: str) -> dict | None:
        # 店舗名 = 情報表の直前の見出し
        name = ""
        heading = table.find_previous(["h2", "h3", "h4"])
        if heading:
            name = _RANK_PREFIX.sub("", heading.get_text(" ", strip=True)).strip()

        # ラベル→値、およびラベル→リンク を収集
        fields: dict[str, str] = {}
        anchors: list[str] = []
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            label = cells[0].get_text(" ", strip=True)
            value_cell = cells[1]
            # リンク先(HP候補)を保持しつつ、値からアンカー文字列(例:「Google MAPへ移動」)を除去
            for a in value_cell.find_all("a", href=True):
                anchors.append(a["href"])
            value = value_cell.get_text(" ", strip=True)
            for a in value_cell.find_all("a"):
                at = a.get_text(" ", strip=True)
                if at:
                    value = value.replace(at, " ")
            value = re.sub(r"\s{2,}", " ", value).strip(" 　・|")
            fields[label] = value

        def pick(*keywords: str) -> str:
            for label, value in fields.items():
                if any(k in label for k in keywords):
                    return value
            return ""

        addr_raw = pick("住所")
        post_code, pref, addr = self._split_address(addr_raw)

        # HP: 情報表内のリンクから地図/SNS/自サイトを除いた最初の URL
        hp = ""
        for href in anchors:
            if href and not _NON_HP.search(href):
                hp = href
                break

        item = {
            # 記事(掲載ページ)の URL。Schema.URL("取得URL")ではなく専用カラム名で出力する。
            "掲載ページURL": article_url,
            # スクレイピング実行時のタイムスタンプ（YYYY-MM-DD HH:MM:SS）。
            Schema.GET_TIME: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: pick("電話", "TEL"),
            Schema.TIME: pick("営業時間", "営業日"),
            Schema.HOLIDAY: pick("定休日"),
            Schema.CAT_SITE: pick("コンセプト", "ジャンル", "業種"),
            Schema.PAYMENTS: pick("支払"),
            Schema.HP: hp,
            "アクセス": pick("アクセス", "最寄"),
            "掲載媒体名": self.MEDIA_NAME,
        }
        return item

    @staticmethod
    def _split_address(addr_raw: str) -> tuple[str, str, str]:
        """住所文字列を 郵便番号 / 都道府県 / それ以降 に分解する。"""
        if not addr_raw:
            return "", "", ""
        post_code = ""
        m = _POST_PATTERN.search(addr_raw)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
            addr_raw = _POST_PATTERN.sub("", addr_raw, count=1).strip()
        pref = ""
        addr = addr_raw
        pm = _PREF_PATTERN.search(addr_raw)
        if pm:
            pref = pm.group(1)
            addr = addr_raw[pm.end():].strip()
        return post_code, pref, addr


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Lightbaito()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://lightbaito.com/category/introduction/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
