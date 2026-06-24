"""
シーシャマップジャパン (SHISHA MAP JAPAN) — 全国のシーシャカフェ・バー店舗情報

取得対象:
    - 全国のシーシャカフェ・バー店舗 (店舗名・住所・都道府県・電話番号・営業時間・
      アクセス・席数・Instagram)

取得フロー:
    一覧ページ (Webflow + Jetboost コレクションリスト, 静的 SSR) をページ送りしながら
    各店舗の詳細ページ (/shishacafe/{slug}) へのリンクを収集し、詳細ページを 1 件取得
    するごとに即 yield する (Pattern B)。
    ページ送りは `?f4c83311_page=N` (Webflow ネイティブのページャ id)。
    一覧には固定の「おすすめ枠」も混在するため slug を集合で重複排除し、
    新規 slug が 0 件のページに達したらクロールを終了する。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/japanshishatimes.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id japanshishatimes
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 詳細ページの slug 形式: /shishacafe/{slug}
_SHOP_HREF = re.compile(r"^/shishacafe/[^/]+$")

# 都道府県 (住所先頭から都道府県を切り出す)
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# サイト運営側 (ATAR / SHISHA MAP) の公式 SNS。店舗固有の Instagram と区別するため除外する。
_SITE_SNS = re.compile(r"atar\.shisha|atarshisha|shishamap", re.I)

# 店舗の住所らしさ判定 (都道府県 + 数字/丁目を含む)
_ADDR_NUM = re.compile(r"[0-9０-９\-－丁目番号]")

# 安全弁: ページ送りの上限 (実測の総ページ ≒ 41)
_MAX_PAGES = 60


class JapanShishaTimes(StaticCrawler):
    """シーシャマップジャパン スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["アクセス", "席数"]

    def parse(self, url: str):
        # 進捗表示用の概算 (おすすめ枠 ≒ 68 + ページ送り 20 件 × 41 ページ)
        self.total_items = 68 + 20 * 41

        seen: set[str] = set()
        page = 1
        while page <= _MAX_PAGES:
            list_url = url if page == 1 else f"{url}?f4c83311_page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            # このページに現れる店舗詳細リンク (相対パス) を抽出
            slugs = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if _SHOP_HREF.match(href) and href not in seen:
                    seen.add(href)
                    slugs.append(href)

            # 新規 slug が 0 件 = ページ送りが尽きた (おすすめ枠は毎ページ固定で既出)
            if not slugs:
                logger.info("page=%d: 新規店舗なし。クロールを終了します。", page)
                break

            logger.info("page=%d: 新規店舗 %d 件", page, len(slugs))
            for href in slugs:
                detail_url = urljoin(url, href)
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # 個別ページのエラーはスキップして継続
                    logger.warning("詳細ページ取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item = {Schema.URL: url}

        # --- 店舗名 / カナ ---
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)
            item[Schema.NAME] = name
            # 末尾の全角カッコ内カタカナを読み仮名として切り出す
            m = re.search(r"（([ァ-ヶー・　\s]+)）\s*$", name)
            if m:
                item[Schema.NAME_KANA] = m.group(1).strip()

        # --- 住所 / 都道府県 (都道府県 + 数字を含む短い文字列) ---
        for s in soup.find_all(string=_PREF_PATTERN):
            t = s.strip()
            if t and len(t) < 80 and _ADDR_NUM.search(t):
                pm = _PREF_PATTERN.match(t)
                if pm:
                    item[Schema.PREF] = pm.group(1)
                    item[Schema.ADDR] = t
                    break

        # --- 電話番号 (店舗の tel: リンク) ---
        for a in soup.select('a[href^="tel:"]'):
            tel = a["href"][4:].strip()
            if tel:
                item[Schema.TEL] = tel
                break

        # --- 営業時間 (曜日別の rich text) ---
        for rtb in soup.select('[class*="rich-text"]'):
            txt = rtb.get_text("\n", strip=True)
            if re.search(r"[月火水木金土日][：:]", txt) and ":" in txt:
                item[Schema.TIME] = re.sub(r"[ \t]+", " ", txt).strip()
                break

        # --- アクセス (最寄駅・徒歩分数の短いフレーズ) ---
        # <title> やパンくず (区切り文字 ｜ / SHISHA MAP を含む) は除外する
        for s in soup.find_all(string=re.compile(r"(駅|より|から).*(徒歩|分)")):
            if getattr(s.parent, "name", None) in ("title", "script", "style"):
                continue
            t = re.sub(r"\s+", " ", s.strip())
            if 3 < len(t) < 40 and "｜" not in t and "SHISHA MAP" not in t:
                item["アクセス"] = t
                break

        # --- 席数 (「席数」ラベル直後の数値。情報入力待ちプレースホルダは除外) ---
        for box in soup.select(".information-text-box"):
            if box.get_text(strip=True) == "席数":
                for sib in box.find_next_siblings():
                    st = sib.get_text(strip=True)
                    if re.fullmatch(r"\d+", st):
                        item["席数"] = st
                        break
                    if st in ("個室", "情報入力待ち", ""):
                        continue
                    break
                break

        # --- Instagram (店舗固有のみ。運営公式は除外) ---
        for a in soup.select('a[href*="instagram.com"]'):
            href = a["href"]
            if not _SITE_SNS.search(href):
                item[Schema.INSTA] = href
                break

        return item if item.get(Schema.NAME) else None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JapanShishaTimes()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.japanshishatimes.jp/shoplist")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
