"""
スナックアドバイザー (sunakkuadvisor.com) — 宮崎・ニシタチのスナック紹介ポータル

取得対象:
    - ニシタチ(宮崎市)のスナック各店の店舗情報
      (店名 / ママ・マスター名 / 住所 / 電話 / 営業時間 / 定休日 / 決済方法 /
       HP・SNS / 喫煙可否 / 席数 / 料金体系 / 軽食 / タグ)

取得フロー:
    1. ルート URL から派生した post-sitemap.xml を取得し、/sunakku/ 記事 URL を列挙
    2. 各記事(詳細ページ)を 1 件ずつ取得し、その場で yield (取得即 yield / 早期 yield)

備考・方針:
    - 利用規約に相当する明示的なスクレイピング禁止条項は確認されなかった。
      /about/ に「当サイト内の文章・画像等の無断転載及び複製等はご遠慮ください」との
      記載があるため、記事本文・キャッチコピー等の自由記述(プロース)は取得しない。
    - 店名は記事見出し(キャッチコピー)ではなく p.article__name の店舗名を採用。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/sunakkuadvisor.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id sunakkuadvisor
"""

import re
import sys
import logging
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 47 都道府県 (住所からの都道府県抽出用)
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# SNS ボタンの class サフィックス → Schema 定数
_SNS_MAP = {
    "web": Schema.HP,
    "fb": Schema.FB,
    "tw": Schema.X,
    "insta": Schema.INSTA,
    "line": Schema.LINE,
    "tiktok": Schema.TIKTOK,
}


class SunakkuAdvisor(StaticCrawler):
    """スナックアドバイザー スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["喫煙", "席数", "料金体系", "軽食", "タグ"]

    def parse(self, url: str):
        # ルート URL から post-sitemap を派生 (url を唯一のルートとする)
        sitemap_url = urljoin(url, "/post-sitemap.xml")
        soup = self.get_soup(sitemap_url)
        detail_urls = []
        seen = set()
        for loc in soup.find_all("loc"):
            href = loc.get_text(strip=True)
            if "/sunakku/" in href and href not in seen:
                seen.add(href)
                detail_urls.append(href)

        self.total_items = len(detail_urls)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:  # 個別ページの失敗はログして継続
                logger.warning("detail failed: %s (%s)", detail_url, e)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        # --- 店名 (p.article__name。<time> と先頭の区切り文字を除去) ---
        name = ""
        name_el = soup.select_one(".article__name")
        if name_el:
            time_el = name_el.find("time")
            if time_el:
                time_el.extract()
            raw = name_el.get_text(strip=True)
            # 例: "2022.10.7｜スナックCandy宮崎" 形式にも耐えるよう区切りで分割
            parts = re.split(r"[｜|]", raw)
            name = parts[-1].strip() if parts else raw.strip()
        if not name:
            return None

        item = {
            Schema.URL: url,
            Schema.NAME: name,
        }

        # --- ママ・マスター名 → 代表者名 ---
        mama = soup.select_one(".data__mama")
        if mama:
            item[Schema.REP_NM] = mama.get_text(strip=True)

        # --- Data テーブル (dl.data__meta) ---
        meta = {}
        dl = soup.select_one("dl.data__meta")
        if dl:
            for dt in dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                key = dt.get_text(strip=True)
                if dd is None:
                    meta[key] = ""
                    continue
                # SNS の dd はリンクを個別処理するので後段で扱う
                meta[key] = dd.get_text(" ", strip=True)

        addr = meta.get("住所", "")
        if addr:
            item[Schema.ADDR] = addr
            m = _PREF_PATTERN.search(addr)
            if m:
                item[Schema.PREF] = m.group(1)
            else:
                # 当サイトはニシタチ(宮崎市)専門。住所に県名が無い場合は宮崎県を補完
                item[Schema.PREF] = "宮崎県"

        # prefが削除されたaddrに関して戻す作業を行う
        if not item[Schema.ADDR].startswith("宮崎県"):
            item[Schema.ADDR] = "宮崎県" +  item[Schema.ADDR]

        item[Schema.TEL] = meta.get("電話", "")
        item[Schema.TIME] = meta.get("営業時間", "")
        item[Schema.HOLIDAY] = meta.get("定休日", "")
        item[Schema.PAYMENTS] = meta.get("決済方法", "")

        # EXTRA (構造化された短い項目のみ。自由記述は含めない)
        item["喫煙"] = meta.get("喫煙", "")
        item["席数"] = meta.get("席数", "")
        item["料金体系"] = meta.get("料金体系", "")
        item["軽食"] = meta.get("軽食", "")

        # --- SNS / HP リンク (class サフィックスで振り分け) ---
        for a in soup.select("dd.data__data--sns a[href]"):
            classes = a.get("class") or []
            for cls in classes:
                if cls.startswith("sns-btn--"):
                    suffix = cls.replace("sns-btn--", "")
                    schema_key = _SNS_MAP.get(suffix)
                    if schema_key and not item.get(schema_key):
                        item[schema_key] = a["href"].strip()
                    break

        # --- タグ (この記事のタグのみ。footer タグは除外) ---
        tags = [
            a.get_text(strip=True).lstrip("#")
            for a in soup.select("ul.articletag__list a[href*='/tag/']")
        ]
        item["タグ"] = "、".join(t for t in tags if t)

        return item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SunakkuAdvisor()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://sunakkuadvisor.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
