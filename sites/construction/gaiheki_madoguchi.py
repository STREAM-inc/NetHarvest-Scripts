"""
外壁塗装の窓口 — 加盟外壁塗装会社(掲載クライアント)情報スクレイパー

取得対象:
    - 掲載されている外壁・屋根塗装会社 (`/clients/P_xxxxx`) の会社概要を取得する。

取得フロー:
    1. サイトの sitemap.xml (urljoin(url, "sitemap.xml") が S3 の gzip に 301) を取得・解凍
    2. sitemap から会社詳細ページ URL (/clients/P_xxx) を列挙 (約2,800件)
    3. 各詳細ページの会社概要ブロック (p-client__card_status) から会社名・所在地・
       事業内容・代表者・ホームページ等を取得し、1件ずつ即 yield する

実行方法:
    # ローカルテスト
    python scripts/sites/construction/gaiheki_madoguchi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id gaiheki_madoguchi
"""

import gzip
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 所在地文字列から都道府県を切り出す正規表現
_PREF_RE = re.compile(
    r"^(東京都|北海道|(?:京都|大阪)府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|"
    r"長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)
_POSTAL_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_WORKS_RE = re.compile(r"累計\s*([\d,]+)\s*件の施工実績")


class GaihekiMadoguchiScraper(StaticCrawler):
    """外壁塗装の窓口 会社情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "総合評価",
        "累計施工件数",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("会社URL収集完了: %d 件", len(detail_urls))

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得エラー: %s — %s", detail_url, e)
                continue

    def _collect_detail_urls(self, url: str) -> list[str]:
        """sitemap.xml (gzip) を取得・解凍して会社詳細ページ URL を列挙する。"""
        sitemap_url = urljoin(url, "sitemap.xml")
        self.logger.info("sitemap 取得中: %s", sitemap_url)
        try:
            resp = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            self.logger.error("sitemap 取得失敗: %s", e)
            return []

        raw = resp.content
        try:
            xml = gzip.decompress(raw).decode("utf-8", "replace")
        except (OSError, EOFError):
            # gzip でなければそのままテキストとして扱う
            xml = raw.decode("utf-8", "replace")

        # 会社詳細ページ (/clients/P_xxx) のみを対象にする。都道府県・記事一覧等は除外。
        locs = re.findall(
            r"<loc>\s*(https?://[^<]*?/clients/P_[A-Za-z0-9]+)\s*</loc>", xml
        )
        # 重複排除 (出現順を保持)
        seen: set[str] = set()
        result: list[str] = []
        for loc in locs:
            clean = loc.split("?")[0].split("#")[0]
            if clean not in seen:
                seen.add(clean)
                result.append(clean)
        return result

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        name_el = soup.select_one("h1.p-client__card_name")
        if name_el:
            data[Schema.NAME] = name_el.get_text(strip=True)

        # 会社概要ブロック: head(ラベル) / body(値) のペア
        for head in soup.select(".p-client__card_status_head"):
            label = head.get_text(" ", strip=True)
            body = head.find_next_sibling(class_="p-client__card_status_body")
            if body is None:
                body = head.find_next(class_="p-client__card_status_body")
            if body is None:
                continue
            value = body.get_text(" ", strip=True)

            if label == "所在地":
                self._fill_address(data, value)
            elif label == "事業内容":
                # 事業内容(短い業種の列挙)。改行を読点に整形。
                data[Schema.LOB] = re.sub(r"\s*\n\s*", "、", value).strip("、 ")
            elif label == "代表者":
                data[Schema.REP_NM] = value
            elif label == "ホームページ":
                a = body.select_one("a[href]")
                href = a.get("href", "").strip() if a else ""
                # 外部の会社HPのみ採用 (自サイトの /clients/... 自己リンクは除外)
                if href.startswith("http") and "gaiheki-madoguchi.com" not in href:
                    data[Schema.HP] = href
            # 電話番号は全社共通の紹介ダイヤル(0120-945-990,
            # 「会社ではなく外壁塗装の窓口につながります」)であり、
            # 会社固有の実番号ではないため TEL には採用しない。

        # 総合評価 (会社サマリの数値。個別口コミの星評価とは別)
        rating = soup.select_one(
            ".p-client__card_summary_review .p-client__review_num, "
            ".p-client__summary_review .p-client__review_num"
        )
        if rating:
            data["総合評価"] = rating.get_text(strip=True)

        # 累計施工件数
        m = _WORKS_RE.search(soup.get_text(" ", strip=True))
        if m:
            data["累計施工件数"] = m.group(1).replace(",", "")

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _fill_address(data: dict, value: str) -> None:
        """所在地文字列を 郵便番号 / 都道府県 / 住所 に分解する。"""
        text = value.strip()
        pm = _POSTAL_RE.match(text)
        if pm:
            data[Schema.POST_CODE] = pm.group(1)
            text = text[pm.end():].strip()
        prm = _PREF_RE.match(text)
        if prm:
            data[Schema.PREF] = prm.group(1)
            data[Schema.ADDR] = text[prm.end():].strip()
        else:
            data[Schema.ADDR] = text


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GaihekiMadoguchiScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://gaiheki-madoguchi.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
