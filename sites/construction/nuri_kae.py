"""
ヌリカエ — 外壁・屋根塗装会社情報スクレイパー

取得対象:
    - `/sitemap/company` (gzip 圧縮 XML) に列挙された全会社の
      詳細ページ `/company/{id}` から会社概要を取得する。

取得フロー:
    1. `/sitemap/company` を取得し gzip 展開して `/company/{id}` URL を全件抽出
       （`/area/{都道府県}/part/exterior_outer-wall` の一覧は AJAX で数件ずつしか
        描画されず全社を辿れないため、robots.txt にも記載された公式 sitemap を採用）
    2. 各詳細ページの info-table から会社概要を取得

実行方法:
    python scripts/sites/construction/nuri_kae.py
    python bin/run_flow.py --site-id nuri_kae
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


_PREF_RE = re.compile(
    r"^(東京都|北海道|(?:京都|大阪)府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|"
    r"長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

# sitemap 内で会社詳細ページを表す URL パターン（/company/123 のみ。review 等は除外）
_COMPANY_LOC_RE = re.compile(r"https?://[^<>\s]+?/company/\d+(?=[<\s\"']|$)")


class NurikaeScraper(StaticCrawler):
    """ヌリカエ 外壁塗装会社情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "自社職人数",
        "対応エリア",
        "建設業許可内容",
        "加入保険",
        "加入団体",
        "保有資格",
        "表彰実績",
        "評価",
        "口コミ件数",
        "ランキング",
        "最終更新日",
    ]

    # 会社一覧の列挙元 sitemap（parse() の url から派生させる相対パス）
    COMPANY_SITEMAP_PATH = "/sitemap/company"

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("会社URL収集完了: %d 件 (重複排除後)", len(detail_urls))

        for company_url in detail_urls:
            try:
                item = self._scrape_detail(company_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得エラー: %s — %s", company_url, e)
                continue

    def _collect_detail_urls(self, root_url: str) -> list[str]:
        """公式 sitemap を展開して会社詳細URLを全件集める。"""
        sitemap_url = urljoin(root_url, self.COMPANY_SITEMAP_PATH)
        xml = self._fetch_sitemap_text(sitemap_url)
        if not xml:
            self.logger.warning("sitemap取得失敗: %s", sitemap_url)
            return []

        seen: dict[str, None] = {}
        for m in _COMPANY_LOC_RE.finditer(xml):
            full = m.group(0).split("?")[0].split("#")[0]
            if full not in seen:
                seen[full] = None
        self.logger.info("sitemapから会社URL抽出: %d 件", len(seen))
        return list(seen.keys())

    def _fetch_sitemap_text(self, sitemap_url: str) -> str:
        """gzip 圧縮された sitemap を取得して展開し、テキストを返す。"""
        try:
            resp = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning("sitemap HTTP エラー: %s — %s", sitemap_url, e)
            return ""

        raw = resp.content
        # gzip マジックナンバー or Content-Type が gzip の場合は展開
        if raw[:2] == b"\x1f\x8b" or "gzip" in resp.headers.get("Content-Type", ""):
            try:
                raw = gzip.decompress(raw)
            except OSError:
                pass  # 既に非圧縮だった場合
        return raw.decode("utf-8", "replace")

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        h1 = soup.select_one("h1")
        if h1:
            data[Schema.NAME] = h1.get_text(strip=True)

        for row in soup.select("#company-show-overview .info-table__row"):
            label_el = row.select_one(".info-table__label")
            if not label_el:
                continue
            label = label_el.get_text(strip=True)
            value = self._extract_row_value(row, label)
            if not value:
                continue

            if label == "会社名" and not data.get(Schema.NAME):
                data[Schema.NAME] = value
            elif label == "会社HP":
                data[Schema.HP] = value
            elif label == "会社所在地":
                m = _PREF_RE.match(value)
                if m:
                    data[Schema.PREF] = m.group(1)
                    data[Schema.ADDR] = value[m.end():].strip()
                else:
                    data[Schema.ADDR] = value
            elif label == "従業員数":
                data[Schema.EMP_NUM] = value
            elif label == "定休日":
                data[Schema.HOLIDAY] = value
            elif label == "自社職人数":
                data["自社職人数"] = value
            elif label == "対応エリア":
                data["対応エリア"] = value
            elif label == "建設業許可内容":
                data["建設業許可内容"] = value
            elif label == "加入保険":
                data["加入保険"] = value
            elif label == "加入団体":
                data["加入団体"] = value
            elif label == "保有資格":
                data["保有資格"] = value
            elif label == "表彰実績":
                data["表彰実績"] = value

        rating_el = soup.select_one(".fv-images-and-info-pc__rating")
        if rating_el:
            txt = rating_el.get_text(" ", strip=True)
            m = re.match(r"([\d.]+)", txt)
            if m:
                data["評価"] = m.group(1)
            cm = re.search(r"\((\d+)\s*件\)", txt)
            if cm:
                data["口コミ件数"] = f"{cm.group(1)}件"

        ranking_el = soup.select_one(".company-ranking__ranking-text")
        if ranking_el:
            data["ランキング"] = ranking_el.get_text(" ", strip=True)

        lastmod_el = soup.select_one(".company-show__last-modified")
        if lastmod_el:
            txt = lastmod_el.get_text(" ", strip=True)
            m = re.search(r"(\d{4}/\d{1,2}/\d{1,2})", txt)
            data["最終更新日"] = m.group(1) if m else txt

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _extract_row_value(row, label: str) -> str:
        """info-table__row から値テキスト or リンクURLを抽出する。"""
        if label == "会社HP":
            link = row.select_one(".info-table__value a[href]")
            if link:
                href = link.get("href", "").strip()
                if href:
                    return href
        truncated = row.select_one(
            ".pc-view.info-table__value .js-toggleText-truncated-target"
        )
        if truncated:
            return truncated.get_text(" ", strip=True)
        value_el = row.select_one(".pc-view.info-table__value, .info-table__value")
        if value_el:
            return value_el.get_text(" ", strip=True)
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NurikaeScraper()
    scraper.execute("https://www.nuri-kae.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
