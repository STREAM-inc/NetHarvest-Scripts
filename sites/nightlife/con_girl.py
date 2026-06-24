"""
コンがーる (con-girl.com) — 全国コンカフェ＆ガールズバー店舗情報スクレイパー

取得対象:
    - 全国のコンカフェ／ガールズバー店舗の基本情報
      (店名・住所・電話番号・営業時間・定休日・支払方法・特徴・アクセス・料金システム)

取得フロー:
    sitemap.xml から記事 (/articles/{id}) 一覧を取得
        → 各記事は「○○エリアのコンカフェ10選」形式のまとめ記事
        → 記事本文 (div.article-body) 内の各 <table> が 1 店舗に対応
        → 店名は直前の <h3> 見出し、店舗情報は table の th/td ペア
        → 1 店舗パースするごとに即 yield (途中中断しても無駄通信が起きない)

備考:
    - サイトは Nuxt (microCMS) 製だが記事ページは SSR 済みのため Static で取得可能。
    - th ラベルは全記事で安定:
      最大の特徴 / 住所 / アクセス / 営業時間 / 定休日 / 電話番号 / 料金システム / 支払方法

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/con_girl.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id con_girl
"""

from __future__ import annotations

import re
import sys
import warnings
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

from bs4 import XMLParsedAsHTMLWarning

# sitemap.xml を html.parser で読む際の警告を抑制 (取得自体は問題なく動作する)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class ConGirlCrawler(StaticCrawler):
    """コンがーる クローラー — 全国コンカフェ／ガールズバー店舗情報を取得"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["最大の特徴", "アクセス", "料金システム", "掲載記事タイトル"]

    # 記事 URL の判定 (/articles/{数字})
    _ARTICLE_RE = re.compile(r"/articles/\d+/?$")

    # 店名見出しの先頭装飾 (①②… / 1. / 1、 等) を除去するための正規表現
    _PREFIX_RE = re.compile(r"^[\s0-9０-９①-⑳㉑-㉟()（）.．,，、・:：\-―ー]+")

    # 都道府県抽出
    _PREF_RE = re.compile(
        r"^(北海道|東京都|大阪府|京都府|"
        r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|石川|福井|"
        r"山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|徳島|"
        r"香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
    )

    def parse(self, url: str) -> Generator[dict, None, None]:
        # url (= sites.yml の正規 URL) を唯一のルートとして sitemap を派生
        sitemap_url = urljoin(url, "/sitemap.xml")
        sm = self.get_soup(sitemap_url)
        if sm is None:
            self.logger.warning("sitemap を取得できませんでした: %s", sitemap_url)
            return

        article_urls: list[str] = []
        seen: set[str] = set()
        for loc in sm.find_all("loc"):
            href = (loc.get_text(strip=True) or "")
            if not href or not self._ARTICLE_RE.search(href):
                continue
            full = urljoin(url, href)
            if full not in seen:
                seen.add(full)
                article_urls.append(full)

        # 進捗表示用 (実レコード数は記事ごとに変動するため記事数を目安に設定)
        self.total_items = len(article_urls)
        self.logger.info("対象記事数: %d", len(article_urls))

        for article_url in article_urls:
            try:
                yield from self._scrape_article(article_url)
            except Exception as e:  # noqa: BLE001 — 1記事の失敗で全体を止めない
                self.logger.warning("記事の処理に失敗 (スキップ): %s — %s", article_url, e)
                continue

    def _scrape_article(self, article_url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(article_url)
        if soup is None:
            return

        title_el = soup.select_one("title")
        article_title = title_el.get_text(strip=True) if title_el else ""

        body = soup.find("div", class_=lambda c: bool(c) and "article-body" in c)
        if body is None:
            return

        for table in body.find_all("table"):
            fields = self._parse_table(table)
            # 店舗テーブルの判定: 住所 もしくは 電話番号 が含まれるものだけを対象
            if "住所" not in fields and "電話番号" not in fields:
                continue

            name = self._store_name(table)
            if not name:
                continue

            item = {
                Schema.NAME: name,
                Schema.URL: article_url,
                "掲載記事タイトル": article_title,
            }

            # 住所 → 都道府県 + 住所 ("Google Mapはこちら" 等の付随リンク行は除去)
            addr = self._clean_address(fields.get("住所", ""))
            if addr:
                m = self._PREF_RE.match(addr)
                if m:
                    item[Schema.PREF] = m.group(1)
                    item[Schema.ADDR] = addr[m.end():].strip()
                else:
                    item[Schema.ADDR] = addr

            if fields.get("電話番号"):
                item[Schema.TEL] = fields["電話番号"]
            if fields.get("営業時間"):
                item[Schema.TIME] = fields["営業時間"]
            if fields.get("定休日"):
                item[Schema.HOLIDAY] = fields["定休日"]
            if fields.get("支払方法"):
                item[Schema.PAYMENTS] = fields["支払方法"]

            # EXTRA (構造化された短い属性のみ)
            if fields.get("最大の特徴"):
                item["最大の特徴"] = fields["最大の特徴"]
            if fields.get("アクセス"):
                item["アクセス"] = fields["アクセス"]
            if fields.get("料金システム"):
                item["料金システム"] = fields["料金システム"]

            yield item

    @staticmethod
    def _parse_table(table) -> dict[str, str]:
        """table の <th>ラベル</th><td>値</td> を dict 化する。"""
        fields: dict[str, str] = {}
        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value = td.get_text("\n", strip=True)
            if label:
                fields[label] = value
        return fields

    def _store_name(self, table) -> str:
        """table の直前にある見出し (h3 優先, なければ h2) を店名として取得。"""
        heading = table.find_previous(["h3", "h2"])
        if heading is None:
            return ""
        raw = heading.get_text(strip=True)
        return self._PREFIX_RE.sub("", raw).strip()

    @staticmethod
    def _clean_address(value: str) -> str:
        """住所セルから本体の住所行のみを抽出 (Google Map リンク行などを除去)。"""
        for line in value.splitlines():
            line = line.strip()
            if not line:
                continue
            if "Google Map" in line or "Map" in line and "はこちら" in line:
                continue
            return line
        return value.strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ConGirlCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://con-girl.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
