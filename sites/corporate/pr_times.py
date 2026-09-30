"""
PR TIMES — プレスリリース配信企業情報

取得対象:
    - PR TIMES に配信されたプレスリリース (1行 = 1プレスリリース)
    - 配信企業の会社概要 (プレスリリース下部「会社概要」欄に記載がある項目のみ)

取得フロー:
    トップ (https://prtimes.jp/) から新着一覧 /main/html/newarrival を起点にする。
    一覧は「もっと見る」= ?pagination_cursor=<unixtime>_<id> のカーソル方式 (40件/ページ)。
    各カードからプレスリリースURL・企業ページURL・配信日時を取得し、
    プレスリリース詳細ページの「会社概要」dl (URL / 業種 / 本社所在地 / 電話番号 /
    代表者名 / 上場 / 資本金 / 設立) を 1件ずつ取得して即 yield する。

    ※ 会社概要欄は配信企業が任意で記載するため、未記載の項目は "-" で表示される。
      "-" は空文字として扱う (備考「会社概要欄記載時のみ」に対応)。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/pr_times.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id pr_times
"""

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


# 新着プレスリリース一覧 (ルート URL からの相対パス)
_LIST_PATH = "/main/html/newarrival"

# カーソルページの巡回上限 (40件/ページ)。PR TIMES は全期間分を遡れるため上限を設ける。
_MAX_PAGES = 250

# 会社概要欄で「未記載」を表すプレースホルダ
_EMPTY_VALUES = {"", "-", "ー", "―", "‐", "−", "なし", "無し"}

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|"
    r"千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|"
    r"愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|"
    r"広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|"
    r"宮崎県|鹿児島県|沖縄県)"
)


def _clean(text) -> str:
    """連続する空白・改行を 1 個の半角スペースに畳んで前後を除去する。"""
    if text is None:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", str(text)).strip()


def _value(text) -> str:
    """会社概要欄の値。未記載プレースホルダ ("-" 等) は空文字にする。"""
    cleaned = _clean(text)
    return "" if cleaned in _EMPTY_VALUES else cleaned


def _dl_pairs(dl) -> dict:
    """<dl> 内の dt / dd を出現順にペアリングして辞書化する。"""
    pairs: dict = {}
    current_label = None
    for tag in dl.find_all(["dt", "dd"]):
        if tag.name == "dt":
            current_label = _clean(tag.get_text(" ", strip=True))
        elif current_label:
            # 同一ラベルに複数 dd がある場合は最初の値を採用する
            pairs.setdefault(current_label, _clean(tag.get_text(" ", strip=True)))
    return pairs


class PrTimes(StaticCrawler):
    """PR TIMES スクレイパー (プレスリリース単位)"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "プレスリリースURL",
        "企業ページURL",
        "配信日",
        "プレスリリース種類",
        "ビジネスカテゴリ",
        "上場",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        next_url = urljoin(url, _LIST_PATH)
        seen: set[str] = set()

        for _ in range(_MAX_PAGES):
            soup = self.get_soup(next_url)
            if soup is None:
                break

            articles = soup.select("article.list-article")
            if not articles:
                break

            for article in articles:
                try:
                    link = article.select_one("a.list-article__link[href]")
                    if link is None:
                        continue
                    detail_url = urljoin(url, link["href"])
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)

                    time_tag = article.select_one("time[datetime]")
                    published = _clean(time_tag["datetime"]) if time_tag else ""

                    company_link = article.select_one(
                        'a[href*="/main/html/searchrlp/company_id/"]'
                    )
                    company_url = (
                        urljoin(url, company_link["href"]) if company_link else ""
                    )
                    company_name = (
                        _clean(company_link.get_text(" ", strip=True))
                        if company_link
                        else ""
                    )

                    item = self._scrape_detail(
                        detail_url,
                        company_name=company_name,
                        company_url=company_url,
                        published=published,
                    )
                    if item:
                        yield item
                except Exception as e:  # 個別アイテムの失敗は握って継続
                    self.logger.warning("アイテム処理に失敗しました: %s", e)
                    continue

            more = soup.select_one("a.js-new-arrival-list-article-more-button[href]")
            if more is None:
                break
            candidate = urljoin(url, more["href"])
            if candidate == next_url:
                # カーソルが進まない = 最終ページ
                break
            next_url = candidate

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(
        self,
        detail_url: str,
        company_name: str = "",
        company_url: str = "",
        published: str = "",
    ) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        # 会社概要 dl (代表者名 などを含む) と リリース情報 dl (種類 などを含む) を判別する
        company_info: dict = {}
        release_info: dict = {}
        for dl in soup.find_all("dl"):
            pairs = _dl_pairs(dl)
            if not pairs:
                continue
            if not company_info and ("代表者名" in pairs or "本社所在地" in pairs):
                company_info = pairs
            elif not release_info and ("種類" in pairs or "ビジネスカテゴリ" in pairs):
                release_info = pairs

        # 企業名: 詳細ページのリンク表記を優先し、無ければ一覧の表記を使う
        detail_company_link = soup.select_one(
            'a[href*="/main/html/searchrlp/company_id/"]'
        )
        if detail_company_link is not None:
            name = _clean(detail_company_link.get_text(" ", strip=True)) or company_name
            company_url = urljoin(detail_url, detail_company_link["href"]) or company_url
        else:
            name = company_name

        if not name:
            return None

        # 配信日時: 詳細ページの <time datetime="..."> を優先
        detail_time = soup.select_one("time[datetime]")
        if detail_time is not None:
            published = _clean(detail_time["datetime"]) or published

        address = _value(company_info.get("本社所在地"))
        pref = ""
        m = _PREF_RE.match(address)
        if m:
            pref = m.group(1)

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.REP_NM: _value(company_info.get("代表者名")),
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.TEL: _value(company_info.get("電話番号")),
            Schema.CAP: _value(company_info.get("資本金")),
            Schema.OPEN_DATE: _value(company_info.get("設立")),
            Schema.CAT_SITE: _value(company_info.get("業種")),
            Schema.HP: _value(company_info.get("URL")),
            "プレスリリースURL": detail_url,
            "企業ページURL": company_url,
            "配信日": published,
            "プレスリリース種類": _value(release_info.get("種類")),
            "ビジネスカテゴリ": _value(release_info.get("ビジネスカテゴリ")),
            "上場": _value(company_info.get("上場")),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PrTimes()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://prtimes.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
