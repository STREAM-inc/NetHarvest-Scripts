"""
徳島ナイトスタイルリスト (tokushima-night-style.com) — 徳島県ナイトビジネス店舗スクレイパー

取得対象:
    - 徳島県 (主に徳島市) のキャバクラ・スナック/ラウンジ・Girl's Bar・Bar・
      ホストクラブ・ヘルス/ソープ等ナイトビジネス店舗の店舗概要
      (店名・ジャンル・住所・電話・営業時間・定休日・支払い方法・座席数)

取得フロー:
    サイトは WordPress (All in One SEO)。店舗は 1 店舗 = 1 投稿 (?p=N) で、
    店舗ジャンルごとにカテゴリ (?cat=N) が割り当てられている。求人・ブログ・お知らせ
    カテゴリは店舗ではないため除外し、店舗カテゴリのみを WP REST API
    (/wp/v2/posts?categories=N) でページングして列挙する。各投稿の content.rendered に
    店舗情報テーブル (td ラベル / td 値) が含まれるため、投稿を 1 件取得するごとに
    その場でパースして即 yield する (追加のページ取得は発生しない)。

備考:
    - 電話番号・住所・営業時間・定休日・支払い方法・座席数など構造化された短い値のみ取得。
    - 「content-exp」(店舗紹介文) や「注意事項」「メニュー」等の長文自由記述 (プロース) は
      著作権リスクを避けて取得しない。
    - SNS アカウントの掲載はほぼ無いため取得対象外。HP リンクが表内にある場合のみ取得する。
    - 住所は原則「徳島県…」始まり。都道府県で始まらない住所は PREF を空にする (捏造しない)。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/tokushima_night_style.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id tokushima_night_style
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 店舗ジャンルのカテゴリ ID と表示名 (求人/ブログ/お知らせ/未分類は店舗ではないので除外)
#   5:キャバクラ 3:スナック・ラウンジ 9:Girl's Bar 6:Bar 11:ホストクラブ 10:ヘルス・ソープ
_SHOP_CATEGORIES = {
    5: "キャバクラ",
    3: "スナック・ラウンジ",
    9: "Girl's Bar",
    6: "Bar",
    11: "ホストクラブ",
    10: "ヘルス・ソープ",
}

_PER_PAGE = 50  # read timeout を避けるため 50 件以下でページング

# 47 都道府県の先頭一致パターン (住所の先頭から都道府県を切り出す)
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POSTCODE_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")


class TokushimaNightStyleScraper(StaticCrawler):
    """徳島ナイトスタイルリスト (tokushima-night-style.com) 店舗情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["座席数"]  # 構造化された短い値。長文プロースは含めない

    def parse(self, url: str):
        # WP REST エンドポイントは引数 url (SSOT) から派生させる
        api = urljoin(url, "index.php")
        self.total_items = 0

        for cat_id, cat_name in _SHOP_CATEGORIES.items():
            page = 1
            while True:
                params = {
                    "rest_route": "/wp/v2/posts",
                    "categories": cat_id,
                    "per_page": _PER_PAGE,
                    "page": page,
                }
                resp = self.session.get(api, params=params, timeout=self.TIMEOUT)
                if resp.status_code == 400:
                    # ページ範囲外 (rest_post_invalid_page_number) → このカテゴリ終了
                    break
                resp.raise_for_status()

                if page == 1:
                    try:
                        self.total_items += int(resp.headers.get("X-WP-Total", 0))
                    except (TypeError, ValueError):
                        pass

                posts = resp.json()
                if not posts:
                    break

                for post in posts:
                    try:
                        item = self._parse_post(post, cat_name)
                        if item:
                            yield item
                    except Exception as exc:  # noqa: BLE001
                        self.error_count += 1
                        import logging
                        logging.getLogger(__name__).warning(
                            "投稿の解析に失敗 (スキップ): id=%s — %s",
                            post.get("id"), exc,
                        )
                        continue

                if len(posts) < _PER_PAGE:
                    break
                page += 1

    def _parse_post(self, post: dict, cat_name: str) -> dict | None:
        soup = BeautifulSoup(post.get("content", {}).get("rendered", ""), "html.parser")

        # 店名: 投稿タイトルを優先
        name = BeautifulSoup(
            post.get("title", {}).get("rendered", ""), "html.parser"
        ).get_text(strip=True)

        # サイト定義ジャンル: content-title "店名 / ジャンル" の "/" 以降を採用、無ければカテゴリ名
        cat_site = cat_name
        ct = soup.select_one("p.content-title")
        if ct:
            txt = ct.get_text(strip=True)
            if "/" in txt:
                cat_site = txt.rsplit("/", 1)[1].strip()

        # 店舗情報テーブル (td ラベル / td 値) を辞書化
        fields: dict[str, tuple[str, "BeautifulSoup"]] = {}
        for tr in soup.select("table.wp-block-table tr"):
            tds = tr.find_all("td")
            if len(tds) >= 2:
                label = tds[0].get_text(strip=True)
                if label:
                    fields[label] = (tds[1].get_text(" ", strip=True), tds[1])

        if not name:
            return None

        item = {
            Schema.NAME: name,
            Schema.CAT_SITE: cat_site,
            Schema.URL: post.get("link", ""),
        }

        # 住所 → PREF / POST_CODE / ADDR
        addr = fields.get("住所", ("", None))[0]
        if addr:
            pc = _POSTCODE_PATTERN.search(addr)
            if pc:
                item[Schema.POST_CODE] = pc.group(1)
                addr = _POSTCODE_PATTERN.sub("", addr).strip()
            m = _PREF_PATTERN.match(addr)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr[m.end():].strip()
            else:
                item[Schema.ADDR] = addr

        if fields.get("TEL"):
            item[Schema.TEL] = fields["TEL"][0]
        if fields.get("営業時間"):
            item[Schema.TIME] = fields["営業時間"][0]
        if fields.get("定休日"):
            item[Schema.HOLIDAY] = fields["定休日"][0]
        if fields.get("カード"):
            item[Schema.PAYMENTS] = fields["カード"][0]
        if fields.get("座席数"):
            item["座席数"] = fields["座席数"][0]

        # HP: 表内の外部リンク (自サイト/SNS/tel を除く) があれば取得
        for a in soup.select("table.wp-block-table a[href]"):
            href = a.get("href", "")
            if href.startswith("http") and not re.search(
                r"tokushima-night-style|instagram|line\.me|twitter|x\.com|"
                r"facebook|tiktok",
                href,
            ):
                item[Schema.HP] = href
                break

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = TokushimaNightStyleScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.tokushima-night-style.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
