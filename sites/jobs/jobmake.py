"""
ジョブメイク — ナイトワーク求人情報サイト (jobmake.jp) のスクレイパー

取得対象:
    - 検索結果ページに掲載されるナイトワーク求人店舗（キャバクラ/ラウンジ/スナック等）
    - 店舗名・住所・業種(ジャンル)・職種・給与・エリア・勤務時間・こだわり条件

取得フロー:
    1. 検索ページ (引数 url) を取得する。url のリージョン(例 /48/=東京)は
       在庫ゼロのことがあるため、ページ内「地域から探す」ナビに並ぶ
       都道府県リンク (/NN/) を全件抽出し、各都道府県の検索ページ
       /NN/search を巡回対象にする（いずれも引数 url から urljoin で派生）。
    2. 各検索ページの `div.shop` (求人店舗カード) を全件列挙
       ※ このサイトの検索結果はページネーション無し（全件を1ページに表示）
    3. 各カードの `a.shop-more-info` から詳細ページ (/{region}/shops/{id}) へ遷移
    4. 詳細ページの基本情報 dl から店舗名・住所・エリア・勤務時間を取得し、
       カード側の業種/職種/給与/こだわり条件とマージして 1 件ずつ即 yield

備考:
    - 「取れそうなカラムは全部取る」方針。ただしキャッチコピー/待遇/Message 等の
      自由記述プロースは著作権リスク回避のため取得しない。
    - TEL/メール/HP/SNS/設立/代表者/資本金 等は当サイトに掲載が無い（応募はフォーム）。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/jobmake.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jobmake
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 住所先頭から都道府県を抽出する（掲載されている場合のみ。無ければ空文字）
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県|大分県)"
)


# 「地域から探す」ナビの都道府県トップへのリンク (例: /48/, /33/) のパス形式
_REGION_PATH = re.compile(r"^/(\d+)/$")


def _norm_label(text: str) -> str:
    """dt ラベルから空白（全角含む）を除去して照合用に正規化する。"""
    return re.sub(r"\s+", "", text or "")


def _clean(text: str) -> str:
    """セル値の前後空白を整え、連続する空白・改行を 1 つの空白にまとめる。"""
    return re.sub(r"[　\s]+", " ", (text or "").strip())


class Jobmake(StaticCrawler):
    """ジョブメイク スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["職種", "給与", "エリア", "勤務時間", "こだわり条件"]

    def parse(self, url: str):
        root = self.get_soup(url)
        region_urls = self._discover_region_search_urls(root, url)
        logger.info("「地域から探す」ナビから %d 都道府県を巡回対象に検出", len(region_urls))

        for region_url in region_urls:
            soup = root if region_url == url else self.get_soup(region_url)
            if soup is None:
                continue
            cards = soup.select("div.shop")
            logger.info("%s から %d 件の店舗カードを検出", region_url, len(cards))

            for card in cards:
                try:
                    item = self._parse_card(card, region_url)
                    if not item:
                        continue
                    # 詳細ページからクリーンな店舗名・住所等を補完
                    if item.pop("_detail_url", None):
                        detail = self._scrape_detail(item.pop("_detail_url_value"))
                        if detail:
                            item.update({k: v for k, v in detail.items() if v})
                    yield item
                except Exception as exc:  # noqa: BLE001 — 個別カードのエラーで全体を止めない
                    logger.warning("カードの処理に失敗: %s", exc)
                    continue

    def _discover_region_search_urls(self, soup, root_url: str) -> list[str]:
        """検索ページの「地域から探す」ナビから都道府県別検索ページ URL を収集する。

        引数 root_url のリージョン(例 /48/=東京)は在庫が無いことがあるため、
        ナビに並ぶ全都道府県トップ (/NN/) を抽出し、それぞれの検索ページ
        /NN/search を巡回対象にする。URL はすべて root_url から urljoin で派生。
        """
        # 取得失敗時のフォールバックとして root_url 自身の検索ページを必ず先頭に含める
        ordered: dict[str, None] = {root_url: None}
        if soup is not None:
            for a in soup.select("a[href]"):
                abs_url = urljoin(root_url, (a.get("href") or "").strip())
                if _REGION_PATH.match(urlsplit(abs_url).path):
                    # /NN/ → /NN/search （root_url から派生したいので search を結合）
                    ordered.setdefault(urljoin(abs_url, "search"), None)
        return list(ordered)

    def _parse_card(self, card, root_url: str) -> dict | None:
        """検索結果カード (div.shop) から取得できるフィールドを抽出する。"""
        link_el = card.select_one("a.shop-more-info")
        if not link_el:
            return None
        detail_url = urljoin(root_url, link_el.get("href", ""))

        # カードのタイトルは「<店名>のナイトワーク情報」形式 → 接尾辞を除去
        raw_name = link_el.get_text(strip=True)
        name = re.sub(r"の(ナイトワーク情報|求人.*)$", "", raw_name).strip()

        cat_el = card.select_one("span.shop-cat")
        category = cat_el.get_text(strip=True) if cat_el else ""

        # カード内 INFORMATION dl（ジャンル/職種/給与/勤務地）
        dl_map: dict[str, str] = {}
        for dt in card.select("div.dl-inline dl dt"):
            dd = dt.find_next_sibling("dd")
            if dd is not None:
                dl_map[_norm_label(dt.get_text())] = _clean(dd.get_text(" "))

        if not category:
            category = dl_map.get("ジャンル", "")

        # こだわり条件アイコン（.off クラスは「該当なし」なので除外）
        kodawari = [
            li.img.get("alt", "").strip()
            for li in card.select("ul.kodawari li:not(.off)")
            if li.img and li.img.get("alt")
        ]

        item = {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.CAT_SITE: category,
            "職種": dl_map.get("職種", ""),
            "給与": dl_map.get("給与", ""),
            "こだわり条件": " / ".join(kodawari),
            # フォールバック住所（詳細で上書きされる）
            Schema.ADDR: dl_map.get("勤務地", ""),
            "_detail_url": True,
            "_detail_url_value": detail_url,
        }
        # フォールバック住所から都道府県を抽出
        self._apply_pref(item, item[Schema.ADDR])
        return item

    def _scrape_detail(self, url: str) -> dict | None:
        """詳細ページ (/{region}/shops/{id}) から店舗名・住所・エリア・勤務時間を取得する。"""
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 求人募集要項 + 店舗基本情報の dl をまとめて辞書化（ラベルは空白除去）
        labels: dict[str, str] = {}
        for dt in soup.select("div.dl-block dl dt"):
            dd = dt.find_next_sibling("dd")
            if dd is not None:
                labels[_norm_label(dt.get_text())] = _clean(dd.get_text(" "))

        detail: dict = {}
        if labels.get("店舗名"):
            detail[Schema.NAME] = labels["店舗名"]
        if labels.get("業種"):
            detail[Schema.CAT_SITE] = labels["業種"]
        if labels.get("エリア"):
            detail["エリア"] = labels["エリア"]
        if labels.get("勤務時間"):
            detail["勤務時間"] = labels["勤務時間"]

        address = labels.get("住所", "")
        if address:
            detail[Schema.ADDR] = address
            self._apply_pref(detail, address)
        return detail

    @staticmethod
    def _apply_pref(item: dict, address: str) -> None:
        """住所先頭に都道府県があれば PREF と ADDR を分離する。"""
        m = _PREF_PATTERN.match(address or "")
        if m:
            item[Schema.PREF] = m.group(1)
            item[Schema.ADDR] = address[m.end():].strip()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Jobmake()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.jobmake.jp/48/search?gf=f&kt=t")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
