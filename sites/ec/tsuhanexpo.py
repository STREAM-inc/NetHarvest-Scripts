"""
通販食品展示商談会 出展者一覧 (tsuhanexpo.com) スクレイパー

取得対象:
    第16回「通販食品展示商談会」の出展者一覧 (https://tsuhanexpo.com/exhibitor/)
    会社名 / 出展分野 (カテゴリー) / 出展製品 (取扱商品) / 所在地 (都道府県) / HP

取得フロー:
    1. ルート (sites.yml の url) の一覧ページを取得し、`div.exh_box_list` から
       出展者カード (会社名・詳細ページ URL・出展予定製品) を列挙する。
       ページ送りは wp-pagenavi の `a.nextpostslink` を辿る (= /exhibitor/page/N/)。
    2. 出展者 1 件ごとに詳細ページ (/mailorder/{id}/) を取得し、
       会社名 (h2.detail_title) / HP (div.detail_url a) / カテゴリー (div.detail_cat a)
       / 出展製品名 / 製品カテゴリタグ を抽出して即 yield する (Pattern B)。
    3. 所在地: 詳細・一覧ページには住所表記が無く、サイト内の唯一の所在地情報は
       `pref` タクソノミ (一覧の絞り込み「都道府県」) である。
       wp-sitemap-taxonomies-pref-1.xml で実在する都道府県を特定し、
       一覧の絞り込み検索 (`/?s=&check02[]={term_id}`) を都道府県ごとに 1 回ずつ
       実行して「詳細ページ URL → 都道府県」の対応表を一度だけ構築する。
       掲載が無い出展者は空欄のままとする (備考「所在地（掲載があれば）」)。

備考の遵守:
    - 取得カラムは 会社名 / 出展分野・取扱商品 / 所在地 / HP を中心に構成。
    - 「企業PR」「製品説明」は長文の自由記述 (プロース) のため著作権リスクを避けて取得しない。

利用規約:
    - サイトに利用規約ページは存在せず (固定ページ一覧に無し)、
      唯一のポリシー (exhibitiontech.com/privacy.html = 個人情報保護方針) にも
      スクレイピング・クローリングを禁止する記述は無い。
    - robots.txt は /wp-admin/ のみ Disallow で、本クロール対象は許可されている。

実行方法:
    python scripts/sites/ec/tsuhanexpo.py
    docker compose exec worker python /app/bin/run_flow.py --site-id tsuhanexpo
"""

import logging
import re
import sys
import time
import warnings
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import XMLParsedAsHTMLWarning

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# sitemap (XML) を html.parser で読むため、bs4 の警告を抑止する
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

logger = logging.getLogger(__name__)

# JIS コード順の都道府県名 (index 1 = 北海道)。pref タクソノミの slug (p13tokyo) から引く。
_PREF_BY_JIS = [
    "",
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# pref タクソノミの sitemap (都道府県 slug の一覧)
_PREF_SITEMAP_PATH = "/wp-sitemap-taxonomies-pref-1.xml"
# 都道府県 slug: /pref/p13tokyo/ → JIS 13
_PREF_SLUG_RE = re.compile(r"/pref/p(\d{2})[^/]*/?$")
# 絞り込みチェックボックスの term_id と JIS コードのオフセット既定値 (北海道 = 28)
_DEFAULT_PREF_TERM_OFFSET = 27

# 出展者詳細ページ
_DETAIL_RE = re.compile(r"/mailorder/\d+/?$")

# 安全弁 (一覧ページ / 都道府県検索ページの上限)
_MAX_LIST_PAGES = 50
_MAX_SEARCH_PAGES = 20


class Tsuhanexpo(StaticCrawler):
    """通販食品展示商談会 出展者一覧スクレイパー"""

    DELAY = 1.0
    CONTINUE_ON_ERROR = True
    # Schema に該当が無いサイト固有カラム (いずれも短いラベル/製品名で自由記述文ではない)
    EXTRA_COLUMNS = ["出展製品", "出展製品カテゴリ"]

    def parse(self, url: str):
        seen: set[str] = set()
        # 都道府県対応表 (詳細ページ URL → 都道府県)。最初のアイテムの直前に一度だけ構築する。
        self._pref_map: dict[str, str] | None = None

        page_url = url
        for page_no in range(1, _MAX_LIST_PAGES + 1):
            soup = self.get_soup(page_url)
            if soup is None:
                logger.warning("一覧ページを取得できませんでした (中断): %s", page_url)
                return

            cards = self._extract_cards(soup, page_url)
            if not cards:
                logger.info("出展者カードが見つからないため終了しました: %s", page_url)
                return
            logger.info("一覧 %d ページ目: %d 件", page_no, len(cards))

            # 都道府県対応表はルート (1 ページ目) の絞り込みフォームから構築する
            if self._pref_map is None:
                self._pref_map = self._build_pref_map(soup, url)

            for card in cards:
                if card["detail_url"] in seen:
                    continue
                seen.add(card["detail_url"])
                yield self._build_item(card)

            next_url = self._next_page(soup, page_url)
            if not next_url or next_url == page_url:
                logger.info("最終ページに到達しました (計 %d 件)", len(seen))
                return
            page_url = next_url

    # ------------------------------------------------------------------
    # 一覧ページ
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_cards(soup, page_url: str) -> list[dict]:
        """一覧ページの出展者カードから 詳細 URL / 会社名 / 出展予定製品 を取り出す。"""
        cards = []
        for box in soup.select("div.exh_box_list"):
            link = box.select_one("div.exh_con_list h2 a[href]")
            if link is None:
                continue
            detail_url = urljoin(page_url, link["href"])
            if not _DETAIL_RE.search(urlparse(detail_url).path):
                continue
            products = [
                li.get_text(" ", strip=True)
                for li in box.select("div.exh_cat_list li")
                if li.get_text(strip=True)
            ]
            cards.append({
                "detail_url": detail_url,
                "name": link.get_text(" ", strip=True),
                "products": products,
            })
        return cards

    @staticmethod
    def _next_page(soup, page_url: str) -> str | None:
        """wp-pagenavi の「次のページ」リンクを返す。無ければ None。"""
        nxt = soup.select_one("div.pnavi a.nextpostslink[href], .wp-pagenavi a.nextpostslink[href]")
        if nxt is None:
            return None
        return urljoin(page_url, nxt["href"])

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _build_item(self, card: dict) -> dict:
        detail_url = card["detail_url"]
        name = card["name"]
        products = list(card["products"])
        categories: list[str] = []
        product_tags: list[str] = []
        homepage = ""

        soup = self.get_soup(detail_url)
        if soup is None:
            logger.warning("詳細ページ取得失敗 (一覧の情報のみ採用): %s", detail_url)
        else:
            title = soup.select_one("h2.detail_title")
            if title and title.get_text(strip=True):
                name = title.get_text(" ", strip=True)

            hp_link = soup.select_one("div.detail_url a[href]")
            if hp_link:
                homepage = hp_link["href"].strip()

            # 出展分野 (カテゴリー タクソノミ)
            categories = self._uniq(
                a.get_text(" ", strip=True)
                for a in soup.select("div.detail_cat a")
            )
            # 出展製品 (詳細ページの製品名。一覧の「出展予定製品」より正確)
            detail_products = self._uniq(
                h.get_text(" ", strip=True)
                for h in soup.select("div.exh_con h2, div.exh_con_li h2")
            )
            if detail_products:
                products = detail_products
            # 製品ごとに付与された分類タグ
            product_tags = self._uniq(
                s.get_text(" ", strip=True)
                for s in soup.select("div.exh_cat span, div.exh_cat_li span")
            )

        if not name:
            logger.warning("会社名がサイト上に掲載されていません (空欄で出力): %s", detail_url)

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: self._pref_of(detail_url),
            Schema.HP: homepage,
            Schema.CAT_SITE: "、".join(categories),
            "出展製品": "、".join(products),
            "出展製品カテゴリ": "、".join(product_tags),
        }

    @staticmethod
    def _uniq(values) -> list[str]:
        out: list[str] = []
        for v in values:
            v = re.sub(r"\s+", " ", v or "").strip()
            if v and v not in out:
                out.append(v)
        return out

    # ------------------------------------------------------------------
    # 所在地 (都道府県) — pref タクソノミの絞り込み検索から対応表を作る
    # ------------------------------------------------------------------
    def _pref_of(self, detail_url: str) -> str:
        return (self._pref_map or {}).get(detail_url.rstrip("/") + "/", "")

    def _build_pref_map(self, list_soup, root_url: str) -> dict[str, str]:
        """詳細ページ URL → 都道府県 の対応表を構築する (1 都道府県あたり 1 リクエスト)。"""
        pref_map: dict[str, str] = {}
        offset = self._pref_term_offset(list_soup)

        jis_codes = self._pref_jis_codes(root_url)
        if not jis_codes:
            logger.warning("都道府県タクソノミを取得できませんでした。所在地は空欄になります。")
            return pref_map

        origin = "{0.scheme}://{0.netloc}".format(urlparse(root_url))
        for jis in jis_codes:
            pref_name = _PREF_BY_JIS[jis] if 1 <= jis <= 47 else ""
            if not pref_name:
                continue
            search_url = f"{origin}/?s=&check02%5B%5D={jis + offset}"
            for _ in range(_MAX_SEARCH_PAGES):
                soup = self.get_soup(search_url)
                if soup is None:
                    break
                for card in self._extract_cards(soup, search_url):
                    key = card["detail_url"].rstrip("/") + "/"
                    # 複数都道府県が紐づく出展者があるため、JIS 順で最初の 1 件を採用する
                    pref_map.setdefault(key, pref_name)
                nxt = self._next_page(soup, search_url)
                if not nxt or nxt == search_url:
                    break
                search_url = nxt
            time.sleep(0.2)  # 連続アクセスの負荷軽減

        logger.info("都道府県対応表: %d 件 (%d 都道府県)", len(pref_map), len(jis_codes))
        return pref_map

    def _pref_jis_codes(self, root_url: str) -> list[int]:
        """pref タクソノミの sitemap から、実際に使われている都道府県の JIS コードを取得する。"""
        sitemap_url = urljoin(root_url, _PREF_SITEMAP_PATH)
        soup = self.get_soup(sitemap_url)
        if soup is None:
            return []
        codes: list[int] = []
        for loc in soup.find_all("loc"):
            m = _PREF_SLUG_RE.search(loc.get_text(strip=True))
            if m:
                jis = int(m.group(1))
                if 1 <= jis <= 47 and jis not in codes:
                    codes.append(jis)
        return codes

    @staticmethod
    def _pref_term_offset(list_soup) -> int:
        """絞り込みフォームの「北海道」チェックボックス値から term_id と JIS のオフセットを求める。"""
        for inp in list_soup.select('input[name="check02[]"][value]'):
            label = inp.find_next("span", class_="item-label")
            if label is not None and label.get_text(strip=True) == "北海道":
                try:
                    return int(inp["value"]) - 1
                except (TypeError, ValueError):
                    break
        logger.debug("北海道のチェックボックスを特定できませんでした。既定オフセットを使用します。")
        return _DEFAULT_PREF_TERM_OFFSET


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Tsuhanexpo()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://tsuhanexpo.com/exhibitor/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
