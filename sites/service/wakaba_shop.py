"""
買取店わかば — 新規オープン情報 (リユースショップ WAKABA / BuySell Technologies)

取得対象:
    - オープン情報一覧 (https://wakaba-shop.jp/news/) の記事のうち、
      件名に「オープン」を含むもの
    - オープン日が 2026-01-01 以降の記事のみ (一覧は新しい順。
      2026-01-01 より前の記事が出た時点でページ送りを打ち切る)
    - 都道府県が対象 14 都県 (東京/神奈川/埼玉/千葉/茨城/栃木/群馬/
      新潟/山梨/長野/岐阜/静岡/愛知/三重) のものだけを出力する

取得フロー:
    - 起点 URL (/news/) を `?paged=N` でページ送りし、
      `li.p-open-list__item` から 掲載日 + 記事タイトル + 記事 URL を得る。
      タイトル「買取店わかば○○店がオープン！」から店舗名を切り出す。
    - 店舗の連絡先 (郵便番号/住所/TEL/FAX/営業時間/定休日/店舗ページ URL) は
      全国店舗一覧 /shop/zenkoku_all/ を 1 回だけ取得してインデックス化し、
      店舗名で引き当てる (都道府県は見出し h2 のセクション名)。
    - 全国店舗一覧に載っていない店舗 (閉店済み・表記揺れ等) のみ、
      記事本文 (/news/{id}/) をフォールバックで取得し、
      本文中の「郵便番号：〒...／住所：...」から郵便番号・住所・都道府県を得る。
    - 1 店舗ぶん組み立てたらその場で yield する (全件バッファしない)。

備考:
    - 記事本文の紹介文など長文の自由記述は著作権リスクのため取得しない。
    - FAX は全国店舗一覧に掲載がある店舗のみ (約 1/3)。無い場合は空文字。
    - robots.txt は /shops/ ・/shop/*/results/ ・/shoplist/ を Disallow。
      本クローラーが見る /news/ と /shop/zenkoku_all/ はいずれも許可範囲。

実行方法:
    # ローカルテスト
    python scripts/sites/service/wakaba_shop.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id wakaba_shop
"""

import datetime
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 全国店舗一覧 (起点 URL からの相対パスで解決する)
_SHOP_LIST_PATH = "/shop/zenkoku_all/"

# これより前のオープン日は対象外 (一覧は新しい順なので到達時点で打ち切る)
_SINCE = datetime.date(2026, 1, 1)

# 出力対象の都道府県
_TARGET_PREFS = {
    "東京都", "神奈川県", "埼玉県", "千葉県", "茨城県", "栃木県", "群馬県",
    "新潟県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県", "三重県",
}

# 暴走防止のページ上限 (2026-10 時点で一覧は全 43 ページ)
_MAX_PAGES = 100

# 一覧の掲載日「2026年10月02日」
_DATE_RE = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")

# 記事タイトル「買取店わかば天白八事店がオープン！」→ 店舗名「天白八事店」
_TITLE_RE = re.compile(r"^買取店わかば\s*(.+?)\s*が(?:新規)?オープン")

# 郵便番号 (〒496-8001 形式)
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-\d{4})")

# 記事本文「郵便番号：〒496-8001」「住所：愛知県愛西市…」
_BODY_POST_RE = re.compile(r"郵便番号\s*[:：]\s*〒?\s*(\d{3}-?\d{4})")
_BODY_ADDR_RE = re.compile(r"住所\s*[:：]\s*(.+)")

# 都道府県
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|"
    r"千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|"
    r"愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|"
    r"広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|"
    r"宮崎県|鹿児島県|沖縄県)"
)

# EXTRA カラム名
_COL_FAX = "FAX"


class WakabaShop(StaticCrawler):
    """買取店わかば 新規オープン情報 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [_COL_FAX]

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        # 全国店舗一覧のインデックス (遅延ロード。1 回だけ取得する)
        shop_index: dict[str, dict] | None = None

        seen: set[str] = set()
        for page in range(1, _MAX_PAGES + 1):
            soup = self.get_soup(self._page_url(url, page))
            if soup is None:
                break

            items = soup.select("li.p-open-list__item")
            if not items:
                logger.info("記事が無くなったため終了しました (page=%s)", page)
                break

            reached_cutoff = False
            for li in items:
                open_date = self._parse_date(li.select_one(".p-open-list__date"))
                if open_date is None:
                    continue
                if open_date < _SINCE:
                    # 一覧は新しい順なので、ここから先はすべて対象外
                    logger.info("オープン日が %s より前になったため打ち切ります", _SINCE)
                    reached_cutoff = True
                    break

                title_el = li.select_one(".p-open-list__ttl")
                title = title_el.get_text(strip=True) if title_el else ""
                if "オープン" not in title:
                    continue

                anchor = li.find("a", href=True)
                if not anchor:
                    continue
                article_url = urljoin(url, anchor["href"])
                if article_url in seen:
                    continue
                seen.add(article_url)

                if shop_index is None:
                    shop_index = self._build_shop_index(url)

                item = self._build_item(url, article_url, title, open_date, shop_index)
                if item:
                    yield item

            if reached_cutoff:
                break

    # ------------------------------------------------------------------ #
    # 1 店舗分の組み立て
    # ------------------------------------------------------------------ #
    def _build_item(
        self,
        root_url: str,
        article_url: str,
        title: str,
        open_date: datetime.date,
        shop_index: dict[str, dict],
    ) -> dict | None:
        m = _TITLE_RE.match(title)
        if not m:
            logger.debug("店舗名を切り出せませんでした: %s", title)
            return None
        short_name = m.group(1)

        # 「○○駅前」のように末尾の「店」が抜けている表記ゆれを吸収する
        shop = shop_index.get(short_name) or shop_index.get(short_name + "店") or {}

        pref = shop.get("pref", "")
        post_code, addr = self._split_address(shop.get("address", ""))

        if not pref or not addr:
            # 全国店舗一覧に無い店舗だけ、記事本文から住所を補う
            body_post, body_pref, body_addr = self._scrape_article_address(article_url)
            pref = pref or body_pref
            post_code = post_code or body_post
            addr = addr or body_addr

        if pref not in _TARGET_PREFS:
            logger.debug("対象外の都道府県のためスキップ: %s (%s)", title, pref or "不明")
            return None

        shop_url = shop.get("shop_url", "")
        return {
            Schema.NAME: f"買取店わかば{short_name}",
            Schema.URL: article_url,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: shop.get("tel", ""),
            _COL_FAX: shop.get("fax", ""),
            Schema.TIME: shop.get("time", ""),
            Schema.HOLIDAY: shop.get("holiday", ""),
            Schema.OPEN_DATE: open_date.isoformat(),
            Schema.HP: urljoin(root_url, shop_url) if shop_url else "",
        }

    # ------------------------------------------------------------------ #
    # 全国店舗一覧 (/shop/zenkoku_all/) のインデックス化
    # ------------------------------------------------------------------ #
    def _build_shop_index(self, root_url: str) -> dict[str, dict]:
        """店舗名 -> {pref, address, tel, fax, time, holiday, shop_url} を返す。"""
        index: dict[str, dict] = {}

        soup = self.get_soup(urljoin(root_url, _SHOP_LIST_PATH))
        if soup is None:
            logger.warning("全国店舗一覧を取得できませんでした。記事本文のみで組み立てます")
            return index

        for section in soup.select("div.p-shop-list-all__list"):
            heading = section.find("h2")
            pref = heading.get_text(strip=True) if heading else ""

            for dl in section.select("dl.c-acc__item"):
                dt = dl.find("dt")
                if not dt:
                    continue
                name = dt.get_text(strip=True)
                if not name or name in index:
                    continue
                index[name] = self._parse_shop_block(dl, pref)

        logger.info("全国店舗一覧から %s 店舗をインデックス化しました", len(index))
        return index

    def _parse_shop_block(self, dl, pref: str) -> dict:
        """全国店舗一覧の 1 店舗ぶん (dl.c-acc__item) から項目を取り出す。"""
        address = holiday = time_ = ""
        for li in dl.select("li.c-shop-info-list__item"):
            if li.select_one(".c-shop-info-img-list"):
                # スマホ用の店舗写真リストは項目ではない
                continue
            icon = li.select_one(".c-shop-info-list__icon")
            icon_class = " ".join(icon.get("class", [])) if icon else ""
            value = self._text(li.select_one(".c-shop-info-list__txt"))
            if not value:
                continue
            if "c-shop-info-list__icon--address" in icon_class:
                address = value
            elif "c-shop-info-list__icon--time" in icon_class:
                time_ = value
            else:
                # 住所/営業時間 以外のアイコン付き項目は定休日
                holiday = value

        tel = fax = ""
        # 連絡先ブロックは PC 用 / SP 用で重複して出力されるため先勝ちで拾う
        for li in dl.select("ul.c-shop-info-list__contact li"):
            label = self._text(li.select_one(".c-shop-info-list__contact-item-ttl"))
            value = self._text(li.select_one(".c-shop-info-list__contact-item-contents"))
            if not value:
                continue
            if "電話" in label and not tel:
                tel = value
            elif "FAX" in label.upper() and not fax:
                fax = value

        anchor = dl.select_one("a.c-btn__red[href]")
        return {
            "pref": pref,
            "address": address,
            "tel": tel,
            "fax": fax,
            "time": time_,
            "holiday": holiday,
            "shop_url": anchor["href"] if anchor else "",
        }

    # ------------------------------------------------------------------ #
    # 記事本文からの住所フォールバック
    # ------------------------------------------------------------------ #
    def _scrape_article_address(self, article_url: str) -> tuple[str, str, str]:
        """記事本文の「郵便番号：／住所：」から (郵便番号, 都道府県, 住所) を返す。"""
        soup = self.get_soup(article_url)
        if soup is None:
            return "", "", ""

        # 本文の段落だけを対象にする (ページ送りリンク等を住所として拾わないため)。
        # 各項目は <br> 区切りなので、改行をそのまま残して行単位で照合する。
        paragraphs = soup.select("div.c-post p.wp-block-paragraph")
        text = "\n".join(p.get_text("\n") for p in paragraphs)

        post_code = ""
        m = _BODY_POST_RE.search(text)
        if m:
            post_code = m.group(1)
            if "-" not in post_code:
                post_code = f"{post_code[:3]}-{post_code[3:]}"

        raw_addr = ""
        m = _BODY_ADDR_RE.search(text)
        if m:
            raw_addr = m.group(1).strip()

        _, pref, addr = "", "", ""
        if raw_addr:
            _, addr = self._split_address(raw_addr)
            pm = _PREF_RE.search(raw_addr)
            pref = pm.group(1) if pm else ""
        return post_code, pref, addr

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #
    @staticmethod
    def _page_url(url: str, page: int) -> str:
        """起点 URL からページ送り URL (?paged=N) を組み立てる。"""
        if page <= 1:
            return url
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}paged={page}"

    @staticmethod
    def _text(element) -> str:
        """改行・連続空白を 1 つの空白に潰したテキストを返す。"""
        if element is None:
            return ""
        return re.sub(r"\s+", " ", element.get_text(" ", strip=True)).strip()

    @staticmethod
    def _parse_date(element) -> datetime.date | None:
        """「2026年10月02日」→ date。読めなければ None。"""
        if element is None:
            return None
        m = _DATE_RE.search(element.get_text(strip=True))
        if not m:
            return None
        try:
            return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str]:
        """「〒468-0066 愛知県名古屋市天白区…」→ (郵便番号, 都道府県以降を除いた住所)。"""
        text = (raw or "").strip()
        if not text:
            return "", ""

        post_code = ""
        m = _POST_CODE_RE.search(text)
        if m:
            post_code = m.group(1)
            text = (text[: m.start()] + text[m.end():]).strip()

        m = _PREF_RE.search(text)
        if m:
            # Schema.ADDR は「市区町村以降」。都道府県名は PREF に分離する
            text = (text[: m.start()] + text[m.end():]).strip()

        return post_code, re.sub(r"\s+", " ", text).strip()


if __name__ == "__main__":
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = WakabaShop()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://wakaba-shop.jp/news/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
