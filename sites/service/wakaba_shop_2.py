"""
買取店わかば — 全国店舗一覧 (リユースショップ WAKABA / BuySell Technologies)

対象サイト: https://wakaba-shop.jp/shop/zenkoku_all/

取得対象:
    - 全国店舗一覧 1 ページ (約 1.6MB・約 290 店) に掲載された全店舗
      (都道府県の絞り込みは後工程で行うため全国分を出力する)

取得フロー:
    - 起点 URL を 1 回だけ取得する (ページ送り・詳細ページ取得は無し)。
    - `div.p-shop-list-all__list` が都道府県セクションで、見出し h2 が都道府県名。
    - 各店舗は `dl.c-acc__item`。dt=店舗名、`li.c-shop-info-list__item` が
      住所 / 定休日 / 営業時間 (アイコンの `--address` / `--time` クラスで判別、
      定休日だけ識別子が無いので残り物として拾う)。
    - 連絡先 `ul.c-shop-info-list__contact` は PC 用 / SP 用で 2 重出力されるため先勝ち。
      「無料通話」0800-888-8883 は全店共通の窓口番号なので TEL には使わない。
      電話番号欄が無く店舗固有の 0120 番号だけ載っている店舗 (約 18 店) はそれを TEL にする。
    - 店舗 URL は `a.c-btn__red` の href (/shop/<slug>/)。約 14 店はボタン自体が無く空。
    - 営業状態注記は「※2026年12月1日オープン予定」(営業時間欄) や「臨時休業中」
      (定休日欄) などの表記を拾い、元の欄からは取り除く。
      「臨時休業 11月22日」のような日付付きの休業日は定休日として残す。

備考:
    - robots.txt は /wpold/ ・/shops/ ・/shop/*/results/ ・/shoplist/ を Disallow。
      本クローラーは /shop/zenkoku_all/ のみを取得し、店舗個別ページにもアクセスしない。
    - 利用規約 (https://buysell-technologies.com/terms/) にスクレイピングの明示禁止は無い。

実行方法:
    # ローカルテスト
    python scripts/sites/service/wakaba_shop_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id wakaba_shop_2
"""

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

# 郵便番号 (〒096-0071 形式)
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-\d{4})")

# 都道府県
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|"
    r"千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|"
    r"愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|"
    r"広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|"
    r"宮崎県|鹿児島県|沖縄県)"
)

# 営業状態注記 (「※2026年12月1日オープン予定」「臨時休業中」など)
_STATUS_RE = re.compile(
    r"※?\s*[^\s※（）()]*?(?:オープン予定|臨時休業中|休業中|閉店予定|閉店しました|移転予定|リニューアル(?:オープン)?予定)"
)

# 全店共通の無料通話窓口 (店舗固有番号ではないので TEL に使わない)
_COMMON_FREE_DIAL = "0800-888-8883"

# EXTRA カラム名
_COL_FAX = "FAX"


class WakabaShop2(StaticCrawler):
    """買取店わかば 全国店舗一覧 スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = [_COL_FAX]

    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("全国店舗一覧を取得できませんでした: %s", url)
            return

        seen: set[str] = set()
        for section in soup.select("div.p-shop-list-all__list"):
            heading = section.find("h2")
            section_pref = heading.get_text(strip=True) if heading else ""

            for dl in section.select("dl.c-acc__item"):
                item = self._parse_shop(url, dl, section_pref)
                if not item:
                    continue
                key = item[Schema.HP] or f"{item[Schema.NAME]}|{item[Schema.ADDR]}"
                if key in seen:
                    continue
                seen.add(key)
                yield item

    # ------------------------------------------------------------------ #
    # 1 店舗分 (dl.c-acc__item)
    # ------------------------------------------------------------------ #
    def _parse_shop(self, root_url: str, dl, section_pref: str) -> dict | None:
        dt = dl.find("dt")
        name = self._text(dt)
        if not name:
            return None

        address = holiday = time_ = ""
        statuses: list[str] = []
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
                continue

            # 営業時間 / 定休日 欄に混ざる営業状態注記を切り出す
            value = self._extract_status(value, statuses)
            if "c-shop-info-list__icon--time" in icon_class:
                time_ = value
            else:
                # 住所/営業時間 以外のアイコン付き項目は定休日
                holiday = value

        tel = fax = free_dial = ""
        # 連絡先ブロックは PC 用 / SP 用で重複して出力されるため先勝ちで拾う
        for li in dl.select("ul.c-shop-info-list__contact li"):
            label = self._text(li.select_one(".c-shop-info-list__contact-item-ttl"))
            value = self._text(li.select_one(".c-shop-info-list__contact-item-contents"))
            if not value:
                continue
            if "電話番号" in label and not tel:
                tel = value
            elif "無料通話" in label and not free_dial and value != _COMMON_FREE_DIAL:
                free_dial = value
            elif "FAX" in label.upper() and not fax:
                fax = value

        # 電話番号欄が無く店舗固有のフリーダイヤルだけ載っている店舗はそれを使う
        tel = tel or free_dial

        post_code, pref, addr = self._split_address(address)

        anchor = dl.select_one("a.c-btn__red[href]")
        shop_url = urljoin(root_url, anchor["href"]) if anchor else ""

        return {
            Schema.NAME: name,
            Schema.URL: root_url,
            Schema.PREF: pref or section_pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            _COL_FAX: fax,
            Schema.TIME: time_,
            Schema.HOLIDAY: holiday,
            Schema.HP: shop_url,
            Schema.STS_NM: " ".join(statuses),
        }

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract_status(value: str, statuses: list[str]) -> str:
        """value から営業状態注記を取り除き、注記は statuses に追加する。"""
        for m in _STATUS_RE.finditer(value):
            note = m.group(0).strip()
            if note and note not in statuses:
                statuses.append(note)
        return re.sub(r"\s+", " ", _STATUS_RE.sub(" ", value)).strip()

    @staticmethod
    def _text(element) -> str:
        """改行・連続空白を 1 つの空白に潰したテキストを返す。"""
        if element is None:
            return ""
        return re.sub(r"\s+", " ", element.get_text(" ", strip=True)).strip()

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        """「〒096-0071 北海道名寄市…」→ (郵便番号, 都道府県, 都道府県以降の住所)。"""
        text = (raw or "").strip()
        if not text:
            return "", "", ""

        post_code = ""
        m = _POST_CODE_RE.search(text)
        if m:
            post_code = m.group(1)
            text = (text[: m.start()] + text[m.end():]).strip()

        pref = ""
        m = _PREF_RE.search(text)
        if m:
            # Schema.ADDR は「市区町村以降」。都道府県名は PREF に分離する
            pref = m.group(1)
            text = (text[: m.start()] + text[m.end():]).strip()

        return post_code, pref, re.sub(r"\s+", " ", text).strip()


if __name__ == "__main__":
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = WakabaShop2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://wakaba-shop.jp/shop/zenkoku_all/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
