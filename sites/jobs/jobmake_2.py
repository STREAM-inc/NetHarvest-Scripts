"""
ジョブメイク岡山版リスト — ナイトワーク求人情報サイト (jobmake.jp) 岡山県版のスクレイパー

取得対象:
    - 岡山県 (/33/) の検索結果に掲載されるナイトワーク求人店舗
      (キャバクラ / ラウンジ / スナック / クラブ 等)
    - 店舗名・住所・都道府県・業種(ジャンル)・電話番号・職種・給与・エリア・
      最寄り駅・勤務時間・こだわり条件・担当者

取得フロー:
    1. 引数 url (= https://jobmake.jp/33/) から検索ページ /33/search を
       urljoin で派生させて取得する。
       ※ トップ /33/ には注目店舗しか出ないため、全件が並ぶ /search を起点にする。
       ※ このサイトの検索結果はページネーション無し（岡山県の全店舗を 1 ページに表示）。
    2. 検索結果の `div.shop` (求人店舗カード) を全件列挙し、カードから
       こだわり条件アイコンと詳細ページ URL (`a.shop-more-info`) を取得。
    3. 詳細ページ (/33/shops/{id}) の基本情報 dl から
       店舗名・住所・業種・職種・給与・エリア・最寄り駅・勤務時間 を取得。
    4. 電話番号は特殊: 詳細ページの「TELで応募する」先 /33/shops/{id}/tel は
       Laravel の CSRF 保護付きフォームで、ニックネーム(name)を付けて POST 送信
       すると初めて電話番号と担当者名が表示される。GET で _token を取得 →
       ニックネーム付きで POST → レスポンスから TEL(a[href^=tel:]) と担当者を抽出。
    5. 1 店舗ぶんの情報が揃ったら即 yield する（早期 yield / 逐次取得）。

備考:
    - 「岡山版リスト」のため、対象は引数 url のリージョン (/33/=岡山県) のみ。
      他都道府県への横断クロールは行わない（検索ページ・詳細 URL がいずれも
      /33/ 配下なので構造的に岡山県限定になる）。
    - 電話番号は上記のニックネーム送信フローでのみ取得可能（掲載仕様）。
    - 「取れそうな構造化カラムは全部取る」方針。ただし待遇/キャッチコピー/
      店舗紹介文/Message 等の自由記述プロースは著作権リスク回避のため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/jobmake_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jobmake_2
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 電話番号表示に必要なニックネーム（任意の文字列で可）
_NICKNAME = "たろう"

# JIS 都道府県コード → 都道府県名（jobmake の /NN/ はこのコード体系）。
# 岡山県 = 33。住所に都道府県表記が無い(例: "岡山市北区...")ため、URL の
# リージョンコードから都道府県を補完するのに使う。
_JIS_PREF = {
    "01": "北海道", "02": "青森県", "03": "岩手県", "04": "宮城県", "05": "秋田県",
    "06": "山形県", "07": "福島県", "08": "茨城県", "09": "栃木県", "10": "群馬県",
    "11": "埼玉県", "12": "千葉県", "13": "東京都", "14": "神奈川県", "15": "新潟県",
    "16": "富山県", "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県",
    "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県", "25": "滋賀県",
    "26": "京都府", "27": "大阪府", "28": "兵庫県", "29": "奈良県", "30": "和歌山県",
    "31": "鳥取県", "32": "島根県", "33": "岡山県", "34": "広島県", "35": "山口県",
    "36": "徳島県", "37": "香川県", "38": "愛媛県", "39": "高知県", "40": "福岡県",
    "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県", "45": "宮崎県",
    "46": "鹿児島県", "47": "沖縄県",
}

# 住所先頭に都道府県が明記されている場合の抽出用（多くは市名始まりで非該当）
_PREF_PATTERN = re.compile("(" + "|".join(_JIS_PREF.values()) + ")")


def _norm_label(text: str) -> str:
    """dt ラベルから空白（全角含む）を除去して照合用に正規化する。"""
    return re.sub(r"\s+", "", text or "")


def _clean(text: str) -> str:
    """セル値の前後空白を整え、連続する空白・改行を 1 つの空白にまとめる。"""
    return re.sub(r"[　\s]+", " ", (text or "").strip())


class Jobmake2(StaticCrawler):
    """ジョブメイク岡山版リスト スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["職種", "給与", "エリア", "最寄り駅", "勤務時間", "こだわり条件", "担当者"]

    def parse(self, url: str):
        # 引数 url (= /33/) を唯一のルートとし、全件が並ぶ検索ページを派生させる
        search_url = urljoin(url, "search")
        pref = self._region_pref(url)

        soup = self.get_soup(search_url)
        if soup is None:
            logger.warning("検索ページを取得できませんでした: %s", search_url)
            return

        cards = soup.select("div.shop")
        self.total_items = len(cards)
        logger.info("%s から %d 件の店舗カードを検出", search_url, len(cards))

        for card in cards:
            try:
                item = self._parse_card(card, url, pref)
                if item:
                    yield item
            except Exception as exc:  # noqa: BLE001 — 個別カードのエラーで全体を止めない
                logger.warning("カードの処理に失敗: %s", exc)
                continue

    @staticmethod
    def _region_pref(url: str) -> str:
        """引数 url のパス先頭 (/NN/) から JIS コードを取り都道府県名を返す。"""
        m = re.match(r"/(\d+)/", urlsplit(url).path)
        return _JIS_PREF.get(m.group(1), "") if m else ""

    def _parse_card(self, card, root_url: str, pref: str) -> dict | None:
        """検索結果カード (div.shop) を起点に詳細・電話番号までまとめて取得する。"""
        link_el = card.select_one("a.shop-more-info")
        if not link_el:
            return None
        detail_url = urljoin(root_url, link_el.get("href", ""))

        # こだわり条件アイコン（.off クラスは「該当なし」なので除外）
        kodawari = [
            li.img.get("alt", "").strip()
            for li in card.select("ul.kodawari li:not(.off)")
            if li.img and li.img.get("alt")
        ]

        # カード内 dl から勤務地（詳細に住所が無い店舗のフォールバック用）
        card_addr = ""
        for dt in card.select("div.dl-inline dl dt"):
            if _norm_label(dt.get_text()) == "勤務地":
                dd = dt.find_next_sibling("dd")
                if dd is not None:
                    card_addr = _clean(dd.get_text(" "))
                break

        item = {
            Schema.URL: detail_url,
            Schema.PREF: pref,
            "こだわり条件": " / ".join(kodawari),
        }

        detail = self._scrape_detail(detail_url)
        if detail:
            item.update(detail)

        # 詳細に住所が無ければカードの勤務地で補完
        if not item.get(Schema.ADDR) and card_addr:
            self._apply_addr(item, card_addr)

        # 電話番号（ニックネーム送信フロー経由）と担当者
        tel, staff = self._scrape_tel(detail_url)
        if tel:
            item[Schema.TEL] = tel
        if staff:
            item["担当者"] = staff

        if not item.get(Schema.NAME):
            return None
        return item

    def _scrape_detail(self, url: str) -> dict | None:
        """詳細ページ (/33/shops/{id}) の基本情報 dl から各フィールドを取得する。"""
        soup = self.get_soup(url)
        if soup is None:
            return None

        labels: dict[str, str] = {}
        for dt in soup.select("div.dl-block dl dt"):
            dd = dt.find_next_sibling("dd")
            if dd is not None:
                labels[_norm_label(dt.get_text())] = _clean(dd.get_text(" "))

        detail: dict = {
            Schema.NAME: labels.get("店舗名", ""),
            Schema.CAT_SITE: labels.get("業種", ""),
            "職種": labels.get("職種", ""),
            "給与": labels.get("給与", ""),
            "エリア": labels.get("エリア", ""),
            "最寄り駅": labels.get("最寄り駅", ""),
            "勤務時間": labels.get("勤務時間", ""),
        }

        address = labels.get("住所", "")
        if address:
            self._apply_addr(detail, address)
        return detail

    def _scrape_tel(self, detail_url: str) -> tuple[str, str]:
        """詳細ページの「TELで応募する」フォーム (/shops/{id}/tel) から電話番号を取得する。

        GET で CSRF トークン(_token)を取得 → ニックネーム付きで POST 送信すると、
        レスポンスに電話番号と担当者名が表示される。取得できなければ空文字を返す。
        """
        tel_url = urljoin(detail_url, f"{urlsplit(detail_url).path.rstrip('/')}/tel")
        try:
            form_soup = self.get_soup(tel_url)
            if form_soup is None:
                return "", ""
            token_el = form_soup.select_one("input[name=_token]")
            if not token_el or not token_el.get("value"):
                logger.warning("TEL フォームの _token を取得できません: %s", tel_url)
                return "", ""

            resp = self.session.post(
                tel_url,
                data={"_token": token_el["value"], "name": _NICKNAME},
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
            result = BeautifulSoup(resp.text, "html.parser")

            tel = ""
            tel_link = result.select_one('a[href^="tel:"]')
            if tel_link:
                tel = tel_link.get("href", "")[len("tel:"):].strip()

            staff = ""
            box = result.select_one(".tel-box-main")
            if box:
                m = re.search(r"担当者[:：]\s*(\S+)", box.get_text(" ", strip=True))
                if m:
                    staff = m.group(1)
            return tel, staff
        except Exception as exc:  # noqa: BLE001 — 電話番号が取れなくても店舗情報は残す
            logger.warning("電話番号の取得に失敗 (%s): %s", tel_url, exc)
            return "", ""

    @staticmethod
    def _apply_addr(item: dict, address: str) -> None:
        """住所に都道府県表記があれば PREF を補正し、ADDR から取り除く。

        岡山県版の住所は市名始まり(例: 岡山市北区...)が大半で PREF は URL から
        補完済みのため、その場合は住所をそのまま格納する。
        """
        m = _PREF_PATTERN.match(address or "")
        if m:
            item[Schema.PREF] = m.group(1)
            item[Schema.ADDR] = address[m.end():].strip()
        else:
            item[Schema.ADDR] = address


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Jobmake2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://jobmake.jp/33/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
