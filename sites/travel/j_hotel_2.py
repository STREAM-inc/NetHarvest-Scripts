"""
日本ホテル協会 加盟ホテル一覧 (j_hotel_2) — 全カラム取得版スクレイパー

取得対象:
    - 一般社団法人日本ホテル協会 (j-hotel.or.jp) の会員ホテル一覧 (/memberlist/)
      に掲載されている全国の加盟ホテル (2026-08 時点 230 件)。
    - 既存の `travel.j_hotel` (基本 6 項目のみ) に対し、本スクリプトは
      「サイト上で取得可能な構造化項目をすべて取る」方針で実装している。

取得フロー:
    1. 会員ホテル一覧 (引数 url = /memberlist/) を 1 回だけ取得し、3 つの情報源を統合:
       a. #accessionWrap01 (都道府県順タブ) … <em>[都道府県]</em> 見出し配下のリンク
          → ホテル ID と「都道府県」の対応 (222 件)
       b. #accessionWrap02 (五十音順タブ) … <em>[あ]</em> 見出し配下のリンク
          → ホテル ID と「五十音」の対応
       c. インライン JS の `var hotel_list = {...}` … 郵便番号 / 住所 / TEL /
          オフィシャルサイト URL / 緯度経度 (225 件)
       ※ a と c は母集団が微妙に異なる (a のみ 5 件 / c のみ 8 件) ため和集合を取る。
          和集合 230 件 = ページ上部の「会員ホテル 230 件」カウンタと一致する。
    2. 詳細ページ (/hotel/{ID}/) を 1 件取得するたびに即 yield する
       (途中中断に強い Pattern B / 早期 yield)。詳細ページからは
       ホテル名 / 郵便番号 / 住所 / TEL / チェックイン・アウト / 公式サイト /
       客室 / レストラン / 宴会場・会議室 / ウェディング / 施設・サービス /
       バリアフリー対応 / 利用可能クレジットカードを取得する。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。別 URL はハードコードしない。
    - 詳細ページの「ホテル概要」「アクセス方法 (道順文)」は自由記述のプロース
      (著作権リスク) のため取得しない。取得するのは施設名・区分・数量など
      構造化された短いラベルのみ。
    - 代表者 / 法人番号 / 資本金 / 売上 / 従業員数 / 設立日 / FAX / メール /
      SNS はサイト上に掲載が無いため取得できない。
    - 一覧の五十音タブには「(休業中)」でリンクを持たない項目があるが、
      詳細ページが存在しないため対象外とする。
    - 利用規約 (/policy/) は著作権・転載に関する一般的な記載のみで、
      スクレイピング/クローリングを明示的に禁止する条項は無い (2026-08 確認)。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/j_hotel_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id j_hotel_2
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 詳細ページ URL (/hotel/{ID}/) を判定するパターン
_DETAIL_HREF = re.compile(r"^/hotel/(\d+)/?$")

# 一覧ページのインライン JS に埋め込まれたホテル一覧 JSON の開始位置
# (末尾にセミコロンが無いため、開始 `{` から括弧の対応を数えて切り出す)
_HOTEL_LIST_HEAD = re.compile(r"var\s+hotel_list\s*=\s*\{")

# address テキスト先頭の郵便番号 (〒 省略・ハイフン有/無 いずれも許容)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")

# 「252 室」形式の室数
_ROOM_NUM = re.compile(r"([\d,]+)\s*室")

# 住所先頭から都道府県を切り出すためのパターン
_PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 詳細ページ内の「項目セクション id」→ 出力カラム接頭辞
_ITEM_SECTIONS = [
    ("rooms", "客室"),
    ("restaurants", "レストラン・バー"),
    ("halls", "宴会場・会議室"),
    ("weddings", "ウェディング"),
    ("recreations", "施設・サービス"),
]


class JHotel2(StaticCrawler):
    """日本ホテル協会 加盟ホテル情報スクレイパー (全カラム取得版)"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "ホテルID",
        "五十音",
        "緯度",
        "経度",
        "チェックイン",
        "チェックアウト",
        "客室数",
        "客室タイプ数",
        "客室タイプ",
        "レストラン・バー数",
        "レストラン・バー",
        "レストラン・バー種別",
        "宴会場・会議室数",
        "宴会場・会議室",
        "ウェディング会場数",
        "ウェディング会場",
        "施設・サービス数",
        "施設・サービス",
        "バリアフリー_共用部分",
        "バリアフリー_客室",
        "バリアフリー_人的対応・サービス",
    ]

    def parse(self, url: str):
        # ------------------------------------------------------------------
        # 1. 一覧ページを 1 回取得し、3 つの情報源 (都道府県順 / 五十音順 / JSON) を統合
        # ------------------------------------------------------------------
        soup = self.get_soup(url)
        if soup is None:
            return

        pref_map = self._collect_group_map(soup, "accessionWrap01")   # ID → 都道府県
        kana_map = self._collect_group_map(soup, "accessionWrap02")   # ID → 五十音
        json_map = self._collect_hotel_json(soup)                     # ID → 一覧 JSON

        # 掲載順 (都道府県順タブ) を優先し、JSON にしか無い ID を末尾に足す
        hotel_ids = list(pref_map.keys())
        hotel_ids += [hid for hid in json_map if hid not in pref_map]

        self.total_items = len(hotel_ids)
        self.logger.info(
            "加盟ホテル %d 件 (都道府県順タブ %d / 一覧JSON %d)",
            len(hotel_ids), len(pref_map), len(json_map),
        )

        # ------------------------------------------------------------------
        # 2. 詳細ページを 1 件取得するたびに即 yield (早期 yield / 途中中断に強い)
        # ------------------------------------------------------------------
        for hotel_id in hotel_ids:
            detail_url = urljoin(url, f"/hotel/{hotel_id}/")
            try:
                item = self._scrape_detail(
                    detail_url,
                    hotel_id,
                    pref_map.get(hotel_id, ""),
                    kana_map.get(hotel_id, ""),
                    json_map.get(hotel_id, {}),
                )
                if item:
                    yield item
            except Exception as e:  # 個別ホテルの失敗は握りつぶして継続
                self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                continue

    # ----------------------------------------------------------------------
    # 一覧ページの解析
    # ----------------------------------------------------------------------
    def _collect_group_map(self, soup, wrap_id: str) -> dict[str, str]:
        """`<em>[見出し]</em>` + 配下リンク構造から ID → 見出し の辞書を作る。

        - #accessionWrap01 (都道府県順タブ) → 見出しは "北海道" 等の都道府県
        - #accessionWrap02 (五十音順タブ)   → 見出しは "あ" 等の五十音
        """
        result: dict[str, str] = {}
        wrap = soup.select_one(f"#{wrap_id}")
        if wrap is None:
            return result

        for li in wrap.find_all("li"):
            em = li.find("em", recursive=False)
            if em is None:
                continue
            label = em.get_text(strip=True).strip("[]").strip()
            for a in li.select("ul a[href]"):
                m = _DETAIL_HREF.match(a["href"])
                if m and m.group(1) not in result:
                    result[m.group(1)] = label
        return result

    def _collect_hotel_json(self, soup) -> dict[str, dict]:
        """一覧ページのインライン JS `var hotel_list = {...}` を辞書化する。"""
        for script in soup.find_all("script"):
            text = script.string or script.get_text() or ""
            if "hotel_list" not in text:
                continue
            m = _HOTEL_LIST_HEAD.search(text)
            if not m:
                continue
            raw = self._slice_json_object(text, m.end() - 1)
            if raw is None:
                self.logger.warning("一覧 JSON の終端を特定できませんでした")
                return {}
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                self.logger.warning("一覧 JSON の解析に失敗しました — %s", e)
                return {}
            return {str(k): v for k, v in data.items() if isinstance(v, dict)}
        self.logger.warning("一覧ページに hotel_list JSON が見つかりませんでした")
        return {}

    @staticmethod
    def _slice_json_object(text: str, start: int) -> str | None:
        """`text[start]` の `{` に対応する `}` までを切り出す (文字列リテラル考慮)。"""
        depth = 0
        in_str = False
        escaped = False
        for i in range(start, len(text)):
            ch = text[i]
            if in_str:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start:i + 1]
        return None

    # ----------------------------------------------------------------------
    # 詳細ページの解析
    # ----------------------------------------------------------------------
    def _scrape_detail(
        self,
        detail_url: str,
        hotel_id: str,
        pref: str,
        kana: str,
        listed: dict,
    ) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        box = soup.select_one("#hotelDetail")
        if box is None:
            return None

        item = {Schema.URL: detail_url, "ホテルID": hotel_id, "五十音": kana}

        # --- ホテル名 (H1) ---
        h1 = box.select_one("#ttlWrap h1")
        name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None  # 無効なページとして捨てる
        item[Schema.NAME] = name

        # --- 郵便番号 / 住所 (address = "{郵便番号} {都道府県+住所}") ---
        post_code, addr = "", ""
        addr_el = box.select_one("#ttlWrap address")
        if addr_el:
            addr_raw = addr_el.get_text(" ", strip=True)
            mp = _POST_PATTERN.match(addr_raw)
            if mp:
                post_code = mp.group(1)
                addr = addr_raw[mp.end():].strip()
            else:
                addr = addr_raw.strip()
        # 詳細ページに無ければ一覧 JSON をフォールバックに使う
        post_code = post_code or (listed.get("zipCode") or "").strip()
        addr = addr or (listed.get("address") or "").strip()
        if post_code:
            item[Schema.POST_CODE] = post_code
        if addr:
            item[Schema.ADDR] = addr

        # --- 都道府県 (一覧の都道府県順タブ優先、無ければ住所先頭から導出) ---
        if not pref:
            m_pref = _PREF_PATTERN.match(addr)
            pref = m_pref.group(1) if m_pref else ""
        if pref:
            item[Schema.PREF] = pref

        # --- 公式サイト URL ---
        hp = box.select_one("#info .btn a[href]")
        hp_url = hp["href"].strip() if hp else ""
        hp_url = hp_url or (listed.get("officialURL") or "").strip()
        if hp_url:
            item[Schema.HP] = hp_url

        # --- TEL / チェックイン / チェックアウト (#info .note 内の各段落) ---
        tel = ""
        note = box.select_one("#info .note")
        if note:
            for p in note.find_all("p"):
                txt = p.get_text(strip=True)
                if not txt:
                    continue
                if not tel and ("TEL" in txt or "電話" in txt):
                    # (0142)89-3333 形式のカッコを区切りに正規化
                    tel = re.sub(r"^\s*(?:TEL|電話)\s*[：:]?\s*", "", txt)
                    tel = tel.replace("(", "").replace(")", "-").strip()
                elif txt.startswith("チェックイン"):
                    item["チェックイン"] = txt.replace("チェックイン", "").strip()
                elif txt.startswith("チェックアウト"):
                    item["チェックアウト"] = txt.replace("チェックアウト", "").strip()
        tel = tel or (listed.get("tel") or "").strip()
        if tel:
            item[Schema.TEL] = tel

        # --- 緯度経度 (一覧 JSON のみ) ---
        lat = (listed.get("lat") or "").strip()
        lng = (listed.get("lng") or "").strip()
        if lat:
            item["緯度"] = lat
        if lng:
            item["経度"] = lng

        # --- 客室 / レストラン / 宴会場 / ウェディング / 施設・サービス ---
        self._extract_item_sections(box, item)

        # --- バリアフリー対応 (構造化されたチェック項目) ---
        self._extract_barrierfree(box, item)

        # --- 利用可能クレジットカード ---
        cards = [
            (img.get("alt") or "").strip()
            for img in box.select("#cardList img")
            if (img.get("alt") or "").strip()
        ]
        if cards:
            item[Schema.PAYMENTS] = " / ".join(cards)

        return item

    def _extract_item_sections(self, box, item: dict) -> None:
        """客室・レストラン等のカード一覧から件数と名称 (構造化ラベル) を取り出す。"""
        for sec_id, label in _ITEM_SECTIONS:
            sec = box.select_one(f"#{sec_id}")
            if sec is None:
                continue
            names, genres, total_rooms = [], [], 0
            entries = sec.select("ul.itemList > li")
            for li in entries:
                h4 = li.find("h4")
                if h4:
                    nm = h4.get_text(" ", strip=True).replace("　", " ")
                    if nm:
                        names.append(nm)
                genre_el = li.select_one(".hd p")
                if genre_el:
                    gn = genre_el.get_text(strip=True)
                    if gn and gn not in genres:
                        genres.append(gn)
                room_el = li.select_one(".room")
                if room_el:
                    m = _ROOM_NUM.search(room_el.get_text(strip=True))
                    if m:
                        total_rooms += int(m.group(1).replace(",", ""))

            if sec_id == "rooms":
                if total_rooms:
                    item["客室数"] = str(total_rooms)
                if entries:
                    item["客室タイプ数"] = str(len(entries))
                if names:
                    item["客室タイプ"] = " / ".join(names)
            elif sec_id == "restaurants":
                if entries:
                    item["レストラン・バー数"] = str(len(entries))
                if names:
                    item["レストラン・バー"] = " / ".join(names)
                if genres:
                    item["レストラン・バー種別"] = " / ".join(genres)
            elif sec_id == "weddings":
                if entries:
                    item["ウェディング会場数"] = str(len(entries))
                if names:
                    item["ウェディング会場"] = " / ".join(names)
            else:  # halls / recreations は「{ラベル}数」「{ラベル}」で揃える
                if entries:
                    item[f"{label}数"] = str(len(entries))
                if names:
                    item[label] = " / ".join(names)

    def _extract_barrierfree(self, box, item: dict) -> None:
        """バリアフリー対応 (共用部分 / 客室 / 人的対応・サービス) の項目を取り出す。"""
        sec = box.select_one("#barrierfree")
        if sec is None:
            return
        for div in sec.find_all("div", recursive=False):
            h3 = div.find("h3")
            if h3 is None:
                continue
            group = h3.get_text(" ", strip=True)
            column = f"バリアフリー_{group}"
            if column not in self.EXTRA_COLUMNS:
                continue
            values = [
                li.get_text(" ", strip=True)
                for li in div.select("ul.possibleList > li, ul.otherList > li")
                if li.get_text(strip=True)
            ]
            if values:
                item[column] = " / ".join(values)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JHotel2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を唯一の起点とし、配下 URL は urljoin で派生させる。
    scraper.execute("https://www.j-hotel.or.jp/memberlist/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
