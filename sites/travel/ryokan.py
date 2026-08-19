"""
やど日本 (日本旅館協会 加盟施設一覧) — 全国の旅館・ホテル情報スクレイパー

取得対象:
    - やど日本 (www.ryokan.or.jp) の「加盟施設一覧」に掲載されている
      全国の加盟旅館・ホテル 約 1,974 件 (2026-08 確認 / 10 件 × 198 ページ)。
    - 施設名 / カナ / 都道府県 / 郵便番号 / 住所 / TEL / 予約用TEL / FAX /
      公式サイト URL / 創業年月 / 休館日 / クレジットカード などの構造化情報。

取得フロー:
    1. 一覧 (引数 url からの派生 = search/result/result?page=N) を 1 ページずつ取得し、
       各カードの詳細リンク /inn/redirect_detail/{ID} から施設 ID を列挙する。
       ※ 一覧はセッション不要 (Cookie なしでも page=N が単独で機能する)。
    2. 施設 ID から詳細ページ (/inn/{ID}) を 1 件取得するたびに即 yield する
       (途中中断に強い Pattern B / 早期 yield)。
       ※ /inn/redirect_detail/{ID} は /inn/{ID} への 302 なので直接後者を叩く。
    3. 詳細ページのリンクが無くなる (= 最終ページ超過) まで繰り返す。

詳細ページの構造:
    - 施設名/カナ: div.st h2 (名称<br><span>かな</span>)
    - エリア     : p.kanko / p.col_a ("北海道　釧路市")
    - 連絡先表   : div.box_intro 内の table (URL/住所/TEL/予約用TEL/FAX/最寄り〜)
    - 詳細表     : div.wrp 内の table。セクション見出しは h3 > img[src*="/inn/images/st_"]
      の alt (基本情報 / 宿泊料金 / お食事 / お風呂 / 温泉 / 設備&サービス)。
      ラベル "その他" と "特記事項" はセクション違いで重複するためセクション込みで解決する。
    - 客室数のみ入れ子 table.tbl_txt (客室タイプ th × 室数 td)。

著作権配慮:
    - 施設のキャッチコピー (h3)・紹介文 (自由記述プロース)・各セクションの
      「特記事項」は取得しない。構造化された短いラベル/数値のみ取得する。

利用規約:
    - サイトポリシー (https://www.ryokan.or.jp/sitepolicy/) にスクレイピング・
      クローリングを禁止する条項は無い (リンク方針と著作権表示のみ / 2026-08 確認)。

備考対応:
    - 「取れるカラムは全部取ってください」の指示に従い、詳細ページ上の構造化情報を
      Schema + EXTRA_COLUMNS で網羅的に取得する。エリア等のフィルター指示は無いため全件取得。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/ryokan.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ryokan
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

# sys.path を調整 (src/ を含むディレクトリを探す)
base_dir = Path(__file__).resolve().parent.parent.parent.parent
if not (base_dir / "src").exists():
    base_dir = base_dir / "NetHarvest"
sys.path.insert(0, str(base_dir))

from src.const.schema import Schema  # noqa: E402
from src.framework.static import StaticCrawler  # noqa: E402

# 一覧カードの詳細リンク /inn/redirect_detail/{ID} から施設 ID を取り出す
_DETAIL_HREF = re.compile(r"/inn/redirect_detail/(\d+)")

# 住所先頭の郵便番号
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")

# 住所先頭から都道府県を切り出すためのパターン (「京都府」を誤マッチさせないため列挙)
_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 施設ごとの外部予約ページ (末尾の ID がある場合のみ / ヘッダーの「やど検索」を除外)
_RESERVE_HREF = re.compile(r"tour\.ne\.jp/ext/yadonihon/j_hotel/\d+")

# 詳細ページのセクション見出し画像 (h3 > img)
_SECTION_IMG_SRC = re.compile(r"/inn/images/st_")

# 創業年月 "2006年06月" / "1989年1月"
_BORN_PATTERN = re.compile(r"(\d{4})\s*年\s*(\d{1,2})?\s*月?")

# 値が未登録であることを示すプレースホルダ
_EMPTY_VALUES = {"", "-", "－", "ー"}


class RyokanCrawler(StaticCrawler):
    """やど日本 (日本旅館協会 加盟施設一覧) の全国旅館・ホテル情報を取得するクローラー。"""

    # 1 ページあたり 10 件。最終ページ (198) を大きく超えたら安全のため打ち切る
    MAX_PAGES = 400

    # 詳細ページ取得の間隔 (秒)
    DELAY = 0.3

    EXTRA_COLUMNS = [
        "やど日本番号",
        "エリア",
        "FAX",
        "最寄り駅",
        "最寄りIC",
        "最寄り空港",
        "その他アクセス",
        "チェックイン",
        "チェックアウト",
        "休業期間",
        "駐車場",
        "送迎",
        "宿泊プラン",
        "宿泊料金",
        "食事対応",
        "お食事場所",
        "お食事料金",
        "大浴場",
        "大浴場備品",
        "露天風呂",
        "日帰り入浴",
        "貸切風呂",
        "温泉",
        "ジャグジー",
        "サウナ",
        "お部屋",
        "総客室数",
        "客室数内訳",
        "客室備品",
        "サービス",
        "広間（宴会場）",
        "会議室",
        "その他設備",
        "ペット対応",
        "バリアフリー対応",
        "宿泊予約ページURL",
    ]

    # (セクション見出し, ラベル) → 出力カラム名
    # セクション "" は div.box_intro の連絡先テーブル。
    _FIELD_MAP: dict[tuple[str, str], str] = {
        ("", "URL"): Schema.HP,
        ("", "TEL"): Schema.TEL,
        ("", "予約用TEL"): Schema.PHONE,
        ("", "FAX"): "FAX",
        ("", "最寄り駅"): "最寄り駅",
        ("", "最寄りIC"): "最寄りIC",
        ("", "最寄り空港"): "最寄り空港",
        ("", "その他"): "その他アクセス",
        ("基本情報", "チェックイン"): "チェックイン",
        ("基本情報", "チェックアウト"): "チェックアウト",
        ("基本情報", "休館日"): Schema.HOLIDAY,
        ("基本情報", "休業期間"): "休業期間",
        ("基本情報", "駐車場"): "駐車場",
        ("基本情報", "送迎"): "送迎",
        ("宿泊料金", "宿泊プラン"): "宿泊プラン",
        ("宿泊料金", "宿泊料金(お１人様)"): "宿泊料金",
        ("宿泊料金", "クレジットカード"): Schema.PAYMENTS,
        ("お食事", "お食事"): "食事対応",
        ("お食事", "お食事場所"): "お食事場所",
        ("お食事", "お食事料金"): "お食事料金",
        ("お風呂 / 温泉", "大浴場"): "大浴場",
        ("お風呂 / 温泉", "大浴場備品"): "大浴場備品",
        ("お風呂 / 温泉", "露天風呂"): "露天風呂",
        ("お風呂 / 温泉", "日帰り入浴"): "日帰り入浴",
        ("お風呂 / 温泉", "貸切風呂"): "貸切風呂",
        ("お風呂 / 温泉", "温泉"): "温泉",
        ("お風呂 / 温泉", "ジャグジー"): "ジャグジー",
        ("お風呂 / 温泉", "サウナ"): "サウナ",
        ("設備&サービス", "お部屋"): "お部屋",
        ("設備&サービス", "客室備品"): "客室備品",
        ("設備&サービス", "サービス"): "サービス",
        ("設備&サービス", "広間（宴会場）"): "広間（宴会場）",
        ("設備&サービス", "会議室"): "会議室",
        ("設備&サービス", "その他"): "その他設備",
        ("設備&サービス", "ペット対応"): "ペット対応",
        ("設備&サービス", "バリアフリー対応"): "バリアフリー対応",
    }

    def prepare(self):
        """クロール開始前の状態初期化。"""
        self.seen_ids: set[str] = set()

    def parse(self, url: str) -> Generator[dict, None, None]:
        """一覧をページ送りしつつ、詳細ページを 1 件取得するたびに即 yield する。

        Args:
            url (str): sites.yml に登録された正規 URL (https://www.ryokan.or.jp/)。
                配下の URL はすべてこの url から urljoin で派生させる。

        Yields:
            dict: 施設 1 件分のデータ。
        """
        for page in range(1, self.MAX_PAGES + 1):
            list_url = urljoin(url, f"search/result/result?page={page}")
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧ページを取得できませんでした: %s", list_url)
                break

            inn_ids: list[str] = []
            for a in soup.find_all("a", href=True):
                m = _DETAIL_HREF.search(a["href"])
                if m and m.group(1) not in self.seen_ids:
                    self.seen_ids.add(m.group(1))
                    inn_ids.append(m.group(1))

            if not inn_ids:
                self.logger.info("ページ %s に施設リンクがないため終了します。", page)
                break

            self.logger.info("ページ %s: 施設 %s 件", page, len(inn_ids))

            for inn_id in inn_ids:
                item = self._parse_detail(url, inn_id)
                if item:
                    yield item

    def _parse_detail(self, root_url: str, inn_id: str) -> dict | None:
        """詳細ページ (/inn/{ID}) を取得して 1 件分の dict を組み立てる。"""
        detail_url = urljoin(root_url, f"inn/{inn_id}")
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        st = soup.select_one("div.box_intro div.st")
        if st is None:
            self.logger.warning("施設情報ブロックが見つかりません: %s", detail_url)
            return None

        item: dict = {
            Schema.URL: detail_url,
            "やど日本番号": inn_id,
        }

        # --- 施設名 / カナ (h2 に「名称<br><span>かな</span>」) ---
        h2 = st.find("h2")
        if h2:
            span = h2.find("span")
            kana = span.get_text(strip=True) if span else ""
            if span:
                span.extract()
            name = h2.get_text(" ", strip=True)
            if not name:
                return None
            item[Schema.NAME] = name
            if kana:
                item[Schema.NAME_KANA] = kana
        else:
            return None

        # --- エリア (p.col_a = "北海道　釧路市" / p.kanko = "釧路市") ---
        area = ""
        col_a = st.select_one("p.col_a")
        if col_a:
            parts = [p for p in re.split(r"[\s　]+", col_a.get_text(" ", strip=True)) if p]
            if len(parts) > 1:
                area = parts[-1]
        if not area:
            kanko = st.select_one("p.kanko")
            if kanko:
                area = kanko.get_text(strip=True)
        if area:
            item["エリア"] = area

        # --- 各テーブルの th/td をセクション込みで収集 ---
        rows = self._collect_rows(soup)

        for (section, label), value in rows.items():
            column = self._FIELD_MAP.get((section, label))
            if column and value:
                item[column] = value

        # --- 住所 (郵便番号 + 都道府県 + 以降) ---
        addr_raw = rows.get(("", "住所"), "")
        if addr_raw:
            mp = _POST_PATTERN.match(addr_raw)
            if mp:
                item[Schema.POST_CODE] = mp.group(1)
                addr = addr_raw[mp.end():].strip()
            else:
                addr = addr_raw
            mpref = _PREF_PATTERN.match(addr)
            if mpref:
                item[Schema.PREF] = mpref.group(1)
            if addr:
                item[Schema.ADDR] = addr

        # --- 創業年月 "2006年06月" → 設立年月日 "2006-06-01" ---
        born = rows.get(("設備&サービス", "創業年月"), "")
        if born:
            mb = _BORN_PATTERN.search(born)
            if mb:
                month = int(mb.group(2) or 1)
                item[Schema.OPEN_DATE] = f"{mb.group(1)}-{month:02d}-01"

        # --- 客室数 (入れ子 table.tbl_txt: 客室タイプ th × 室数 td) ---
        total_rooms, room_breakdown = self._parse_room_counts(soup)
        if total_rooms:
            item["総客室数"] = total_rooms
        if room_breakdown:
            item["客室数内訳"] = room_breakdown

        # --- 宿泊予約ページ (tour.ne.jp の外部予約ページ) ---
        # ヘッダーの「やど検索」(施設 ID 無し) と区別するため末尾の ID を必須にする
        rsv = soup.find("a", href=_RESERVE_HREF)
        if rsv:
            item["宿泊予約ページURL"] = rsv["href"].strip()

        return item

    def _collect_rows(self, soup) -> dict[tuple[str, str], str]:
        """詳細ページの全テーブルから (セクション名, ラベル) → 値 を収集する。

        セクション名は直前の見出し画像 (h3 > img[src*="/inn/images/st_"]) の alt。
        div.box_intro の連絡先テーブルは見出し画像より前にあるためセクション名 "" になる。
        ラベル "その他"/"特記事項" は複数セクションに存在するためキーにセクションを含める。
        """
        rows: dict[tuple[str, str], str] = {}
        for table in soup.select("#contents table"):
            classes = table.get("class") or []
            # 客室数の入れ子テーブルは _parse_room_counts() で別途処理する
            if "tbl_txt" in classes:
                continue

            img = table.find_previous("img", src=_SECTION_IMG_SRC)
            section = (img.get("alt") or "").strip() if img else ""
            # 見出し「施設詳細情報」はメニュー用のため空セクション扱いにはしない
            if section == "施設詳細情報":
                section = ""

            for tr in table.find_all("tr"):
                th = tr.find("th", recursive=False)
                td = tr.find("td", recursive=False)
                if th is None or td is None:
                    continue
                label = self._clean(th.get_text(" ", strip=True))
                # "客室数 ※カッコ内は、 バス・トイレ付の室数" のような注釈付きラベルを正規化
                label = label.split("※")[0].strip()
                if not label:
                    continue
                # 入れ子テーブルを含むセル (客室数) は個別処理に委ねる
                if td.find("table"):
                    continue
                value = self._clean(td.get_text(" ", strip=True))
                if value in _EMPTY_VALUES:
                    continue
                rows.setdefault((section, label), value)
        return rows

    def _parse_room_counts(self, soup) -> tuple[str, str]:
        """入れ子テーブル table.tbl_txt から総客室数と客室タイプ別内訳を取り出す。

        Returns:
            tuple[str, str]: (総客室数, 客室数内訳) 例 ("100室 (100室)", "シングル:89室 (89室) / ...")
        """
        table = soup.select_one("#contents table.tbl_txt")
        if table is None:
            return "", ""

        headers = [self._clean(th.get_text(" ", strip=True)) for th in table.find_all("th")]
        values = [self._clean(td.get_text(" ", strip=True)) for td in table.find_all("td")]
        if not headers or not values:
            return "", ""

        total = ""
        pairs: list[str] = []
        for head, value in zip(headers, values):
            if not value or value in _EMPTY_VALUES:
                continue
            if head == "総客室数":
                total = value
            else:
                pairs.append(f"{head}:{value}")
        return total, " / ".join(pairs)

    @staticmethod
    def _clean(text: str) -> str:
        """改行・連続空白を 1 つの半角スペースに畳む。"""
        return re.sub(r"[\s　]+", " ", text or "").strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = RyokanCrawler()
    scraper.execute("https://www.ryokan.or.jp/")
