"""
全国スーパー銭湯検索（supersento.com）— 全国のスーパー銭湯・サウナ・健康ランド施設

取得対象:
    - 施設単位の基本情報（温泉地紹介ページではなく個別施設ページ）
    - 施設名 / 都道府県 / 住所 / 電話番号(TEL) / 公式HP
    - 営業時間 / 定休日 / 営業状態、EXTRA: 地方・入館料・岩盤浴・特徴アイコン
    - 公式SNS (Instagram / X / Facebook / TikTok / LINE)

取得フロー (一覧 → 詳細, Pattern B: 詳細を1件取得するごとに即 yield):
    1. 引数 url（都道府県ページ 例: /kanto/tokyo.html）の施設一覧を先に巡回
       → 最初の1件を数秒以内に yield する
    2. 続けて url から導出したトップページ ( urljoin(url, "/") ) の
       都道府県リンク（region/pref.html）を列挙し、全47都道府県を横断
    3. 各都道府県ページの一覧テーブル ( table.tenpo_ichiran_box2 tr[data-href] )
       から施設名・詳細URLを取得（このテーブル外の他県ナビ等は拾わない）
    4. 詳細ページ ( 例: /kanto/tokyo/gokurakuyu_kanda.html ) を1件ずつ取得
       - 住所/電話番号/営業時間/定休日: 情報テーブル (td.date_box5=ラベル / td.date_box2=値)
       - 公式HP・公式SNS: 「公式ホームページ」ブロック (div.koshiki_waku1) 内に限定
         ※ 本文中の埋め込みツイート等は施設の公式SNSではないため対象外
       - 都道府県は住所先頭から抽出し、無い場合は URL スラッグから補完

サイト構造メモ (2026-08 調査):
    - ページネーションは無く、都道府県ページ1枚に全施設行が載る（東京都=213件、静岡県=78件）
    - 電話番号は未掲載の施設が多く `<a href="tel:"></a>-` のように値が "-" になる
      → ハイフンのみ/空は空文字に正規化する
    - 閉館済み施設は h1 内 span.covid に「YYYY年M月D日 閉館」と表示される

備考対応:
    - 「全都道府県を横断して巡回」→ トップページ経由で47都道府県すべてを巡回
    - 「施設単位で名称・住所・TEL・公式HP を取得」→ Schema にマッピング済み
    - 温泉地紹介ページ等は一覧テーブルの行に含まれないため自然に除外される

著作権配慮:
    - 体験レポート本文・口コミ・温泉データ・駐車場案内などの自由記述プロースは取得しない
      （構造化された短いラベル項目のみ取得）

利用規約:
    - https://www.supersento.com/sonota/about.html はサイト紹介のみで禁止条項なし。
      robots.txt はトップへ 302 リダイレクト（クロール制限の記載なし）。
      スクレイピング/クローリングを禁止する記載は確認されなかった (2026-08 時点)。

実行方法:
    # ローカルテスト
    python scripts/sites/leisure/supersento_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id supersento_2
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 地方ディレクトリ（都道府県ページ URL の第1階層） → 地方名
_REGIONS = {
    "hokaidotohoku": "北海道・東北地方",
    "kanto": "関東地方",
    "hokuriku": "北陸・甲信越地方",
    "chubu": "中部地方",
    "kinki": "近畿地方",
    "chugoku": "中国地方",
    "shikoku": "四国地方",
    "kyusyu": "九州地方",
    "okinawa": "沖縄地方",
}

# 都道府県ページ URL の形（region/pref.html）。施設詳細は region/pref/xxx.html なので除外される
_PREF_PAGE_RE = re.compile(r"(?:^|/)(%s)/([a-z0-9_-]+)\.html$" % "|".join(_REGIONS))

# 都道府県ページのスラッグ → 都道府県名（サイト表記のゆらぎ totori / ooita / hokaido / kyusyu 等を含む）
_PREF_SLUG = {
    "hokaido": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
    "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県",
    "ibaraki": "茨城県", "tochigi": "栃木県", "gunma": "群馬県", "saitama": "埼玉県",
    "chiba": "千葉県", "tokyo": "東京都", "kanagawa": "神奈川県", "yamanashi": "山梨県",
    "niigata": "新潟県", "nagano": "長野県", "toyama": "富山県", "ishikawa": "石川県",
    "fukui": "福井県", "gifu": "岐阜県", "shizuoka": "静岡県", "aichi": "愛知県",
    "mie": "三重県", "shiga": "滋賀県", "kyoto": "京都府", "osaka": "大阪府",
    "hyogo": "兵庫県", "nara": "奈良県", "wakayama": "和歌山県",
    "totori": "鳥取県", "shimane": "島根県", "okayama": "岡山県", "hiroshima": "広島県",
    "yamaguchi": "山口県", "tokushima": "徳島県", "kagawa": "香川県", "ehime": "愛媛県",
    "kochi": "高知県", "fukuoka": "福岡県", "saga": "佐賀県", "nagasaki": "長崎県",
    "kumamoto": "熊本県", "ooita": "大分県", "miyazaki": "宮崎県",
    "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}

# 住所先頭の都道府県表記
_PREF_RE = re.compile(r"^(北海道|東京都|(?:大阪|京都)府|.{2,3}?県)")

# 値が未掲載であることを示す記号（電話番号などが "-" で埋められている）
_EMPTY_MARKS = {"", "-", "‐", "―", "ー", "−", "--", "なし", "無し"}

# 公式SNS リンクのドメイン → Schema 定数
_SNS_MAP = [
    (re.compile(r"instagram\.com", re.I), Schema.INSTA),
    (re.compile(r"(?:twitter\.com|x\.com)", re.I), Schema.X),
    (re.compile(r"facebook\.com", re.I), Schema.FB),
    (re.compile(r"tiktok\.com", re.I), Schema.TIKTOK),
    (re.compile(r"line\.me|lin\.ee", re.I), Schema.LINE),
]

# 営業状態（h1 内の注記から判定する短いラベル）
_STATUS_PATTERNS = [
    (re.compile(r"閉館|閉店|廃業"), "閉館"),
    (re.compile(r"休館|休業"), "休館"),
    (re.compile(r"リニューアル"), "リニューアル"),
]


class Supersento2Scraper(StaticCrawler):
    """全国スーパー銭湯検索（supersento.com）スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "region",     # 地方（関東地方 など）
        "fee",        # 入館料（一覧の料金列。例: 600円）
        "rock_bath",  # 岩盤浴（一覧の岩盤浴列。例: 有 / -）
        "features",   # 特徴アイコンの短ラベル（例: クーポンあり/炭酸泉/宿泊OK）
    ]

    # 都道府県を絞りたい場合に県名を入れる（例: ["東京都"]）。空なら全国全件。
    AREA_PREFIXES: list[str] = []

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_details: set[str] = set()
        seen_pages: set[str] = set()

        # 1) 引数 url（都道府県ページ）を最初に巡回 → 早期に1件目を yield する
        start_region, start_pref = self._pref_from_url(url)
        seen_pages.add(self._normalize(url))
        yield from self._crawl_pref_page(url, start_region, start_pref, seen_details)

        # 2) url から導出したトップページ経由で残りの都道府県を巡回（全国横断）
        top_url = urljoin(url, "/")
        root = self.get_soup(top_url)
        if root is None:
            self.logger.warning("トップページを取得できませんでした: %s", top_url)
            return

        pref_pages = self._collect_pref_pages(top_url, root)
        self.logger.info("都道府県ページ %d 件を列挙しました", len(pref_pages))

        for pref_url, region, pref_ja in pref_pages:
            if self._normalize(pref_url) in seen_pages:
                continue
            seen_pages.add(self._normalize(pref_url))
            if self.AREA_PREFIXES and pref_ja and pref_ja not in self.AREA_PREFIXES:
                continue
            yield from self._crawl_pref_page(pref_url, region, pref_ja, seen_details)

    # ------------------------------------------------------------------ #
    # 都道府県ページ → 施設詳細
    # ------------------------------------------------------------------ #
    def _crawl_pref_page(
        self, pref_url: str, region: str, pref_ja: str, seen_details: set[str]
    ) -> Generator[dict, None, None]:
        soup = self.get_soup(pref_url)
        if soup is None:
            self.logger.warning("都道府県ページ取得失敗（スキップ）: %s", pref_url)
            return

        # 施設一覧テーブルの行に限定（他県ナビ table.hokaken2 等を拾わないため）
        rows = soup.select("table.tenpo_ichiran_box2 tr[data-href]")
        if not rows:
            rows = soup.select("tr[data-href]")
        self.logger.info("一覧 %d 件: %s", len(rows), pref_url)

        for row in rows:
            href = (row.get("data-href") or "").strip()
            if not href:
                continue
            detail_url = urljoin(pref_url, href)
            if detail_url in seen_details:
                continue
            seen_details.add(detail_url)

            list_info = self._parse_list_row(row)
            try:
                item = self._scrape_detail(detail_url, list_info, region, pref_ja)
            except Exception as e:  # noqa: BLE001 — 個別施設の失敗は握りつぶして継続
                self.logger.warning("詳細ページ処理に失敗: %s — %s", detail_url, e)
                continue
            if item:
                yield item

    # ------------------------------------------------------------------ #
    # 都道府県ページの列挙
    # ------------------------------------------------------------------ #
    def _collect_pref_pages(self, base_url: str, root_soup) -> list[tuple[str, str, str]]:
        """トップページから (都道府県ページURL, 地方名, 都道府県名) を重複排除して列挙する。"""
        pages: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for a in root_soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href or href.startswith(("#", "javascript:")):
                continue
            abs_url = urljoin(base_url, href)
            m = _PREF_PAGE_RE.search(urlparse(abs_url).path)
            if not m:
                continue
            key = self._normalize(abs_url)
            if key in seen:
                continue
            seen.add(key)
            pages.append((abs_url, _REGIONS.get(m.group(1), ""), _PREF_SLUG.get(m.group(2), "")))
        return pages

    @staticmethod
    def _pref_from_url(url: str) -> tuple[str, str]:
        """URL（都道府県ページ）から (地方名, 都道府県名) を導出する。"""
        m = _PREF_PAGE_RE.search(urlparse(url).path)
        if not m:
            return "", ""
        return _REGIONS.get(m.group(1), ""), _PREF_SLUG.get(m.group(2), "")

    @staticmethod
    def _normalize(url: str) -> str:
        """比較用に URL を正規化する（クエリ/フラグメントを落とす）。"""
        p = urlparse(url)
        return f"{p.netloc}{p.path}"

    # ------------------------------------------------------------------ #
    # 一覧行の解析
    # ------------------------------------------------------------------ #
    @staticmethod
    def _parse_list_row(row) -> dict:
        """一覧行から名称・入館料・岩盤浴・特徴アイコンを取り出す（いずれも短いラベル）。"""
        info: dict = {}

        name_a = row.select_one("td.meisyo2 a")
        if name_a:
            info["name"] = name_a.get_text(strip=True)

        fee = row.select_one("td.ryokin")
        if fee:
            value = fee.get_text(" ", strip=True)
            if value not in _EMPTY_MARKS:
                info["fee"] = value

        ganban = row.select_one("td.ganban")
        if ganban:
            value = ganban.get_text(" ", strip=True)
            if value not in _EMPTY_MARKS:
                info["rock_bath"] = value

        features = []
        for img in row.select("td.meisyo2 img[alt]"):
            alt = (img.get("alt") or "").strip()
            # 「〇〇：詳細ページへ」「〇〇：クーポン」等の画像説明は特徴ラベルではない
            if alt and "：" not in alt and alt not in features:
                features.append(alt)
        if features:
            info["features"] = "/".join(features)

        return info

    # ------------------------------------------------------------------ #
    # 詳細ページの解析
    # ------------------------------------------------------------------ #
    def _scrape_detail(
        self, detail_url: str, list_info: dict, region: str, pref_ja: str
    ) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        # 名称: 一覧の名称を優先し、無ければ公式ブロック / title から補完
        name = list_info.get("name", "")
        if not name:
            koshiki_nm = soup.select_one("p.koshiki_tenpomei")
            if koshiki_nm:
                name = koshiki_nm.get_text(strip=True)
        if not name and soup.title:
            name = re.split(r"[｜|(（]", soup.title.get_text(strip=True))[0].strip()
        if not name:
            self.logger.warning("施設名を取得できませんでした: %s", detail_url)
            return None

        data: dict = {
            Schema.NAME: name,
            Schema.URL: detail_url,
        }
        if region:
            data["region"] = region
        for key in ("fee", "rock_bath", "features"):
            if list_info.get(key):
                data[key] = list_info[key]

        # 都道府県は URL スラッグ由来を既定にし、住所から取れたら上書きする
        if pref_ja:
            data[Schema.PREF] = pref_ja

        # 情報テーブル（住所 / 電話番号 / 営業時間 / 定休日 ほか）
        info = self._parse_info_table(soup)

        address = info.get("住所", "")
        if address:
            m = _PREF_RE.match(address)
            if m:
                data[Schema.PREF] = m.group(1)
                data[Schema.ADDR] = address[m.end():].strip()
            else:
                data[Schema.ADDR] = address

        tel = self._clean_tel(info.get("電話番号", ""))
        if tel:
            data[Schema.TEL] = tel

        if info.get("営業時間") not in (None, *_EMPTY_MARKS):
            data[Schema.TIME] = info["営業時間"]
        if info.get("定休日") not in (None, *_EMPTY_MARKS):
            data[Schema.HOLIDAY] = info["定休日"]

        # 営業状態（h1 内の閉館・休館注記。無ければ空欄）
        status = self._parse_status(soup)
        if status:
            data[Schema.STS_NM] = status

        # 公式HP・公式SNS（「公式ホームページ」ブロック内に限定）
        block = soup.select_one("div.koshiki_waku1")
        if block:
            hp_a = block.select_one("p.koshiki_hp a[href]")
            if hp_a:
                hp = (hp_a.get("href") or "").strip()
                if hp.startswith("http"):
                    data[Schema.HP] = hp
            for a in block.select("p.koshiki_icon a[href]"):
                href = (a.get("href") or "").strip()
                for pat, col in _SNS_MAP:
                    if pat.search(href) and col not in data:
                        data[col] = href
                        break

        # 備考: 都道府県フィルター（AREA_PREFIXES 設定時のみ適用）
        if self.AREA_PREFIXES and data.get(Schema.PREF, "") not in self.AREA_PREFIXES:
            return None

        return data

    # ------------------------------------------------------------------ #
    # 詳細ページのパーツ
    # ------------------------------------------------------------------ #
    @staticmethod
    def _parse_info_table(soup) -> dict:
        """情報テーブル（td.date_box5=ラベル / td.date_box2=値）を dict 化する。

        テーブルの class はページによって date_box6 / date_box4 等ゆらぐため、
        テーブル class に依存せず tr 単位でラベル/値のペアを拾う。
        """
        info: dict = {}
        for tr in soup.select("tr"):
            label_td = tr.select_one("td.date_box5")
            value_td = tr.select_one("td.date_box2")
            if not label_td or not value_td:
                continue
            label = label_td.get_text(strip=True)
            value = value_td.get_text(" ", strip=True)
            if label and value and label not in info:
                info[label] = value
        return info

    @staticmethod
    def _clean_tel(value: str) -> str:
        """電話番号の値を正規化する（"-" 等の未掲載マークは空文字にする）。"""
        tel = re.sub(r"\s+", "", value or "")
        if tel in _EMPTY_MARKS:
            return ""
        # 数字が5桁未満なら電話番号とみなさない（"-" 混じりの空値対策）
        if len(re.sub(r"\D", "", tel)) < 5:
            return ""
        return tel

    @staticmethod
    def _parse_status(soup) -> str:
        """h1 内の注記（span.covid）から営業状態の短いラベルを判定する。"""
        h1 = soup.select_one("h1")
        if not h1:
            return ""
        note = h1.select_one("span.covid")
        text = note.get_text(" ", strip=True) if note else ""
        if not text:
            return ""
        for pat, label in _STATUS_PATTERNS:
            if pat.search(text):
                return label
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Supersento2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.supersento.com/kanto/tokyo.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
