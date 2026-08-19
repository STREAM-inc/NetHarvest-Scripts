"""
e燃費【ガソリンスタンド一覧】— 全国のガソリンスタンド (給油所) クローラー。

備考「取れるカラムは全部取ってください」に従い、追加通信なしで詳細ページから
取得できる構造化項目をすべて出力する:
    店舗名 / 都道府県 / 郵便番号 / 住所 / 電話番号 / 業種 / 系列ブランド /
    定休日 / 営業時間 / 評価 / 市区町村 / 店舗ID / 24時間営業 / 給油形態 /
    車検整備 / 機械洗車 / 手洗い洗車 / 地図URL /
    実売価格 (レギュラー・ハイオク・軽油 + 更新日時) /
    看板価格 (レギュラー・ハイオク・軽油 + 支払方法 + 更新日時)

取得フロー (すべて引数 url から派生させる。ルート URL のハードコードはしない):
    1. 引数 url (= https://e-nenpi.com/gs/shoplist) から都道府県リンク
       「北海道(1635)」形式の `/gs/pref/{code}/` を 47 件収集する
       (2026-08-19 実測: リンク文言の件数合計 = 25,563 件。total_items に設定)。
    2. 各都道府県ページ `/gs/pref/{code}/` の市区町村絞り込みから
       `/gs/city/{JISコード}/` を収集する (全国で約 1,900 件)。
    3. 市区町村一覧 `/gs/city/{code}/{page}` を 1 ページ 20 件ずつ巡回する。
       行 (td.gs) が 0 件になったページが終端 (「次の20件」リンクは末尾を超えても
       常に描画されるため、リンクの有無では終端を判定できない)。
       2026-08-19 実測: /gs/city/47201/ → 20件, /2 → 20件, /3 → 3件, 以降 0 件。
    4. 店舗詳細 `/gs/shop/{id}` を 1 件取得するたびに即 yield する (Pattern B)。
       市区町村リンクが 1 件も無い都道府県は、都道府県一覧
       `/gs/pref/{code}/{page}` のページ送りにフォールバックする。

なぜ市区町村ルートを主に使うか:
    都道府県ページ送り `/gs/pref/{code}/{page}` の一覧には閉店等で詳細が 404 に
    なった店舗が大量に残留している (約 5 割が死にリンク)。市区町村一覧は現存店舗が
    並ぶため無駄な通信を大幅に減らせる。取りこぼしを防ぐため、市区町村リンクが
    無い都道府県は都道府県ページ送りにフォールバックする。

実装メモ:
    - 店舗名は JSON-LD (LocalBusiness) の name を使う。h1.contentsTitle は系列名が
      前置される (例: 「ENEOS 共栄石油 神保町SS」) ため名称には使わない。
    - 「店舗情報」欄のアイコンは詳細ページでは alt 付き (例: alt="セルフ給油") だが、
      一覧ページでは alt が空。詳細ページの alt を主に使い、欠落時は画像ファイル名
      (gs_ico_self.gif / gs_wash_machine.gif / gs_brandNN.png 等) から判定する。
    - 系列ブランドは `a[href^="/gs/brand/"]` の画像 alt。独自ブランドは brand/1000。
    - e燃費の都道府県コード (`/gs/pref/3/` = 秋田県) は JIS コードと一致しないため、
      市区町村コード (JIS 5桁) の先頭 2 桁との突き合わせ判定はしない。巡回中の
      都道府県ページに載っている市区町村リンクだけを使い、店舗 URL 単位で重複排除する。
    - 緯度経度は詳細ページに無く `/gs/map/{id}` の追加取得が必要なため取得しない
      (店舗数 25,563 件に対し通信量が倍増するため)。地図URLのみ出力する。
    - FAX / メール / HP / 代表者 / 資本金 / 従業員数 / 法人番号 はサイト上に存在しない。

除外方針 (著作権リスク回避):
    - 「備考」欄・クチコミ本文はユーザー投稿の自由記述 (プロース) のため取得しない。
    - 価格は数値・日時の構造化データのため取得する (自由記述ではない)。

利用規約 (https://e-nenpi.com/guide/rule):
    第3条は「本サービスに掲載された記事」の無断複製・自動公衆送信・転載等を禁止する
    のみで、スクレイピング/クローリングを明示的に禁止する条項は無い。
    robots.txt も /gs/ 配下は投稿フォーム系 (rating_form / price_form 等) のみ Disallow で、
    /gs/shoplist・/gs/pref/・/gs/city/・/gs/shop/ はいずれも許可されている。

実行方法:
    python bin/smoke_test.py scripts/sites/auto/e_nenpi_6.py \
        "https://e-nenpi.com/gs/shoplist" --limit 3 --timeout 90
    python scripts/sites/auto/e_nenpi_6.py
    docker compose exec worker python /app/bin/run_flow.py --site-id e_nenpi_6
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 都道府県リンク: /gs/pref/13/ (ページ番号付き /gs/pref/13/2 は拾わない)
_PREF_LINK_RE = re.compile(r"^/gs/pref/(\d+)/?$")
# 市区町村リンク: /gs/city/13101/ (JIS 5桁。ページ番号付きは拾わない)
_CITY_LINK_RE = re.compile(r"^/gs/city/(\d{5})/?$")
# 店舗詳細リンク: /gs/shop/47402
_SHOP_LINK_RE = re.compile(r"/gs/shop/(\d+)")
# リンク文言「東京都(808)」→ 件数
_COUNT_SUFFIX_RE = re.compile(r"[（(]\s*([\d,]+)\s*[）)]\s*$")
# 住所先頭の都道府県 (47 件を明示列挙。`.+?[都道府県]` は「京都府」等を誤マッチする)
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 詳細ページ .infoArea のフォールバック用
# (例: 評価： 0.0pt 住所：〒900-0031 沖縄県那覇市若狭2-19-1 [ 地図 ] 電話番号：0988680531)
_POST_TEXT_RE = re.compile(r"〒\s*(\d{3})[-－]?(\d{4})")
_ADDR_TEXT_RE = re.compile(
    r"住所[：:]\s*(?:〒\s*\d{3}[-－]?\d{4}\s*)?(.*?)(?:\[\s*地図\s*\]|電話番号|$)", re.S
)
_ADDR_TAIL_RE = re.compile(r"\s*\[[^\]]*\]\s*$")
_TEL_TEXT_RE = re.compile(r"電話番号[：:]\s*([\d\-－()（）]+)")
_SCORE_RE = re.compile(r"([\d.]+)\s*pt")
# JSON-LD の postalCode は "9000031" のようにハイフン無し
_POST_DIGITS_RE = re.compile(r"^(\d{3})(\d{4})$")
# 価格セル「158円」→ 158 ("---" は未投稿)
_PRICE_RE = re.compile(r"(\d[\d,]*)\s*円")
# アイコン画像ファイル名 → 意味 (詳細ページの alt が欠落したときのフォールバック)
_ICON_FILE_MAP = {
    "gs_ico_24h": "24時間営業",
    "gs_ico_self": "セルフ給油",
    "gs_ico_full": "フル給油",
    "gs_ico_maintenance": "車検整備",
    "gs_ico_repair_service": "車検整備",
    "gs_wash_machine": "機械洗車",
    "gs_wash_byhand": "手洗い洗車",
}
# 給油形態を表すラベル (残りは設備フラグ扱い)
_FUEL_STYLE_LABELS = ("セルフ給油", "フル給油")
# 設備フラグの出力カラム名 (アイコンの有無で「あり」/空文字)
_FACILITY_LABELS = ("24時間営業", "車検整備", "機械洗車", "手洗い洗車")


class ENenpi6Scraper(StaticCrawler):
    """e燃費【ガソリンスタンド一覧】スクレイパー (市区町村ルート + 都道府県フォールバック)"""

    DELAY = 0.5
    # 1 エリアあたりの一覧ページ数上限 (20 件/ページ。暴走防止のガード)
    MAX_PAGES_PER_AREA = 100

    EXTRA_COLUMNS = [
        "店舗ID",
        "市区町村",
        "給油形態",
        "24時間営業",
        "車検整備",
        "機械洗車",
        "手洗い洗車",
        "地図URL",
        "レギュラー実売価格",
        "ハイオク実売価格",
        "軽油実売価格",
        "実売価格更新日時",
        "レギュラー看板価格",
        "ハイオク看板価格",
        "軽油看板価格",
        "看板価格支払方法",
        "看板価格更新日時",
    ]

    # ------------------------------------------------------------------ #
    #  メイン処理
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            self.logger.error("ガソリンスタンド一覧ページを取得できませんでした: %s", url)
            return

        prefs = self._collect_prefs(soup, url)
        if not prefs:
            self.logger.error("都道府県リンクを抽出できませんでした: %s", url)
            return

        total = sum(p["count"] for p in prefs)
        if total > 0:
            self.total_items = total
        self.logger.info(
            "都道府県 %d 件 / サイト表記の掲載件数合計 %d 件 (全都道府県を対象)",
            len(prefs), total,
        )

        seen: set[str] = set()
        for pref in prefs:
            cities = self._collect_cities(pref, url)
            if cities:
                self.logger.info("%s: 市区町村 %d 件", pref["name"], len(cities))
                for city in cities:
                    yield from self._crawl_area(url, pref, city["url"], city["name"], seen)
            else:
                # 市区町村リンクが無い都道府県は都道府県一覧のページ送りで巡回する
                self.logger.warning(
                    "%s: 市区町村リンク無し → 都道府県一覧のページ送りにフォールバック",
                    pref["name"],
                )
                yield from self._crawl_area(url, pref, pref["url"], "", seen)

    # ------------------------------------------------------------------ #
    #  リンク収集
    # ------------------------------------------------------------------ #
    def _collect_prefs(self, soup: bs4.BeautifulSoup, root_url: str) -> list[dict]:
        """一覧トップから /gs/pref/{code}/ の コード / 名称 / 件数 / URL を収集する。"""
        prefs: list[dict] = []
        seen_codes: set[str] = set()
        for a in soup.select('a[href*="/gs/pref/"]'):
            href = (a.get("href") or "").strip()
            m = _PREF_LINK_RE.match(href)
            if not m or m.group(1) in seen_codes:
                continue
            text = a.get_text(strip=True)
            cm = _COUNT_SUFFIX_RE.search(text)          # 「北海道(1635)」
            name = _COUNT_SUFFIX_RE.sub("", text).strip()
            if not name:
                continue
            seen_codes.add(m.group(1))
            prefs.append({
                "code": m.group(1),
                "name": name,
                "count": int(cm.group(1).replace(",", "")) if cm else 0,
                "url": urljoin(root_url, href),
            })
        return prefs

    def _collect_cities(self, pref: dict, root_url: str) -> list[dict]:
        """都道府県ページの市区町村絞り込みから /gs/city/{JISコード}/ を収集する。"""
        soup = self.get_soup(pref["url"])
        if soup is None:
            return []

        cities: list[dict] = []
        seen_codes: set[str] = set()
        for a in soup.select('a[href^="/gs/city/"]'):
            href = (a.get("href") or "").strip()
            m = _CITY_LINK_RE.match(href)
            if not m or m.group(1) in seen_codes:
                continue
            name = _COUNT_SUFFIX_RE.sub("", a.get_text(strip=True)).strip()
            if not name:
                continue
            seen_codes.add(m.group(1))
            cities.append({
                "code": m.group(1),
                "name": name,
                "url": urljoin(root_url, href),
            })
        return cities

    # ------------------------------------------------------------------ #
    #  エリア (市区町村 or 都道府県) のページ巡回 — 1 件ごとに即 yield
    # ------------------------------------------------------------------ #
    def _crawl_area(self, root_url: str, pref: dict, area_url: str,
                    city_name: str, seen: set[str]) -> Generator[dict, None, None]:
        base = area_url.rstrip("/")
        for page in range(1, self.MAX_PAGES_PER_AREA + 1):
            list_url = f"{base}/{page}"
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗 (このエリアを打ち切り): %s", list_url)
                return

            rows = soup.select("td.gs")
            if not rows:
                return          # 末尾を超えたページは行が 0 件 = 終端

            for row in rows:
                a = row.select_one('a[href*="/gs/shop/"]')
                if a is None:
                    continue
                href = a.get("href", "")
                detail_url = urljoin(root_url, href)
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                sm = _SHOP_LINK_RE.search(href)
                shop_id = sm.group(1) if sm else ""
                list_name = a.get_text(strip=True)
                # <a>店舗名</a><br />住所 の形。リンク文言を除いた残りが住所
                list_addr = row.get_text("\n", strip=True).replace(list_name, "", 1).strip()

                try:
                    item = self._scrape_detail(
                        detail_url, shop_id, pref["name"], city_name, list_name, list_addr
                    )
                except Exception as e:      # 1 件の失敗で全体を止めない
                    self.error_count += 1
                    self.logger.warning("詳細取得に失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

        self.logger.warning(
            "%s %s: ページ数上限 (%d) に到達", pref["name"], city_name or "(都道府県一覧)",
            self.MAX_PAGES_PER_AREA,
        )

    # ------------------------------------------------------------------ #
    #  詳細ページの解析
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, url: str, shop_id: str, pref_name: str, city_name: str,
                       list_name: str, list_addr: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            # 閉店等で 404 になった店舗 (一覧に残留していることがある)
            return None

        ld = self._parse_jsonld(soup)
        ld_addr = ld.get("address") or {}

        info = soup.select_one(".infoArea")
        info_text = info.get_text(" ", strip=True) if info is not None else ""

        # --- 店舗名 (h1 は系列名が前置されるので使わない) ---
        name = (ld.get("name") or "").strip() or list_name

        # --- 郵便番号 ---
        post_code = self._format_post_code((ld_addr.get("postalCode") or "").strip())
        if not post_code and info_text:
            m = _POST_TEXT_RE.search(info_text)
            if m:
                post_code = f"{m.group(1)}-{m.group(2)}"

        # --- 住所 (都道府県を含む生の住所) ---
        addr_full = (ld_addr.get("addressLocality") or "").strip()
        if not addr_full and info_text:
            m = _ADDR_TEXT_RE.search(info_text)
            if m:
                addr_full = _ADDR_TAIL_RE.sub(
                    "", re.sub(r"\s+", " ", m.group(1)).strip()
                ).strip()
        if not addr_full:
            addr_full = list_addr

        # --- 都道府県 / 市区町村以降 に分割 ---
        pref = (ld_addr.get("addressRegion") or "").strip()
        addr = addr_full
        m = _PREF_RE.match(addr_full)
        if m:
            pref = pref or m.group(1)
            addr = addr_full[m.end():].strip()
        if not pref:
            pref = pref_name       # 巡回中の都道府県名で補完

        # --- 電話番号 ---
        tel = (ld.get("telephone") or "").strip()
        if not tel and info_text:
            m = _TEL_TEXT_RE.search(info_text)
            if m:
                tel = m.group(1).strip()

        # --- 評価 (0.0pt 表記) ---
        score = ""
        if info_text:
            m = _SCORE_RE.search(info_text)
            if m:
                score = m.group(1)

        shop_info = self._parse_shop_info(soup)
        hours = shop_info["hours"] or self._jsonld_hours(ld)

        # --- 地図URL (詳細ページの [地図] リンク) ---
        map_url = ""
        map_a = soup.select_one('a[href*="/gs/map/"]')
        if map_a is not None:
            map_url = urljoin(url, map_a.get("href", ""))
        elif shop_id:
            map_url = urljoin(url, f"/gs/map/{shop_id}")

        actual = self._parse_price_table(soup, "実売価格")
        board = self._parse_price_table(soup, "看板価格")

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_LV1: "ガソリンスタンド",
            Schema.CAT_SITE: shop_info["brand"],
            Schema.HOLIDAY: shop_info["holiday"],
            Schema.TIME: hours,
            Schema.SCORES: score,
            "店舗ID": shop_id,
            "市区町村": city_name,
            "給油形態": shop_info["fuel_style"],
            "24時間営業": shop_info["24時間営業"],
            "車検整備": shop_info["車検整備"],
            "機械洗車": shop_info["機械洗車"],
            "手洗い洗車": shop_info["手洗い洗車"],
            "地図URL": map_url,
            "レギュラー実売価格": actual.get("レギュラー", ""),
            "ハイオク実売価格": actual.get("ハイオク", ""),
            "軽油実売価格": actual.get("軽油", ""),
            "実売価格更新日時": actual.get("更新日時", ""),
            "レギュラー看板価格": board.get("レギュラー", ""),
            "ハイオク看板価格": board.get("ハイオク", ""),
            "軽油看板価格": board.get("軽油", ""),
            "看板価格支払方法": board.get("支払方法", ""),
            "看板価格更新日時": board.get("更新日時", ""),
        }

    # ------------------------------------------------------------------ #
    #  補助メソッド
    # ------------------------------------------------------------------ #
    @staticmethod
    def _parse_jsonld(soup: bs4.BeautifulSoup) -> dict:
        """JSON-LD の LocalBusiness を取り出す (無ければ空 dict)。"""
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            for cand in (data if isinstance(data, list) else [data]):
                if isinstance(cand, dict) and cand.get("@type") == "LocalBusiness":
                    return cand
        return {}

    @staticmethod
    def _format_post_code(value: str) -> str:
        """「9000031」→「900-0031」(既にハイフン付きならそのまま)。"""
        if not value:
            return ""
        m = _POST_DIGITS_RE.match(value)
        return f"{m.group(1)}-{m.group(2)}" if m else value

    @staticmethod
    def _jsonld_hours(ld: dict) -> str:
        """openingHoursSpecification から「7:00 ～ 20:00」を組み立てる。"""
        spec = ld.get("openingHoursSpecification") or {}
        if isinstance(spec, list):
            spec = spec[0] if spec and isinstance(spec[0], dict) else {}
        opens = (spec.get("opens") or "").strip()
        closes = (spec.get("closes") or "").strip()
        if opens and closes:
            return f"{opens} ～ {closes}"
        return opens or closes

    @staticmethod
    def _icon_label(img: bs4.Tag) -> str:
        """アイコン img から意味ラベルを取り出す (alt 優先、無ければファイル名で判定)。"""
        alt = (img.get("alt") or "").strip()
        if alt:
            return alt
        src = img.get("src") or ""
        stem = src.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        return _ICON_FILE_MAP.get(stem, "")

    def _parse_shop_info(self, soup: bs4.BeautifulSoup) -> dict:
        """div.shopInfo の「店舗情報」表から 系列 / 給油形態 / 設備 / 定休日 / 営業時間 を取り出す。

        「備考」行はユーザー投稿の自由記述なので意図的に取得しない。
        """
        result = {
            "brand": "",
            "fuel_style": "",
            "holiday": "",
            "hours": "",
            **{label: "" for label in _FACILITY_LABELS},
        }
        block = soup.select_one("div.shopInfo")
        if block is None:
            return result

        for tr in block.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th is None or td is None:
                continue
            label = th.get_text(strip=True)
            if label == "店舗情報":
                brand_img = td.select_one('a[href*="/gs/brand/"] img')
                if brand_img is not None:
                    result["brand"] = self._icon_label(brand_img)
                styles: list[str] = []
                for img in td.select("img"):
                    if brand_img is not None and img is brand_img:
                        continue
                    icon = self._icon_label(img)
                    if icon in _FUEL_STYLE_LABELS:
                        styles.append(icon)
                    elif icon in _FACILITY_LABELS:
                        result[icon] = "あり"
                result["fuel_style"] = " / ".join(dict.fromkeys(styles))
            elif label == "定休日":
                result["holiday"] = re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip()
            elif label == "営業時間":
                result["hours"] = re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip()
        return result

    def _parse_price_table(self, soup: bs4.BeautifulSoup, caption: str) -> dict:
        """caption が「実売価格」/「看板価格」の表から最新 1 行を取り出す。

        ヘッダー (レギュラー / ハイオク / 軽油 / 支払方法 / 投稿ユーザ / 更新日時) の
        並び順に依存しないよう、ヘッダー名とセルを対応付けて読む。
        投稿ユーザ名 (匿名ID) は個人に紐づく情報なので取得しない。
        """
        table = None
        for cand in soup.select("table"):
            cap = cand.find("caption")
            if cap is not None and cap.get_text(strip=True) == caption:
                table = cand
                break
        if table is None:
            return {}

        rows = table.select("tr")
        if len(rows) < 2:
            return {}
        headers = [th.get_text(" ", strip=True) for th in rows[0].select("th")]
        if not headers:
            return {}

        # 先頭のデータ行 = 最新の投稿
        cells = rows[1].select("td")
        result: dict[str, str] = {}
        for header, cell in zip(headers, cells):
            if header not in ("レギュラー", "ハイオク", "軽油", "支払方法", "更新日時"):
                continue
            text = re.sub(r"\s+", " ", cell.get_text(" ", strip=True)).strip()
            if header in ("レギュラー", "ハイオク", "軽油"):
                m = _PRICE_RE.search(text)          # 「158円」→ 158 / 「---」→ 空
                result[header] = m.group(1).replace(",", "") if m else ""
            else:
                result[header] = "" if text in ("---", "-", "") else text
        return result


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ENenpi6Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://e-nenpi.com/gs/shoplist")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
