"""
ゼクシィ (ZEXY) — 全国の結婚式場ポータル 式場情報スクレイパー

取得対象:
    - ゼクシィ (zexy.net) に掲載されている全国の結婚式場 (詳細ページ
      /wedding/c_{id}/) の構造化情報
    - 式場名 / 都道府県 / 住所 / TEL / 営業時間 / 口コミ採点 / 口コミ件数 /
      最寄り駅 / エリア(市区町村) / 駐車場 / 緯度 / 経度
    - 詳細ページ「基本情報」セクションの構造化項目 (挙式スタイル / 料理料金 /
      飲物料金 / 収容人数 / 持込料金 / 設備 / 宿泊施設 / 二次会 / 送迎 /
      支払方法 / キャンセル料金 / その他)

取得フロー:
    1. ルート URL (https://zexy.net/) を起点に、全件リストが存在しないため
       47 都道府県のクライアント一覧 (/wedding/{pref}/clientList/) を順に巡回する。
       ページ送りは `/wedding/{pref}/clientList/p_{N}/` (1ページ約50件)。
    2. 各一覧ページから式場詳細 ID (/wedding/c_{id}/) を抽出する。
    3. 詳細ページ (JSON-LD: 名称/都道府県/住所/緯度経度/口コミ採点/最寄り駅/エリア、
       および同ページ内「基本情報」dl: 挙式スタイル/料理飲物料金/収容人数 等)
       と電話番号・地図ページ (/wedding/c_{id}/mapTel/: TEL/営業時間/駐車場) を
       1 件取得するたびに即 yield する (早期 yield / 途中中断に強い Pattern B)。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、一覧・詳細・mapTel の URL は
      すべて `url` から派生させる (`urljoin(url, ...)`)。別 URL はハードコードしない。
    - 電話番号・営業時間は詳細ページに載らず「電話予約する」遷移先の mapTel ページ
      (/wedding/c_{id}/mapTel/) にのみ掲載されるため、そちらから取得する。
    - 「基本情報」セクション (挙式スタイル / 料理飲物料金 / 収容人数 / 持込料金 / 設備 /
      宿泊施設 / 二次会 / 送迎 / 支払方法 / キャンセル料金 / その他) は会場掲載の構造化
      項目 (dt/dd) として取得する。口コミ本文 / 交通(道順説明) 等の長文自由記述は
      著作権リスク回避のため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/service/zexy.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id zexy
"""

import html
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


# 全 47 都道府県のスラッグ (地域スラッグ tohoku/kansai 等は含めない)
_PREF_SLUGS = [
    "hokkaido",
    "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano",
    "gifu", "shizuoka", "aichi", "mie",
    "shiga", "kyoto", "osaka", "hyogo", "nara", "wakayama",
    "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi",
    "fukuoka", "saga", "nagasaki", "kumamoto", "oita", "miyazaki",
    "kagoshima", "okinawa",
]

# 式場詳細 ID (/wedding/c_{id}/) を抽出するパターン
_DETAIL_ID_PATTERN = re.compile(r"/wedding/c_(\d+)/")

# mapTel ページ「その他の情報」セルの「ラベル／値」を分割するためのラベル
_OTHER_LABELS = ["営業時間", "駐車場", "予約", "担当", "利用可能時間", "お問い合わせ", "その他"]
_OTHER_PATTERN = re.compile(r"(" + "|".join(_OTHER_LABELS) + r")／")

# 電話番号抽出 (半角化は Pipeline 側で処理されるため整形は最小限)
_TEL_PATTERN = re.compile(r"0\d{1,4}[-－]\d{1,4}[-－]\d{3,4}")

# Schema に無いサイト固有の構造化項目 (いずれも短いラベル/数値。自由記述は含めない)
_COL_STATION = "最寄り駅"
_COL_AREA = "エリア"
_COL_PARKING = "駐車場"
_COL_LAT = "緯度"
_COL_LNG = "経度"

# 詳細ページ内「基本情報」セクション (data-js-hook="basic-info") の dl ラベル。
# 各 dl は 1 個の dt(ラベル)/dd(値) を持ち、値は会場掲載の構造化情報。
_BASIC_INFO_LABELS = [
    "挙式スタイル",
    "料理料金",
    "飲物料金",
    "収容人数",
    "持込料金",
    "設備",
    "宿泊施設",
    "二次会",
    "送迎",
    "支払方法",
    "キャンセル料金",
    "その他",
]

# 都道府県ごとのページ巡回安全上限 (最大規模の都道府県でも十分な余裕)
_MAX_PAGES = 30


class Zexy(StaticCrawler):
    """ゼクシィ (ZEXY) 全国の結婚式場 情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        _COL_STATION, _COL_AREA, _COL_PARKING, _COL_LAT, _COL_LNG,
    ] + _BASIC_INFO_LABELS

    def parse(self, url: str):
        seen: set[str] = set()

        for pref in _PREF_SLUGS:
            # 当該都道府県の一覧 1 ページ目 (url からの派生)
            list_base = urljoin(url, f"/wedding/{pref}/clientList/")

            page = 1
            while page <= _MAX_PAGES:
                list_url = list_base if page == 1 else urljoin(list_base, f"p_{page}/")
                soup = self.get_soup(list_url)
                if soup is None:
                    break

                # このページに含まれる式場詳細 ID を出現順で抽出 (重複除去)
                page_ids: list[str] = []
                for a in soup.find_all("a", href=True):
                    m = _DETAIL_ID_PATTERN.search(a["href"])
                    if not m:
                        continue
                    cid = m.group(1)
                    if cid not in seen:
                        seen.add(cid)
                        page_ids.append(cid)

                # 新規 ID が無ければこの都道府県は末尾ページに到達したとみなす
                if not page_ids:
                    break

                # 詳細を 1 件取得するたびに即 yield (早期 yield / 途中中断に強い)
                for cid in page_ids:
                    detail_url = urljoin(url, f"/wedding/c_{cid}/")
                    try:
                        item = self._scrape_detail(url, cid, detail_url)
                        if item:
                            yield item
                    except Exception as e:  # 個別式場の失敗は握りつぶして継続
                        self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                        continue

                page += 1

    def _scrape_detail(self, root_url: str, cid: str, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        # --- 詳細ページの JSON-LD (LocalBusiness / BreadcrumbList) ---
        local_business = None
        breadcrumb = None
        for s in soup.find_all("script", type="application/ld+json"):
            raw = s.string or s.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if isinstance(data, dict):
                if data.get("@type") == "LocalBusiness":
                    local_business = data
                elif data.get("@type") == "BreadcrumbList":
                    breadcrumb = data

        if local_business:
            name = html.unescape((local_business.get("name") or "").strip())
            if name:
                item[Schema.NAME] = name

            addr = local_business.get("address") or {}
            if isinstance(addr, dict):
                pref = html.unescape((addr.get("addressRegion") or "").strip())
                if pref:
                    item[Schema.PREF] = pref
                addr_local = html.unescape(
                    (addr.get("addressLocality") or addr.get("streetAddress") or "").strip()
                )
                if addr_local:
                    item[Schema.ADDR] = addr_local

            rating = local_business.get("aggregateRating") or {}
            if isinstance(rating, dict):
                if rating.get("ratingValue") not in (None, ""):
                    item[Schema.SCORES] = str(rating.get("ratingValue"))
                if rating.get("reviewCount") not in (None, ""):
                    item[Schema.REV_SCR] = str(rating.get("reviewCount"))

            geo = local_business.get("geo") or {}
            if isinstance(geo, dict):
                if geo.get("latitude") not in (None, ""):
                    item[_COL_LAT] = str(geo.get("latitude"))
                if geo.get("longitude") not in (None, ""):
                    item[_COL_LNG] = str(geo.get("longitude"))

        # パンくずから 最寄り駅 (/st_) とエリア=市区町村 (/sa_) の短いラベルを取得
        if breadcrumb:
            for e in breadcrumb.get("itemListElement", []):
                if not isinstance(e, dict):
                    continue
                link = e.get("item") or ""
                label = html.unescape((e.get("name") or "").strip())
                if not label:
                    continue
                if "/st_" in link:
                    item[_COL_STATION] = label
                elif "/sa_" in link:
                    item[_COL_AREA] = label

        # --- 詳細ページ内「基本情報」セクション (挙式スタイル/料理飲物料金 等) ---
        self._fill_from_basic_info(soup, item)

        # --- mapTel ページ (TEL / 営業時間 / 駐車場) ---
        maptel_url = urljoin(root_url, f"/wedding/c_{cid}/mapTel/")
        self._fill_from_maptel(maptel_url, item)

        # NAME が取れなければ無効なページとして捨てる
        if not item.get(Schema.NAME):
            return None

        return item

    def _fill_from_basic_info(self, soup, item: dict) -> None:
        """詳細ページ内「基本情報」セクションの dl/dt/dd から構造化項目を取得する。

        各項目は <dl class="hnm-clientTop-multiColumn__item"> 内の
        <dt>(ラベル) / <dd>(値) で構成され、_BASIC_INFO_LABELS のラベルのみ採用する。
        """
        section = soup.find(attrs={"data-js-hook": "basic-info"})
        if section is None:
            return

        for dl in section.find_all("dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not (dt and dd):
                continue
            label = dt.get_text(strip=True)
            if label not in _BASIC_INFO_LABELS or label in item:
                continue
            # &yen;/&nbsp; 等は get_text で復号される。連続空白は 1 個に正規化。
            value = re.sub(r"\s+", " ", dd.get_text(" ", strip=True)).strip()
            if value:
                item[label] = value

    def _fill_from_maptel(self, maptel_url: str, item: dict) -> None:
        soup = self.get_soup(maptel_url)
        if soup is None:
            return

        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not (th and td):
                    continue
                header = th.get_text(strip=True)
                value = td.get_text(" ", strip=True)

                if "問合せ" in header:
                    m = _TEL_PATTERN.search(value)
                    if m and Schema.TEL not in item:
                        item[Schema.TEL] = m.group(0)
                elif "その他" in header:
                    parts = self._split_other(value)
                    hours = parts.get("営業時間", "")
                    if hours:
                        item[Schema.TIME] = hours
                    parking = parts.get("駐車場", "")
                    if parking:
                        item[_COL_PARKING] = parking

    @staticmethod
    def _split_other(value: str) -> dict:
        """mapTel「その他の情報」セルの「ラベル／値 ラベル／値…」を辞書化する。"""
        parts: dict[str, str] = {}
        matches = list(_OTHER_PATTERN.finditer(value))
        for i, m in enumerate(matches):
            key = m.group(1)
            start = m.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(value)
            if key not in parts:
                parts[key] = value[start:end].strip()
        return parts


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Zexy()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を唯一の起点とし、一覧・詳細・mapTel URL は url から派生させる。
    scraper.execute("https://zexy.net/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
