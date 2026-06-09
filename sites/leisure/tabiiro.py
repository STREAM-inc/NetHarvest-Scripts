"""
旅色（旅館・ホテル） — tabiiro.jp/yado

取得対象:
    - 全国の旅館・ホテルの基本情報
    - 名称 / 名称カナ / 郵便番号 / 都道府県 / 住所 / TEL / 公式HP / Instagram
    - サイト定義エリア（パンくずのサブエリア）
    - EXTRA: 宿コード・所在地（市区町村）・チェックイン/アウト・客室数・緯度経度・
             関連キーワード（section#hashtags のタグ ＋ こだわり条件タグ）

取得フロー:
    1. 47 都道府県の一覧ページ（/yado/{region}/{pref}/ ＋ /yado/hokkaido/・/yado/okinawa/）
       を 1 ページずつ取得し、宿詳細 URL ( /yado/s/{code}-{slug}/ ) を列挙する。
       ※ このサイトの一覧は 1 ページに全件が載り、?p=N 等のページ送りは効かない
         （?p=2 は 1 ページ目と同一内容）。よって都道府県ページ＝完全な一覧。
    2. 各詳細ページを 1 件ずつ取得して即 yield（Pattern B）。
       - 名称/TEL/住所/郵便番号/チェックイン/客室数/緯度経度/エリアは
         JSON-LD（schema.org Hotel ＋ BreadcrumbList）から抽出（テンプレ差異に強い）。
       - 名称カナはインフォメーション見出し、公式HP/Instagram/関連キーワード
         （section#hashtags のタグ ＋ こだわり条件タグ）は HTML から抽出。
       - JSON-LD に制御文字が混入してパースに失敗するページがあるため、寛容パースを行う。
       - 詳細ページには 2 系統のテンプレートがある（標準: h1.spotName ＋ ul.features、
         特集型: h1.titleBlock_name）。JSON-LD はどちらにも存在し共通の取得元になる。

備考対応:
    - 呼び出し時の備考「関連キーワードを取得してください」に対応し、宿ごとの関連キーワードを
      related_keywords として取得する。取得元は 2 系統:
        (1) section#hashtags（見出し「関連キーワード」）のタグリンク
            （例: バイキング / 岩盤浴 / サウナ / 白浜温泉 / 海鮮 / リゾート）。
        (2) ul.features のこだわり条件タグ（例: 露天風呂付客室あり / ペットOK / 温泉）。
      いずれも構造化された短いラベルであり自由記述プロースではないため著作権上の問題はない。
      2 系統を順序保持で結合し、重複は除去して「 / 」連結する。
    - エリア等のフィルター指示は無かったため全国全件取得。エリア限定が必要になった場合は
      AREA_PREFIXES（都道府県名）を設定すると parse() 側でフィルタする（既定: 空 = 全件）。

著作権配慮:
    - 施設紹介文（JSON-LD description / meta description）・客室紹介・お風呂紹介・
      宿泊プラン本文・関連記事など自由記述プロースは取得しない。
    - サイト共通の LINE 公式（@tabiiro）等サイト運営者の SNS は宿の情報ではないため除外。

実行方法:
    # ローカルテスト
    python scripts/sites/leisure/tabiiro.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id tabiiro
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_BASE = "https://tabiiro.jp"

# 47 都道府県の一覧ページパス。
# 北海道・沖縄は地方＝都道府県のため /yado/{region}/ 直下。その他は /yado/{region}/{pref}/。
_PREF_PATHS = [
    "/yado/hokkaido/",
    # 東北
    "/yado/touhoku/aomori/", "/yado/touhoku/iwate/", "/yado/touhoku/miyagi/",
    "/yado/touhoku/akita/", "/yado/touhoku/yamagata/", "/yado/touhoku/fukushima/",
    # 関東
    "/yado/kantou/ibaraki/", "/yado/kantou/tochigi/", "/yado/kantou/gunma/",
    "/yado/kantou/saitama/", "/yado/kantou/chiba/", "/yado/kantou/tokyo/",
    "/yado/kantou/kanagawa/",
    # 甲信越
    "/yado/koushinetsu/niigata/", "/yado/koushinetsu/nagano/",
    "/yado/koushinetsu/yamanashi/",
    # 北陸
    "/yado/hokuriku/toyama/", "/yado/hokuriku/ishikawa/", "/yado/hokuriku/fukui/",
    # 東海
    "/yado/tokai/gifu/", "/yado/tokai/shizuoka/", "/yado/tokai/aichi/",
    "/yado/tokai/mie/",
    # 近畿
    "/yado/kinki/shiga/", "/yado/kinki/kyoto/", "/yado/kinki/osaka/",
    "/yado/kinki/hyogo/", "/yado/kinki/nara/", "/yado/kinki/wakayama/",
    # 山陰山陽
    "/yado/saninsanyo/tottori/", "/yado/saninsanyo/shimane/",
    "/yado/saninsanyo/okayama/", "/yado/saninsanyo/hiroshima/",
    "/yado/saninsanyo/yamaguchi/",
    # 四国
    "/yado/shikoku/tokushima/", "/yado/shikoku/kagawa/", "/yado/shikoku/ehime/",
    "/yado/shikoku/kouchi/",
    # 九州
    "/yado/kyushu/fukuoka/", "/yado/kyushu/saga/", "/yado/kyushu/nagasaki/",
    "/yado/kyushu/kumamoto/", "/yado/kyushu/oita/", "/yado/kyushu/miyazaki/",
    "/yado/kyushu/kagoshima/",
    # 沖縄
    "/yado/okinawa/",
]

# 宿詳細 URL の正規化用: /yado/s/{code}-{slug}/ を抽出（/report/N/ 等の下層を除去）
_DETAIL_RE = re.compile(r"(/yado/s/(\d+)-[^/?#]+/)")
# 都道府県の先頭マッチ（住所からの分離用）
_PREF_RE = re.compile(r"(北海道|東京都|(?:大阪|京都)府|.{2,3}?県)")


class TabiiroScraper(StaticCrawler):
    """旅色（旅館・ホテル）スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "yado_code",          # 宿コード（URL 内の数字）
        "city",               # 所在地（市区町村）
        "checkin",            # チェックイン時間
        "checkout",           # チェックアウト時間
        "num_rooms",          # 客室数
        "latitude",           # 緯度
        "longitude",          # 経度
        "related_keywords",   # 関連キーワード（section#hashtags のタグ ＋ こだわり条件タグを「 / 」連結）
    ]

    # エリア限定が必要なときに都道府県名を入れる（例: ["千葉県", "東京都"]）。空なら全件。
    AREA_PREFIXES: list[str] = []

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()

        for path in _PREF_PATHS:
            list_url = _BASE + path
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗（スキップ）: %s", list_url)
                continue

            detail_urls = self._extract_detail_urls(soup)
            self.logger.info("%s: 宿 %d 件", path, len(detail_urls))

            for detail_url in detail_urls:
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # noqa: BLE001 — 個別アイテムのエラーは握りつぶして継続
                    self.logger.warning("詳細ページ処理に失敗: %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

    # ------------------------------------------------------------------ #
    # 一覧ページから詳細 URL を列挙
    # ------------------------------------------------------------------ #
    def _extract_detail_urls(self, soup) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            m = _DETAIL_RE.search(a["href"])
            if not m:
                continue
            canonical = _BASE + m.group(1)
            if canonical in seen:
                continue
            seen.add(canonical)
            urls.append(canonical)
        return urls

    # ------------------------------------------------------------------ #
    # 詳細ページの解析
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        hotel, breadcrumb = self._parse_jsonld(soup)

        name = (hotel.get("name") or "").strip()
        if not name:
            # 名称が取れないページ（休止中・無効コード等）はスキップ
            h1 = soup.select_one("h1.spotName, h1.titleBlock_name, h1")
            name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        data: dict = {
            Schema.NAME: name,
            Schema.URL: detail_url,
        }

        # 宿コード
        m = _DETAIL_RE.search(detail_url)
        if m:
            data["yado_code"] = m.group(2)

        # 住所・郵便番号・都道府県（JSON-LD）
        addr = hotel.get("address") or {}
        if isinstance(addr, dict):
            if addr.get("postalCode"):
                data[Schema.POST_CODE] = str(addr["postalCode"]).strip()
            region = (addr.get("addressRegion") or "").strip()
            if region:
                data[Schema.PREF] = region
            if addr.get("addressLocality"):
                data["city"] = str(addr["addressLocality"]).strip()
            street = (addr.get("streetAddress") or "").strip()
            if street:
                # streetAddress は都道府県を含むことが多いので分離する
                if region and street.startswith(region):
                    data[Schema.ADDR] = street[len(region):].strip()
                else:
                    pm = _PREF_RE.match(street)
                    if pm and Schema.PREF not in data:
                        data[Schema.PREF] = pm.group(1)
                        data[Schema.ADDR] = street[pm.end():].strip()
                    else:
                        data[Schema.ADDR] = street

        # TEL
        tel = (hotel.get("telephone") or "").strip()
        if tel:
            data[Schema.TEL] = tel

        # チェックイン / チェックアウト / 客室数（JSON-LD）
        if hotel.get("checkinTime"):
            data["checkin"] = str(hotel["checkinTime"]).strip()
        if hotel.get("checkoutTime"):
            data["checkout"] = str(hotel["checkoutTime"]).strip()
        if hotel.get("numberOfRooms"):
            data["num_rooms"] = str(hotel["numberOfRooms"]).strip()

        # 緯度経度（JSON-LD geo）
        geo = hotel.get("geo") or {}
        if isinstance(geo, dict):
            if geo.get("latitude") not in (None, ""):
                data["latitude"] = str(geo["latitude"]).strip()
            if geo.get("longitude") not in (None, ""):
                data["longitude"] = str(geo["longitude"]).strip()

        # サイト定義エリア（パンくずの宿名直前 = サブエリア）
        if breadcrumb and len(breadcrumb) >= 2:
            area = breadcrumb[-2].replace("のおすすめの旅館・ホテル", "").strip()
            if area:
                data[Schema.CAT_SITE] = area

        # 名称カナ（インフォメーション見出し: "名称 ( カナ ) 県 / 市"）
        kana = self._extract_kana(soup, name)
        if kana:
            data[Schema.NAME_KANA] = kana

        # 公式 HP（「公式HPを見る」リンク）
        hp = self._extract_official_hp(soup)
        if hp:
            data[Schema.HP] = hp

        # Instagram（宿固有のもの。サイト共通 SNS は除外）
        insta = self._extract_instagram(soup)
        if insta:
            data[Schema.INSTA] = insta

        # 関連キーワード（こだわり条件タグの有効項目）— 備考対応
        keywords = self._extract_keywords(soup)
        if keywords:
            data["related_keywords"] = " / ".join(keywords)

        # 備考: エリアフィルター（AREA_PREFIXES が設定されている場合のみ適用）
        if self.AREA_PREFIXES and data.get(Schema.PREF, "") not in self.AREA_PREFIXES:
            return None

        return data

    # ------------------------------------------------------------------ #
    # ヘルパー
    # ------------------------------------------------------------------ #
    @staticmethod
    def _parse_jsonld(soup) -> tuple[dict, list[str]]:
        """schema.org Hotel と BreadcrumbList(name 配列) を返す（制御文字に寛容）。"""
        hotel: dict = {}
        breadcrumb: list[str] = []
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = script.string or script.get_text()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except (ValueError, TypeError):
                # 文字列値に生の改行・タブ等が混入しているページがあるので除去して再試行
                try:
                    obj = json.loads(re.sub(r"[\x00-\x1f]+", " ", raw))
                except (ValueError, TypeError):
                    continue
            for c in (obj if isinstance(obj, list) else [obj]):
                if not isinstance(c, dict):
                    continue
                if c.get("@type") == "Hotel" and not hotel:
                    hotel = c
                elif c.get("@type") == "BreadcrumbList" and not breadcrumb:
                    try:
                        breadcrumb = [
                            el["item"]["name"] for el in c.get("itemListElement", [])
                        ]
                    except (KeyError, TypeError):
                        pass
        return hotel, breadcrumb

    @staticmethod
    def _extract_kana(soup, name: str) -> str:
        """インフォメーション見出し "名称 ( カナ ) 県 / 市" からカナを抽出する。"""
        for h in soup.find_all(["h4", "h2", "h3", "p", "span"]):
            t = h.get_text(" ", strip=True)
            if name[:4] in t and "（" not in name and "(" not in name:
                m = re.search(r"[（(]\s*([ァ-ヶ][ァ-ヶー・\s]*)\s*[）)]", t)
                if m:
                    return re.sub(r"\s+", " ", m.group(1).strip())
        return ""

    @staticmethod
    def _extract_official_hp(soup) -> str:
        for a in soup.find_all("a", href=True):
            if a.get_text(strip=True) == "公式HPを見る":
                href = a["href"].strip()
                if href.startswith("http") and "tabiiro.jp" not in href:
                    return href
        return ""

    @staticmethod
    def _extract_instagram(soup) -> str:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "instagram.com" in href and "tabiiro" not in href.lower():
                return href
        return ""

    # 関連キーワードのタグリンク href 判定用。
    #   /yado/hashtag/{id}/  … 宿固有のハッシュタグ（＝真の「関連キーワード」）
    #   /yado/theme/{slug}/  … テーマタグ。ただしフッター/ナビにも出るため、
    #                          関連キーワードセクション内に限って採用する。
    _HASHTAG_HREF_RE = re.compile(r"/yado/hashtag/\d+/")
    _TAG_HREF_RE = re.compile(r"/yado/(?:hashtag/\d+|theme/[^/?#]+)/")

    @classmethod
    def _extract_keywords(cls, soup) -> list[str]:
        """関連キーワードを返す（順序保持で結合・重複除去）。

        3 つの構造化ソースから収集する（取りこぼしを防ぐため冗長に拾う）:
          1. 関連キーワードセクション内のタグリンク。
             基本は section#hashtags、見出し「関連キーワード」を持つ section にも対応。
             ここでは hashtag / theme 双方のタグを採用する
             （例: バイキング / 岩盤浴 / 白浜温泉 / グランピング / 和歌山 / リゾート など）。
          2. ページ全体の /yado/hashtag/{id}/ リンク。
             section の id / 見出しがテンプレ差異で変わっても確実に拾うための保険。
             検証の結果このリンクは関連キーワード欄にのみ出現し、フッター/ナビには
             テーマ(/yado/theme/)しか出ないため、誤検出の心配がない。
          3. ul.features のこだわり条件タグ（有効項目 = disabled でない li）。

        いずれも短い構造化ラベルであり自由記述プロースではないため著作権上の問題はない。
        ※ ハッシュタグ未設定かつ全こだわり条件が disabled の宿（例: BLUE STEAK WONDER）は
          サイト側に関連キーワードのデータが無いため、空のままとなる（取得漏れではない）。
        """
        kws: list[str] = []
        seen: set[str] = set()

        def _add(text: str) -> None:
            t = (text or "").strip()
            if t and t not in seen:
                seen.add(t)
                kws.append(t)

        # 1. 関連キーワードセクション内のタグリンク（hashtag / theme）。
        #    id="hashtags" を第一候補にしつつ、見出し「関連キーワード」を持つ
        #    section も対象にして取りこぼしを防ぐ。
        sections = list(soup.select("section#hashtags"))
        for sec in soup.find_all("section"):
            if sec in sections:
                continue
            heading = sec.find(["h2", "h3", "h4"])
            if heading and heading.get_text(strip=True) == "関連キーワード":
                sections.append(sec)
        for sec in sections:
            for a in sec.find_all("a", href=True):
                if cls._TAG_HREF_RE.search(a["href"]):
                    _add(a.get_text(strip=True))

        # 2. ページ全体のハッシュタグリンク（section の構造が崩れていても拾う保険）。
        #    theme リンクはフッター/ナビにも出るためここでは hashtag のみを対象にする。
        for a in soup.find_all("a", href=True):
            if cls._HASHTAG_HREF_RE.search(a["href"]):
                _add(a.get_text(strip=True))

        # 3. こだわり条件タグ（ul.features の有効項目）
        for li in soup.select("ul.features > li"):
            classes = li.get("class") or []
            if "disabled" in classes:
                continue
            _add(li.get_text(strip=True))

        return kws


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TabiiroScraper()
    scraper.execute("https://tabiiro.jp/yado/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
