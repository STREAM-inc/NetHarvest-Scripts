"""
一休.comレストラン — restaurant.ikyu.com のレストラン情報スクレイパー

取得対象:
    - 一休.com レストラン掲載店舗 (名称/カナ/住所/電話/ジャンル/営業時間/定休日/
      支払い方法/座席数/最寄駅/サービス料/喫煙/駐車場/口コミ ほか)

取得フロー:
    1. 列挙: ルート URL から派生した公開 JSON API
       `/api/v2/restaurants?from=N&to=N+20&input={"visitorsCount":2,...}` を
       オフセット送りして全国の restaurantId を取得する (2026-09 時点 約15,600件)。
       API は Cookie / CSRF 不要、robots.txt でも Disallow されていない
       (Disallow なのは /search — こちらは使わない)。
       API が使えない場合は `/area/{都道府県}/` の HTML 一覧をフォールバックに使う。
    2. 詳細: `/{restaurantId}` を静的取得。ページは JSON-LD を持たず、
       「店舗情報」セクションの h3 ラベル (住所/電話番号/営業時間/…) と
       同一セクション内テキストのペアで構成されるため、ラベル駆動で抽出する。
       ジャンル/カナは <title> ("店名 (カナ) - エリア/ジャンル [一休.comレストラン]") から補う。

    ⚠ アクセス上の注意 (2026-09 時点):
      Fastly edge が User-Agent とヘッダ構成を見ており、requests の既定ヘッダでは
      全パス 403 になる。実ブラウザ相当のヘッダ (UA/Accept/Accept-Language/
      Sec-Fetch-*) を付けると 200 で取得できる。IP 起因の恒久ブロックではない。

実行方法:
    python scripts/sites/food/com_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id com_2
"""

import json
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 詳細ページのパスは数値ID (例: /106791)
_DETAIL_PATH_RE = re.compile(r"^/(\d{4,})(?:[/?]|$)")
# <title> = "店名 (カナ) - エリア/ジャンル [一休.comレストラン]"
_TITLE_RE = re.compile(r"^(?P<name>.+?)(?:\s*[（(](?P<kana>[^（()）]+)[)）])?\s*-\s*(?P<area>[^/]+)/(?P<genre>.+?)\s*\[")
# 都道府県抽出 (住所先頭)
_PREF_RE = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|"
    r"熊本|大分|宮崎|鹿児島|沖縄)県)"
)
_TEL_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")


class IkyuRestaurant(StaticCrawler):
    """一休.comレストラン スクレイパー"""

    DELAY = 0.5
    TIMEOUT = 30

    # 一覧 API の 1 リクエスト取得件数 (サイト実装と同じ 20 件刻み)
    PAGE_SIZE = 20
    API_PATH = "/api/v2/restaurants"
    # 全国を対象にした素の検索条件 (人数 2 名・おすすめ順)。エリア条件を付けないと
    # 全掲載店舗が totalCount として返る。
    SEARCH_INPUT = {"visitorsCount": 2, "sortOrder": "RECOMMEND"}
    MAX_OFFSET = 60_000  # 暴走ガード

    # 実ブラウザ相当のヘッダを付けないと Fastly WAF に 403 で弾かれる
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    )
    BROWSER_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
    }

    EXTRA_COLUMNS = [
        "料理ジャンル",
        "席数",
        "最寄駅",
        "サービス料",
        "喫煙",
        "駐車場",
    ]

    # 詳細ページ「店舗情報」セクションの h3 ラベル → 取り込み先
    _DETAIL_LABELS = {
        "名前",
        "住所",
        "アクセス",
        "電話番号",
        "営業時間",
        "定休日",
        "お支払い",
        "サービス料・チャージ",
        "座席",
        "喫煙可否",
        "駐車場",
    }

    def _setup(self):
        super()._setup()
        # StaticCrawler の既定 UA (Chrome 94) では 403 になるため上書き
        self.session.headers.update({"User-Agent": self.USER_AGENT})
        self.session.headers.update(self.BROWSER_HEADERS)

    # ------------------------------------------------------------------ #
    # enumeration
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        seen: set[str] = set()

        for node in self._iter_api_nodes(url):
            rid = str(node.get("restaurantId") or "").strip()
            if not rid or rid in seen:
                continue
            seen.add(rid)
            item = self._build_item(url, rid, node)
            if item:
                yield item

        if seen:
            return

        # API が使えないときの保険: エリア一覧 HTML から詳細リンクを拾う
        logger.warning("API から 0 件。エリア一覧 HTML にフォールバックします")
        for detail_url in self._iter_area_detail_urls(url):
            rid = _DETAIL_PATH_RE.match(urlparse(detail_url).path).group(1)
            if rid in seen:
                continue
            seen.add(rid)
            item = self._build_item(url, rid, {})
            if item:
                yield item

    def _iter_api_nodes(self, url: str):
        """公開検索 API をオフセット送りして店舗ノードを列挙する。"""
        api_url = urljoin(url, self.API_PATH)
        payload = json.dumps(self.SEARCH_INPUT, ensure_ascii=False, separators=(",", ":"))
        total = None
        offset = 0

        while offset < self.MAX_OFFSET:
            params = {"from": offset, "to": offset + self.PAGE_SIZE, "input": payload}
            data = self._get_json(api_url, params)
            if not data:
                return
            result = data.get("searchRestaurants") or {}
            edges = result.get("edges") or []
            if total is None:
                total = result.get("totalCount")
                logger.info("一休.comレストラン 掲載件数: %s", total)
            if not edges:
                return
            for edge in edges:
                node = edge.get("node") or {}
                if node:
                    yield node
            offset += self.PAGE_SIZE
            if total is not None and offset >= total:
                return

    def _get_json(self, api_url: str, params: dict) -> dict | None:
        try:
            resp = self.session.get(
                api_url,
                params=params,
                timeout=self.TIMEOUT,
                headers={"Accept": "*/*", "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
                         "Sec-Fetch-Site": "same-origin", "Referer": urljoin(api_url, "/")},
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.warning("API 取得失敗 %s (%s): %s", api_url, params.get("from"), exc)
            return None

    def _iter_area_detail_urls(self, url: str):
        """フォールバック用: /area/ 配下のエリア一覧から詳細URLを拾う。"""
        index = self.get_soup(urljoin(url, "/area/"))
        if index is None:
            return
        area_paths = []
        seen_area = set()
        for a in index.select("a[href]"):
            href = a.get("href", "")
            path = urlparse(urljoin(url, href)).path
            if path.startswith("/area/") and path.rstrip("/") != "/area" and path not in seen_area:
                seen_area.add(path)
                area_paths.append(path)

        seen_detail = set()
        for path in area_paths:
            soup = self.get_soup(urljoin(url, path))
            if soup is None:
                continue
            for a in soup.select("a[href]"):
                full = urljoin(url, a.get("href", ""))
                m = _DETAIL_PATH_RE.match(urlparse(full).path)
                if not m:
                    continue
                canon = urljoin(url, f"/{m.group(1)}")
                if canon not in seen_detail:
                    seen_detail.add(canon)
                    yield canon

    # ------------------------------------------------------------------ #
    # detail
    # ------------------------------------------------------------------ #
    def _build_item(self, root_url: str, restaurant_id: str, node: dict) -> dict | None:
        detail_url = urljoin(root_url, f"/{restaurant_id}")
        item = {Schema.URL: detail_url}
        for col in self.EXTRA_COLUMNS:
            item[col] = ""

        self._apply_api_node(item, node)

        soup = self.get_soup(detail_url)
        if soup is not None:
            self._apply_detail(item, soup)

        if not item.get(Schema.NAME):
            return None

        # 住所 → 都道府県分離
        addr = item.get(Schema.ADDR, "")
        if addr:
            m = _PREF_RE.match(addr)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr[m.end():].strip()
        return item

    def _apply_api_node(self, item: dict, node: dict):
        """一覧 API のノード (詳細ページに無い口コミ点数なども持つ) を反映する。"""
        if not node:
            return
        name = (node.get("name") or "").strip()
        if name:
            item[Schema.NAME] = name

        genre = (node.get("genre") or {}).get("displayName")
        if genre:
            item[Schema.CAT_SITE] = genre.strip()
            item["料理ジャンル"] = genre.strip()

        tel = (node.get("phoneNumber") or "").strip()
        if tel:
            item[Schema.TEL] = tel

        desc = (node.get("description") or "").strip()
        if desc:
            item[Schema.DESCRIPTION] = desc

        rating = node.get("rating") or {}
        if rating.get("totalValue"):
            item[Schema.SCORES] = str(rating["totalValue"])
        if rating.get("totalCount"):
            item[Schema.REV_SCR] = str(rating["totalCount"])

        station = (node.get("nearestStation") or {}).get("name")
        if station:
            item["最寄駅"] = station.strip()

    def _apply_detail(self, item: dict, soup: BeautifulSoup):
        h1 = soup.select_one("h1")
        if h1:
            text = h1.get_text(" ", strip=True)
            if text:
                item[Schema.NAME] = text

        title = soup.title.get_text(strip=True) if soup.title else ""
        m = _TITLE_RE.match(title)
        if m:
            if m.group("kana"):
                item[Schema.NAME_KANA] = m.group("kana").strip()
            if not item.get(Schema.NAME):
                item[Schema.NAME] = m.group("name").strip()
            genre = m.group("genre").strip()
            if genre and not item.get(Schema.CAT_SITE):
                item[Schema.CAT_SITE] = genre
                item["料理ジャンル"] = genre

        labeled = self._extract_labeled(soup)

        addr = labeled.get("住所", "")
        if addr:
            item[Schema.ADDR] = addr

        tel = labeled.get("電話番号", "")
        if tel:
            m_tel = _TEL_RE.search(tel)
            item[Schema.TEL] = m_tel.group(0) if m_tel else tel

        for label, key in (
            ("営業時間", Schema.TIME),
            ("定休日", Schema.HOLIDAY),
            ("お支払い", Schema.PAYMENTS),
            ("座席", "席数"),
            ("サービス料・チャージ", "サービス料"),
            ("喫煙可否", "喫煙"),
            ("駐車場", "駐車場"),
        ):
            value = labeled.get(label, "")
            if value:
                item[key] = value

        if not item.get("最寄駅"):
            access = labeled.get("アクセス", "")
            m_st = re.match(r"^(.{1,40}?駅)", access.replace(" ", ""))
            if m_st:
                item["最寄駅"] = m_st.group(1)

    def _extract_labeled(self, soup: BeautifulSoup) -> dict[str, str]:
        """h3 ラベル → 同一セクション内の値テキスト。

        店舗情報は table/dl ではなく `<section><h3>ラベル</h3>…値…</section>` 構造。
        注釈 (※…) と導線リンク (…はこちら) は値から除く。
        """
        out: dict[str, str] = {}
        for heading in soup.find_all(["h2", "h3"]):
            label = heading.get_text(" ", strip=True)
            if label not in self._DETAIL_LABELS or label in out:
                continue
            section = heading.parent
            if section is None:
                continue
            if label == "住所":
                # 住所欄の末尾には「〈ホテル名〉の宿泊予約はこちら」等の導線リンクが
                # 続くため、アンカー内テキストはまるごと除外する
                chunks = [
                    s.strip() for s in section.find_all(string=True)
                    if s.strip() and s.find_parent("a") is None
                ]
            else:
                chunks = [c.strip() for c in section.stripped_strings]
            if chunks and chunks[0] == label:
                chunks = chunks[1:]
            chunks = [c for c in chunks if c and not c.startswith("※") and not c.endswith("こちら")]
            if not chunks:
                continue
            # 住所は要素分割 (都道府県 / 市区町村以降) されているため連結する
            sep = "" if label == "住所" else " "
            out[label] = re.sub(r"[ \t]+", " ", sep.join(chunks)).strip()
        return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = IkyuRestaurant()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://restaurant.ikyu.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
