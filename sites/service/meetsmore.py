"""
ミツモア (meetsmore.com) — 事業者プロフィール収集クローラー

構造メモ (2026-09 実地調査):
  - 入口 https://meetsmore.com/services は SSR ではなく ``window.__STATE__`` に
    全サービス定義 (key / name / categoryKey / categoryName / categoryParent) を
    埋め込んでいる。個人向けの親カテゴリは lifestyle / business / event の 3 種。
  - レイヤー1: /services/{サービスキー} は事業者カードを SSR で吐き、
    href="/p/{事業者ID}/{サービスキー}" 形式のリンクが取れる。
  - レイヤー2: /t/{カテゴリキー} → /t/{カテゴリキー}/{都道府県} →
    /t/{カテゴリキー}/{都道府県}/{市区町村}。市区町村ページのカードは
    href="/p/{事業者ID}" (サービスキー無し) なので、素の /p/{ID} を 1 度引いて
    その事業者が出品しているサービスキーを取得してから詳細へ進む。
    レイヤー1 とはほぼ重複しないため全国網羅にはこちらが必須。
  - 詳細 /p/{ID}/{キー} は application/ld+json (LocalBusiness / BreadcrumbList) に
    名称・郵便番号・都道府県・市区町村・口コミ評価/件数を持ち、
    経験年数・従業員数・営業時間・対応エリアは HTML セクションにある。
    同ページ内の /p/{同ID}/{別キー} リンクからその事業者の全出品サービスが
    分かるため、事業者ID 単位の集約は 1 リクエストで完結する。

robots.txt 遵守:
  取得するのは /services , /services/{key} , /t/{cat}[/{pref}[/{city}]] ,
  /p/{id}[/{key}] のみ。Disallow 対象 (/pro/ /pros /requests /new-requests /api
  /articles /meets /product-providers /product-requests /services/*?page=*
  /t/*/search/* /services/*/search/* 末尾スラッシュ URL) には一切アクセスしない。

出力カラムの対応 (指定名 → 本フレームワークの Schema 定数):
  名称→NAME / 郵便番号→POST_CODE / 都道府県→PREF / 市区町村→ADDR (市区町村以降) /
  従業員数→EMP_NUM / 営業時間→TIME / 口コミ評価→SCORES(口コミ採点) /
  口コミ件数→REV_SCR / 取得元URL→URL(取得URL)。
  Schema に対応が無い ミツモア掲載カテゴリ / ミツモア掲載サービス / 経験年数 /
  対応エリア のみ EXTRA_COLUMNS で追加する。
"""

import html as html_lib
import json
import re
import sys
import time
from pathlib import Path
from typing import Generator, Iterator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# --- 一覧ページから事業者リンクを拾う正規表現 ---
# 事業者IDは英数字に _ と - を含む (例: W54jdrqWsF4_od-g)
KEYED_PRO_RE = re.compile(r'href="/p/([A-Za-z0-9_-]{8,})/([a-z0-9][a-z0-9-]*)"')
BARE_PRO_RE = re.compile(r'href="/p/([A-Za-z0-9_-]{8,})"')

# --- 詳細ページの HTML セクションから値を拾う正規表現 ---
JSONLD_RE = re.compile(
    r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', re.S
)
EXPERIENCE_RE = re.compile(r"経験年数\s*([0-9][0-9,]*)\s*年")
EMPLOYEE_RE = re.compile(r"従業員\s*([0-9][0-9,]*)\s*人")

# 個人向けの親カテゴリ (サイトの /services で扱われる 3 種)
PARENT_CATEGORIES = ("lifestyle", "business", "event")


class MeetsMoreScraper(StaticCrawler):
    """ミツモア 事業者情報スクレイパー"""

    # 1 事業者 = 1 リクエスト以上になるため、待機は parse() 側 (_get_html) に寄せる
    DELAY = 0.0
    ITEM_DELAY = 0.0
    TIMEOUT = 30

    # リクエスト間の待機秒数 (アクセス配慮。キャッシュヒット時は待たない)
    SLEEP = 0.4

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 "
        "(NetHarvest crawler; contact via meetsmore.com)"
    )

    EXTRA_COLUMNS = [
        "ミツモア掲載カテゴリ",
        "ミツモア掲載サービス",
        "経験年数",
        "対応エリア",
    ]

    # =====================================================================
    # メイン
    # =====================================================================

    def parse(self, url: str) -> Generator[dict, None, None]:
        """サービス定義取得 → レイヤー1/2 で事業者IDを列挙 → 詳細を1件ずつ yield。"""
        base = self._base_of(url)
        services = self._load_services(url)
        if not services:
            self.logger.error("サービス定義を取得できませんでした: %s", url)
            return

        self.logger.info(
            "サービス定義: %d 件 / カテゴリ: %d 件",
            len(services), len({s["categoryKey"] for s in services}),
        )

        seen_ids: set[str] = set()

        # レイヤー1 (/services/{key}) → レイヤー2 (/t/{cat}/{pref}[/{city}]) の順。
        # レイヤー1 の方が 1 ページあたりの収穫が大きいため先に回す。
        candidates = self._chain(
            self._iter_layer1(base, services),
            self._iter_layer2(base, services),
        )

        for pro_id, service_key in candidates:
            if pro_id in seen_ids:
                continue
            seen_ids.add(pro_id)
            item = self._scrape_pro(base, pro_id, service_key)
            if item:
                yield item

    # =====================================================================
    # 一覧レイヤー
    # =====================================================================

    def _iter_layer1(self, base: str, services: list[dict]) -> Iterator[tuple[str, str]]:
        """レイヤー1: /services/{サービスキー} の事業者カード (1ページ最大56件)。"""
        for svc in services:
            list_url = f"{base}/services/{svc['key']}"
            html = self._get_html(list_url)
            if not html:
                continue
            found = {pid: key for pid, key in KEYED_PRO_RE.findall(html)}
            self.logger.info(
                "[L1] %s (%s): %d 件", svc["name"], svc["key"], len(found)
            )
            for pro_id, key in found.items():
                yield pro_id, key

    def _iter_layer2(self, base: str, services: list[dict]) -> Iterator[tuple[str, str]]:
        """レイヤー2: /t/{カテゴリ}/{都道府県}[/{市区町村}] の事業者カード。

        市区町村ページはレイヤー1 とほぼ重複しないため、全国網羅にはこちらが必須。
        カードのリンクはサービスキーを含まないので、キーは詳細側で解決する。
        """
        for cat_key, cat_name in self._categories(services):
            cat_url = f"{base}/t/{cat_key}"
            cat_html = self._get_html(cat_url)
            if not cat_html:
                continue
            pref_paths = self._sub_paths(cat_html, f"/t/{cat_key}")
            self.logger.info(
                "[L2] %s (%s): 都道府県 %d 件", cat_name, cat_key, len(pref_paths)
            )

            for pref_path in pref_paths:
                pref_html = self._get_html(base + pref_path)
                if not pref_html:
                    continue
                # 都道府県ページ自体にも数件カードがある
                for pro_id in dict.fromkeys(BARE_PRO_RE.findall(pref_html)):
                    yield pro_id, ""

                for city_path in self._sub_paths(pref_html, pref_path):
                    city_html = self._get_html(base + city_path)
                    if not city_html:
                        continue
                    pro_ids = list(dict.fromkeys(BARE_PRO_RE.findall(city_html)))
                    self.logger.debug("[L2] %s: %d 件", city_path, len(pro_ids))
                    for pro_id in pro_ids:
                        yield pro_id, ""

    @staticmethod
    def _sub_paths(html: str, parent_path: str) -> list[str]:
        """`parent_path` の直下 1 階層のリンクパスを重複排除して返す。"""
        pattern = re.compile(
            r'href="(' + re.escape(parent_path) + r'/[a-z0-9][a-z0-9-]*)"'
        )
        return list(dict.fromkeys(pattern.findall(html)))

    # =====================================================================
    # 詳細ページ
    # =====================================================================

    def _scrape_pro(self, base: str, pro_id: str, service_key: str) -> dict | None:
        """/p/{事業者ID}/{サービスキー} を取得して 1 行分の dict を作る。"""
        html = ""
        if not service_key:
            # レイヤー2 由来: 素の /p/{ID} からこの事業者の出品サービスキーを解決する
            bare_html = self._get_html(f"{base}/p/{pro_id}")
            if not bare_html:
                return None
            keys = self._own_service_keys(bare_html, pro_id)
            if keys:
                service_key = keys[0]
            else:
                # サービスキーが判らない事業者は素のページの情報で作る
                html = bare_html

        detail_url = f"{base}/p/{pro_id}/{service_key}" if service_key else f"{base}/p/{pro_id}"
        if not html:
            html = self._get_html(detail_url)
        if not html:
            return None

        ld = self._json_ld(html)
        local = ld.get("LocalBusiness") or {}
        crumbs = ld.get("BreadcrumbList") or {}

        name = self._pro_name(crumbs, pro_id) or self._fallback_name(local)
        if not name:
            self.logger.debug("名称を取得できずスキップ: %s", detail_url)
            return None

        address = local.get("address") or {}
        rating = local.get("aggregateRating") or {}

        # この事業者が出品している全サービス → カテゴリ/サービスを集約
        keys = self._own_service_keys(html, pro_id)
        if service_key and service_key not in keys:
            keys.append(service_key)
        cat_names: list[str] = []
        svc_names: list[str] = []
        for key in keys:
            svc = self._svc_by_key.get(key)
            if not svc:
                continue
            if svc["categoryName"] not in cat_names:
                cat_names.append(svc["categoryName"])
            if svc["name"] not in svc_names:
                svc_names.append(svc["name"])

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.POST_CODE: (address.get("postalCode") or "").strip(),
            Schema.PREF: (address.get("addressRegion") or "").strip(),
            Schema.ADDR: (address.get("addressLocality") or "").strip(),
            "ミツモア掲載カテゴリ": ", ".join(cat_names),
            "ミツモア掲載サービス": ", ".join(svc_names),
            Schema.CAT_SITE: ", ".join(svc_names),
        }

        basic = self._section_text(html, ">基本情報<", "</article>")
        m = EMPLOYEE_RE.search(basic)
        if m:
            item[Schema.EMP_NUM] = f"{m.group(1)}人"
        m = EXPERIENCE_RE.search(basic)
        if m:
            item["経験年数"] = f"{m.group(1)}年"

        hours = self._section_text(html, ">営業時間<", "</article>")
        hours = re.sub(r"^営業時間\s*", "", hours)
        if hours:
            item[Schema.TIME] = hours

        areas = self._section_text(html, 'id="job-areas"', "</section>")
        areas = re.sub(r"^対応エリア\s*", "", areas)
        areas = re.sub(r"\s*(もっと見る|続きを見る|閉じる)$", "", areas)
        if areas:
            item["対応エリア"] = areas

        value = rating.get("ratingValue")
        if isinstance(value, (int, float)):
            item[Schema.SCORES] = f"{round(float(value), 2)}"
        count = rating.get("reviewCount")
        if isinstance(count, (int, float)):
            item[Schema.REV_SCR] = str(int(count))

        return item

    @staticmethod
    def _own_service_keys(html: str, pro_id: str) -> list[str]:
        """ページ内の /p/{同じ事業者ID}/{キー} リンクからサービスキーを列挙する。"""
        pattern = re.compile(
            r'href="/p/' + re.escape(pro_id) + r'/([a-z0-9][a-z0-9-]*)"'
        )
        return list(dict.fromkeys(pattern.findall(html)))

    @staticmethod
    def _pro_name(crumbs: dict, pro_id: str) -> str:
        """BreadcrumbList の /p/{ID} 項目 = 装飾の無い事業者名。"""
        for el in crumbs.get("itemListElement") or []:
            item = el.get("item")
            if isinstance(item, dict):
                item = item.get("@id") or item.get("url") or ""
            if isinstance(item, str) and item.rstrip("/").endswith(f"/p/{pro_id}"):
                return (el.get("name") or "").strip()
        return ""

    @staticmethod
    def _fallback_name(local: dict) -> str:
        """LocalBusiness の name は「社名の○○ | 都道府県 - ミツモア」形式。"""
        name = (local.get("name") or "").split("|")[0].strip()
        return re.sub(r"の(評判・口コミ|[^のは]{2,30}の?(料金・評判))$", "", name).strip()

    @staticmethod
    def _json_ld(html: str) -> dict[str, dict]:
        """application/ld+json を @type ごとに 1 件だけ拾う。"""
        result: dict[str, dict] = {}
        for raw in JSONLD_RE.findall(html):
            try:
                data = json.loads(raw.strip())
            except (ValueError, TypeError):
                continue
            for node in data if isinstance(data, list) else [data]:
                if isinstance(node, dict):
                    result.setdefault(str(node.get("@type", "")), node)
        return result

    @classmethod
    def _section_text(cls, html: str, marker: str, end_tag: str) -> str:
        """`marker` を含むタグから `end_tag` までを平文化して返す。"""
        start = html.find(marker)
        if start < 0:
            return ""
        # 見出しタグの終端 '>' 以降が本文
        body_start = html.find(">", start + len(marker) - 1)
        if body_start < 0:
            return ""
        end = html.find(end_tag, body_start)
        if end < 0:
            end = body_start + 20000
        return cls._to_text(html[body_start + 1:end])

    @staticmethod
    def _to_text(fragment: str) -> str:
        """HTML 断片を平文化する。React の <!-- --> 分断を先に潰すのが要点。"""
        fragment = re.sub(r"<!--.*?-->", "", fragment, flags=re.S)
        fragment = re.sub(
            r"<(script|style)[^>]*>.*?</\1>", " ", fragment, flags=re.S | re.I
        )
        fragment = re.sub(r"<[^>]+>", " ", fragment)
        return re.sub(r"\s+", " ", html_lib.unescape(fragment)).strip()

    # =====================================================================
    # サービス定義 (window.__STATE__)
    # =====================================================================

    def _load_services(self, url: str) -> list[dict]:
        """入口ページの window.__STATE__ から個人向けサービス定義を取り出す。"""
        self._svc_by_key: dict[str, dict] = {}
        html = self._get_html(url)
        if not html:
            return []

        marker = "window.__STATE__="
        pos = html.find(marker)
        if pos < 0:
            self.logger.error("window.__STATE__ が見つかりません: %s", url)
            return []
        try:
            state, _ = json.JSONDecoder().raw_decode(html[pos + len(marker):])
        except ValueError as e:
            self.logger.error("window.__STATE__ の解析に失敗しました: %s", e)
            return []

        services = []
        for svc in (state.get("service") or {}).get("allServices") or []:
            key = svc.get("key")
            if not key or svc.get("is404") or not svc.get("enabled", True):
                continue
            if svc.get("categoryParent") not in PARENT_CATEGORIES:
                continue
            entry = {
                "key": key,
                "name": svc.get("name") or key,
                "categoryKey": svc.get("categoryKey") or "",
                "categoryName": svc.get("categoryName") or "",
                "categoryParent": svc.get("categoryParent") or "",
            }
            services.append(entry)
            self._svc_by_key[key] = entry
        return services

    @staticmethod
    def _categories(services: list[dict]) -> list[tuple[str, str]]:
        """サービス定義からカテゴリ (キー, 表示名) を出現順で重複排除して返す。"""
        cats: dict[str, str] = {}
        for svc in services:
            if svc["categoryKey"] and svc["categoryKey"] not in cats:
                cats[svc["categoryKey"]] = svc["categoryName"]
        return list(cats.items())

    # =====================================================================
    # 取得ユーティリティ
    # =====================================================================

    @staticmethod
    def _base_of(url: str) -> str:
        """引数 URL からスキーム+ホストを取り出す (他 URL はここから派生させる)。"""
        parts = urlparse(urljoin(url, "/"))
        return f"{parts.scheme}://{parts.netloc}"

    def _get_html(self, url: str) -> str | None:
        """HTML を文字列で取得する (詳細ページが大きく BeautifulSoup は使わない)。"""

        def _fetch() -> str:
            self.logger.debug("取得中: %s", url)
            response = self.session.get(url, timeout=self.TIMEOUT)
            response.raise_for_status()
            if "charset=" not in response.headers.get("Content-Type", "").lower():
                response.encoding = response.apparent_encoding
            return response.text

        try:
            html = self._fetch_html_cached(url, variant="raw", fetcher=_fetch)
        except requests.exceptions.RequestException as e:
            if not self.CONTINUE_ON_ERROR:
                raise
            self.error_count += 1
            self.logger.warning("通信エラー (スキップして継続): %s — %s", url, e)
            return None

        # アクセス配慮: 実取得したときだけ待つ (キャッシュリプレイ時は早送り)
        if html and not self._last_fetch_from_cache and self.SLEEP > 0:
            time.sleep(self.SLEEP)
        return html

    @staticmethod
    def _chain(*iterables: Iterator[tuple[str, str]]) -> Iterator[tuple[str, str]]:
        """itertools.chain 相当 (レイヤーを跨いだ遅延評価を明示するため自前定義)。"""
        for it in iterables:
            yield from it


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    MeetsMoreScraper().execute("https://meetsmore.com/services")
