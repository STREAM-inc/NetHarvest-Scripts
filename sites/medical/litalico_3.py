"""
LITALICO仕事ナビ — 就労移行支援・就労継続支援A型/B型 事業所 (関西2府4県 + 徳島・高知)

対象サイト: https://snabi.jp/ikou/27

取得対象:
    - 就労移行支援 (/ikou/{県コード})、就労継続支援A型 (/keizoku/type_a/{県コード})、
      B型 (/keizoku/type_b/{県コード}) の一覧を、対象8府県
      (25滋賀,26京都,27大阪,28兵庫,29奈良,30和歌山,36徳島,39高知) × 3種別の 24 組で
      ?page=N (20件/ページ) 全ページ巡回する (2026-11 時点 計6,240件/324ページ)。
    - 掲載プラン (有料/無料) を問わず全件取得する。
    - 起点 url (= /ikou/27) の組を最初に巡回し、残りは url から urljoin で導出する。

取得フロー:
    一覧 HTML の div[data-facilities] (JSON) から ID/名称/プラン/市区町村/050転送番号を取得し、
    事業所ごとに詳細を1件取得して即 yield する。
      - 掲載契約あり (contract_information_renderable_flag=true):
          /facility/{id} の JSON-LD (LocalBusiness: telephone/address/sameAs=HP) と
          div[data-facility] JSON の corporation (id/name/address)
      - 掲載契約なし: 詳細 HTML は「読み込み中」の殻のみ (JSON-LD/corporation 無し) のため
          /api/v1/facilities/{id}/v2 の not_contracted_facility (WAM NET 由来) から
          postcode/address と wamnet_operator (id/name/address) を取得する。
          ※ この場合の運営法人ID は WAM NET 法人ID (契約ありの corporation.id とは別採番)。
      ページ送りは data-pagination-attributes の total_pages まで。

備考:
    - 掲載電話番号は LITALICO の 050 転送番号のため Schema.TEL ではなく EXTRA に格納。
      掲載契約なしの事業所には転送番号が無い (空)。
    - 自由記述 (キャッチコピー/PR文/サービスの特色) は著作権リスクのため取得しない。
    - 利用規約 (https://snabi.jp/terms) にスクレイピング/クローリングの明示禁止条項なし。

更新履歴:
    - 2026-11-06: 2026-11-02 の登録 (b79b077) 後に S3 への到着が無かったため、初回実行の
      再トリガーとしてコメントのみ更新 (処理内容の変更なし。STREAMREQ-28564)。

実行方法:
    # ローカルテスト
    python scripts/sites/medical/litalico_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id litalico_3
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 対象府県コード (備考指定: 滋賀・京都・大阪・兵庫・奈良・和歌山 + 徳島・高知)
_TARGET_PREFS = ["25", "26", "27", "28", "29", "30", "36", "39"]
# 一覧パス接頭辞 → サービス種別名
_SERVICE_PATHS = {
    "/ikou/": "就労移行支援",
    "/keizoku/type_a/": "就労継続支援A型",
    "/keizoku/type_b/": "就労継続支援B型",
}
# JSON の type 値 → サービス種別名 (一覧パスより優先)
_TYPE_NAMES = {
    "Ikou": "就労移行支援",
    "KeizokuA": "就労継続支援A型",
    "KeizokuB": "就労継続支援B型",
}
MAX_ATTEMPTS = 3


def _clean(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


class Litalico3Scraper(StaticCrawler):
    """LITALICO仕事ナビ 就労移行・就労継続A型/B型 (関西・徳島高知) スクレイパー"""

    DELAY = 3.0  # 待機時間（秒）。1事業所あたり詳細1リクエスト
    EXTRA_COLUMNS = [
        "事業所ID",
        "サービス種別",
        "運営法人ID",
        "運営法人名",
        "運営法人住所",
        "市区町村",
        "掲載電話番号",
        "掲載プラン",
        "詳細URL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[int] = set()
        for list_url, type_name in self._list_urls(url):
            yield from self._crawl_list(list_url, type_name, seen)

    @staticmethod
    def _list_urls(url: str) -> list[tuple[str, str]]:
        """起点 url の組を先頭に、3種別 × 8府県の一覧 URL を返す。"""
        start = url.split("?")[0].rstrip("/")
        urls = [(urljoin(url, f"{path}{pref}"), name)
                for path, name in _SERVICE_PATHS.items() for pref in _TARGET_PREFS]
        start_path = urlparse(start).path
        urls.sort(key=lambda t: urlparse(t[0]).path != start_path)
        return urls

    # ------------------------------------------------------------------
    # 一覧
    # ------------------------------------------------------------------
    def _crawl_list(self, list_url: str, type_name: str, seen: set) -> Generator[dict, None, None]:
        page = 1
        total_pages = 1
        while page <= total_pages:
            page_url = list_url if page == 1 else f"{list_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break
            holder = soup.find(attrs={"data-facilities": True})
            if holder is None:
                self.logger.warning("一覧 JSON が見つかりません: %s", page_url)
                break
            facilities = json.loads(holder["data-facilities"] or "[]")
            try:
                pagination = json.loads(holder.get("data-pagination-attributes") or "{}")
                total_pages = int(pagination.get("total_pages") or 1)
            except ValueError:
                total_pages = page
            if not facilities:
                break

            for fac in facilities:
                fac_id = fac.get("id")
                if not fac_id or fac_id in seen:
                    continue
                seen.add(fac_id)
                yield self._build_item(fac, type_name, page_url)

            page += 1
            if page <= total_pages and not self._last_fetch_from_cache:
                time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _build_item(self, fac: dict, type_name: str, page_url: str) -> dict:
        fac_id = fac["id"]
        detail_url = urljoin(page_url, f"/facility/{fac_id}")

        info = None
        if fac.get("contract_information_renderable_flag"):
            info = self._from_contracted(detail_url)
        if info is None:
            api = self._get_json(urljoin(page_url, f"/api/v1/facilities/{fac_id}/v2"))
            info = self._from_not_contracted((api or {}).get("not_contracted_facility") or {})

        service = _TYPE_NAMES.get(fac.get("type"), type_name)
        return {
            Schema.URL: detail_url,
            Schema.NAME: _clean(fac.get("name")) or info.get("name", ""),
            Schema.NAME_KANA: _clean(fac.get("hurigana")),
            Schema.PREF: _clean(fac.get("prefecture_name")) or info.get("pref", ""),
            Schema.POST_CODE: info.get("postcode", ""),
            Schema.ADDR: info.get("address", ""),
            Schema.CAT_SITE: service,
            Schema.HP: info.get("hp", ""),
            "事業所ID": str(fac_id),
            "サービス種別": service,
            "運営法人ID": info.get("corp_id", ""),
            "運営法人名": info.get("corp_name", ""),
            "運営法人住所": info.get("corp_address", ""),
            "市区町村": _clean(fac.get("city_name")) or info.get("city", ""),
            "掲載電話番号": _clean(fac.get("formatted_twilio_number")) or info.get("tel", ""),
            "掲載プラン": _clean(fac.get("plan")),
            "詳細URL": detail_url,
        }

    def _from_contracted(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None
        ld = {}
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except ValueError:
                continue
            if isinstance(data, dict) and data.get("@type") == "LocalBusiness":
                ld = data
                break
        facility = {}
        holder = soup.find(attrs={"data-facility": True})
        if holder is not None:
            try:
                facility = json.loads(holder["data-facility"])
            except ValueError:
                self.logger.warning("詳細 JSON の解析失敗: %s", detail_url)
        if not ld and not facility:
            # 契約フラグありでも殻ページのことがある → API にフォールバック
            return None

        addr = ld.get("address") or {}
        corp = facility.get("corporation") or facility.get("display_corporation") or {}
        locality = _clean(addr.get("addressLocality")) or _clean(facility.get("city_name"))
        street = _clean(addr.get("streetAddress"))
        address = f"{locality}{street}" if street else ""
        if not address:
            pref = _clean(facility.get("prefecture_name"))
            address = _strip_pref(_clean(facility.get("full_address")), pref)
        sameas = ld.get("sameAs")
        if isinstance(sameas, list):
            sameas = sameas[0] if sameas else ""
        return {
            "name": _clean(ld.get("name") or facility.get("name")),
            "pref": _clean(addr.get("addressRegion") or facility.get("prefecture_name")),
            "city": locality,
            "postcode": _clean(addr.get("postalCode") or facility.get("postcode")),
            "address": address,
            "hp": _clean(sameas or facility.get("homepage_url")),
            "tel": _format_tel(_clean(ld.get("telephone"))),
            "corp_id": _clean(corp.get("id")),
            "corp_name": _clean(corp.get("name")),
            "corp_address": _clean(corp.get("address")),
        }

    @staticmethod
    def _from_not_contracted(d: dict) -> dict:
        operator = d.get("wamnet_operator") or {}
        pref = _clean((d.get("prefecture") or {}).get("name"))
        return {
            "name": _clean(d.get("name")),
            "pref": pref,
            "city": "",
            "postcode": _clean(d.get("postcode")),
            "address": _strip_pref(_clean(d.get("address")), pref),
            "hp": "",
            "tel": "",
            "corp_id": _clean(operator.get("id")),
            "corp_name": _clean(operator.get("name")),
            "corp_address": _clean(operator.get("address")),
        }

    # ------------------------------------------------------------------
    # JSON API 取得 (上限付きリトライ。全失敗時は None → 一覧情報のみで出力)
    # ------------------------------------------------------------------
    def _get_json(self, api_url: str) -> dict | None:
        for attempt in range(MAX_ATTEMPTS):
            try:
                self.logger.info("取得中: %s", api_url)
                response = self.session.get(
                    api_url, headers={"Accept": "application/json"}, timeout=self.TIMEOUT
                )
                response.raise_for_status()
                return response.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                self.logger.warning(
                    "API 取得失敗 (%d/%d): %s — %s", attempt + 1, MAX_ATTEMPTS, api_url, e
                )
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(min(2 ** (attempt + 1), 8))
        self.error_count += 1
        return None


def _strip_pref(address: str, pref: str) -> str:
    """住所先頭の都道府県名を除去して市区町村以降を返す。"""
    if pref and address.startswith(pref):
        return address[len(pref):].strip()
    return re.sub(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)", "", address).strip()


def _format_tel(tel: str) -> str:
    """05017225927 → 050-1722-5927 (050 転送番号のみ整形)。"""
    if re.fullmatch(r"050\d{8}", tel):
        return f"{tel[:3]}-{tel[3:7]}-{tel[7:]}"
    return tel


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Litalico3Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://snabi.jp/ikou/27")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
