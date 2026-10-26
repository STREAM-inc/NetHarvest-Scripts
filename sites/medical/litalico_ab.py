"""
LITALICO仕事ナビ — 就労継続支援A型/B型 事業所 (東北6県 + 新潟・富山・石川・福井)

対象サイト: https://snabi.jp/keizoku/type_a/02

取得対象:
    - 就労継続支援A型 (/keizoku/type_a/{県コード}) と B型 (/keizoku/type_b/{県コード}) の
      一覧を、対象10県 (02青森,03岩手,04宮城,05秋田,06山形,07福島,15新潟,16富山,17石川,18福井)
      × type_a/type_b の 20 組で ?page=N (20件/ページ) 全ページ巡回する。

取得フロー:
    一覧 HTML の div[data-facilities] (JSON) から ID/名称/カナ/都道府県/キャッチコピー/
    050転送番号/サービス種別 を取得し、事業所ごとに詳細を1件取得して即 yield する。
      - 掲載契約あり (contract_information_renderable_flag=true):
          /facility/{id} の div#KFacilityContractedShow[data-facility] (JSON)
      - 掲載契約なし: 詳細 HTML は「読み込み中」の殻のみで、本体は
          /api/v1/facilities/{id}/v2 の not_contracted_facility (JSON, WAM NET 由来)
      ページ送りは data-pagination-attributes の total_pages まで。

備考 (呼び出し指示より):
    - 「PR文・詳細本文」は開所・オープン記載抽出用として明示的に取得指示あり (自由記述)。
      契約あり=メッセージ/就職へのこだわり/体験・支援内容/備考/サービスの特色、
      契約なし=サービスの特色。
    - 媒体掲載TEL は LITALICO の 050 転送番号 (参考用) のため Schema.TEL ではなく EXTRA に格納。
      掲載契約なしの事業所には転送番号が無い (空)。
    - 開所日: 契約あり=open_date、契約なし=established_at (事業開始年月日) → Schema.OPEN_DATE。
    - 利用規約 (https://snabi.jp/terms) にスクレイピング/クローリングの明示禁止条項なし。

実行方法:
    # ローカルテスト
    python scripts/sites/medical/litalico_ab.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id litalico_ab
"""

import json
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 対象県コード (備考指定: 東北6県 + 新潟・富山・石川・福井)
_TARGET_PREFS = ["02", "03", "04", "05", "06", "07", "15", "16", "17", "18"]
# 一覧のサービス種別パス → 表示名
_SERVICE_TYPES = {
    "type_a": "就労継続支援A型",
    "type_b": "就労継続支援B型",
}
# JSON の type 値 → 表示名 (一覧パスより優先)
_TYPE_NAMES = {
    "KeizokuA": "就労継続支援A型",
    "KeizokuB": "就労継続支援B型",
}
MAX_ATTEMPTS = 3


def _clean(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _clean_block(value) -> str:
    """改行を保ったまま前後空白を落とす (PR文用)。"""
    if not value:
        return ""
    lines = [line.strip() for line in str(value).splitlines()]
    return "\n".join(line for line in lines if line)


class LitalicoAbScraper(StaticCrawler):
    """LITALICO仕事ナビ 就労継続支援A型/B型 スクレイパー"""

    DELAY = 3.0  # 待機時間（秒）。1事業所あたり詳細1リクエスト
    EXTRA_COLUMNS = [
        "事業所ID",
        "サービス種別",
        "キャッチコピー",
        "PR文・詳細本文",
        "媒体掲載TEL",
        "事業所番号",
        "運営法人",
        "定員",
        "掲載区分",
        "一覧URL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[int] = set()
        for type_path, type_name in _SERVICE_TYPES.items():
            for pref_code in _TARGET_PREFS:
                list_url = urljoin(url, f"/keizoku/{type_path}/{pref_code}")
                yield from self._crawl_list(list_url, type_name, seen)

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

        detail = None
        contracted = bool(fac.get("contract_information_renderable_flag"))
        if contracted:
            detail = self._get_contracted_detail(detail_url)
        if detail is not None:
            info = self._from_contracted(detail)
            kubun = "掲載契約あり"
        else:
            api = self._get_json(urljoin(page_url, f"/api/v1/facilities/{fac_id}/v2"))
            info = self._from_not_contracted((api or {}).get("not_contracted_facility") or {})
            kubun = "掲載契約なし"

        pref = _clean(fac.get("prefecture_name")) or info.get("pref", "")
        return {
            Schema.URL: detail_url,
            Schema.NAME: _clean(fac.get("name")) or info.get("name", ""),
            Schema.NAME_KANA: _clean(fac.get("hurigana")) or info.get("kana", ""),
            Schema.PREF: pref,
            Schema.POST_CODE: info.get("postcode", ""),
            Schema.ADDR: info.get("address", ""),
            Schema.CAT_SITE: _TYPE_NAMES.get(fac.get("type"), type_name),
            Schema.OPEN_DATE: info.get("open_date", ""),
            Schema.HP: info.get("hp", ""),
            "事業所ID": str(fac_id),
            "サービス種別": _TYPE_NAMES.get(fac.get("type"), type_name),
            "キャッチコピー": _clean(fac.get("catch_copy")) or info.get("catch_copy", ""),
            "PR文・詳細本文": info.get("pr_text", ""),
            "媒体掲載TEL": _clean(fac.get("formatted_twilio_number")) or info.get("tel", ""),
            "事業所番号": info.get("identification_number", ""),
            "運営法人": info.get("operator", ""),
            "定員": info.get("capacity", ""),
            "掲載区分": kubun,
            "一覧URL": page_url,
        }

    def _get_contracted_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None
        holder = soup.find(attrs={"data-facility": True})
        if holder is None:
            # 契約フラグありでも殻ページのことがある → API にフォールバック
            return None
        try:
            return json.loads(holder["data-facility"])
        except ValueError:
            self.logger.warning("詳細 JSON の解析失敗: %s", detail_url)
            return None

    @staticmethod
    def _from_contracted(d: dict) -> dict:
        parts = []
        message = d.get("k_message") or {}
        parts += [message.get("title"), message.get("body")]
        commitment = d.get("k_employment_commitment") or {}
        parts.append(commitment.get("body"))
        for key in ("k_experiences", "k_support_contents"):
            for entry in d.get(key) or []:
                parts += [entry.get("title"), entry.get("body")]
        parts += [d.get("service_characteristics"), d.get("remark")]
        pr_text = "\n".join(t for t in (_clean_block(p) for p in parts) if t)

        corp = d.get("display_corporation") or d.get("corporation") or {}
        return {
            "name": _clean(d.get("name")),
            "kana": _clean(d.get("hurigana")),
            "pref": _clean(d.get("prefecture_name")),
            "postcode": _clean(d.get("postcode")),
            "address": _clean(d.get("full_address")),
            "open_date": _clean(d.get("open_date")),
            "hp": _clean(d.get("homepage_url")),
            "catch_copy": _clean(d.get("catch_copy")),
            "pr_text": pr_text,
            "tel": _clean(d.get("formatted_twilio_number")),
            "identification_number": _clean(d.get("identification_number")),
            "operator": _clean(corp.get("name")),
            "capacity": _clean(d.get("capacity")),
        }

    @staticmethod
    def _from_not_contracted(d: dict) -> dict:
        operator = d.get("wamnet_operator") or {}
        return {
            "name": _clean(d.get("name")),
            "kana": _clean(d.get("name_hurigana")),
            "pref": _clean((d.get("prefecture") or {}).get("name")),
            "postcode": _clean(d.get("postcode")),
            "address": _clean(d.get("address")),
            "open_date": _clean(d.get("established_at") or d.get("designation_date")),
            "hp": "",
            "catch_copy": "",
            "pr_text": _clean_block(d.get("service_characteristics")),
            "tel": "",
            "identification_number": _clean(d.get("identification_number")),
            "operator": _clean(operator.get("name")),
            "capacity": _clean(d.get("capacity")),
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


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = LitalicoAbScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://snabi.jp/keizoku/type_a/02")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
