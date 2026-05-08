"""
ii家yeah! — 工務店・ハウスメーカー情報スクレイパー

取得対象:
    - 全国の工務店・ハウスメーカー（4,838件 / 49ページ）

取得フロー:
    1. /shops HTMLからAPIアクセストークンを取得
    2. rcms-api/4/general/shops?pageID=N&cnt=100 で全ページをAPI取得
    3. 各アイテムのフィールドをSchemaにマッピング（詳細ページアクセス不要）

実行方法:
    # ローカルテスト
    python scripts/sites/construction/iiyeah_tateru.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id iiyeah_tateru
"""

import re
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_BASE_URL = "https://iiyeah-tateru.jp"
_API_BASE = "https://api.iiyeah-tateru.jp/rcms-api/4/general/shops"
_TOKEN_RE = re.compile(r"[0-9a-f]{64}")

# prefecture.label → 都道府県名
_PREF_SUFFIX = {
    "北海道": "北海道",
    "東京": "東京都",
    "大阪": "大阪府",
    "京都": "京都府",
}


def _normalize_pref(label: str) -> str:
    if not label:
        return ""
    if label in _PREF_SUFFIX:
        return _PREF_SUFFIX[label]
    return label if label.endswith(("県", "都", "道", "府")) else label + "県"


class IiYeahTateruScraper(StaticCrawler):
    """ii家yeah! スクレイパー（API直接取得）"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "法人名",
        "メール",
        "紹介文",
        "会社種別",
        "対応範囲",
        "施工エリア",
        "工法",
        "参考価格",
        "建設許可番号",
        "建築士事務所登録番号",
        "保証・保険等",
    ]

    def prepare(self):
        """HTMLページからAPIアクセストークンを取得してセッションに設定する。"""
        resp = self.session.get(f"{_BASE_URL}/shops", timeout=self.TIMEOUT)
        resp.raise_for_status()
        m = _TOKEN_RE.search(resp.text)
        if not m:
            raise RuntimeError("APIアクセストークンをHTMLから取得できませんでした")
        token = m.group(0)
        self.session.headers.update({
            "x-rcms-api-access-token": token,
            "Referer": f"{_BASE_URL}/",
            "Accept": "application/json",
        })
        self.logger.info("APIトークン取得完了: %s...", token[:8])

    def parse(self, url: str):  # noqa: ARG002
        page = 1
        total_pages = 9999

        while page <= total_pages:
            api_url = f"{_API_BASE}?pageID={page}&cnt=100&filter="
            self.logger.info("API取得: page=%d / %s", page, total_pages if total_pages < 9999 else "?")

            resp = self.session.get(api_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            if data.get("errors"):
                self.logger.warning("APIエラー: %s", data["errors"])
                break

            if page == 1:
                page_info = data.get("pageInfo", {})
                total_pages = page_info.get("totalPageCnt", 1)
                self.total_items = page_info.get("totalCnt", 0)
                self.logger.info("総件数: %d件 / %dページ", self.total_items, total_pages)

            items = data.get("list", [])
            if not items:
                break

            for item in items:
                try:
                    yield self._map_item(item)
                except Exception as e:
                    self.logger.warning("マッピングエラー: topics_id=%s %s", item.get("topics_id"), e)

            page += 1
            time.sleep(self.DELAY)

    def _map_item(self, item: dict) -> dict:
        member_id = item.get("member_info", {}).get("member_id", "")
        detail_url = f"{_BASE_URL}/shops/{member_id}" if member_id else ""

        pref_label = (item.get("prefecture") or {}).get("label", "")
        pref = _normalize_pref(pref_label)
        city = item.get("city", "") or ""
        address = item.get("address", "") or ""
        full_addr = (city + address).strip()

        # free_contents から事業内容・工法・施工エリアを取得
        lob = construction_method = area_text = ""
        for fc in item.get("free_contents") or []:
            ttl = (fc.get("free_ttl") or "").strip()
            txt = (fc.get("free_txt") or "").strip()
            if "事業内容" in ttl:
                lob = txt
            elif "工法" in ttl:
                construction_method = txt
            elif "施工エリア" in ttl or "対応エリア" in ttl:
                area_text = txt

        # 対応範囲（range[]）をカンマ区切りで結合
        range_labels = ", ".join(r.get("label", "") for r in (item.get("range") or []) if r.get("label"))

        return {
            Schema.URL: detail_url,
            Schema.NAME: (item.get("common_name") or item.get("subject") or "").strip(),
            Schema.PREF: pref,
            Schema.ADDR: full_addr,
            Schema.TEL: (item.get("tel") or "").strip(),
            Schema.REP_NM: (item.get("representative") or "").strip(),
            Schema.OPEN_DATE: (item.get("establishment") or "").strip(),
            Schema.HP: (item.get("website_url") or "").strip(),
            Schema.LOB: lob,
            Schema.TIME: (item.get("hours") or "").strip(),
            Schema.HOLIDAY: (item.get("holiday") or "").strip(),
            "法人名": (item.get("company_name") or "").strip(),
            "メール": (item.get("mail") or "").strip(),
            "紹介文": (item.get("catch_txt") or "").strip(),
            "会社種別": (item.get("corporation_type") or {}).get("label", ""),
            "対応範囲": range_labels,
            "施工エリア": area_text,
            "工法": construction_method,
            "参考価格": (item.get("reference_price") or "").strip(),
            "建設許可番号": (item.get("num_construction") or "").strip(),
            "建築士事務所登録番号": (item.get("num_office") or "").strip(),
            "保証・保険等": (item.get("insurance") or "").strip(),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = IiYeahTateruScraper()
    scraper.execute("https://iiyeah-tateru.jp/shops")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
