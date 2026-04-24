# scripts/sites/medical/kaigo_kensaku.py
"""
介護事業所・生活関連情報 — 厚生労働省「介護サービス情報公表システム」全国版クローラー

取得対象:
    - 全47都道府県の介護事業所情報

取得フロー:
    1. 都道府県コードをループ（01〜47）
    2. 各都道府県でPOSTリクエストによりPHPセッション確立
    3. JSON APIをページネーション（50件/ページ、p_offset制御）しながら取得
    4. SCRAPE_DETAIL=True の場合、各レコードの詳細ページ（kani/kihon）も取得し
       法人番号・従業員数（総従業者数）を補完する

注意:
    SCRAPE_DETAIL=True にすると1件あたりリクエスト数が 1 → 3 に増加するため、
    全件取得には数日を要します。

実行方法:
    python scripts/sites/medical/kaigo_kensaku.py
    python bin/run_flow.py --site-id kaigo_kensaku
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE = "https://www.kaigokensaku.mhlw.go.jp"
PAGE_SIZE = 50

_PREF_MAP = {
    "01": "北海道",   "02": "青森県",   "03": "岩手県",   "04": "宮城県",   "05": "秋田県",
    "06": "山形県",   "07": "福島県",   "08": "茨城県",   "09": "栃木県",   "10": "群馬県",
    "11": "埼玉県",   "12": "千葉県",   "13": "東京都",   "14": "神奈川県", "15": "新潟県",
    "16": "富山県",   "17": "石川県",   "18": "福井県",   "19": "山梨県",   "20": "長野県",
    "21": "岐阜県",   "22": "静岡県",   "23": "愛知県",   "24": "三重県",   "25": "滋賀県",
    "26": "京都府",   "27": "大阪府",   "28": "兵庫県",   "29": "奈良県",   "30": "和歌山県",
    "31": "鳥取県",   "32": "島根県",   "33": "岡山県",   "34": "広島県",   "35": "山口県",
    "36": "徳島県",   "37": "香川県",   "38": "愛媛県",   "39": "高知県",   "40": "福岡県",
    "41": "佐賀県",   "42": "長崎県",   "43": "熊本県",   "44": "大分県",   "45": "宮崎県",
    "46": "鹿児島県", "47": "沖縄県",
}

_PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

# 介護サービスコード → 種類名（サービス情報公表システム JS serviceNameList 準拠）
_SERVICE_NAME_MAP = {
    "110": "訪問介護",
    "120": "訪問入浴",
    "130": "訪問看護",
    "140": "訪問リハビリ",
    "150": "デイサービス",
    "155": "療養通所",
    "160": "デイケア",
    "170": "福祉用具貸与",
    "210": "ショートステイ(福祉)",
    "220": "ショートステイ(老健)",
    "230": "ショートステイ(医療)",
    "320": "グループホーム",
    "331": "特定施設(有料老人ホーム)",
    "332": "特定施設(軽費老人ホーム)",
    "334": "特定施設(サービス付き高齢者住宅)",
    "335": "特定施設(有料老人ホーム・外部サービス利用)",
    "336": "特定施設(軽費老人ホーム・外部サービス利用)",
    "337": "特定施設(サービス付き高齢者住宅・外部サービス利用)",
    "361": "地域特定施設(有料老人ホーム)",
    "362": "地域特定施設(軽費老人ホーム)",
    "364": "地域特定施設(サービス付き高齢者住宅)",
    "410": "福祉用具販売",
    "430": "居宅介護支援",
    "510": "老人福祉施設",
    "520": "老人保健施設",
    "530": "療養医療施設",
    "540": "地域老人福祉施設",
    "550": "介護医療院",
    "551": "ショートステイ（介護医療院）",
    "710": "夜間対応訪問介護",
    "720": "認知症対応デイサービス",
    "730": "小規模多機能型",
    "760": "定期巡回・随時対応サービス",
    "770": "複合型サービス",
    "780": "地域密着デイ",
}


def _parse_hours(s: str) -> str:
    """'9,00,17,00' → '9:00〜17:00'。空または無効な値は空文字を返す。"""
    if not s:
        return ""
    parts = s.split(",")
    if len(parts) != 4 or not (parts[0] or "").strip():
        return ""
    h_s, m_s, h_e, m_e = parts
    if not h_e.strip():
        return ""
    return f"{h_s}:{m_s}〜{h_e}:{m_e}"


def _strip_unit(s: str) -> str:
    """'97人' → '97'"""
    return re.sub(r"[人名]$", "", (s or "").strip())


class KaigoKensakuScraper(StaticCrawler):
    """介護サービス情報公表システム スクレイパー（全47都道府県）"""

    DELAY = 0  # ページ間ディレイは _scrape_pref 内で time.sleep() で制御
    EXTRA_COLUMNS = [
        "事業所番号", "サービスコード", "介護サービスの種類", "FAX", "法人種別コード",
        "利用者数", "定員数", "空き人数", "空き情報更新日",
        "公表日", "サービス提供地域", "祝日営業時間", "留意事項",
    ]

    # True にすると各レコードの詳細ページも取得し、法人番号と従業員数を補完する
    # 全件取得時は数日を要するため、デフォルト True（ユーザー指定）
    SCRAPE_DETAIL = True

    def parse(self, url: str) -> Generator[dict, None, None]:
        for pref_cd in _PREF_CODES:
            self.logger.info("都道府県取得中: %s %s", pref_cd, _PREF_MAP.get(pref_cd, ""))
            yield from self._scrape_pref(pref_cd)

    # ------------------------------------------------------------------
    # 都道府県ループ
    # ------------------------------------------------------------------

    def _scrape_pref(self, pref_cd: str) -> Generator[dict, None, None]:
        list_action = f"{BASE}/{pref_cd}/index.php?action_kouhyou_pref_search_list_list=true"

        # PHPセッション確立（Referer 必須）
        try:
            self.session.post(
                list_action,
                data={
                    "method": "search",
                    "action_kouhyou_pref_topjigyosyo_index": "true",
                    "PrefCd": pref_cd,
                    "FromPage": "kaigoTopPage",
                    "SearchConditions": "",
                    "LatLng": "",
                    "SearchKeyword": "",
                    "KeywordConjunction": "0",
                },
                headers={
                    "Referer": f"{BASE}/{pref_cd}/index.php?action_kouhyou_pref_topjigyosyo_index=true",
                    "Accept": "text/html,application/xhtml+xml",
                },
                timeout=self.TIMEOUT,
            )
        except Exception as e:
            self.logger.warning("セッション確立失敗 pref=%s: %s", pref_cd, e)
            return

        time.sleep(1.0)
        offset = 0

        while True:
            try:
                resp = self.session.get(
                    list_action,
                    params={
                        "action_kouhyou_pref_search_search": "true",
                        "method": "search",
                        "p_count": PAGE_SIZE,
                        "p_offset": offset,
                        "p_sort_name": "FreeNumUpdateDate",
                        "p_order": "1",
                    },
                    headers={
                        "Accept": "application/json, text/javascript, */*; q=0.01",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": list_action,
                    },
                    timeout=self.TIMEOUT,
                )
                resp.raise_for_status()
                data = json.loads(resp.content)
            except Exception as e:
                self.logger.warning("取得失敗 pref=%s offset=%d: %s", pref_cd, offset, e)
                break

            if data.get("status") != "success":
                self.logger.warning("APIエラー pref=%s offset=%d: %s", pref_cd, offset, data.get("data", ""))
                break

            items = data.get("data") or []
            if not items:
                break

            pager = data.get("pager") or {}
            if self.total_items is None:
                total_pref = int(pager.get("total", 0))
                self.total_items = total_pref * 47  # 47都道府県の概算

            for raw in items:
                try:
                    item = self._map_item(raw, list_action, pref_cd)
                    if self.SCRAPE_DETAIL:
                        self._enrich_detail(raw, item, pref_cd)
                    yield item
                except Exception as e:
                    self.logger.warning("アイテム処理失敗 %s: %s", raw.get("JigyosyoCd", ""), e)
                    continue

            total = int(pager.get("total", 0))
            if offset + len(items) >= total:
                break
            offset += PAGE_SIZE
            time.sleep(1.0)

    # ------------------------------------------------------------------
    # 一覧APIデータのマッピング
    # ------------------------------------------------------------------

    def _map_item(self, item: dict, url: str, pref_cd: str) -> dict:
        pref_name = _PREF_MAP.get(pref_cd, "")

        address = (item.get("JigyosyoJyusho") or "").strip()
        if pref_name and address.startswith(pref_name):
            addr = address[len(pref_name):].strip()
        else:
            addr = address

        yubin = (item.get("JigyosyoYubinbangou") or "").replace(",", "-").strip()
        heijitu = _parse_hours(item.get("HeijituHours") or "")
        service_cd = item.get("ServiceCd") or ""

        return {
            Schema.NAME:      item.get("JigyosyoName") or "",
            Schema.PREF:      pref_name,
            Schema.POST_CODE: yubin,
            Schema.ADDR:      addr,
            Schema.TEL:       item.get("JigyosyoTel") or "",
            Schema.CO_NUM:    "",  # kihon詳細ページから補完
            Schema.EMP_NUM:   "",  # kani詳細ページから補完
            Schema.HP:        item.get("JHPUrl") or "",
            Schema.OPEN_DATE: item.get("StartDate") or "",
            Schema.TIME:      heijitu,
            Schema.TIME_MON:  heijitu,
            Schema.TIME_TUE:  heijitu,
            Schema.TIME_WED:  heijitu,
            Schema.TIME_THU:  heijitu,
            Schema.TIME_FRI:  heijitu,
            Schema.TIME_SAT:  _parse_hours(item.get("DoyoubiHours") or ""),
            Schema.TIME_SUN:  _parse_hours(item.get("NichiyoubiHours") or ""),
            Schema.HOLIDAY:   item.get("Teikyubi") or "",
            Schema.URL:       url,
            "事業所番号":           item.get("JigyosyoCd") or "",
            "サービスコード":       service_cd,
            "介護サービスの種類":   _SERVICE_NAME_MAP.get(service_cd, item.get("ShortName") or ""),
            "FAX":                 item.get("JigyosyoFax") or "",
            "法人種別コード":       item.get("HoujinType") or "",
            "利用者数":             item.get("TotalUserNum") or "",
            "定員数":               item.get("CapacityNum") or "",
            "空き人数":             item.get("FreeNum") or "",
            "空き情報更新日":       item.get("FreeNumUpdateDate") or "",
            "公表日":               item.get("Kohyobi") or "",
            "サービス提供地域":     item.get("ServiceArea") or "",
            "祝日営業時間":         _parse_hours(item.get("ShukujituHours") or ""),
            "留意事項":             item.get("Consideration") or "",
        }

    # ------------------------------------------------------------------
    # 詳細ページ補完（法人番号 / 従業員数）
    # ------------------------------------------------------------------

    def _enrich_detail(self, raw: dict, item: dict, pref_cd: str) -> None:
        jigyosyo_cd = raw.get("JigyosyoCd") or ""
        jigyosyo_sub_cd = raw.get("JigyosyoSubCd") or "00"
        version_cd = raw.get("VersionCd") or ""
        service_cd = raw.get("ServiceCd") or ""
        jcd = f"{jigyosyo_cd}-{jigyosyo_sub_cd}"

        kani_url = (
            f"{BASE}/{pref_cd}/index.php"
            f"?action_kouhyou_detail_{version_cd}_kani=true"
            f"&JigyosyoCd={jcd}&ServiceCd={service_cd}"
        )
        kihon_url = (
            f"{BASE}/{pref_cd}/index.php"
            f"?action_kouhyou_detail_{version_cd}_kihon=true"
            f"&JigyosyoCd={jcd}&ServiceCd={service_cd}"
        )

        # kaniページ: 総従業者数
        try:
            soup = self.get_soup(kani_url)
            if soup:
                th = soup.find("th", attrs={"abbr": "総従業者数"})
                if th:
                    td = th.find_next_sibling("td")
                    if td:
                        item[Schema.EMP_NUM] = _strip_unit(td.get_text(strip=True))
            time.sleep(1.0)
        except Exception as e:
            self.logger.warning("kani取得失敗 %s: %s", jcd, e)

        # kihonページ: 法人番号
        try:
            soup = self.get_soup(kihon_url)
            if soup:
                p = soup.find("p", id="check_CorporateNumber")
                if p:
                    co_num = p.get_text(strip=True)
                    # 13桁の数字のみ抽出
                    m = re.search(r"\d{13}", co_num)
                    if m:
                        item[Schema.CO_NUM] = m.group(0)
            time.sleep(1.0)
        except Exception as e:
            self.logger.warning("kihon取得失敗 %s: %s", jcd, e)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = KaigoKensakuScraper()
    scraper.execute(f"{BASE}/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
