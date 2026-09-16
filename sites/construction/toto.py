"""
TOTOリモデルクラブ (リフォーム店検索) — 北海道の会員店舗スクレイパー

取得対象:
    - リモデルクラブ店 (北海道のみ / 約159店)
    - 店名・都道府県・住所・TEL・HP・代表者・設立・従業員数・資本金・年商
    - 得意分野・リフォーム対応箇所・保有資格・建設業許可番号・SNS 等

取得フロー:
    検索ページ (https://jp.toto.com/reform/remodelclub/result/) は Next.js の
    クライアントサイド検索。一覧は内部 API で取得している:
        GET /api/remodel/remodelclubs/?prefecturesId=1000&pageNo={0起算}&perPage=50
            -> {count, maxPage, remodelClubs[...]}
        GET /api/remodel/remodelclubs/info/?remodelClubId={RCxxxxxx}
            -> 店舗詳細 (HP/代表者/資本金/SNS 等)
    prefecturesId は /api/remodel/masters/prefectures/ のマスタ (北海道=1000)。
    一覧 1 ページ取得 -> 1 店ごとに info を引いて即 yield する。

実行方法:
    python scripts/sites/construction/toto.py
    python bin/run_flow.py --site-id toto
"""

import re
import sys
import time
from pathlib import Path
from typing import Any, Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 備考指定: 北海道の会員店舗のみ抽出する
TARGET_PREF_NAME = "北海道"
TARGET_PREF_ID = 1000

LIST_API_PATH = "/api/remodel/remodelclubs/"
INFO_API_PATH = "/api/remodel/remodelclubs/info/"
DETAIL_PAGE_PATH = "/reform/remodelclub/{club_id}/"

PER_PAGE = 50          # ネットワーク堅牢性のため 50 件以下に抑える
MAX_ATTEMPTS = 3       # リトライ上限 (無限ループ禁止)
MAX_PAGE_GUARD = 200   # 万一 maxPage が壊れていた場合の保険


def _clean(value: Any) -> str:
    """API 値を CSV 用の文字列に整形する (全角空白の連続を 1 つに畳む)。"""
    if value is None:
        return ""
    text = str(value)
    return re.sub(r"[\s　]+", " ", text).strip()


def _join_names(items: Any, sep: str = "、") -> str:
    """[{code, name}, ...] 形式のリストを name の連結文字列にする。"""
    if not items:
        return ""
    return sep.join(_clean(i.get("name")) for i in items if i and i.get("name"))


def _flag(value: Any, yes: str = "あり", no: str = "") -> str:
    """0/1 フラグを表示用文字列にする。"""
    return yes if value == 1 else no


def _format_founding(value: Any) -> str:
    """会社設立 (例: "1999/2") を "1999-02" 形式に正規化する。日が無いので日は付けない。"""
    text = _clean(value)
    if not text:
        return ""
    m = re.match(r"^(\d{4})[/年.-]?(\d{1,2})?[/月.-]?(\d{1,2})?", text)
    if not m:
        return text
    year, month, day = m.group(1), m.group(2), m.group(3)
    if month and day:
        return f"{year}-{int(month):02d}-{int(day):02d}"
    if month:
        return f"{year}-{int(month):02d}"
    return year


def _format_license_number(info: dict) -> str:
    """建設業許可番号を "知事 (般-26) 第3217号" 形式に組み立てる。"""
    a1 = _clean(info.get("allowedNumber1"))
    a2 = _clean(info.get("allowedNumber2"))
    a3 = _clean(info.get("allowedNumber3"))
    a4 = _clean(info.get("allowedNumber4"))
    if not any([a1, a2, a3, a4]):
        return ""
    parts = []
    if a1:
        parts.append(a1)
    if a2 or a3:
        parts.append("({})".format("-".join(p for p in (a2, a3) if p)))
    if a4:
        parts.append(f"第{a4}号")
    return " ".join(parts)


class TotoRemodelClubScraper(StaticCrawler):
    """TOTOリモデルクラブ (北海道) スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "リモデルクラブID",
        "FAX",
        "市区町村",
        "リフォーム対応箇所",
        "保有資格",
        "建設業許可番号",
        "建築士事務所登録",
        "新築工事",
        "工事保険加入",
        "リフォーム瑕疵保険",
        "住宅設備保証",
        "ショールーム",
        "駐車場",
        "女性アドバイザー",
        "加盟団体",
        "年間リフォーム件数",
        "施工事例件数",
        "コンテスト受賞",
        "YouTubeアカウント",
        "緯度",
        "経度",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        list_api = urljoin(url, LIST_API_PATH)
        info_api = urljoin(url, INFO_API_PATH)

        seen: set[str] = set()
        page_no = 0
        max_page = None

        while page_no < MAX_PAGE_GUARD:
            data = self._get_json(
                list_api,
                {
                    "prefecturesId": TARGET_PREF_ID,
                    "pageNo": page_no,
                    "perPage": PER_PAGE,
                },
            )
            clubs = data.get("remodelClubs") or []
            if page_no == 0:
                self.total_items = data.get("count") or 0
                max_page = data.get("maxPage") or 0
                self.logger.info(
                    "%s のリモデルクラブ店: %s 件 (%s ページ)",
                    TARGET_PREF_NAME, self.total_items, max_page,
                )
            if not clubs:
                break

            for club in clubs:
                club_id = _clean(club.get("remodelClubId"))
                if not club_id or club_id in seen:
                    continue
                seen.add(club_id)

                # 念のためレスポンス側でも都道府県を確認して北海道以外を除外する
                pref = _clean((club.get("jisLocation") or {}).get("prefecturesName"))
                if pref != TARGET_PREF_NAME:
                    continue

                try:
                    item = self._build_item(url, info_api, club)
                except Exception:
                    self.logger.exception("詳細取得に失敗: %s", club_id)
                    continue
                if item:
                    yield item

            page_no += 1
            if max_page and page_no >= max_page:
                break

    # ------------------------------------------------------------------
    # 詳細 (info API) + 一覧の情報をマージして 1 レコードを組み立てる
    # ------------------------------------------------------------------
    def _build_item(self, url: str, info_api: str, club: dict) -> dict | None:
        club_id = _clean(club.get("remodelClubId"))
        detail_url = urljoin(url, DETAIL_PAGE_PATH.format(club_id=club_id))

        info: dict = {}
        try:
            info = self._get_json(info_api, {"remodelClubId": club_id}) or {}
        except Exception:
            self.logger.warning("店舗詳細 API 取得失敗 (一覧の情報のみ使用): %s", club_id)

        jis = info.get("jisLocation") or club.get("jisLocation") or {}
        city = _clean(jis.get("name"))
        addr_others = _clean(info.get("addressOthers") or club.get("addressOthers"))
        sns = info.get("sns") or {}

        employees = info.get("employees")
        employees_text = "" if employees in (None, "") else _clean(employees)

        reform_per_year = info.get("reformNumberPerYear")
        awards = [
            label
            for label, key in (
                ("全国コンテスト", "winningNationalContestFlag"),
                ("エリアコンテスト", "winningAreaContestFlag"),
                ("オーナーズコンテスト", "winningOwnerContestFlag"),
            )
            if info.get(key) == 1
        ]

        return {
            Schema.URL: detail_url,
            Schema.NAME: _clean(info.get("storeName") or club.get("storeName")),
            Schema.PREF: _clean(jis.get("prefecturesName")),
            Schema.ADDR: f"{city}{addr_others}",
            Schema.TEL: _clean(info.get("tel") or club.get("tel")),
            Schema.HP: _clean(info.get("siteUrl")),
            Schema.REP_NM: _clean(info.get("presidentName")),
            Schema.EMP_NUM: employees_text,
            Schema.CAP: _clean(info.get("capital")),
            Schema.SALES: _clean(info.get("annualSales")),
            Schema.OPEN_DATE: _format_founding(info.get("founding")),
            Schema.TIME: _clean(info.get("openHours") or club.get("openHours")),
            Schema.HOLIDAY: _clean(info.get("closed") or club.get("closed")),
            Schema.CAT_SITE: _join_names(info.get("specialties") or club.get("specialties")),
            Schema.PAYMENTS: "キャッシュレス決済可" if info.get("cashlessFlag") == 1 else "",
            Schema.LINE: _clean(sns.get("lineId")),
            Schema.INSTA: _clean(sns.get("instagram")),
            Schema.X: _clean(sns.get("x")),
            Schema.FB: _clean(sns.get("facebook")),
            "リモデルクラブID": club_id,
            "FAX": _clean(info.get("fax")),
            "市区町村": city,
            "リフォーム対応箇所": _join_names(info.get("remodelTargets")),
            "保有資格": _join_names(info.get("licenses")),
            "建設業許可番号": _format_license_number(info),
            "建築士事務所登録": _flag(info.get("architectFlag")),
            "新築工事": _flag(info.get("newConstructionFlag"), yes="可"),
            "工事保険加入": _flag(info.get("constructionInsuranceFlag")),
            "リフォーム瑕疵保険": _flag(info.get("defectsInsuranceFlag")),
            "住宅設備保証": _flag(info.get("housingEquipmentGuaranteeFlag")),
            "ショールーム": _flag(info.get("showroomFlag")),
            "駐車場": _flag(info.get("parkingFlag")),
            "女性アドバイザー": _flag(info.get("femaleAdviserFlag")),
            "加盟団体": _join_names(info.get("groupEntries")),
            "年間リフォーム件数": "" if reform_per_year in (None, "") else _clean(reform_per_year),
            "施工事例件数": _clean(info.get("numberOfCases") or club.get("numberOfCases") or 0),
            "コンテスト受賞": "、".join(awards),
            "YouTubeアカウント": _clean(sns.get("youtube")),
            "緯度": _clean(info.get("latitude") or club.get("latitude")),
            "経度": _clean(info.get("longitude") or club.get("longitude")),
        }

    # ------------------------------------------------------------------
    # JSON API 取得 (上限付きリトライ)
    # ------------------------------------------------------------------
    def _get_json(self, api_url: str, params: dict) -> dict:
        last_error: Exception | None = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = self.session.get(api_url, params=params, timeout=self.TIMEOUT)
                response.raise_for_status()
                return response.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                last_error = e
                self.logger.warning(
                    "API 取得失敗 (%d/%d): %s %s — %s",
                    attempt + 1, MAX_ATTEMPTS, api_url, params, e,
                )
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"API 取得に {MAX_ATTEMPTS} 回失敗: {api_url} {params}") from last_error


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TotoRemodelClubScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://jp.toto.com/reform/remodelclub/result/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
