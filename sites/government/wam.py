"""
障害福祉サービス情報検索 (WAM NET) — 全国の障害福祉サービス事業所の公表情報

運営: 独立行政法人福祉医療機構 (WAM NET)
一覧URL: https://www.wam.go.jp/sfkohyoout/COP000100E0000.do

取得対象:
    - 全国 (47都道府県) の障害福祉サービス事業所 × サービス種別ごとの公表レコード
    - 事業所名 / サービス種別 / 住所 / 電話番号 / 事業所番号 / 緯度経度 等の構造化情報

取得フロー (EWB/dojo フレームワークの AJAX/POST を直接再現する):
    1. トップページを GET し、_TOKEN (タイムスタンプ) を取得
    2. 都道府県コード 01〜47 ごとに COP000100E01.do へ POST
       → JSON で市区町村リスト (政令市の区 + 一般市区町村) と各件数を取得
    3. 各市区町村コードで COP000100E02.do へ POST
       → 検索結果ページ (地図用に全件 div[id^=idList-] が埋め込まれている) を取得
    4. idList 1件ごとに即 yield (リストページに名称/種別/住所/TEL/詳細リンクが揃う)

設計メモ:
    - 1事業所が複数サービスを提供する場合、(事業所 × サービス種別) ごとに 1 レコードになる
      (詳細リンクの serviceSubNumber で一意。これで重複除去する)
    - 事業所番号・法人番号・サービス種別コードは詳細リンクのクエリから構造化値として取得できる。
    - 詳細ページ (COP020100E00.do) は運営方針等の長文 (著作権リスク) を多く含むため本文は取得せず、
      「法人等代表者の氏名」(代表者名) のみ td ラベルから抜き出して取得する。
    - リストページには市区町村の全件が埋め込まれており (idList 件数 == E01 の件数で検証済み)、
      ページ送りは不要。

実行方法:
    # ローカルテスト
    python scripts/sites/government/wam.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id wam
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, parse_qs

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE = "https://www.wam.go.jp/sfkohyoout/"
TOP_URL = BASE + "COP000100E0000.do"
E01_URL = BASE + "COP000100E01.do"  # 市区町村リスト (JSON)
E02_URL = BASE + "COP000100E02.do"  # 検索結果一覧 (HTML)

# 都道府県コード 01〜47
PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

_TOKEN_RE = re.compile(r'name="_TOKEN"\s+value="(\d+)"')
_NAME_PREFIX_RE = re.compile(r"^\s*\d+\.\s*")
# 住所先頭の都道府県を切り出す
_PREF_RE = re.compile(
    r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)"
)


def _clean(s) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class WamScraper(StaticCrawler):
    """障害福祉サービス情報検索 (WAM NET) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "代表者名",           # 例: 入澤知子 (詳細ページの「法人等代表者の氏名」)
        "サービス種別",       # 例: 児童発達支援 (構造化された短いラベル)
        "サービス種別コード",  # 例: 63
        "事業所番号",         # 例: 1350100051 (障害福祉サービス事業所番号 10桁)
        "事業者番号",         # 例: A1310700000035 (WAM 内部の事業者識別子)
        "緯度",
        "経度",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        token = self._fetch_token()
        seen: set[str] = set()

        for pref_code in PREF_CODES:
            towns = self._fetch_town_codes(token, pref_code)
            if not towns:
                self.logger.info("都道府県 %s: 市区町村なし (スキップ)", pref_code)
                continue
            self.logger.info("都道府県 %s: %d 市区町村", pref_code, len(towns))

            for town_code in towns:
                soup = self._fetch_list(token, pref_code, town_code)
                if soup is None:
                    continue
                count = 0
                for item in self._parse_list(soup):
                    key = item.get("_dedup_key") or ""
                    if key and key in seen:
                        continue
                    if key:
                        seen.add(key)
                    item.pop("_dedup_key", None)
                    count += 1
                    yield item
                self.logger.info(
                    "  pref=%s town=%s: %d 件", pref_code, town_code, count
                )
                time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # トークン取得
    # ------------------------------------------------------------------

    def _fetch_token(self) -> str:
        soup = self.get_soup(TOP_URL)
        if soup is not None:
            el = soup.find("input", attrs={"name": "_TOKEN"})
            if el and el.get("value"):
                return el["value"].strip()
            m = _TOKEN_RE.search(str(soup))
            if m:
                return m.group(1)
        # トークン検証は緩く、タイムスタンプ形式であれば概ね通る
        self.logger.warning("トークン取得に失敗。フォールバック値を使用します。")
        return "1700000000000"

    # ------------------------------------------------------------------
    # 市区町村コード取得 (COP000100E01.do → JSON)
    # ------------------------------------------------------------------

    def _fetch_town_codes(self, token: str, pref_code: str) -> list[str]:
        data = self._base_form(token)
        data["vo_headVO_prefCode"] = pref_code
        try:
            resp = self.session.post(E01_URL, data=data, timeout=self.TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            payload = resp.text.split("&& ", 1)[-1]
            obj = __import__("json").loads(payload)
        except Exception as e:
            self.logger.warning("E01 失敗 pref=%s: %s", pref_code, e)
            return []

        rst = obj.get("rstMap", {})
        codes: list[str] = []
        # 政令指定都市の区 (kbn=0 のみ。kbn=1 は親見出しで件数0なので除外)
        for c in rst.get("Citylist", []) or []:
            if c.get("designatedCityKbn") == "0":
                v = c.get("designatedCityNameSelectValue")
                if v:
                    codes.append(v)
        # 一般市区町村
        for c in rst.get("CityTownlist", []) or []:
            v = c.get("prefNameSelectTownValue")
            if v:
                codes.append(v)
        return codes

    # ------------------------------------------------------------------
    # 検索結果一覧取得 (COP000100E02.do → HTML)
    # ------------------------------------------------------------------

    def _fetch_list(self, token: str, pref_code: str, town_code: str):
        data = self._base_form(token)
        data["vo_headVO_prefCode"] = pref_code
        data["vo_headVO_townCode"] = town_code
        try:
            resp = self.session.post(E02_URL, data=data, timeout=self.TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return bs4.BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            self.logger.warning(
                "E02 失敗 pref=%s town=%s: %s", pref_code, town_code, e
            )
            return None

    def _parse_list(self, soup) -> Generator[dict, None, None]:
        for d in soup.select("div[id^=idList-]"):
            try:
                item = self._parse_item(soup, d)
            except Exception as e:
                self.logger.warning("アイテムのパースに失敗: %s", e)
                continue
            if item:
                yield item

    def _parse_item(self, soup, d) -> dict | None:
        list_id = d.get("id", "")
        idx = list_id.split("-")[-1] if "-" in list_id else ""

        name_el = d.select_one(".lst-name")
        name = _NAME_PREFIX_RE.sub("", _clean(name_el.get_text(" ") if name_el else ""))
        if not name:
            return None

        service = _clean(d.select_one(".lst-service").get_text()) if d.select_one(".lst-service") else ""
        address = _clean(d.select_one(".lst-address").get_text()) if d.select_one(".lst-address") else ""
        tel = _clean(d.select_one(".lst-telephone").get_text()) if d.select_one(".lst-telephone") else ""

        lat = _clean(soup.select_one(f"#lat{idx}").get_text()) if idx and soup.select_one(f"#lat{idx}") else ""
        lng = _clean(soup.select_one(f"#lng{idx}").get_text()) if idx and soup.select_one(f"#lng{idx}") else ""

        # 都道府県を住所先頭から分離
        pref = ""
        addr_rest = address
        m = _PREF_RE.match(address)
        if m:
            pref = m.group(1)
            addr_rest = address[m.end():].strip()

        # 詳細リンクのクエリから構造化コードを取得 (詳細ページ自体は取得しない)
        detail_url = ""
        fac_no = corp_no = svc_type = svc_sub = ""
        a = d.select_one("a.detail-button[href]")
        if a:
            detail_url = urljoin(BASE, a.get("href"))
            q = parse_qs(urlparse(detail_url).query)
            fac_no = (q.get("facilityNumber") or [""])[0]
            corp_no = (q.get("corporationNumber") or [""])[0]
            svc_type = (q.get("serviceType") or [""])[0]
            svc_sub = (q.get("serviceSubNumber") or [""])[0]

        # 詳細ページから代表者名のみ取得 (本文は保存しない)
        rep_name = self._fetch_rep_name(detail_url)

        return {
            Schema.NAME: name,
            Schema.URL: detail_url or TOP_URL,
            Schema.PREF: pref,
            Schema.ADDR: addr_rest,
            Schema.TEL: tel,
            Schema.CAT_SITE: service,
            "代表者名": rep_name,
            "サービス種別": service,
            "サービス種別コード": svc_type,
            "事業所番号": fac_no,
            "事業者番号": corp_no,
            "緯度": lat,
            "経度": lng,
            # 重複除去用 (serviceSubNumber が一意。無ければ番号+種別で代替)
            "_dedup_key": svc_sub or f"{fac_no}-{svc_type}",
        }

    # ------------------------------------------------------------------
    # 詳細ページから代表者名を取得 (COP020100E00.do → HTML)
    # ------------------------------------------------------------------

    def _fetch_rep_name(self, detail_url: str) -> str:
        """詳細ページの「法人等代表者の氏名」だけを抜き出す。

        詳細ページは ``<td>法人等代表者の氏名</td><td colspan="4">入澤知子</td>``
        という 2 セル構成。ラベル td を探し、その隣の td のテキストを返す。
        運営方針等の長文 (著作権リスク) は一切保存しない。
        """
        if not detail_url:
            return ""
        soup = self.get_soup(detail_url)
        time.sleep(self.DELAY)
        if soup is None:
            return ""
        for td in soup.find_all("td"):
            label = _clean(td.get_text())
            if "代表者" in label and "氏名" in label:
                val_td = td.find_next_sibling("td")
                if val_td:
                    return _clean(val_td.get_text())
        return ""

    # ------------------------------------------------------------------
    # 共通フォームパラメータ
    # ------------------------------------------------------------------

    def _base_form(self, token: str) -> dict:
        return {
            "_FRAMEID": "root",
            "_TARGETID": "_NOTARGETWINID",
            "_LUID": "",
            "_TOKEN": token,
            "_FORMID": "COP000100",
            "vo_headVO_adress": "",
            "vo_headVO_corporation_name": "",
            "vo_headVO_facility_name": "",
            "vo_headVO_no": "",
            "vo_headVO_townCode": "",
            "vo_headVO_prefCode": "",
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = WamScraper()
    scraper.execute(TOP_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
