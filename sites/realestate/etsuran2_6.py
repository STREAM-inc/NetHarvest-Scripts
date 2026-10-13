# -*- coding: utf-8 -*-
"""
国土交通省 建設業者・宅建業者等企業情報検索システム【賃貸住宅管理業者検索】

対象サイト: https://etsuran2.mlit.go.jp/TAKKEN/chintaiKensaku.do
出典      : 国土交通省 建設業者・宅建業者等企業情報検索システム、PDL1.0
            (https://www.digital.go.jp/resources/open_data/public_data_license_v1.0)

取得フロー (すべて requests の POST — Struts の画面遷移をそのまま再現):
    1. chintaiKensaku.do (GET) で検索フォームの初期値を取得
    2. kenCode=13 (東京都) / choice=2 (事務所所在地も検索対象) / dispCount=50 で
       CMD=search を POST → 検索結果一覧 (1 行 = 1 事務所。本店・支店が展開済み)
    3. 各行の商号リンク onclick="js_ShowDetail('NNNNNN')" から登録番号キーを取得し、
       ctGaiyo.do へ POST して業者概要 (フリガナ・登録年月日・有効期間・TEL 等) を取得
    4. 1 事務所 = 1 レコードとして即 yield
    5. CMD=selectPage + pageListNo1/2 で次ページへ

注意:
    - 一覧の「所在地」は事務所ごとの住所、詳細の「主たる事務所の所在地」は本店住所。
      本店が他県にある業者の東京支店も kenCode=13 でヒットするため、両方を保持する。
    - 「業務管理者の数」「所属団体」「兼業区分・事業内容」はこの公表システムには
      項目自体が存在しないため取得できない (備考の「表示があれば」に該当)。
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 備考の指示: 所在地 = 東京都 (kenCode=13) に絞り込む
TARGET_KEN_CODE = "13"
TARGET_PREF_NAME = "東京都"
# 空欄 = 本店だけでなく事務所所在地も検索対象 (支店を一覧に展開するために必須)
OFFICE_SCOPE = "2"
# 1 ページあたりの表示件数 (ネットワーク堅牢性のため 50 件以下)
DISP_COUNT = "50"

MAX_ATTEMPTS = 3
PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")
DETAIL_KEY_RE = re.compile(r"js_ShowDetail\(\s*'([^']+)'\s*\)")


def _norm(text: Optional[str]) -> str:
    """全角スペースを半角に均して前後・連続空白を潰す。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("　", " ")).strip()


def _split_pref(address: str) -> Tuple[str, str]:
    """住所を (都道府県, 市区町村以降) に分割する。"""
    addr = _norm(address)
    m = PREF_RE.match(addr)
    if not m:
        return "", addr
    return m.group(1), addr[m.end():].strip()


class MlitChintaiKanriScraper(StaticCrawler):
    """国土交通省 賃貸住宅管理業者検索 — 東京都内の事務所を本店・支店とも 1 行ずつ取得"""

    DELAY = 0.3
    ITEM_DELAY = 0.0

    EXTRA_COLUMNS = [
        "登録番号",
        "最初の登録年月日",
        "有効期間（起算日）",
        "有効期間（満了日）",
        "法人・個人の別",
        "代表者名_カナ",
        "事務所名",
        "本店・支店区分",
        "主たる事務所の郵便番号",
        "主たる事務所の都道府県",
        "主たる事務所の所在地",
        "都道府県コード",
    ]

    # ------------------------------------------------------------------ setup
    def _setup(self):
        super()._setup()
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.9",
            }
        )

    # -------------------------------------------------------------- utilities
    def _post(self, url: str, data: Dict[str, str]) -> Optional[bs4.BeautifulSoup]:
        """Shift_JIS の Struts 画面へ POST する。上限付きリトライ、全滅で raise。"""
        last_error: Optional[Exception] = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                res = self.session.post(
                    url, data=data, timeout=self.TIMEOUT, headers={"Referer": url}
                )
                res.raise_for_status()
                res.encoding = "cp932"
                return bs4.BeautifulSoup(res.text, "html.parser")
            except requests.exceptions.RequestException as e:
                last_error = e
                self.logger.warning(
                    "POST 失敗 (%d/%d): %s — %s", attempt + 1, MAX_ATTEMPTS, url, e
                )
                time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"POST が {MAX_ATTEMPTS} 回失敗しました: {url}") from last_error

    @staticmethod
    def _form_state(soup: bs4.BeautifulSoup) -> Dict[str, str]:
        """画面内 form の input/select の現在値をそのまま辞書化する。"""
        state: Dict[str, str] = {}
        form = soup.find("form", id="ctModel") or soup
        for inp in form.find_all("input"):
            name = inp.get("name")
            if not name:
                continue
            itype = (inp.get("type") or "text").lower()
            if itype in ("radio", "checkbox"):
                if inp.has_attr("checked"):
                    state[name] = inp.get("value", "")
            else:
                state[name] = inp.get("value", "") or ""
        for sel in form.find_all("select"):
            name = sel.get("name")
            if not name:
                continue
            chosen = sel.find("option", selected=True) or sel.find("option")
            state[name] = (chosen.get("value", "") if chosen else "") or ""
        return state

    @staticmethod
    def _result_count(soup: bs4.BeautifulSoup) -> int:
        m = re.search(r"検索結果：\s*(\d+)\s*件", soup.get_text(" ", strip=True))
        return int(m.group(1)) if m else 0

    @staticmethod
    def _page_count(state: Dict[str, str]) -> int:
        try:
            return max(1, int(state.get("pageCount") or 1))
        except ValueError:
            return 1

    @staticmethod
    def _rows(soup: bs4.BeautifulSoup) -> List[Dict[str, str]]:
        """検索結果テーブルの 1 行 (= 1 事務所) を辞書化する。"""
        table = soup.find("table", class_="re_disp")
        if not table:
            return []
        rows: List[Dict[str, str]] = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 6:
                continue
            link = tds[2].find("a")
            if not link:
                continue
            m = DETAIL_KEY_RE.search(link.get("onclick", "") or "")
            rows.append(
                {
                    "detail_key": m.group(1) if m else "",
                    "登録番号": _norm(tds[1].get_text()),
                    "名称": _norm(link.get_text()),
                    "代表者名": _norm(tds[3].get_text()),
                    "事務所名": _norm(tds[4].get_text()),
                    "所在地": _norm(tds[5].get_text()),
                }
            )
        return rows

    @staticmethod
    def _detail_fields(soup: bs4.BeautifulSoup) -> Dict[str, Dict[str, str]]:
        """業者概要画面の th/td を {ラベル: {"value":..., "kana":...}} へ。"""
        fields: Dict[str, Dict[str, str]] = {}
        for tr in soup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = _norm(th.get_text(" ")).replace(" ", "")
            if not label:
                continue
            phonetic = td.find("p", class_="phonetic")
            kana = _norm(phonetic.get_text()) if phonetic else ""
            if phonetic:
                phonetic.extract()
            fields[label] = {
                "value": _norm(td.get_text("\n")),
                "lines": [_norm(x) for x in td.get_text("\n").split("\n") if _norm(x)],
                "kana": kana,
            }
        return fields

    def _fetch_detail(
        self, base_url: str, list_state: Dict[str, str], detail_key: str
    ) -> Dict[str, Dict[str, str]]:
        """ctGaiyo.do (業者概要) を取得する。同一業者は支店行で使い回す。"""
        if detail_key in self._detail_cache:
            return self._detail_cache[detail_key]
        detail_url = base_url.replace("chintaiKensaku.do", "ctGaiyo.do")
        data = dict(list_state)
        data["CMD"] = ""
        data["sv_licenseNo"] = detail_key
        soup = self._post(detail_url, data)
        fields = self._detail_fields(soup) if soup else {}
        self._detail_cache[detail_key] = fields
        if self.DELAY:
            time.sleep(self.DELAY)
        return fields

    # ------------------------------------------------------------------ parse
    def parse(self, url: str) -> Generator[dict, None, None]:
        self._detail_cache: Dict[str, Dict[str, Dict[str, str]]] = {}

        # 1) 検索フォーム初期画面 (get_soup 経由 = テストランナーの中断ポイント)
        form_soup = self.get_soup(url)
        if form_soup is None:
            self.logger.error("検索フォームを取得できませんでした: %s", url)
            return

        search_data = self._form_state(form_soup)
        search_data.update(
            {
                "CMD": "search",
                "kenCode": TARGET_KEN_CODE,   # 備考の指示: 東京都のみ
                "choice": OFFICE_SCOPE,       # 本店に限定せず事務所所在地も対象にする
                "dispCount": DISP_COUNT,
                "sortValue": "1",
                "dispPage": "1",
            }
        )

        list_soup = self._post(url, search_data)
        if list_soup is None:
            return

        total = self._result_count(list_soup)
        state = self._form_state(list_soup)
        total_pages = self._page_count(state)
        self.logger.info(
            "%s: 検索結果 %d 件 / %d ページ", TARGET_PREF_NAME, total, total_pages
        )

        page_no = 1
        while True:
            rows = self._rows(list_soup)
            if not rows:
                self.logger.warning("%d ページ目に明細がありません → 終了", page_no)
                break
            self.logger.info("%d/%d ページ: %d 事務所", page_no, total_pages, len(rows))

            for row in rows:
                detail = (
                    self._fetch_detail(url, state, row["detail_key"])
                    if row["detail_key"]
                    else {}
                )

                def f(label: str, key: str = "value") -> str:
                    return detail.get(label, {}).get(key, "")

                # 主たる事務所 (本店) の所在地は 〒 と住所の 2 行
                head_lines = detail.get("主たる事務所の所在地", {}).get("lines", [])
                head_zip = ""
                head_addr = ""
                for line in head_lines:
                    if line.startswith("〒"):
                        head_zip = line.lstrip("〒").strip()
                    else:
                        head_addr = line
                head_pref, _ = _split_pref(head_addr)

                office_name = row["事務所名"]
                office_kbn = "本店" if office_name in ("本店", "本社") else "支店"
                pref, addr = _split_pref(row["所在地"])

                # 備考の指示: 東京都のみ (サーバ側で絞り込み済みだが念のため)
                if pref and pref != TARGET_PREF_NAME:
                    continue

                item = {
                    Schema.URL: url,
                    Schema.NAME: f("商号又は名称") or row["名称"],
                    Schema.NAME_KANA: f("商号又は名称", "kana"),
                    Schema.PREF: pref or TARGET_PREF_NAME,
                    # 郵便番号は本店分のみ公表されているため、本店行だけに載せる
                    Schema.POST_CODE: head_zip if office_kbn == "本店" else "",
                    Schema.ADDR: addr,
                    Schema.TEL: f("電話番号"),
                    Schema.REP_NM: f("代表者の氏名") or row["代表者名"],
                    "登録番号": f("登録番号") or row["登録番号"],
                    "最初の登録年月日": f("最初の登録年月日"),
                    "有効期間（起算日）": f("有効期間（起算日）"),
                    "有効期間（満了日）": f("有効期間（満了日）"),
                    "法人・個人の別": f("法人・個人の別"),
                    "代表者名_カナ": f("代表者の氏名", "kana"),
                    "事務所名": office_name,
                    "本店・支店区分": office_kbn,
                    "主たる事務所の郵便番号": head_zip,
                    "主たる事務所の都道府県": head_pref,
                    "主たる事務所の所在地": head_addr,
                    "都道府県コード": TARGET_KEN_CODE,
                }
                yield item

            page_no += 1
            if page_no > total_pages:
                break

            next_data = dict(state)
            next_data.update(
                {
                    "CMD": "selectPage",
                    "sv_licenseNo": "",
                    "pageListNo1": str(page_no),
                    "pageListNo2": str(page_no),
                }
            )
            list_soup = self._post(url, next_data)
            if list_soup is None:
                break
            state = self._form_state(list_soup)
            if self.DELAY:
                time.sleep(self.DELAY)


# ====================== ローカル実行用エントリーポイント ======================
if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = MlitChintaiKanriScraper()
    scraper.execute("https://etsuran2.mlit.go.jp/TAKKEN/chintaiKensaku.do")
    print(f"\n取得件数: {scraper.item_count}")
    print(f"出力先:   {scraper.output_filepath}")
