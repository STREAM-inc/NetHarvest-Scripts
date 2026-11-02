# -*- coding: utf-8 -*-
"""
国土交通省 建設業者・宅建業者等企業情報検索システム【宅地建物取引業者】
    全国 47 都道府県 / 新規免許 (更新回数 (1)) かつ 最初の免許年月日が令和8年以降のみ

対象サイト: https://etsuran2.mlit.go.jp/TAKKEN/takkenKensaku.do
出典      : 国土交通省 建設業者・宅建業者等企業情報検索システム、PDL1.0
            (https://www.digital.go.jp/resources/open_data/public_data_license_v1.0)

既存サイト mlit_takken と同一の画面 (都道府県プルダウン kenCode → 検索 →
結果一覧 table.re_disp + ページ送り pageListNo1 → 商号リンクで詳細 → 前画面に戻る)
・同一の出力カラムを対象にするが、Playwright のクリック操作ではなく
その画面遷移が送信している Struts の POST をそのまま再現する (= StaticCrawler)。

取得フロー:
    1. takkenKensaku.do (GET) で検索フォーム form#tkModel の初期値を取得
    2. 47 都道府県それぞれについて CMD=search を POST
       (kenCode=都道府県 / dispCount=50。並び順は既定のまま = 免許行政庁→免許証番号の昇順)
    3. CMD=selectPage + pageListNo1/dispPage で全ページを「最終ページ → 1 ページ目」の
       降順に走査する (新規免許は番号が大きく最終ページ側に固まるため初回 yield が速い)
    4. 一覧の免許証番号列 (例 "(01)第010286号") の括弧内=更新回数が 1 の行だけ
       詳細 tkGaiyo.do を POST で取得する (知事免許・国土交通大臣免許とも同じ扱い)
    5. 詳細の「最初の免許年月日」が令和8年 (R08年) 以降のものだけを 1 件ずつ即 yield

ページ送りの検証と都道府県単位のリトライ:
    - selectPage の応答で選択中ページ (pageListNo1) が要求ページと一致しない、
      一覧行が 0 行、または走査行数が「検索結果件数」と一致しない場合は
      ページ送りが止まったとみなし、その都道府県を検索からやり直す (最大 PREF_ATTEMPTS 回)。
    - 都道府県ごとに「検索結果件数 / 一覧で走査した件数 / 新規(1)行数 / 詳細取得数 /
      出力件数」をログに残す。
    - リトライ上限に達した都道府県は残りの都道府県を取得し終えた後に例外で報告する。

詳細取得の打ち切り (高速化):
    免許証番号は免許行政庁ごとに付与順の連番のため、番号の降順に見ていくと
    最初の免許年月日も (免許換え等の例外を除き) 降順に並ぶ。同一都道府県・同一免許行政庁で
    「令和8年より前」の (1) 行が OLD_STREAK_LIMIT 件連続したら、その行政庁の残り (より古い番号)
    の詳細取得を省略する (一覧の走査自体は件数検証のため最後まで行う)。
    OLD_STREAK_LIMIT = None で無効化でき、その場合は (1) 行すべての詳細を開く。

注意:
    - 並び替え (免許証番号の降順ソート) はページ間で行が重複し取得漏れが出るため指定しない。
    - kenCode は「事務所の所在地」での絞り込みのため、国土交通大臣免許の業者は複数の
      都道府県でヒットする。1 業者が複数事務所を持つと同一都道府県内でも複数行になる。
      取得項目はすべて業者単位 (詳細ページ) のため、詳細キーで全国通しの重複排除をする。
    - 都道府県 / 住所 = 詳細の「主たる事務所の所在地」(本店)、都道府県コード = 検索に
      使った都道府県コード。大臣免許では両者が一致しないことがある。
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from typing import Dict, Generator, List, Optional, Set, Tuple
from urllib.parse import urljoin

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

PREFS: List[Tuple[str, str]] = [
    ("01", "北海道"), ("02", "青森県"), ("03", "岩手県"), ("04", "宮城県"),
    ("05", "秋田県"), ("06", "山形県"), ("07", "福島県"), ("08", "茨城県"),
    ("09", "栃木県"), ("10", "群馬県"), ("11", "埼玉県"), ("12", "千葉県"),
    ("13", "東京都"), ("14", "神奈川県"), ("15", "新潟県"), ("16", "富山県"),
    ("17", "石川県"), ("18", "福井県"), ("19", "山梨県"), ("20", "長野県"),
    ("21", "岐阜県"), ("22", "静岡県"), ("23", "愛知県"), ("24", "三重県"),
    ("25", "滋賀県"), ("26", "京都府"), ("27", "大阪府"), ("28", "兵庫県"),
    ("29", "奈良県"), ("30", "和歌山県"), ("31", "鳥取県"), ("32", "島根県"),
    ("33", "岡山県"), ("34", "広島県"), ("35", "山口県"), ("36", "徳島県"),
    ("37", "香川県"), ("38", "愛媛県"), ("39", "高知県"), ("40", "福岡県"),
    ("41", "佐賀県"), ("42", "長崎県"), ("43", "熊本県"), ("44", "大分県"),
    ("45", "宮崎県"), ("46", "鹿児島県"), ("47", "沖縄県"),
]

# 1 ページあたりの表示件数 (選択肢の最大。ネットワーク堅牢性のため 50 件以下)
DISP_COUNT = "50"

# 1 リクエストあたりの POST リトライ上限
MAX_ATTEMPTS = 3
# ページ送りが止まった都道府県を検索からやり直す上限
PREF_ATTEMPTS = 3

# 備考の指示: 対象は最初の免許年月日が令和8年 (R08年) 以降
MIN_REIWA_YEAR = 8
# 詳細取得の打ち切り: 同一行政庁で令和8年より前の (1) 行がこの件数連続したら残りを省略
OLD_STREAK_LIMIT: Optional[int] = 50

# 免許証番号の括弧内 (更新回数) が 1 の行だけを対象にする
LICENSE_FIRST_RE = re.compile(r"[(（]\s*0?1\s*[)）]")
LICENSE_NO_RE = re.compile(r"第\s*(\d+)\s*号")
DETAIL_KEY_RE = re.compile(r"js_ShowDetail\(\s*'([^']+)'\s*\)")
PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")
REIWA_RE = re.compile(r"(?:R|Ｒ|令和)\s*(\d+|元)\s*年")


class _PageStall(Exception):
    """ページ送りが途中で止まった (都道府県単位でやり直す)。"""


def _norm(text: Optional[str]) -> str:
    """全角スペースを半角に均して前後・連続空白を潰す。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("　", " ")).strip()


def _compact(text: Optional[str]) -> str:
    """数量系 ("3 人" / "1,000 千円") の内部空白を除去する。"""
    return _norm(text).replace(" ", "")


def _split_pref(address: str) -> Tuple[str, str]:
    """住所を (都道府県, 市区町村以降) に分割する。"""
    addr = _norm(address)
    m = PREF_RE.match(addr)
    if not m:
        return "", addr
    return m.group(1), addr[m.end():].strip()


def _is_reiwa8_or_later(date_text: str) -> bool:
    """"R08年01月15日" / "令和8年…" が令和8年以降なら True (平成・昭和は False)。"""
    m = REIWA_RE.search(_norm(date_text))
    if not m:
        return False
    year = 1 if m.group(1) == "元" else int(m.group(1))
    return year >= MIN_REIWA_YEAR


class MlitTakkenNewLicenseNationwideScraper(StaticCrawler):
    """宅地建物取引業者 — 全国の、新規免許 (1) かつ令和8年以降に最初の免許を受けた業者のみ"""

    DELAY = 0.3  # 備考: サーバーが弱いので 200ms 以上空ける
    ITEM_DELAY = 0.0

    EXTRA_COLUMNS = [
        "都道府県コード",
        "免許証番号",
        "免許の有効期間",
        "法人・個人の別",
        "最初の免許年月日",
        "代表者名_カナ",
        "うち、専任の宅地建物取引士の数",
        "加入している宅地建物取引業保証協会",
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
    def _post(self, url: str, data: Dict[str, str]) -> bs4.BeautifulSoup:
        """Shift_JIS の Struts 画面へ POST する。送信前に必ず DELAY 待機、上限付きリトライ。"""
        last_error: Optional[Exception] = None
        for attempt in range(MAX_ATTEMPTS):
            time.sleep(self.DELAY)
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
        """画面内 form#tkModel の input/select の現在値をそのまま辞書化する。"""
        state: Dict[str, str] = {}
        form = soup.find("form", id="tkModel") or soup
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
        m = re.search(r"検索結果[：:]\s*([\d,]+)\s*件", soup.get_text(" ", strip=True))
        return int(m.group(1).replace(",", "")) if m else 0

    @staticmethod
    def _page_count(state: Dict[str, str]) -> int:
        try:
            return max(1, int(state.get("pageCount") or 1))
        except ValueError:
            return 1

    @staticmethod
    def _rows(soup: bs4.BeautifulSoup) -> List[Dict[str, str]]:
        """検索結果テーブルの 1 行を辞書化する。

        列: No. / 免許行政庁 / 免許証番号 / 商号又は名称 / 代表者名 / 事務所名 / 所在地
        """
        table = soup.find("table", class_="re_disp")
        if not table:
            return []
        rows: List[Dict[str, str]] = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 6:
                continue
            link = tds[3].find("a")
            if not link:
                continue
            m = DETAIL_KEY_RE.search(link.get("onclick", "") or "")
            if not m:
                continue
            rows.append(
                {
                    "detail_key": m.group(1),
                    "免許行政庁": _norm(tds[1].get_text()),
                    # 備考の td[3] = 1 始まりの 3 列目 = 免許証番号
                    "免許証番号": _compact(tds[2].get_text()),
                    "名称": _norm(link.get_text()),
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
            fields[label] = {"value": _norm(td.get_text(" ")), "kana": kana}
        return fields

    @staticmethod
    def _field(fields: Dict[str, Dict[str, str]], *keywords: str) -> Dict[str, str]:
        """ラベルに keywords をすべて含む最初の項目を返す (ラベル揺れ吸収)。"""
        for label, data in fields.items():
            if all(k in label for k in keywords):
                return data
        return {"value": "", "kana": ""}

    def _fetch_detail(
        self, detail_url: str, list_state: Dict[str, str], detail_key: str
    ) -> Dict[str, Dict[str, str]]:
        """tkGaiyo.do (業者概要) を取得する。"""
        data = dict(list_state)
        data["CMD"] = ""
        data["sv_licenseNo"] = detail_key
        return self._detail_fields(self._post(detail_url, data))

    def _build_item(
        self, url: str, pref_code: str, pref_name: str,
        row: Dict[str, str], fields: Dict[str, Dict[str, str]],
    ) -> dict:
        name_f = self._field(fields, "商号")
        rep_f = self._field(fields, "代表者")
        addr_f = self._field(fields, "主たる事務所", "所在地")
        pref_from_addr, addr_rest = _split_pref(addr_f["value"])
        return {
            Schema.URL: url,
            Schema.NAME: name_f["value"] or row["名称"],
            Schema.NAME_KANA: name_f["kana"],
            Schema.PREF: pref_from_addr or pref_name,
            Schema.ADDR: addr_rest,
            Schema.TEL: self._field(fields, "電話番号")["value"],
            Schema.REP_NM: rep_f["value"],
            Schema.EMP_NUM: _compact(self._field(fields, "総従事者数")["value"]),
            Schema.CAP: _compact(self._field(fields, "資本金")["value"]),
            "都道府県コード": pref_code,
            "免許証番号": self._field(fields, "免許証番号")["value"] or row["免許証番号"],
            "免許の有効期間": self._field(fields, "免許の有効期間")["value"],
            "法人・個人の別": self._field(fields, "法人", "個人")["value"],
            "最初の免許年月日": self._field(fields, "最初の免許年月日")["value"],
            "代表者名_カナ": rep_f["kana"],
            "うち、専任の宅地建物取引士の数": _compact(
                self._field(fields, "専任", "宅地建物取引士")["value"]
            ),
            "加入している宅地建物取引業保証協会": self._field(fields, "保証協会")["value"],
        }

    # ------------------------------------------------------------ 都道府県 1 回分
    def _scan_pref(
        self, url: str, detail_url: str, base_state: Dict[str, str],
        pref_code: str, pref_name: str, seen_keys: Set[str],
    ) -> Generator[dict, None, None]:
        """1 都道府県を検索して全ページ走査する。ページ送りが止まったら _PageStall。"""
        search_data = dict(base_state)
        search_data.update(
            {"CMD": "search", "kenCode": pref_code, "dispCount": DISP_COUNT, "dispPage": "1"}
        )
        list_soup = self._post(url, search_data)
        list_state = self._form_state(list_soup)
        total = self._result_count(list_soup)
        total_pages = self._page_count(list_state)
        self.logger.info(
            "=== %s (%s): 検索結果件数 %d 件 / %d ページ ===",
            pref_name, pref_code, total, total_pages,
        )
        if total == 0:
            return

        scanned = first_rows = fetched = yielded = 0
        # (免許行政庁) → 令和8年より前の (1) 行の連続数。None = 打ち切り済み
        old_streak: Dict[str, Optional[int]] = {}

        for page_no in range(total_pages, 0, -1):
            if page_no == 1 and total_pages == 1:
                page_soup = list_soup
            else:
                page_data = dict(list_state)
                page_data.update(
                    {
                        "CMD": "selectPage",
                        "pageListNo1": str(page_no),
                        "pageListNo2": str(page_no),
                        "dispPage": str(page_no),
                    }
                )
                page_soup = self._post(url, page_data)
                shown = self._form_state(page_soup).get("pageListNo1", "")
                if shown != str(page_no):
                    raise _PageStall(
                        f"{pref_name}: {page_no} ページ目を要求したが {shown or '?'} ページ目が返った"
                    )

            rows = self._rows(page_soup)
            if not rows:
                raise _PageStall(f"{pref_name}: {page_no} ページ目の一覧行が 0 行")
            scanned += len(rows)

            # ページ内も番号の降順に見る (打ち切り判定のため)
            for row in reversed(rows):
                if not LICENSE_FIRST_RE.match(row["免許証番号"]):
                    continue
                first_rows += 1
                key = row["detail_key"]
                if key in seen_keys:
                    continue
                authority = row["免許行政庁"]
                if OLD_STREAK_LIMIT is not None and old_streak.get(authority, 0) is None:
                    continue
                seen_keys.add(key)

                fields = self._fetch_detail(detail_url, list_state, key)
                fetched += 1
                if not fields:
                    self.logger.warning("詳細を取得できませんでした: %s", key)
                    continue

                first_date = self._field(fields, "最初の免許年月日")["value"]
                if not _is_reiwa8_or_later(first_date):
                    if OLD_STREAK_LIMIT is not None:
                        streak = (old_streak.get(authority) or 0) + 1
                        if streak >= OLD_STREAK_LIMIT:
                            self.logger.info(
                                "%s / %s: 令和8年より前が %d 件連続 (直近 %s %s) → 以降の詳細取得を省略",
                                pref_name, authority, streak, row["免許証番号"], first_date,
                            )
                            old_streak[authority] = None
                        else:
                            old_streak[authority] = streak
                    continue

                old_streak[authority] = 0
                yielded += 1
                yield self._build_item(url, pref_code, pref_name, row, fields)

        self.logger.info(
            "%s (%s): 検索結果件数 %d / 一覧走査 %d 件 / (1)行 %d / 詳細取得 %d / 出力 %d",
            pref_name, pref_code, total, scanned, first_rows, fetched, yielded,
        )
        if scanned != total:
            raise _PageStall(f"{pref_name}: 一覧走査 {scanned} 件 ≠ 検索結果件数 {total} 件")

    # ------------------------------------------------------------------ parse
    def parse(self, url: str) -> Generator[dict, None, None]:
        # 詳細画面は同一ディレクトリの tkGaiyo.do (ルート URL から派生させる)
        detail_url = urljoin(url, "tkGaiyo.do")

        # 検索フォーム初期画面 (get_soup 経由 = テストランナーの中断ポイント)
        form_soup = self.get_soup(url)
        if form_soup is None:
            raise RuntimeError(f"検索フォームを取得できませんでした: {url}")
        base_state = self._form_state(form_soup)

        seen_keys: Set[str] = set()
        failed_prefs: List[str] = []

        for pref_code, pref_name in PREFS:
            for attempt in range(PREF_ATTEMPTS):
                try:
                    # やり直しでも出力済みの業者は seen_keys で重複しない
                    yield from self._scan_pref(
                        url, detail_url, base_state, pref_code, pref_name, seen_keys
                    )
                    break
                except _PageStall as e:
                    self.logger.warning(
                        "ページ送り停止 (%d/%d): %s → 都道府県単位でリトライ",
                        attempt + 1, PREF_ATTEMPTS, e,
                    )
                    time.sleep(min(2 ** (attempt + 1), 16))
            else:
                self.logger.error("%s (%s): リトライ上限に達したため取得不完全", pref_name, pref_code)
                failed_prefs.append(pref_name)

        if failed_prefs:
            raise RuntimeError(f"ページ送りが完走しなかった都道府県: {', '.join(failed_prefs)}")


# ====================== ローカル実行用エントリーポイント ======================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    scraper = MlitTakkenNewLicenseNationwideScraper()
    scraper.execute("https://etsuran2.mlit.go.jp/TAKKEN/takkenKensaku.do")
    print(f"\n取得件数: {scraper.item_count}")
    print(f"出力先:   {scraper.output_filepath}")
