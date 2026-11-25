"""
国税庁 法人番号公表サイト（新規指定法人） — houjin_bangou_2

運営: 国税庁 (National Tax Agency)
正規URL: https://www.houjin-bangou.nta.go.jp/download/
利用規約: https://www.houjin-bangou.nta.go.jp/riyokiyaku/index.html
    → 公共データ利用規約(第1.0版)準拠。スクレイピング/自動取得の禁止条項なし (出典記載が条件)。

取得対象 (STREAMREQ-32761):
    公式 CSV (Shift_JIS, zip) から
      法人番号指定年月日 >= 2026-01-01
    の法人を全国分出力する。ページ巡回は行わない。
    処理区分での絞り込みはスクレイパー内では行わず、処理区分は全行に出力カラムとして残す
    (01=新規 かどうかは後工程で判断する)。

取得フロー:
    1. url (= /download/) から zenken/ ・ sabun/ を urljoin で導出
    2. 全件ページ (zenken/) の「CSV形式・Shift_JIS 令和X年M月D日更新」から全件スナップショット日を取得
    3. 差分ページ (sabun/) のうち、スナップショット日より新しい日付の差分 zip を新しい順に処理
       - 差分は新しい順に見るので、法人番号の初出 = 最新状態。
         最新状態で指定年月日条件を判定して yield し、全法人番号を seen に記録する
         (スナップショット後に名称変更・閉鎖された法人を全件側の古い状態で重複出力しないため)
    4. 全件の都道府県別 zip (全国一括 242MB は除外) を順に処理し、seen に無い法人のみ判定・yield

設計メモ:
    - 全件 CSV の処理区分は「その法人の最新の処理区分」。2026年に指定された後に
      名称/所在地変更 (11/12) や閉鎖 (21) が登録された法人も、その最新の処理区分のまま出力される。
    - ダウンロードは POST (CSRF トークン必須)。トークンはページごとに取得する。
    - 差分ファイルは 100KB 台と小さいので、最初の1件は数秒以内に yield される。
    - CSV 列定義はリソース定義書 (k-resource-dl.pdf) 準拠、30列・ヘッダ無し。
"""

import csv
import io
import re
import sys
import time
import zipfile
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# --- 備考のフィルター条件 ---
MIN_ASSIGN_DATE = "2026-01-01"   # 法人番号指定年月日の下限 (YYYY-MM-DD 文字列比較)

_TYPE_MAP = {
    "101": "国の機関",
    "201": "地方公共団体",
    "301": "株式会社",
    "302": "有限会社",
    "303": "合名会社",
    "304": "合資会社",
    "305": "合同会社",
    "399": "その他の設立登記法人",
    "401": "外国会社等",
    "499": "その他",
}

_CLOSE_CAUSE_MAP = {
    "01": "清算の結了等",
    "11": "合併による解散等",
    "21": "登記官による閉鎖",
    "31": "その他の清算の結了等",
}

_TOKEN_RE = re.compile(r'<input[^>]*name="([^"]*CNSFWTokenProcessor[^"]*)"[^>]*value="([^"]*)"')
_DL_RE = re.compile(r"doDownload\((\d+)\)")
_REIWA_RE = re.compile(r"令和\s*(\d+|元)\s*年\s*(\d+)\s*月\s*(\d+)\s*日")
_ZIP7_RE = re.compile(r"^\d{7}$")


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("　", " ")).strip()


def _reiwa_to_date(m: re.Match) -> date:
    y = 1 if m.group(1) == "元" else int(m.group(1))
    return date(2018 + y, int(m.group(2)), int(m.group(3)))


def _sjis_section(html: str) -> str:
    """CSV形式・Shift_JIS セクション (id=csv-sjis 〜 id=csv-unicode) を切り出す。"""
    s = html.find('id="csv-sjis"')
    e = html.find('id="csv-unicode"')
    if s < 0:
        return ""
    return html[s:e if e > s else len(html)]


class HoujinBangou2Scraper(StaticCrawler):
    """国税庁 法人番号公表サイト 新規指定法人 (2026-01-01以降指定・処理区分は出力のみで絞り込まない)"""

    DELAY = 0.0      # CSV 行単位では待たない (ファイル間は FILE_DELAY)
    FILE_DELAY = 1.0
    TIMEOUT = 120    # 東京 分割1 は約41MB

    EXTRA_COLUMNS = [
        "市区町村",
        "法人種別",
        "処理区分",
        "法人番号指定年月日",
        "最終更新年月日",
        "閉鎖年月日",
        "閉鎖事由",
    ]

    def parse(self, url: str):
        zenken_url = urljoin(url, "zenken/")
        sabun_url = urljoin(url, "sabun/")
        seen: set[str] = set()

        # ---- 全件ページ: スナップショット日とファイル番号 ----
        zenken_html = self._get_html(zenken_url)
        if not zenken_html:
            self.logger.error("全件ダウンロードページの取得に失敗: %s", zenken_url)
            return
        sec = _sjis_section(zenken_html)
        m = _REIWA_RE.search(sec)
        snapshot = _reiwa_to_date(m) if m else None
        self.logger.info("全件データ スナップショット日: %s", snapshot)
        zenken_files = self._zenken_file_numbers(sec)

        # ---- 差分 (スナップショット後の分のみ、新しい順) ----
        sabun_html = self._get_html(sabun_url)
        if sabun_html:
            sabun_files = self._sabun_file_numbers(_sjis_section(sabun_html), snapshot)
            self.logger.info("差分ファイル (全件以降): %d 件", len(sabun_files))
            for d, no in sabun_files:
                self.logger.info("差分 %s (file_no=%s)", d, no)
                yield from self._download_and_parse(sabun_url, sabun_html, no, url, seen)
                time.sleep(self.FILE_DELAY)
        else:
            self.logger.warning("差分ページ取得失敗のため全件データのみで処理: %s", sabun_url)

        # ---- 全件 (都道府県別) ----
        self.logger.info("全件ファイル数: %d", len(zenken_files))
        self.total_items = len(zenken_files)
        for idx, (label, no) in enumerate(zenken_files):
            self.logger.info("[%d/%d] 全件 %s (file_no=%s)", idx + 1, len(zenken_files), label, no)
            # CSRF トークンは都度ページを取り直す (セッション長時間化への備え)
            if idx and idx % 10 == 0:
                zenken_html = self._get_html(zenken_url) or zenken_html
            yield from self._download_and_parse(zenken_url, zenken_html, no, url, seen)
            time.sleep(self.FILE_DELAY)

    # ------------------------------------------------------------------
    def _get_html(self, page_url: str) -> str:
        """CSRF トークンを含む生 HTML を取得する。
        get_soup() はキャッシュ/再シリアライズで属性順やトークンが変わり得るため
        session.get の生テキストを使う。"""
        for attempt in range(3):
            try:
                resp = self.session.get(page_url, timeout=self.TIMEOUT)
                resp.raise_for_status()
                resp.encoding = "utf-8"
                return resp.text
            except Exception as e:  # noqa: BLE001
                self.logger.warning("ページ取得失敗 (%s, try=%d): %s", page_url, attempt + 1, e)
                time.sleep(min(2 ** attempt, 8))
        return ""

    def _zenken_file_numbers(self, section: str) -> list[tuple[str, str]]:
        """都道府県別ファイル (ラベル, 番号)。全国一括ファイルは除外。"""
        out = []
        for m in _DL_RE.finditer(section):
            head = section[max(0, m.start() - 1500):m.start()]
            head = head[:head.rfind("<a")] if "<a" in head else head  # 当該 <a> タグ自体を除く
            ctx = _clean(re.sub(r"<[^>]*>?", " ", head))
            if ctx.endswith("全国"):
                self.logger.info("全国一括ファイル (file_no=%s) は除外", m.group(1))
                continue
            label = ctx.split(" ")[-1] if ctx else m.group(1)
            out.append((label, m.group(1)))
        return out

    def _sabun_file_numbers(self, section: str, snapshot: date | None) -> list[tuple[date, str]]:
        """差分 (日付, 番号) をスナップショット日より後のものだけ新しい順で返す。"""
        out = []
        for row in re.finditer(r"<tr[^>]*>(.*?)</tr>", section, re.S):
            body = row.group(1)
            dm = _REIWA_RE.search(re.sub(r"<[^>]+>", " ", body))
            nm = _DL_RE.search(body)
            if not (dm and nm):
                continue
            d = _reiwa_to_date(dm)
            if snapshot and d <= snapshot:
                continue
            out.append((d, nm.group(1)))
        out.sort(key=lambda x: x[0], reverse=True)
        return out

    def _download_and_parse(self, page_url, page_html, file_no, source_url, seen):
        tok = _TOKEN_RE.search(page_html)
        if not tok:
            self.logger.error("CSRF トークンが見つかりません: %s", page_url)
            return
        post_url = urljoin(page_url, "index.html")
        resp = None
        for attempt in range(3):
            try:
                resp = self.session.post(
                    post_url,
                    data={tok.group(1): tok.group(2), "event": "download", "selDlFileNo": file_no},
                    timeout=self.TIMEOUT,
                )
                resp.raise_for_status()
                if resp.content[:4] == b"PK\x03\x04":
                    break
                self.logger.warning("ZIP 以外の応答 (file_no=%s): %r", file_no, resp.content[:80])
            except Exception as e:  # noqa: BLE001 - ネットワーク系は再試行
                self.logger.warning("ダウンロード失敗 (file_no=%s, try=%d): %s", file_no, attempt + 1, e)
            resp = None
            time.sleep(min(2 ** attempt, 8))
        if resp is None:
            self.logger.error("ダウンロード断念 (file_no=%s)", file_no)
            return

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".csv"):
                    continue
                count = 0
                with zf.open(name) as fp:
                    reader = csv.reader(io.TextIOWrapper(fp, encoding="cp932", newline=""))
                    for row in reader:
                        if len(row) < 29:
                            continue
                        co_num = row[1].strip()
                        if not co_num or co_num in seen:
                            continue
                        seen.add(co_num)
                        if row[5].strip() < MIN_ASSIGN_DATE:
                            continue
                        item = self._build_item(row, source_url)
                        if item:
                            count += 1
                            yield item
                self.logger.info("  %s → %d 件", name, count)

    def _build_item(self, row: list, source_url: str) -> dict | None:
        """CSV 列: [1]法人番号 [2]処理区分 [4]更新年月日 [5]法人番号指定年月日 [6]商号又は名称
        [8]法人種別 [9]都道府県 [10]市区町村 [11]丁目番地等 [15]郵便番号 [16]国外所在地
        [18]登記記録の閉鎖等年月日 [19]閉鎖等の事由 [28]フリガナ"""
        name = _clean(row[6])
        if not name:
            return None
        co_num = row[1].strip()
        pref, city, street = _clean(row[9]), _clean(row[10]), _clean(row[11])
        addr = "".join(p for p in (pref, city, street) if p) or _clean(row[16])
        zip_raw = row[15].strip()
        zip_fmt = f"{zip_raw[:3]}-{zip_raw[3:]}" if _ZIP7_RE.match(zip_raw) else zip_raw
        type_code = row[8].strip()
        close_code = row[19].strip()
        return {
            Schema.CO_NUM: co_num,
            Schema.NAME: name,
            Schema.NAME_KANA: _clean(row[28]),
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.POST_CODE: zip_fmt,
            Schema.URL: urljoin(source_url, f"/henkorireki-johoto.html?selHoujinNo={co_num}"),
            "市区町村": city,
            "法人種別": _TYPE_MAP.get(type_code, type_code),
            "処理区分": row[2].strip(),
            "法人番号指定年月日": row[5].strip(),
            "最終更新年月日": row[4].strip(),
            "閉鎖年月日": row[18].strip(),
            "閉鎖事由": _CLOSE_CAUSE_MAP.get(close_code, close_code),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    scraper = HoujinBangou2Scraper()
    # 🔒 sites.yml の url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.houjin-bangou.nta.go.jp/download/")
    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
