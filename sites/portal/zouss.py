"""
ZOUSS（ザウス） — 北陸3県(石川・富山・福井)のタウン情報誌 Web 版 店舗/施設紹介

対象サイト: https://zouss.jp/

取得フロー:
    WordPress REST API で全記事を列挙し、記事本文(content.rendered)の自由記述から
    住所・TEL・営業時間([営])・定休日([休]) を正規表現で抽出する。
      1. /wp-json/wp/v2/posts?per_page=50&page=N  (約211件: ニューオープン/イベント情報 等)
      2. /wp-json/wp/v2/recommend?per_page=50&page=N (約5件: ザウスのイチオシ！)
         → 本文末尾「お店情報」欄の「店名：/ジャンル：/住所：/営業時間：/定休日：/駐車場：」
    API ページ単位で取得し、記事ごとに即 yield する(詳細ページへの追加アクセスは無い)。

フィルター (備考準拠):
    - スタッフコーデ / 街角スナップ / ポートレート / 厳選アイテム カテゴリは除外
    - 本文に TEL も住所も無い記事(スナップ・お知らせ等)は除外
    - イチオシ(recommend)は TEL 非掲載だが住所があるため対象

備考:
    - イベント情報記事の TEL は主催者/会場の番号。住所が無い記事は会場名を「会場」に入れる。
    - 紹介文(Schema.DESCRIPTION)は依頼備考で明示的に取得指示があるため本文冒頭の段落を出力する。
    - 都道府県は住所先頭 or 市町村名(北陸3県)から補完する。

実行方法:
    python scripts/sites/portal/zouss.py
"""

import html
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 除外カテゴリ (店舗情報を持たない記事)
_EXCLUDE_CATS = {"スタッフコーデ", "街角スナップ", "ポートレート", "厳選アイテム"}

_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")
_TEL_RE = re.compile(
    r"(?:TEL|ＴＥＬ|Tel|tel|☎)\s*[\.．:：]?\s*"
    r"(0\d{1,4}\s*[-－‐・ー]\s*\d{1,4}\s*[-－‐・ー]\s*\d{3,4}|0\d{9,10})"
)
_HOURS_RE = re.compile(r"\[営\]\s*(.+?)(?=\s*\[(?:休|P|\s*P|HP)\]|\s*TEL|$)")
_HOLIDAY_RE = re.compile(r"\[休\]\s*(.+?)(?=\s*\[(?:営|P|\s*P|HP)\]|\s*TEL|$)")
# 住所: (県)+ 市/郡 + 番地数字
_ADDR_RE = re.compile(
    r"^((?:石川県|富山県|福井県|.{2,3}県)?[^\s@＠「」]{1,8}?[市郡].*?[0-9０-９][^\[]*?)"
    r"(?=\s*(?:TEL|ＴＥＬ|\[営\]|$))"
)
_LABEL_RE = re.compile(r"^\s*(店名|ジャンル|住所|営業時間|定休日|駐車場)\s*[：:]\s*(.*)$")

# 北陸3県の市町(住所が市町村始まりのときの都道府県補完用)
_CITY_PREF = {
    "石川県": "金沢 七尾 小松 輪島 珠洲 加賀 羽咋 かほく 白山 能美 野々市 川北 津幡 内灘 志賀 宝達志水 中能登 穴水 能登 河北郡 羽咋郡 鹿島郡 鳳珠郡 能美郡",
    "富山県": "富山 高岡 魚津 氷見 滑川 黒部 砺波 小矢部 南砺 射水 舟橋 上市 立山 入善 朝日 中新川郡 下新川郡",
    "福井県": "福井 敦賀 小浜 大野 勝山 鯖江 あわら 越前 坂井 永平寺 池田 南越前 美浜 高浜 おおい 若狭 吉田郡 今立郡 南条郡 丹生郡 三方郡 大飯郡 三方上中郡",
}


class ZoussScraper(StaticCrawler):
    """ZOUSS（ザウス） スクレイパー"""

    DELAY = 3.0       # API 呼び出し間隔(秒)
    ITEM_DELAY = 0.0  # 記事データは API レスポンスに含まれ追加通信が無いため item 間待機は不要
    PER_PAGE = 50     # API ページサイズ (read timeout 回避のため小さめ)
    EXTRA_COLUMNS = ["会場", "駐車場", "掲載日"]

    def parse(self, url: str):
        # 引数 url を唯一のルート(SSOT)として API URL を派生させる
        api_base = urljoin(url, "/wp-json/wp/v2/")
        cat_map = self._term_map(api_base + "categories")
        genre_map = self._term_map(api_base + "genre")

        for post_type in ("posts", "recommend"):
            for rec in self._iter_records(api_base + post_type):
                if post_type == "posts":
                    item = self._parse_post(rec, cat_map)
                else:
                    item = self._parse_recommend(rec, genre_map)
                if item:
                    yield item

    # ------------------------------------------------------------------
    # 列挙
    # ------------------------------------------------------------------
    def _iter_records(self, endpoint: str):
        page = 1
        while True:
            resp = self._get_api(f"{endpoint}?per_page={self.PER_PAGE}&page={page}")
            if resp is None:
                return
            records = resp.json()
            if not records:
                return
            yield from records
            total_pages = resp.headers.get("X-WP-TotalPages")
            if total_pages and total_pages.isdigit() and page >= int(total_pages):
                return
            if len(records) < self.PER_PAGE:
                return
            page += 1

    # ------------------------------------------------------------------
    # 通常記事 (ニューオープン / イベント情報 等)
    # ------------------------------------------------------------------
    def _parse_post(self, rec: dict, cat_map: dict):
        cats = [cat_map.get(c, "") for c in rec.get("categories", [])]
        cats = [c for c in cats if c and c != "未分類"]
        if _EXCLUDE_CATS.intersection(cats):
            return None

        lines = self._content_lines(rec["content"]["rendered"])
        tel = addr = hours = holiday = venue = parking = ""
        for i, line in enumerate(lines):
            if not tel:
                m = _TEL_RE.search(line)
                if m:
                    tel = self._norm_tel(m.group(1))
                    # 住所は TEL と同じ行の前方 or 直前行
                    addr = self._match_addr(line[: m.start()])
                    if not addr and i >= 1:
                        addr = self._match_addr(lines[i - 1])
                    # 「住所 / ビル・店内表記 / TEL」の 3 行構成
                    if not addr and i >= 2 and len(lines[i - 1]) <= 30:
                        a = self._match_addr(lines[i - 2])
                        if a:
                            addr = f"{a} {lines[i - 1]}"
            if not addr:
                a = self._match_addr(line)
                if a and ("TEL" in line or "[営]" in line
                          or (i + 1 < len(lines) and re.search(r"TEL|\[営\]", lines[i + 1]))):
                    addr = a
            if not hours:
                m = _HOURS_RE.search(line)
                if m:
                    hours = m.group(1).strip()
            if not holiday:
                m = _HOLIDAY_RE.search(line)
                if m:
                    holiday = m.group(1).strip()
            if not parking:
                m = re.search(r"\[\s*P\s*\]\s*(.+?)(?=\s*\[|$)", line)
                if m:
                    parking = m.group(1).strip()
            if not venue and re.match(r"^[@＠]", line):
                venue = line.lstrip("@＠").strip()

        if not tel and not addr:
            return None

        return {
            Schema.NAME: self._text(rec["title"]["rendered"]),
            Schema.PREF: self._pref(addr),
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.TIME: hours,
            Schema.HOLIDAY: holiday,
            Schema.CAT_SITE: "/".join(cats),
            Schema.DESCRIPTION: self._description(lines),
            Schema.URL: rec.get("link", ""),
            "会場": "" if addr else venue,
            "駐車場": parking,
            "掲載日": (rec.get("date") or "")[:10],
        }

    # ------------------------------------------------------------------
    # ザウスのイチオシ！ (recommend)
    # ------------------------------------------------------------------
    def _parse_recommend(self, rec: dict, genre_map: dict):
        lines = self._content_lines(rec["content"]["rendered"])
        info, key = {}, None
        for line in lines:
            m = _LABEL_RE.match(line)
            if m:
                key = m.group(1)
                info[key] = m.group(2).strip()
            elif key and line.startswith("※"):
                key = None
            elif key in ("営業時間", "定休日") and line:
                info[key] += " " + line  # 複数行の続き
        if not info.get("住所"):
            return None

        # 「駐車場：有 空間・設備：…」の後続ラベルは切り落とす
        parking = re.split(r"\s+空間・設備", info.get("駐車場", ""))[0]
        holiday = re.split(r"\s*※", info.get("定休日", ""))[0]
        genres = info.get("ジャンル") or "/".join(
            genre_map.get(g, "") for g in rec.get("genre", []) if genre_map.get(g)
        )
        name = info.get("店名") or self._text(rec["title"]["rendered"])
        addr = info["住所"]
        intro = lines[0] if lines else ""
        return {
            Schema.NAME: re.sub(r"\s+", " ", name).strip(),
            Schema.PREF: self._pref(addr),
            Schema.ADDR: addr,
            Schema.TEL: "",
            Schema.TIME: re.sub(r"\s+", " ", info.get("営業時間", "")).strip(),
            Schema.HOLIDAY: re.sub(r"\s+", " ", holiday).strip(),
            Schema.CAT_SITE: "ザウスのイチオシ！/" + genres if genres else "ザウスのイチオシ！",
            Schema.DESCRIPTION: self._description(lines) or intro,
            Schema.URL: rec.get("link", ""),
            "会場": "",
            "駐車場": parking.strip(),
            "掲載日": (rec.get("date") or "")[:10],
        }

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------
    def _get_api(self, api_url: str):
        """session.get で API を取得 (ソフトタイムアウトのラップ対象)"""
        time.sleep(self.DELAY)
        try:
            resp = self.session.get(api_url, timeout=self.TIMEOUT)
            if resp.status_code == 400:  # 範囲外ページ (rest_post_invalid_page_number)
                return None
            resp.raise_for_status()
            return resp
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.logger.warning("API取得失敗 (スキップ): %s — %s", api_url, e)
                return None
            raise

    def _term_map(self, api_url: str) -> dict:
        resp = self._get_api(f"{api_url}?per_page=100&_fields=id,name")
        if resp is None:
            return {}
        return {t["id"]: self._text(t["name"]) for t in resp.json()}

    @staticmethod
    def _content_lines(rendered: str) -> list[str]:
        soup = bs4.BeautifulSoup(rendered or "", "html.parser")
        for br in soup.find_all("br"):
            br.replace_with("\n")
        text = soup.get_text("\n")
        lines = [re.sub(r"[ \t　]+", " ", l).strip() for l in text.split("\n")]
        return [l for l in lines if l]

    @staticmethod
    def _match_addr(s: str) -> str:
        s = (s or "").strip()
        if not s or len(s) > 60:
            return ""
        m = _ADDR_RE.match(s)
        return m.group(1).strip() if m else ""

    @staticmethod
    def _description(lines: list[str]) -> str:
        """本文中で最初の段落らしい行(句読点/！を含む 30 字以上)を紹介文とする"""
        for line in lines:
            if len(line) >= 30 and re.search(r"[。！!？?、]", line) and not _TEL_RE.search(line):
                return line
        return ""

    @staticmethod
    def _norm_tel(s: str) -> str:
        s = re.sub(r"\s+", "", s)
        return re.sub(r"[－‐・ー]", "-", s)

    @staticmethod
    def _pref(addr: str) -> str:
        m = _PREF_RE.match(addr or "")
        if m:
            return m.group(1)
        for pref, cities in _CITY_PREF.items():
            for c in cities.split():
                if (addr or "").startswith(c):
                    return pref
        return ""

    @staticmethod
    def _text(s: str) -> str:
        return html.unescape(re.sub(r"<[^>]+>", "", s or "")).strip()


if __name__ == "__main__":
    scraper = ZoussScraper()
    scraper.execute("https://zouss.jp/")
