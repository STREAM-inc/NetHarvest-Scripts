"""
対象サイト: https://doraever.jp/company-list
"""
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import quote, unquote, urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 企業詳細ページ (/company/1346895)
_COMPANY_RE = re.compile(r"/company/(\d+)$")
# 五十音索引ページ (/company-list/か)
_INDEX_RE = re.compile(r"/company-list/([^/?#]+)$")
# 索引ナビが取れなかった場合のフォールバック（サイト上の 10 行）
_KANA_ROWS = ["あ", "か", "さ", "た", "な", "は", "ま", "や", "ら", "わ"]
# 郵便番号 (〒2240034 / 〒224-0034)
_ZIP_RE = re.compile(r"〒\s*(\d{3})-?(\d{4})")
_PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")
# 創立 "2013年5月" / "1945年0月"
_FOUNDED_RE = re.compile(r"(\d{4})年\s*(?:(\d{1,2})月)?")
# 代表者の役職（"代表取締役　小林 博英" の先頭部分）
_POS_RE = re.compile(r"^(\S*(?:代表|社長|会長|理事長|取締役|CEO|所長|支店長)\S*)[\s　]+(.+)$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class EverScraper(StaticCrawler):
    """ドラEVER 企業一覧スクレイパー（doraever.jp / 企業単位）

    - 一覧 `/company-list` は五十音の行ごとの索引ページ `/company-list/{あ..わ}`
      （全 10 ページ・ページ送り無し・1 ページに行内の全社）。ルート自体は「あ行」と同内容。
      各カードは `article > h4 > a[href=/company/{id}]`(会社名) + `p`(支店/営業所名)。
    - 詳細 `/company/{id}` の会社概要は `div.mid_box`（`.mid_box_ttl`=="会社概要"）内の
      `h2.c_ttl`(ラベル) + 直後要素(値)。「募集中の求人」ボックスは除外する。
    - 同一会社でも支店/営業所ごとに別 ID のため、企業 ID 単位で dedup する（名称では dedup しない）。
    - 事業内容は自由記述を含むため取得しない（著作権リスク回避）。
    - 一過性の WAF 403（`<title>403 Forbidden</title>` の極小ページ）を検知して再取得する。
    - 利用規約（/terms）にスクレイピングの明示的禁止は無い（無断転載・営利目的提供のみ禁止）。
    """

    DELAY = 3.0
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "企業ID",
        "支店・営業所名",
        "会社所在地",
        "車両保有台数",
        "取得認証",
        "五十音行",
    ]

    # 連続リクエストの最小間隔（秒）。備考の「1秒以上」+ WAF 対策
    _REQ_INTERVAL = 1.5
    # 403 検知時の待機秒数（リトライ回数 = len）
    _RETRY_WAITS = [10.0, 20.0, 40.0]

    _last_request_at = 0.0

    def prepare(self):
        self._last_request_at = 0.0

    def parse(self, url: str) -> Generator[dict, None, None]:
        root = url.rstrip("/")
        seen: set[str] = set()

        root_soup = self._get_soup_ok(root)
        index_urls = self._index_urls(root, root_soup)

        for kana, index_url in index_urls:
            soup = self._get_soup_ok(index_url)
            if soup is None:
                self.logger.warning("索引ページ取得失敗: %s", index_url)
                continue
            for company_id, detail_url, list_name, branch in self._extract_cards(root, soup):
                if company_id in seen:
                    continue
                seen.add(company_id)
                try:
                    item = self._scrape_detail(detail_url, company_id, list_name, branch, kana)
                except Exception as e:  # 個別企業のエラーはログして継続
                    self.logger.warning("詳細取得エラー: %s — %s", detail_url, e)
                    continue
                if item and item.get(Schema.NAME):
                    yield item

    # ------------------------------------------------------------------
    # 一覧
    # ------------------------------------------------------------------
    def _index_urls(self, root: str, soup) -> list[tuple[str, str]]:
        """索引ナビ（あ行〜わ行）から各行ページの URL を得る。取れなければ既定の 10 行。"""
        kanas: list[str] = []
        if soup is not None:
            for a in soup.select("a[href]"):
                m = _INDEX_RE.search(unquote(a.get("href", "")).split("?")[0])
                if m and m.group(1) not in kanas:
                    kanas.append(m.group(1))
        if not kanas:
            kanas = list(_KANA_ROWS)
        return [(k, f"{root}/{quote(k)}") for k in kanas]

    def _extract_cards(self, root: str, soup) -> list[tuple[str, str, str, str]]:
        cards = []
        for a in soup.select("article h4 a[href]"):
            full = urljoin(root, a.get("href", "")).split("#")[0].split("?")[0]
            m = _COMPANY_RE.search(full)
            if not m:
                continue
            article = a.find_parent("article")
            p = article.select_one("p") if article else None
            cards.append((m.group(1), full, _clean(a.get_text()), _clean(p.get_text()) if p else ""))
        return cards

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _scrape_detail(self, detail_url, company_id, list_name, branch, kana) -> dict | None:
        soup = self._get_soup_ok(detail_url)
        if soup is None:
            return None

        fields: dict[str, object] = {}
        for box in soup.select("div.mid_box"):
            ttl = box.select_one(".mid_box_ttl")
            if not ttl or _clean(ttl.get_text()) != "会社概要":
                continue
            for h in box.select("h2.c_ttl"):
                val = h.find_next_sibling()
                if val is not None:
                    fields[_clean(h.get_text())] = val

        def text(label: str) -> str:
            el = fields.get(label)
            return _clean(el.get_text(" ")) if el is not None else ""

        name = text("会社名") or list_name
        if not name:
            h1 = soup.select_one("h1")
            name = _clean(h1.get_text()) if h1 else ""

        # 所在地: 〒 + 都道府県 + 以降
        addr_raw = text("会社所在地")
        post_code, pref, addr = "", "", addr_raw
        m = _ZIP_RE.search(addr_raw)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
            addr = _clean(addr_raw[m.end():])
        pm = _PREF_RE.match(addr)
        if pm:
            pref = pm.group(1)
            addr = _clean(addr[pm.end():])

        # 創立 → YYYY-MM（月が 0 や欠落なら YYYY）
        founded = text("創立")
        fm = _FOUNDED_RE.search(founded)
        if fm:
            month = int(fm.group(2) or 0)
            founded = f"{fm.group(1)}-{month:02d}" if 1 <= month <= 12 else fm.group(1)

        # 資本金／売上 は "5,000万円/440億1,100万円" の複合値
        cap, sales = "", ""
        cap_sales = text("資本金／売上") or text("資本金/売上")
        if cap_sales:
            parts = [x.strip() for x in re.split(r"[/／]", cap_sales, maxsplit=1)]
            cap = parts[0]
            sales = parts[1] if len(parts) > 1 else ""

        # 代表者: 単独なら役職と氏名に分割。複数（"/" 区切り）はそのまま
        rep = text("代表者名")
        pos = ""
        if rep and not re.search(r"[/／、,]", rep):
            rm = _POS_RE.match(rep)
            if rm:
                pos, rep = rm.group(1), _clean(rm.group(2))

        site = ""
        web = fields.get("Webサイト")
        if web is not None:
            a = web.select_one("a[href^=http]")
            site = a.get("href", "").strip() if a else text("Webサイト")

        certs = ""
        cert_el = fields.get("取得認証")
        if cert_el is not None:
            vals = [_clean(i.get("alt")) for i in cert_el.select("img") if _clean(i.get("alt"))]
            certs = " / ".join(dict.fromkeys(vals)) or text("取得認証")

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.OPEN_DATE: founded,
            Schema.CAP: cap,
            Schema.SALES: sales,
            Schema.EMP_NUM: text("従業員数"),
            Schema.REP_NM: rep,
            Schema.POS_NM: pos,
            Schema.WEBSITE: site,
            "企業ID": company_id,
            "支店・営業所名": branch,
            "会社所在地": addr_raw,
            "車両保有台数": text("車両保有台数"),
            "取得認証": certs,
            "五十音行": f"{kana}行",
        }

    # ------------------------------------------------------------------
    # 取得（レート制御 + 403 リトライ）
    # ------------------------------------------------------------------
    def _wait_turn(self):
        if not self._last_request_at:
            return
        remain = self._REQ_INTERVAL - (time.monotonic() - self._last_request_at)
        if remain > 0:
            time.sleep(remain)

    def _get_soup_ok(self, url: str):
        """403 Forbidden ページ／通信失敗を検知し、上限回数まで再取得する。"""
        for attempt in range(len(self._RETRY_WAITS) + 1):
            self._wait_turn()
            soup = self.get_soup(url)
            if not self._last_fetch_from_cache:
                self._last_request_at = time.monotonic()
            if soup is not None and not self._is_forbidden(soup):
                return soup
            if attempt < len(self._RETRY_WAITS):
                wait = self._RETRY_WAITS[attempt]
                self.logger.warning("403/取得失敗 — %.0f秒待って再取得: %s", wait, url)
                time.sleep(wait)
        return None

    @staticmethod
    def _is_forbidden(soup) -> bool:
        title = _clean(soup.title.get_text()) if soup.title else ""
        if "403 Forbidden" in title:
            return True
        h1 = soup.select_one("h1")
        return bool(h1 and "403 Forbidden" in _clean(h1.get_text()))


if __name__ == "__main__":
    scraper = EverScraper()
    scraper.execute("https://doraever.jp/company-list")
