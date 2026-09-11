"""
求人ジャーナルネット (job-j.net) スクレイパー

取得対象:
    - 新潟県・富山県・石川県・福井県 の 4 県に掲載されている全求人 (全雇用形態・
      全職種)。県別一覧 `/{pref}/search/?page=N` を全ページ辿り、各求人の詳細
      ページ `/{pref}/job/J{id}/` を開いて情報を取得する。

取得カラム:
    名称 (会社名。屋号＋法人名の併記はそのまま) / 住所 (勤務地) / TEL (詳細ページの
    連絡先) / 職種 / 雇用形態 / 給与 / 掲載開始日 / 掲載終了日 / 取得URL

Cloudflare 403 について:
    egress IP によっては全パスで Cloudflare が 403 (error code 1005 = ASN ブロック)
    を返す。ブラウザ相当のヘッダと十分な DELAY を入れたうえで、なお 403 の場合は
    ステータスと返却本文を WARNING/ERROR でログに残し、Wayback Machine の生スナップ
    ショット (…id_/…) にフォールバックして取得を継続する (0 件を「成功」にしない)。
    Wayback 経路では一覧スナップショットがほぼ存在しないため、CDX API の prefix 検索
    (`/{pref}/job/` 配下) で詳細 URL を列挙する。

実行方法:
    python scripts/sites/jobs/kyujin_journal_net.py
"""

import datetime
import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import quote, urljoin, urlsplit

import bs4
import requests

# scripts/sites/jobs/<file>.py → root は .parent x4
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 対象 4 県 (URL スラッグ → 都道府県名)
_PREFS = {
    "niigata": "新潟県",
    "toyama": "富山県",
    "ishikawa": "石川県",
    "fukui": "福井県",
}

# 一覧の安全上限 (1 頁 40 件。新潟で約 140 頁)
_MAX_PAGES = 400

# ブラウザ相当のリクエストヘッダ (Cloudflare 対策)
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

_CDX_ENDPOINT = "http://web.archive.org/cdx/search/cdx"

# 詳細 URL (…/{pref}/job/J12345/) 判定
_DETAIL_RE = re.compile(r"^/([a-z]+)/job/(J\d+)/?$")

# 電話番号
_TEL_RE = re.compile(r"0\d{1,4}[-(]\d{1,4}[-)]\d{3,4}|0\d{9,10}")

# 掲載期間 "2025年01月30日～2025年03月01日"
_PERIOD_RE = re.compile(
    r"(\d{4})年(\d{1,2})月(\d{1,2})日\s*[~〜～-]\s*(\d{4})年(\d{1,2})月(\d{1,2})日"
)
_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")

_PREF_HEAD_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}?県)")


def _clean(value) -> str:
    """空白・改行・全角スペースを 1 つの半角スペースに畳む。"""
    if value is None:
        return ""
    if not isinstance(value, str):
        value = value.get_text(" ") if hasattr(value, "get_text") else str(value)
    return re.sub(r"\s+", " ", value.replace("　", " ")).strip()


def _lines(el) -> list[str]:
    """<br> 区切りのテキストを行リストにする。"""
    if el is None:
        return []
    text = el.get_text("\n")
    return [_clean(line) for line in text.split("\n") if _clean(line)]


def _to_iso(y: str, m: str, d: str) -> str:
    return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"


class KyujinJournalNetScraper(StaticCrawler):
    """求人ジャーナルネット (job-j.net) スクレイパー — 北陸・甲信越 4 県"""

    # Cloudflare 対策で十分に間隔を空ける
    DELAY = 2.0
    TIMEOUT = 30
    CONTINUE_ON_ERROR = True

    EXTRA_COLUMNS = [
        "職種",
        "職種カテゴリ",
        "雇用形態",
        "給与",
        "掲載開始日",
        "掲載終了日",
        "仕事内容",
        "最寄駅",
        "お仕事No.",
    ]

    # live が Cloudflare 403 でブロックされたか (prepare() で初期化)
    _live_blocked = False
    _block_status: int | None = None
    _block_body: str = ""

    def prepare(self):
        super().prepare()
        self._live_blocked = False
        self._block_status = None
        self._block_body = ""

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート(起点)とし、一覧/ページネーション/詳細 URL は
        # すべて url から派生させる。
        root = url.rstrip("/") + "/"
        seen: set[str] = set()

        for pref_slug, pref_name in _PREFS.items():
            if not self._live_blocked:
                yield from self._crawl_live_pref(root, pref_slug, pref_name, seen)

            if self._live_blocked:
                # live が 403 の場合のみ Wayback 経由で列挙する
                yield from self._crawl_wayback_pref(root, pref_slug, pref_name, seen)

    # ------------------------------------------------------------------ #
    # live 経路 (通常時): 県別一覧 → 詳細
    # ------------------------------------------------------------------ #
    def _crawl_live_pref(
        self, root: str, pref_slug: str, pref_name: str, seen: set[str]
    ) -> Generator[dict, None, None]:
        list_root = urljoin(root, f"{pref_slug}/search/")

        for page in range(1, _MAX_PAGES + 1):
            list_url = list_root if page == 1 else f"{list_root}?page={page}"
            html = self._fetch_live(list_url)
            if html is None:
                if self._live_blocked:
                    self.logger.warning(
                        "live 一覧を取得できないため %s は Wayback 経路に切り替えます",
                        pref_name,
                    )
                return

            soup = bs4.BeautifulSoup(html, "html.parser")

            if page == 1:
                total = self._extract_total(soup)
                if total:
                    self.total_items = (self.total_items or 0) + total
                    self.logger.info("%s: 検索結果 %s 件", pref_name, total)

            cards = soup.select("div.c-job__item")
            if not cards:
                return

            for card in cards:
                link = card.select_one("a.c-job__catchcopy-link[href]")
                if not link:
                    continue
                detail_url = urljoin(list_url, link["href"])
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                base = self._parse_card(card)
                item = self._scrape_detail(detail_url, pref_name, base)
                if item:
                    yield item

            # 「次のページへ」が無ければ終了
            if not soup.select_one(f'a[href*="/{pref_slug}/search/?page={page + 1}"]'):
                return

    def _parse_card(self, card) -> dict:
        """一覧カード由来の情報 (詳細が欠けた場合の保険)。"""
        data: dict = {}

        name_el = card.select_one(".c-heading-list__title-text")
        if name_el:
            name = re.sub(r"の求人情報\s*$", "", _clean(name_el.get_text()))
            if name:
                data[Schema.NAME] = name

        label = card.select_one(".c-label__type")
        if label:
            data["雇用形態"] = _clean(label.get_text())

        salary = card.select_one(".c-job__salary-detail")
        if salary:
            data["給与"] = _clean(salary.get_text())

        jobtype = card.select_one("p.c-job__type")
        rows = _lines(jobtype)
        if rows:
            data["職種カテゴリ"] = rows[0]
            data["職種"] = rows[-1]

        return data

    # ------------------------------------------------------------------ #
    # Wayback 経路 (live 403 時): CDX で詳細 URL を列挙
    # ------------------------------------------------------------------ #
    def _crawl_wayback_pref(
        self, root: str, pref_slug: str, pref_name: str, seen: set[str]
    ) -> Generator[dict, None, None]:
        host = urlsplit(root).netloc
        for detail_url, timestamp in self._cdx_enumerate(host, pref_slug):
            if detail_url in seen:
                continue
            seen.add(detail_url)
            item = self._scrape_detail(detail_url, pref_name, {}, snapshot=timestamp)
            if item:
                yield item

    def _cdx_enumerate(self, host: str, pref_slug: str) -> list[tuple[str, str]]:
        """Wayback CDX API で /{pref}/job/ 配下の詳細 URL を列挙する。"""
        # 旧デザイン (数年前) の空ページを拾わないよう直近 2 年に絞る
        since = (datetime.date.today() - datetime.timedelta(days=730)).strftime("%Y%m%d")
        params = (
            f"?url={quote(f'{host}/{pref_slug}/job/', safe='')}"
            "&matchType=prefix&collapse=urlkey&output=json"
            "&fl=original,timestamp&filter=statuscode:200"
            f"&from={since}"
        )
        api = _CDX_ENDPOINT + params

        rows: list = []
        for attempt in range(3):
            try:
                resp = self.session.get(api, headers=_BROWSER_HEADERS, timeout=60)
                if resp.status_code != 200:
                    self.logger.warning(
                        "CDX HTTP %s (%s回目): %s", resp.status_code, attempt + 1, api
                    )
                    time.sleep(5 * (attempt + 1))
                    continue
                rows = json.loads(resp.text or "[]")
                break
            except (requests.exceptions.RequestException, ValueError) as e:
                self.logger.warning("CDX 取得エラー (%s回目): %s", attempt + 1, e)
                time.sleep(5 * (attempt + 1))

        results: list[tuple[str, str]] = []
        for row in rows[1:]:  # 先頭行はヘッダ
            if len(row) < 2:
                continue
            original, timestamp = row[0], row[1]
            parts = urlsplit(original)
            if parts.query:  # ?ref=reco 等の重複 URL は除外
                continue
            if not _DETAIL_RE.match(parts.path):  # /company/ 等を除外
                continue
            # http://host:80/... を正規化して live の正規 URL に揃える
            live_url = f"https://{host}{parts.path}"
            if not live_url.endswith("/"):
                live_url += "/"
            results.append((live_url, timestamp))

        self.logger.info(
            "Wayback CDX: %s 件の詳細 URL を列挙 (%s)", len(results), pref_slug
        )
        self.total_items = (self.total_items or 0) + len(results)
        return results

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #
    def _scrape_detail(
        self,
        detail_url: str,
        pref_name: str,
        base: dict,
        snapshot: str | None = None,
    ) -> dict | None:
        soup = self._get_detail_soup(detail_url, snapshot)
        if soup is None:
            # live/Wayback 双方で取得できない場合、一覧由来情報だけでも残す
            if base.get(Schema.NAME):
                item = dict(base)
                item[Schema.URL] = detail_url
                item[Schema.PREF] = pref_name
                return item
            return None

        data: dict = dict(base)
        data[Schema.URL] = detail_url
        data[Schema.PREF] = pref_name

        # --- c-contentbox__item (ラベル / 内容) ---
        boxes: dict[str, bs4.Tag] = {}
        for box in soup.select(".c-contentbox__item"):
            title_el = box.select_one(".c-contentbox__item-title")
            content_el = box.select_one(".c-contentbox__item-content")
            if not title_el or not content_el:
                continue
            boxes.setdefault(_clean(title_el.get_text()), content_el)

        employment = _clean(boxes.get("雇用形態", "")) if "雇用形態" in boxes else ""
        if employment:
            data["雇用形態"] = employment

        # --- 名称: H1 "{会社名}/{雇用形態}の求人情報" ---
        name = self._extract_name(soup, employment)
        if name:
            data[Schema.NAME] = name

        # --- サマリ dl (仕事内容 / 給与 / 勤務地 / 最寄駅) ---
        summary = self._parse_summary(soup)
        if summary.get("給与"):
            data["給与"] = summary["給与"]
        if summary.get("仕事内容"):
            data["仕事内容"] = summary["仕事内容"]
        if summary.get("最寄駅"):
            data["最寄駅"] = summary["最寄駅"]

        # --- 住所: 勤務地 (1 行目) → 応募先住所 の順で採用 ---
        address = summary.get("勤務地") or _clean(boxes.get("応募先住所", ""))
        if address:
            data[Schema.ADDR] = address
            m = _PREF_HEAD_RE.match(address)
            if m:
                data[Schema.PREF] = m.group(1)

        # --- 職種 ---
        rows = _lines(soup.select_one(".p-detail-sumally__jobtype"))
        if rows:
            data["職種カテゴリ"] = rows[0]
            data["職種"] = rows[-1]

        # --- TEL (応募先・問い合わせ先) ---
        tel = self._extract_tel(boxes, soup)
        if tel:
            data[Schema.TEL] = tel

        # --- 掲載期間 ---
        period = _clean(boxes.get("掲載期間", ""))
        if period:
            m = _PERIOD_RE.search(period)
            if m:
                data["掲載開始日"] = _to_iso(*m.group(1, 2, 3))
                data["掲載終了日"] = _to_iso(*m.group(4, 5, 6))
            else:
                dates = _DATE_RE.findall(period)
                if dates:
                    data["掲載開始日"] = _to_iso(*dates[0])

        if not data.get("掲載終了日"):
            end = soup.select_one(".p-detail-sumally__deadline, .p-detail-sumally__limit")
            m = _DATE_RE.search(_clean(end)) if end else None
            if m:
                data["掲載終了日"] = _to_iso(*m.group(1, 2, 3))

        job_no = _clean(boxes.get("お仕事No.", ""))
        if job_no:
            data["お仕事No."] = job_no

        if not data.get(Schema.NAME):
            return None
        return data

    def _extract_name(self, soup, employment: str) -> str:
        """H1 から会社名 (屋号＋法人名の併記はそのまま) を取り出す。"""
        h1 = soup.select_one("h1")
        if not h1:
            return ""
        text = _clean(h1.get_text(" "))
        text = re.sub(r"の求人情報\s*$", "", text)
        if employment and text.endswith("/" + employment):
            text = text[: -(len(employment) + 1)]
        else:
            # 雇用形態が取れない場合も末尾の "/○○" を落とす
            text = re.sub(
                r"/(正社員|契約社員|パート・アルバイト|アルバイト|パート|派遣|"
                r"紹介予定派遣|業務委託|嘱託|臨時・短期)\s*$",
                "",
                text,
            )
        return _clean(text)

    def _parse_summary(self, soup) -> dict:
        """p-detail-sumally の dl (仕事内容 / 給与 / 勤務地 / 最寄駅) を解析する。"""
        result: dict[str, str] = {}
        for dl in soup.select("dl.p-detail-sumally__text-list"):
            for dt in dl.select("dt.p-detail-sumally__text-term"):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                label = _clean(dt.get_text())
                # 給与欄末尾の特徴ラベル (昇給あり 等) / 地図リンクは除外する
                dd_copy = bs4.BeautifulSoup(str(dd), "html.parser")
                for junk in dd_copy.select(
                    ".c-contentbox__item-content, .p-detail-sumally__text-link, "
                    ".p-detail-info__recruit-style"
                ):
                    junk.decompose()
                rows = _lines(dd_copy)
                if not rows:
                    continue
                if label == "勤務地":
                    result[label] = rows[0]  # 1 行目のみが住所
                else:
                    result[label] = " ".join(rows)
        return result

    def _extract_tel(self, boxes: dict, soup) -> str:
        """詳細ページの連絡先 (応募先・問い合わせ先) から電話番号を抽出する。"""
        for label in ("連絡先", "応募先電話番号", "電話番号", "問い合わせ先", "応募方法"):
            content = boxes.get(label)
            if content is None:
                continue
            m = _TEL_RE.search(_clean(content.get_text(" ")))
            if m:
                return m.group(0)
        link = soup.select_one('a[href^="tel:"]')
        if link:
            tel = _clean(link["href"].replace("tel:", ""))
            if tel:
                return tel
        return ""

    # ------------------------------------------------------------------ #
    # HTML 取得 (live → 403 なら Wayback 生スナップショット)
    # ------------------------------------------------------------------ #
    def _get_detail_soup(self, url: str, snapshot: str | None):
        if not self._live_blocked:
            html = self._fetch_live(url)
            if html:
                return bs4.BeautifulSoup(html, "html.parser")
        html = self._fetch_wayback(url, snapshot)
        if html is None:
            return None
        return bs4.BeautifulSoup(html, "html.parser")

    def _fetch_live(self, url: str) -> str | None:
        """live 取得。403 (Cloudflare) は本文付きでログし、以後 Wayback へ切替。"""
        self.logger.info("取得中: %s", url)
        try:
            resp = self.session.get(url, headers=_BROWSER_HEADERS, timeout=self.TIMEOUT)
        except requests.exceptions.RequestException as e:
            self.error_count += 1
            self.logger.warning("live 通信エラー: %s — %s", url, e)
            return None

        if resp.status_code == 200:
            if "charset=" not in resp.headers.get("Content-Type", "").lower():
                resp.encoding = resp.apparent_encoding
            return resp.text

        self.error_count += 1
        if "charset=" not in resp.headers.get("Content-Type", "").lower():
            resp.encoding = resp.apparent_encoding
        body = _clean(resp.text)[:300]
        self.logger.error(
            "live HTTP %s (Cloudflare ブロックの可能性): %s\n  返却本文(先頭300字): %s",
            resp.status_code,
            url,
            body,
        )
        if resp.status_code in (403, 503, 429) and not self._live_blocked:
            self._live_blocked = True
            self._block_status = resp.status_code
            self._block_body = body
            self.logger.error(
                "live アクセスが HTTP %s で拒否されました。"
                "以降は Wayback Machine の生スナップショットにフォールバックします。",
                resp.status_code,
            )
        return None

    def _fetch_wayback(self, url: str, snapshot: str | None) -> str | None:
        """Wayback Machine の生スナップショット (…id_/…) を取得する。"""
        stamp = snapshot or "2"  # タイムスタンプ不明時は最近傍を Wayback に任せる
        raw = f"https://web.archive.org/web/{stamp}id_/{url}"
        for attempt in range(3):
            try:
                resp = self.session.get(raw, headers=_BROWSER_HEADERS, timeout=self.TIMEOUT)
            except requests.exceptions.RequestException as e:
                self.logger.warning("Wayback 通信エラー (%s回目): %s — %s", attempt + 1, raw, e)
                time.sleep(3 * (attempt + 1))
                continue
            if resp.status_code == 200:
                if "charset=" not in resp.headers.get("Content-Type", "").lower():
                    resp.encoding = resp.apparent_encoding
                return resp.text
            if resp.status_code in (429, 503):
                self.logger.warning("Wayback HTTP %s (待機して再試行): %s", resp.status_code, raw)
                time.sleep(10 * (attempt + 1))
                continue
            self.logger.warning("Wayback HTTP %s: %s", resp.status_code, raw)
            return None
        return None

    # ------------------------------------------------------------------ #
    # ヘルパ
    # ------------------------------------------------------------------ #
    def _extract_total(self, soup) -> int | None:
        el = soup.select_one('[class*="result__num-text"]')
        if el:
            m = re.search(r"([\d,]+)\s*件", el.get_text())
            if m:
                return int(m.group(1).replace(",", ""))
        return None

    def finalize(self):
        if self._live_blocked:
            self.logger.error(
                "live (www.job-j.net) は HTTP %s で全面的にブロックされていました。"
                "取得データは Wayback Machine のスナップショット由来です。"
                "返却本文(先頭300字): %s",
                self._block_status,
                self._block_body,
            )
        super().finalize()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KyujinJournalNetScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.job-j.net")
