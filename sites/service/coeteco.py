"""
対象サイト: https://coeteco.jp/prefecture/hokkaido

コエテコ byGMO (GMOメディア株式会社) の子ども向けプログラミング・ロボット教室検索サイト。
北海道の教室一覧を巡回し、各教室詳細ページから教室情報を取得する。

構造メモ:
  - 全ページ Imperva (Incapsula) WAF 配下。requests では常に 403/JS チャレンジが返るため
    DynamicCrawler (Playwright) が必須。さらに同一 Cookie で連続アクセスすると 2 回目以降
    403 (edet=15) になるため、goto の直前に context.clear_cookies() が必要。
  - 一覧 (/prefecture/hokkaido) はブランド単位のブロック構成。1 ブランドあたり最大 5 教室まで
    しかインラインに出ないため、6 教室以上のブランドは「全ての教室を見る(N教室)」リンク
    (/brand/{brand}/schools?prefecture_id=1) を辿って残りを回収する。
  - 一覧・ブランド別一覧ともページ送りは a[rel=next] (例: /prefecture/hokkaido/2)。
  - 詳細 (/brand/{brand}/schools/{id}) は静的 SSR。JSON-LD (LocalBusiness) と
    table.school-info-list / table.c-school-about のラベル/値で全項目取得できる。
  - 詳細の JSON-LD aggregateRating は教室単位ではなくブランド全体の評価。誤解を避けるため
    Schema.SCORES ではなく「ブランド口コミ評価」列に格納する。
  - robots.txt: /brand/*/schools/*/courses/ や /search? は Disallow だが、本クローラーが辿る
    一覧・詳細パスはいずれも許可されている。
  - 利用規約 (https://coeteco.jp/kiyaku) にスクレイピング/クローリングの明示禁止条項は無い。
    第5条(13) がサーバーへの過負荷を禁じているため DELAY は長めに設定する。
  - 「教室から一言」「備考」は運営者による長文の自由記述のため、著作権リスクを避けて取得しない。
"""

import json
import re
import time
import urllib.parse
from typing import Generator

import bs4

from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler

# /brand/{brand}/schools/{id} だけを教室詳細とみなす (レビュー・コース等のサブパスを除外)
_DETAIL_RE = re.compile(r"^/brand/([^/?#]+)/schools/(\d+)$")
# 「全ての教室を見る」リンク (/brand/{brand}/schools?prefecture_id=N)
_BRAND_LIST_RE = re.compile(r"^/brand/[^/?#]+/schools$")

# 備考指定: 起点 URL の都道府県 (北海道) の教室のみを対象とする
_TARGET_PREF = "北海道"

# 電話番号らしき並び (半角・全角ハイフン/括弧の混在に耐える)
_TEL_RE = re.compile(r"0\d{1,4}[-(－（]?\d{1,4}[-)－）]?\d{3,4}")
# 「2026年10月01日」「2026年10月」形式の開設年月
_DATE_RE = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?")


class CoetecoScraper(DynamicCrawler):
    """コエテコ byGMO (北海道) スクレイパー (動的)"""

    DELAY = 3.0  # 通信前の待機時間（秒）。規約 第5条(13) の過負荷禁止に配慮して長めに取る
    # WAF チャレンジ時のリトライ上限。get_soup のキャッシュキーを変えるため wait_until を変えて再試行する
    _RETRY_WAITS = ("domcontentloaded", "load", "networkidle")

    EXTRA_COLUMNS = [
        "ブランド名",
        "運営本部",
        "ブランド口コミ評価",
        "ブランド口コミ件数",
        "対象学年",
        "対応コース",
        "教材",
        "授業形式",
        "最寄り駅",
        "緯度",
        "経度",
    ]

    # ------------------------------------------------------------------ 取得

    @staticmethod
    def _is_blocked(soup: bs4.BeautifulSoup) -> bool:
        """Incapsula のブロック/チャレンジ画面かどうかを判定する。"""
        if soup.select_one("iframe#main-iframe") is not None:
            return True
        return soup.select_one("script[src*='_Incapsula_Resource']") is not None and soup.title is None

    def _fetch(self, url: str) -> bs4.BeautifulSoup | None:
        """WAF のブロックを検知して上限付きでリトライしつつページを取得する。

        同一 Cookie を使い回すと 2 回目以降 403 になるため、毎回 Cookie を破棄してから開く。
        リトライ時は wait_until を変えることで、ブロック画面が fetch キャッシュに
        居座って再取得できなくなるのを避ける。
        """
        for attempt, wait_until in enumerate(self._RETRY_WAITS):
            time.sleep(self.DELAY)
            if self.context is not None:
                self.context.clear_cookies()
            soup = self.get_soup(url, wait_until=wait_until)
            if soup is not None and not self._is_blocked(soup):
                return soup
            self.logger.warning("WAF ブロックを検知 (試行 %d/%d): %s", attempt + 1, len(self._RETRY_WAITS), url)
            time.sleep(min(5 * 2 ** attempt, 30))
        self.logger.error("WAF ブロックを回避できませんでした: %s", url)
        return None

    # ------------------------------------------------------------------ 一覧

    @staticmethod
    def _next_url(soup: bs4.BeautifulSoup, base_url: str) -> str | None:
        """ページャの a[rel=next] から次ページ URL を求める。無ければ None。"""
        nxt = soup.select_one("a[rel=next][href]")
        if not nxt:
            return None
        return urllib.parse.urljoin(base_url, nxt["href"])

    @staticmethod
    def _detail_urls(soup: bs4.BeautifulSoup, base_url: str) -> list[str]:
        """一覧ページから教室詳細 URL を出現順で重複無く抽出する。"""
        found: list[str] = []
        for a in soup.select("a[href]"):
            parts = urllib.parse.urlparse(urllib.parse.urljoin(base_url, a["href"]))
            if not _DETAIL_RE.match(parts.path):
                continue
            # クエリ/フラグメント付きリンクも同一教室なので正規化して重複を排除する
            detail = urllib.parse.urlunparse((parts.scheme, parts.netloc, parts.path, "", "", ""))
            if detail not in found:
                found.append(detail)
        return found

    @staticmethod
    def _brand_list_urls(soup: bs4.BeautifulSoup, base_url: str) -> list[str]:
        """「全ての教室を見る」リンク (ブランド別の教室一覧) を抽出する。"""
        found: list[str] = []
        for a in soup.select("a[href]"):
            if "教室を見る" not in a.get_text():
                continue
            absolute = urllib.parse.urljoin(base_url, a["href"])
            if not _BRAND_LIST_RE.match(urllib.parse.urlparse(absolute).path):
                continue
            if absolute not in found:
                found.append(absolute)
        return found

    # ------------------------------------------------------------------ 詳細

    @staticmethod
    def _json_ld(soup: bs4.BeautifulSoup, type_name: str) -> dict:
        """指定 @type の JSON-LD を返す (無ければ空 dict)。"""
        for tag in soup.select('script[type="application/ld+json"]'):
            raw = tag.string or tag.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except ValueError:
                continue
            for node in data if isinstance(data, list) else [data]:
                if isinstance(node, dict) and node.get("@type") == type_name:
                    return node
        return {}

    @staticmethod
    def _info_rows(soup: bs4.BeautifulSoup) -> dict:
        """table.school-info-list / table.c-school-about のラベル→<td> を辞書化する。"""
        rows: dict[str, bs4.Tag] = {}
        for table in soup.select("table.school-info-list, table.c-school-about"):
            for tr in table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if th is None or td is None:
                    continue
                label = th.get_text(strip=True)
                # 先に見つかった school-info-list 側を優先する
                rows.setdefault(label, td)
        return rows

    @staticmethod
    def _text(td: bs4.Tag | None) -> str:
        if td is None:
            return ""
        value = td.get_text(" ", strip=True)
        # 「-」のみのプレースホルダは未登録扱い
        return "" if value in ("-", "ー", "－") else value

    @staticmethod
    def _normalize_tel(raw: str) -> str:
        """+81-50-1860-9647 → 050-1860-9647。受付時間等が続く文中からの抽出にも対応する。"""
        if not raw:
            return ""
        value = raw.strip()
        if value.startswith("+81"):
            value = "0" + value[3:].lstrip("-")
        m = _TEL_RE.search(value)
        return m.group(0) if m else ""

    @staticmethod
    def _normalize_date(raw: str) -> str:
        """「2026年10月01日」→ 2026-10-01。日が無い場合は 01 日として扱う。"""
        m = _DATE_RE.search(raw or "")
        if not m:
            return ""
        year, month, day = m.group(1), m.group(2), m.group(3) or "1"
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

    @staticmethod
    def _official_site(td: bs4.Tag | None) -> str:
        """公式サイト欄から素の URL を取り出す (計測用 utm_* パラメータは除去)。"""
        if td is None:
            return ""
        a = td.select_one("a[href^='http']")
        if a is None:
            return ""
        # アンカーテキストに素の URL が出ていればそれを使う
        label = a.get_text(strip=True)
        if label.startswith("http"):
            return label.split()[0]
        parts = urllib.parse.urlsplit(a["href"])
        query = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query) if not k.startswith("utm_")]
        return urllib.parse.urlunsplit(
            (parts.scheme, parts.netloc, parts.path, urllib.parse.urlencode(query), "")
        )

    def _parse_detail(self, detail_url: str) -> dict | None:
        """教室詳細ページ 1 件分を辞書にして返す。取得不可なら None。"""
        soup = self._fetch(detail_url)
        if soup is None:
            return None

        biz = self._json_ld(soup, "LocalBusiness")
        addr = biz.get("address") or {}
        geo = biz.get("geo") or {}
        rating = biz.get("aggregateRating") or {}
        rows = self._info_rows(soup)

        name = self._text(rows.get("教室名")) or str(biz.get("name") or "")
        if not name:
            self.logger.warning("教室名を取得できませんでした: %s", detail_url)
            return None

        pref = str(addr.get("addressRegion") or "")
        # 備考指定: 北海道の教室のみ。都道府県が判明していて北海道以外なら除外する
        if pref and pref != _TARGET_PREF:
            self.logger.info("対象外の都道府県のためスキップ (%s): %s", pref, detail_url)
            return None

        locality = str(addr.get("addressLocality") or "")
        street = str(addr.get("streetAddress") or "")

        tel = self._normalize_tel(str(biz.get("telephone") or "")) or self._normalize_tel(
            self._text(rows.get("お問い合わせ"))
        )

        # アクセス欄は自由記述が混ざるため、駅リンクのテキスト (最寄り駅名) だけを拾う
        access_td = rows.get("アクセス・交通手段")
        stations = (
            [a.get_text(strip=True) for a in access_td.select("a[href*='/station/']")]
            if access_td is not None
            else []
        )

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: str(addr.get("postalCode") or ""),
            Schema.ADDR: f"{locality}{street}",
            Schema.TEL: tel,
            Schema.HP: self._official_site(rows.get("公式サイト")),
            Schema.CAT_SITE: self._text(rows.get("カテゴリ")),
            Schema.TIME: self._text(rows.get("授業スケジュール")),
            Schema.OPEN_DATE: self._normalize_date(self._text(rows.get("開設年月"))),
            "ブランド名": self._text(rows.get("スクール名")),
            "運営本部": self._text(rows.get("運営本部")),
            # JSON-LD の aggregateRating は教室単位ではなくブランド全体の評価なので
            # Schema.SCORES / REV_SCR ではなく、ブランド評価と明示した列に入れる
            "ブランド口コミ評価": str(rating.get("ratingValue") or ""),
            "ブランド口コミ件数": str(rating.get("ratingCount") or ""),
            "対象学年": self._text(rows.get("対象学年")),
            "対応コース": self._text(rows.get("対応コース")),
            "教材": self._text(rows.get("教材")),
            "授業形式": self._text(rows.get("授業形式")),
            "最寄り駅": " / ".join(dict.fromkeys(s for s in stations if s)),
            "緯度": str(geo.get("latitude") or ""),
            "経度": str(geo.get("longitude") or ""),
        }

    # ------------------------------------------------------------------ 本体

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_details: set[str] = set()
        brand_lists: list[str] = []

        # 1. 都道府県一覧 (ページ送りは a[rel=next]) を巡回し、インライン掲載の教室を先に処理する
        page_url: str | None = url
        while page_url:
            soup = self._fetch(page_url)
            if soup is None:
                break

            for brand_url in self._brand_list_urls(soup, page_url):
                if brand_url not in brand_lists:
                    brand_lists.append(brand_url)

            for detail_url in self._detail_urls(soup, page_url):
                if detail_url in seen_details:
                    continue
                seen_details.add(detail_url)
                item = self._parse_detail(detail_url)
                if item:
                    yield item  # 1 件取得ごとに即 yield する

            page_url = self._next_url(soup, page_url)

        # 2. 6 教室以上のブランドは一覧に 5 件しか出ないため、ブランド別一覧で残りを回収する
        for brand_url in brand_lists:
            next_url: str | None = brand_url
            while next_url:
                soup = self._fetch(next_url)
                if soup is None:
                    break

                for detail_url in self._detail_urls(soup, next_url):
                    if detail_url in seen_details:
                        continue
                    seen_details.add(detail_url)
                    item = self._parse_detail(detail_url)
                    if item:
                        yield item

                next_url = self._next_url(soup, next_url)


if __name__ == "__main__":
    scraper = CoetecoScraper()
    scraper.execute("https://coeteco.jp/prefecture/hokkaido")
