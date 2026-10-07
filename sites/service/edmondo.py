"""
エドモンドプログラミングスクール (www.edmondo.jp) — 教室一覧クローラー

構造メモ (2026-10 実地調査):
  - 起点 https://www.edmondo.jp/search は中身が空の JS 描画ページ。
    `#html_top_search` を `search/js/index.js` が以下の 2 つの公開 API で埋める。
      * POST {API_BASE}/countClass           → 教室が存在する都道府県リンク一覧の HTML
      * POST {API_BASE}/prefecture (prefecture=<slug>) → 当該都道府県の教室一覧 HTML
    どちらも Cookie / CSRF / Referer 不要で requests から素通し (Static で完結)。
  - 都道府県 slug は URL 用のラテン表記 (tokyo, osaka, …)。
    **群馬だけ `gumma`** (ページ内 JS の prefectureMapping は `gunma` だが、
    countClass が返す実リンクは `/search/gumma`)。slug はリンクから拾うので取り違えない。
  - 教室が 1 件も無い都道府県は countClass のリンクに現れない
    (2026-10 時点で 37 都道府県 / 全国 116 教室)。ページネーションは無い。
  - prefecture API の返す HTML:
      <li class="area_item">
        <p class="area_lead">市区町村</p>
        <div class="area_container">
          <div class="area_content">
            <p class="area_heading">教室名</p>
            <p class="area_text area_text-station">最寄駅</p>
            <p class="area_text area_text-address">都道府県+市区町村以降+建物名</p>
          </div>
          <a class="area_link" href="…">体験申込をする</a>   ← 任意 (116 件中 114 件)
          <a class="area_link" href="…">教室詳細</a>         ← 任意 (116 件中 2 件)
          … (1 つの市区町村に複数教室がある場合は content → link の順で繰り返す)
        </div>
      </li>
    area_link は area_content の兄弟なので、**出現順で直前の area_content に紐付ける**。
    件数が揃わない li が実在する (リンクを持たない教室がある) ため zip では組めない。

取得方針 (依頼時の備考):
  画面に表示されている項目のみを取る。
  教室名 / 都道府県 / 住所 (建物名含む) / 最寄駅 / 教室ページURL。
  prefecture API のレスポンスには `classes` という内部 JSON (加盟店オーナー氏名・住所、
  契約日、月謝・入会金などの料金、Chatwork ルーム ID 等) も含まれるが、
  **画面非表示の情報なので一切参照しない**。パース対象は `html` フィールドのみ。

出力カラムの対応:
  教室名 → NAME / 都道府県 → PREF / 市区町村以降+建物名 → ADDR /
  教室ページURL → HP / 都道府県一覧ページ → URL(取得URL)。
  Schema に無い 市区町村・最寄駅・教室詳細URL・体験申込URL のみ EXTRA_COLUMNS。
  いずれも短い構造化値・URL のみで、自由記述の文章カラムは含めない。
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import Generator, Iterator
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
import requests

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# index.js が叩く公開 API のベース。起点ページの JS から動的に解決し、
# 取れなかったときだけこの既定値にフォールバックする。
DEFAULT_API_BASE = "https://user.edmondo.jp/api/search"

# index.js 内の `url: 'https://user.edmondo.jp/api/search/prefecture'` を拾う
API_URL_RE = re.compile(
    r"""url:\s*['"](https?://[^'"]+/api/search)/(?:prefecture|countClass)['"]"""
)

# countClass が返す都道府県リンク (/search/tokyo など)
PREF_HREF_RE = re.compile(r"/search/([a-z_]+)/?$")

# 住所欄の先頭に付く都道府県名
PREF_PREFIX_RE = re.compile(r"^(.+?[都道府県])")

# 「体験申込をする」「教室詳細」の判別
TRIAL_LABEL = "体験申込"
DETAIL_LABEL = "教室詳細"

# API 呼び出しのリトライ回数 (無限リトライ禁止)
MAX_ATTEMPTS = 3


class EdmondoScraper(StaticCrawler):
    """エドモンドプログラミングスクール 教室情報スクレイパー"""

    DELAY = 0.0
    ITEM_DELAY = 0.0
    TIMEOUT = 30

    # API 呼び出し間の待機秒数 (アクセス配慮)
    SLEEP = 0.5

    EXTRA_COLUMNS = [
        "市区町村",
        "最寄駅",
        "教室詳細URL",
        "体験申込URL",
    ]

    # =====================================================================
    # メイン
    # =====================================================================

    def parse(self, url: str) -> Generator[dict, None, None]:
        """起点 /search から都道府県 slug を列挙し、各都道府県の教室を 1 件ずつ yield。

        全都道府県分を収集してからまとめて返す形にはせず、都道府県 1 件ぶんの
        レスポンスを得たらその場で順次 yield する (早期 yield)。
        """
        root = url.rstrip("/")
        api_base = self._resolve_api_base(root)

        prefectures = self._fetch_prefecture_links(root, api_base)
        if not prefectures:
            self.logger.error("都道府県一覧を取得できませんでした: %s", root)
            return
        self.logger.info("教室がある都道府県: %d 件", len(prefectures))

        for slug, pref_name in prefectures:
            # 画面上の都道府県別一覧ページ (= この行の出典 URL)
            page_url = f"{root}/{slug}"
            html = self._fetch_prefecture_html(api_base, slug)
            if not html:
                continue

            count = 0
            for item in self._iter_classrooms(html, page_url, pref_name):
                count += 1
                yield item
            self.logger.info("%s (%s): %d 教室", pref_name, slug, count)

    # =====================================================================
    # 一覧 (都道府県の列挙)
    # =====================================================================

    def _resolve_api_base(self, root: str) -> str:
        """起点ページの index.js から API ベース URL を解決する。

        起点 URL からの相対参照のみを使い、失敗したら既定値を使う。
        """
        soup = self.get_soup(f"{root}/")
        if soup is not None:
            for script in soup.find_all("script", src=True):
                src = urljoin(f"{root}/", script["src"])
                # 同一ホストの index.js だけを見る (外部 CDN は読まない)
                if urlsplit(src).netloc != urlsplit(root).netloc:
                    continue
                if "index.js" not in src:
                    continue
                js_soup = self.get_soup(src)
                if js_soup is None:
                    continue
                m = API_URL_RE.search(js_soup.get_text())
                if m:
                    self.logger.info("API ベースを検出: %s", m.group(1))
                    return m.group(1)
        self.logger.info("API ベースを既定値で補完: %s", DEFAULT_API_BASE)
        return DEFAULT_API_BASE

    def _fetch_prefecture_links(
        self, root: str, api_base: str
    ) -> list[tuple[str, str]]:
        """countClass の HTML から (slug, 都道府県名) を重複排除して返す。"""
        payload = self._post_json(f"{api_base}/countClass", {})
        if not payload:
            return []
        soup = bs4.BeautifulSoup(payload.get("html", ""), "html.parser")

        found: dict[str, str] = {}
        for a in soup.find_all("a", href=True):
            href = urljoin(f"{root}/", a["href"])
            if urlsplit(href).netloc != urlsplit(root).netloc:
                continue
            m = PREF_HREF_RE.search(urlsplit(href).path)
            if not m:
                continue
            slug = m.group(1)
            name = a.get_text(strip=True)
            if slug and name and slug not in found:
                found[slug] = name
        return list(found.items())

    def _fetch_prefecture_html(self, api_base: str, slug: str) -> str:
        """prefecture API を叩き、画面に描画される HTML 片だけを返す。

        レスポンスの `classes` (加盟店オーナー・料金等の画面非表示データ) は使わない。
        """
        payload = self._post_json(f"{api_base}/prefecture", {"prefecture": slug})
        if not payload:
            return ""
        return payload.get("html", "") or ""

    # =====================================================================
    # 一覧 HTML → 教室 1 件
    # =====================================================================

    def _iter_classrooms(
        self, html: str, page_url: str, pref_name: str
    ) -> Iterator[dict]:
        """都道府県一覧 HTML から教室を 1 件ずつ組み立てる。"""
        soup = bs4.BeautifulSoup(html, "html.parser")

        for li in soup.select("li.area_item"):
            lead = li.select_one("p.area_lead")
            city = lead.get_text(strip=True) if lead else ""

            # area_content (教室本体) と area_link (任意のリンク) は兄弟同士。
            # 文書順に走査し、直前の area_content にリンクを紐付ける。
            current: dict | None = None
            for node in li.select("div.area_content, a.area_link"):
                if "area_content" in (node.get("class") or []):
                    if current:
                        yield self._build_item(current, page_url, pref_name, city)
                    current = self._parse_content(node)
                    continue

                if current is None:
                    continue
                label = node.get_text(strip=True)
                href = (node.get("href") or "").strip()
                if not href:
                    continue
                if DETAIL_LABEL in label:
                    current["detail_url"] = href
                elif TRIAL_LABEL in label:
                    current["trial_url"] = href

            if current:
                yield self._build_item(current, page_url, pref_name, city)

    @staticmethod
    def _parse_content(content: bs4.Tag) -> dict:
        """1 教室ぶんの area_content から画面表示項目を抜く。"""
        heading = content.select_one("p.area_heading")
        station = content.select_one("p.area_text-station")
        address = content.select_one("p.area_text-address")
        return {
            "name": heading.get_text(strip=True) if heading else "",
            "station": station.get_text(strip=True) if station else "",
            "address": address.get_text(strip=True) if address else "",
            "detail_url": "",
            "trial_url": "",
        }

    def _build_item(
        self, rec: dict, page_url: str, pref_name: str, city: str
    ) -> dict:
        """内部 dict を出力 1 行に変換する。"""
        address = rec["address"]
        # Schema.ADDR は「市区町村以降」。表示住所の先頭にある都道府県名を落とす。
        pref = pref_name or self._pref_of(address)
        addr = address
        if pref and addr.startswith(pref):
            addr = addr[len(pref):]
        elif not pref_name:
            m = PREF_PREFIX_RE.match(addr)
            if m:
                addr = addr[m.end():]

        detail_url = rec["detail_url"]
        trial_url = rec["trial_url"]
        return {
            Schema.URL: page_url,
            Schema.NAME: rec["name"],
            Schema.PREF: pref,
            Schema.ADDR: addr,
            # 画面上の「教室ページ」。教室詳細があればそれを、無ければ体験申込リンク。
            Schema.HP: detail_url or trial_url,
            "市区町村": city,
            "最寄駅": rec["station"],
            "教室詳細URL": detail_url,
            "体験申込URL": trial_url,
        }

    @staticmethod
    def _pref_of(address: str) -> str:
        m = PREF_PREFIX_RE.match(address)
        return m.group(1) if m else ""

    # =====================================================================
    # 通信
    # =====================================================================

    def _post_json(self, api_url: str, data: dict) -> dict | None:
        """API へ POST して JSON を返す。上限付きリトライ、失敗時は None。"""
        last_error: Exception | None = None
        for attempt in range(MAX_ATTEMPTS):
            if attempt:
                time.sleep(min(2 ** attempt, 8))
            try:
                self.logger.info("API 取得中: %s %s", api_url, data or "")
                response = self.session.post(
                    api_url,
                    data=data,
                    timeout=self.TIMEOUT,
                    headers={
                        "X-Requested-With": "XMLHttpRequest",
                        "Accept": "application/json",
                    },
                )
                response.raise_for_status()
                payload = response.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                last_error = e
                self.logger.warning(
                    "API 取得失敗 (%d/%d): %s — %s",
                    attempt + 1, MAX_ATTEMPTS, api_url, e,
                )
                continue

            if self.SLEEP > 0:
                time.sleep(self.SLEEP)
            return payload

        self.error_count += 1
        self.logger.error("API 取得を %d 回試行して失敗: %s — %s",
                          MAX_ATTEMPTS, api_url, last_error)
        if not self.CONTINUE_ON_ERROR:
            raise RuntimeError(f"API 取得に失敗しました: {api_url}") from last_error
        return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    EdmondoScraper().execute("https://www.edmondo.jp/search")
