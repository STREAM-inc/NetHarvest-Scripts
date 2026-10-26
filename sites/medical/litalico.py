"""
LITALICO発達ナビ — 放課後等デイサービス・児童発達支援 施設検索 (東北・新潟・富山・石川・福井)
対象サイト: https://h-navi.jp/support_facility/aomori

取得対象:
    - 対象10県 (aomori, iwate, miyagi, akita, yamagata, fukushima, niigata,
      toyama, ishikawa, fukui) の県別一覧 /support_facility/{pref}?page=N
      (20件/ページ) を全ページ巡回し、全施設の詳細ページ
      /support_facility/facilities/{id} を 1 件取得するごとに即 yield する。

取得フロー:
    引数 url (= /support_facility/aomori) を起点に、同じ階層の兄弟パスとして
    10 県の一覧 URL を urljoin で導出 (起点県を先頭に巡回)
      → 一覧 ?page=N の div.facility-card から施設ID / サービス種別 / タグ /
        キャッチコピーを取得 (範囲外ページは 200 + 0 件なので終端判定に使う)
      → 「OPEN / オープン / 開所」記載のあるカードをページ内で優先して詳細取得
      → 詳細の JSON-LD (ChildCare) で名称 / 住所 / TEL、
        「施設詳細情報」表で URL / 受入障害 / 受入年齢 等を取得

備考 (呼び出し指示より):
    - TEL は媒体 (LITALICO) の 050 転送番号 (参考用)。施設の直通番号ではない。
      未掲載施設もある (無料掲載枠等)。
    - キャッチコピー・紹介文・詳細本文は備考で明示的に取得指示があるため取得する
      (詳細本文は開所・オープン記載の抽出用)。
    - 429 対策: 備考「アクセス間隔は1〜2秒以上」に従い全リクエスト間を
      REQUEST_INTERVAL 秒以上空け、429 は Retry-After を尊重しつつ
      指数バックオフでリトライして継続する (フレームワークの DELAY は
      yield 間にしか効かないため、一覧取得分は自前で間隔を制御する)。
    - 利用規約 (https://h-navi.jp/rule) にスクレイピング/クローリングの明示禁止なし
      (禁止事項(13)「他のユーザーの情報を収集する行為」は会員ユーザー情報が対象)。

実行方法:
    # ローカルテスト
    python scripts/sites/medical/litalico.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id litalico
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 対象10県 (備考指定の順)
_TARGET_PREFS = [
    "aomori", "iwate", "miyagi", "akita", "yamagata",
    "fukushima", "niigata", "toyama", "ishikawa", "fukui",
]

_DETAIL_RE = re.compile(r"/support_facility/facilities/(\d+)$")
_OPEN_RE = re.compile(r"OPEN|ＯＰＥＮ|[Oo]pen|オープン|開所|開設|新規開[所設校]|開校")
_POST_RE = re.compile(r"〒?\s*(\d{3}-\d{4})")


class LitalicoScraper(StaticCrawler):
    """LITALICO発達ナビ 放課後等デイサービス・児童発達支援 スクレイパー"""

    DELAY = 3.0  # 詳細 1 件 yield ごとの待機
    REQUEST_INTERVAL = 2.0  # 全 HTTP リクエスト間の最小間隔 (備考: 1〜2秒以上)
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    EXTRA_COLUMNS = [
        "施設ID",
        "市区町村",
        "受入障害",
        "受入年齢",
        "施設タグ",
        "紹介文",
        "詳細本文",
        "OPEN記載",
    ]

    def _setup(self):
        """429 を自動リトライ対象に追加 (Retry-After 尊重・指数バックオフ)。"""
        super()._setup()
        retries = Retry(
            total=5,
            backoff_factor=10,  # 10, 20, 40, 80... 秒
            status_forcelist=[429, 500, 502, 503, 504],
            respect_retry_after_header=True,
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        })
        self._last_request_at = 0.0

    # ------------------------------------------------------------------ fetch

    def _fetch(self, url: str):
        """リクエスト間隔を REQUEST_INTERVAL 秒以上空けて get_soup する。"""
        wait = self.REQUEST_INTERVAL - (time.monotonic() - self._last_request_at)
        if wait > 0:
            time.sleep(wait)
        try:
            return self.get_soup(url)
        finally:
            self._last_request_at = time.monotonic()

    # ------------------------------------------------------------------ parse

    def parse(self, url: str) -> Generator[dict, None, None]:
        start_slug = url.rstrip("/").split("?")[0].rsplit("/", 1)[-1]
        slugs = [start_slug] + [p for p in _TARGET_PREFS if p != start_slug]
        list_base = url.split("?")[0].rstrip("/")

        seen: set[str] = set()
        for slug in slugs:
            pref_url = urljoin(list_base, slug)
            page = 1
            while True:
                list_url = pref_url if page == 1 else f"{pref_url}?page={page}"
                soup = self._fetch(list_url)
                if soup is None:
                    break
                cards = self._parse_cards(soup, list_url)
                if not cards:
                    break
                # OPEN 記載のあるカードを優先 (安定ソート)
                cards.sort(key=lambda c: 0 if c["is_open"] else 1)
                for card in cards:
                    if card["id"] in seen:
                        continue
                    seen.add(card["id"])
                    item = self._parse_detail(card)
                    if item:
                        yield item
                page += 1

    def _parse_cards(self, soup, list_url: str) -> list[dict]:
        cards = []
        for card in soup.select("div.facility-list div.facility-card"):
            a = card.select_one("a.facility-card__facility-link[href]")
            if not a:
                continue
            m = _DETAIL_RE.search(a["href"])
            if not m:
                continue
            name = _text(card.select_one(".facility-card__facility-name"))
            catch = _text(card.select_one(".facility-card__attracting-message-text"), "\n")
            cards.append({
                "id": m.group(1),
                "url": urljoin(list_url, a["href"]),
                "category": _text(card.select_one(".facility-card__category-container")),
                "tags": " / ".join(_text(t) for t in card.select(".facility-card__tag")),
                "name": name,
                "catch": catch,
                "is_open": bool(_OPEN_RE.search(f"{name} {catch}")),
            })
        return cards

    def _parse_detail(self, card: dict) -> dict | None:
        soup = self._fetch(card["url"])
        if soup is None:
            return None

        ld = {}
        for sc in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(sc.string or "")
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict) and data.get("@type") == "ChildCare":
                ld = data
                break
        addr = ld.get("address") or {}

        # 「施設詳細情報」表
        info: dict[str, str] = {}
        for head in soup.select(".facility-section__table-content--head"):
            val = head.find_next_sibling(class_="facility-section__table-content--info")
            if val:
                info[_text(head).replace(" ", "")] = _text(val, " ")

        sections = {}
        for sec in soup.select("div.facility-section, section.facility-section"):
            title = sec.select_one(".facility-section__title-text")
            if title:
                t = _text(title)
                title.extract()
                sections[t] = _text(sec, "\n")

        name = ld.get("name") or _text(soup.select_one("h1")) or card["name"]
        pref = addr.get("addressRegion", "")
        city = addr.get("addressLocality", "")
        street = addr.get("streetAddress", "")
        post = addr.get("postalCode", "")
        if not (pref and street):
            # JSON-LD 欠落時は表の住所 (〒xxx-xxxx 県市...) から補完
            raw = info.get("住所", "")
            pm = _POST_RE.search(raw)
            post = post or (pm.group(1) if pm else "")
            rest = _POST_RE.sub("", raw).strip()
            pm2 = re.match(r"(.+?[都道府県])(.*)", rest)
            if pm2:
                pref = pref or pm2.group(1)
                street = street or pm2.group(2)
                city = ""

        tel = ld.get("telephone") or info.get("電話番号", "")
        category = (
            _text(soup.select_one(".facility-head-show__facility_category"))
            or card["category"]
        )
        intro = ld.get("description") or sections.get("施設からひとこと", "")
        body = sections.get("支援へのこだわり", "")
        hp = info.get("URL", "")

        open_snips = []
        for src in (name, card["catch"], intro, body):
            for m in _OPEN_RE.finditer(src or ""):
                snip = src[max(0, m.start() - 20): m.end() + 20].replace("\n", " ").strip()
                if snip not in open_snips:
                    open_snips.append(snip)

        return {
            Schema.URL: card["url"],
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post,
            Schema.ADDR: f"{city}{street}",
            Schema.TEL: tel,
            Schema.CAT_SITE: category,
            Schema.HP: hp if hp.startswith("http") else "",
            Schema.DESCRIPTION: card["catch"],
            "施設ID": card["id"],
            "市区町村": city,
            "受入障害": info.get("受入障害", ""),
            "受入年齢": info.get("受入年齢", ""),
            "施設タグ": card["tags"],
            "紹介文": intro,
            "詳細本文": body,
            "OPEN記載": " | ".join(open_snips[:5]),
        }


def _text(el, sep: str = "") -> str:
    if el is None:
        return ""
    return re.sub(r"[ \t　]+", " ", el.get_text(sep, strip=True)).strip()


if __name__ == "__main__":
    scraper = LitalicoScraper()
    scraper.execute("https://h-navi.jp/support_facility/aomori")
    print(f"取得件数: {scraper.item_count}")
