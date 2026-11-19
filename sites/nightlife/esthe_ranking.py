"""
メンズエステランキング (esthe-ranking.jp) — 全国メンズエステ店舗詳細

取得対象:
    - 全国の店舗詳細ページ (/{area}/shop-detail/{UUID}/) を全件取得する。
      取得カラム: 名称 / 都道府県 / エリア / 詳細エリア / 最寄駅 / 住所(掲載あれば) /
      TEL(表示のまま) / HP(サイト掲載のオフィシャルHPリンク) / 営業時間 / 料金 /
      ジャンル(サイト定義業種) / 営業形態 / タグ / 店舗詳細URL。
    - TEL は表示のまま全件取得 (絞り込みは後工程)。HP 探索などの補完はしない。
    - 店舗紹介文などの自由記述プロースは著作権リスクのため取得しない。

取得フロー:
    {url}sitemap-index.xml → sitemap{N}.xml (エリア順) → shop-detail URL を列挙
    (/reserve/ 等のサブページは除外、UUID で重複排除) → 詳細を 1 件取得するごとに即 yield。

    ※ esthe-ranking.jp は nginx で国外/データセンタ IP を 403 拒否する (UA/ヘッダ/
      Playwright でも不可、IP 起因)。本番コンテナ (国内 IP) では live 取得を試み、
      live が 403/失敗した場合は Wayback Machine の生スナップショット (…id_/…) に
      フォールバックする。HTML 構造は live と Wayback で同一。
      Wayback に存在しない店舗はスキップする (取得元カラムに live/wayback を残す)。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/esthe_ranking.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id esthe_ranking
"""

import html
import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}

# 最新スナップショットへリダイレクトさせるための遠未来タイムスタンプ
_WAYBACK_PREFIX = "https://web.archive.org/web/99991231235959id_/"
_WAYBACK_MAX_ATTEMPTS = 3

# live が連続でこの回数 403/失敗したら以降は live を試さない (無駄な通信を避ける)
_LIVE_FAIL_LIMIT = 3

_DETAIL_RE = re.compile(r"/([^/]+)/shop-detail/([0-9a-fA-F-]{36})/?$")
_LOC_RE = re.compile(r"<loc>\s*([^<]+?)\s*</loc>")
_COORD_RE = re.compile(r"^-?\d{1,3}\.\d+\s*,\s*-?\d{1,3}\.\d+$")

_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
# "山形" → "山形県" / "東京" → "東京都" 等の短縮名対応
_PREF_SHORT = {re.sub(r"[都府県]$", "", p) if p != "北海道" else p: p for p in _PREFS}


def _norm_pref(text: str) -> str:
    text = (text or "").strip()
    if text in _PREFS:
        return text
    if text in _PREF_SHORT:
        return _PREF_SHORT[text]
    for short, full in _PREF_SHORT.items():
        if text.startswith(full) or text.startswith(short):
            return full
    return ""


class EstheRanking(StaticCrawler):
    """メンズエステランキング スクレイパー"""

    DELAY = 1.5
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "エリア",
        "詳細エリア",
        "最寄駅",
        "所在地(掲載値)",
        "料金",
        "営業形態",
        "タグ",
        "店舗ID",
        "店舗詳細URL",
        "取得元",
    ]

    # ------------------------------------------------------------------ #
    # 取得 (live → 失敗時 Wayback)
    # ------------------------------------------------------------------ #
    def _fetch(self, url: str) -> tuple[bytes | None, str]:
        """(本文, 取得元) を返す。取得不可なら (None, "")。"""
        if getattr(self, "_live_fail", 0) < _LIVE_FAIL_LIMIT:
            body = self._fetch_live(url)
            if body is not None:
                self._live_fail = 0
                return body, "live"
            self._live_fail = getattr(self, "_live_fail", 0) + 1
            if self._live_fail >= _LIVE_FAIL_LIMIT:
                logger.warning("live が連続 %d 回失敗 → 以降は Wayback のみ使用", self._live_fail)
        body = self._fetch_wayback(url)
        return (body, "wayback") if body is not None else (None, "")

    def _fetch_live(self, url: str) -> bytes | None:
        try:
            resp = self.session.get(url, headers=_BROWSER_HEADERS, timeout=self.TIMEOUT)
        except requests.exceptions.RequestException as e:
            logger.warning("live 取得エラー (Wayback へ): %s — %s", url, e)
            return None
        if resp.status_code != 200:
            logger.warning("live HTTP %s (Wayback へ): %s", resp.status_code, url)
            return None
        return resp.content

    def _fetch_wayback(self, url: str) -> bytes | None:
        raw = _WAYBACK_PREFIX + url
        for attempt in range(_WAYBACK_MAX_ATTEMPTS):
            try:
                resp = self.session.get(raw, headers=_BROWSER_HEADERS, timeout=self.TIMEOUT)
            except requests.exceptions.RequestException as e:
                logger.warning("Wayback 通信エラー (%d/%d): %s — %s",
                               attempt + 1, _WAYBACK_MAX_ATTEMPTS, url, e)
                time.sleep(min(10 * 2 ** attempt, 60))
                continue
            if resp.status_code == 200:
                return resp.content
            if resp.status_code == 404:
                logger.info("Wayback スナップショット無し: %s", url)
                return None
            logger.warning("Wayback HTTP %s (%d/%d): %s",
                           resp.status_code, attempt + 1, _WAYBACK_MAX_ATTEMPTS, url)
            time.sleep(min(10 * 2 ** attempt, 60))
        logger.warning("Wayback 取得断念: %s", url)
        return None

    def get_soup(self, url: str) -> bs4.BeautifulSoup | None:
        body, src = self._fetch(url)
        self._last_source = src
        if body is None:
            return None
        return bs4.BeautifulSoup(body, "html.parser")

    # ------------------------------------------------------------------ #
    # 列挙
    # ------------------------------------------------------------------ #
    def _locs(self, url: str) -> list[str]:
        """sitemap XML の <loc> 一覧 (XML は正規表現で抽出し HTML パーサ警告を避ける)。"""
        body, _ = self._fetch(url)
        if body is None:
            return []
        text = body.decode("utf-8", errors="ignore")
        return [html.unescape(m.strip()) for m in _LOC_RE.findall(text)]

    def parse(self, url: str):
        root = url if url.endswith("/") else url + "/"
        index_url = urljoin(root, "sitemap-index.xml")
        sitemaps = self._locs(index_url)
        if not sitemaps:
            raise RuntimeError(f"sitemap-index を取得できません: {index_url}")
        root_host = urlsplit(root).netloc

        seen: set[str] = set()
        self.total_items = 0
        for sm in sitemaps:
            sm_url = urljoin(root, urlsplit(sm).path)  # ルートは引数 url から派生
            details = []
            for loc in self._locs(sm_url):
                parts = urlsplit(loc)
                if parts.netloc and parts.netloc != root_host:
                    continue
                m = _DETAIL_RE.search(parts.path)
                if not m:
                    continue
                uuid = m.group(2).lower()
                if uuid in seen:
                    continue
                seen.add(uuid)
                details.append((urljoin(root, parts.path), m.group(1), uuid))
            self.total_items += len(details)
            logger.info("%s: 店舗 %d 件", sm_url, len(details))

            for detail_url, area_slug, uuid in details:
                soup = self.get_soup(detail_url)
                if soup is None:
                    continue
                item = self._parse_detail(soup, detail_url, uuid)
                if item:
                    yield item

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #
    def _parse_detail(self, soup: bs4.BeautifulSoup, url: str, uuid: str) -> dict | None:
        crumbs = []
        for sc in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(sc.string or "")
            except ValueError:
                continue
            if isinstance(data, dict) and data.get("@type") == "BreadcrumbList":
                crumbs = [
                    (e.get("item") or {}).get("name", "").strip()
                    for e in data.get("itemListElement", [])
                ]

        # サイドボックス (アイコン付き th / td)
        side: dict[str, bs4.Tag] = {}
        for tr in soup.select("div.sidebox table tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            icon = th.find("i")
            if icon:
                cls = " ".join(icon.get("class") or [])
                m = re.search(r"fa-(link|phone|train|clock-o|jpy|tag)", cls)
                key = m.group(1) if m else cls
            elif th.find("img"):
                key = "name"
            else:
                key = th.get_text(strip=True)
            side.setdefault(key, td)

        def side_text(key: str) -> str:
            td = side.get(key)
            return td.get_text(" ", strip=True) if td else ""

        header = soup.select_one(".shop-detail-main-header")
        name = ""
        if header and header.select_one("h2"):
            name = header.select_one("h2").get_text(strip=True)
        name = name or side_text("name") or (crumbs[-1] if crumbs else "")
        if not name:
            logger.warning("名称なし (スキップ): %s", url)
            return None

        tel = side_text("phone")
        if not tel:
            a = soup.select_one('a[href^="tel:"]')
            tel = unquote(a["href"][4:]).strip() if a else ""

        hp = ""
        if side.get("link"):
            a = side["link"].find("a", href=True)
            hp = a["href"] if a else ""

        badges = []
        if header:
            for b in header.select(".shop-detail-main-info > span.badge"):
                t = b.get_text(strip=True)
                if t:
                    badges.append(t)

        tags, area_tags = [], []
        if side.get("tag"):
            for sp in side["tag"].select("span.badge"):
                t = sp.get_text(strip=True)
                if not t:
                    continue
                (area_tags if "badge-light-dark" in (sp.get("class") or []) else tags).append(t)

        # アクセス欄: 1 ブロック = 1 拠点。複数店舗は p.w80 に【 ○○店 】ラベル、
        # 所在地は「所在地：」で始まる p (駅名・緯度経度・住所のいずれか)
        locations, addrs = [], []
        for box in soup.select(".map-area"):
            label = ""
            loc = ""
            for p in box.find_all("p"):
                t = p.get_text(strip=True)
                if t.startswith("所在地"):
                    loc = re.sub(r"^所在地[：:]\s*", "", t)
                elif not label and "w80" in (p.get("class") or []):
                    label = t
            if not loc:
                continue
            if not _COORD_RE.match(loc) and not loc.endswith("駅") and loc not in addrs:
                addrs.append(loc)
            entry = f"{label}{loc}" if label else loc
            if entry not in locations:
                locations.append(entry)
        addr = " / ".join(addrs)

        pref = _norm_pref(crumbs[1]) if len(crumbs) > 1 else ""
        if not pref and addr:
            pref = _norm_pref(addr)
        area = re.sub(r"エリア$", "", crumbs[2]) if len(crumbs) > 2 else ""
        # Schema.ADDR は市区町村以降: 先頭の都道府県名を除去
        if pref and addr.startswith(pref):
            addr = addr[len(pref):].strip()

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.TIME: side_text("clock-o"),
            Schema.CAT_SITE: badges[0] if badges else "",
            Schema.URL: url,
            "エリア": area,
            "詳細エリア": " / ".join(area_tags),
            "最寄駅": side_text("train"),
            "所在地(掲載値)": " / ".join(locations),
            "料金": side_text("jpy"),
            "営業形態": " / ".join(badges[1:]),
            "タグ": " / ".join(tags),
            "店舗ID": uuid,
            "店舗詳細URL": url,
            "取得元": getattr(self, "_last_source", ""),
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = EstheRanking()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.esthe-ranking.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
