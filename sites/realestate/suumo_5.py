# -*- coding: utf-8 -*-
"""
SUUMO賃貸 不動産会社一覧（東京都） — suumo_5

取得対象:
    - SUUMO 賃貸の「東京都の不動産会社」に掲載されている店舗の店舗概要
      (店舗名・所在地・TEL・FAX・営業時間・定休日・関連サイト・免許番号・店舗の特徴)

取得フロー:
    https://suumo.jp/chintai/kaisha/tokyo/            (起点 = sites.yml の url)
      → area/                                        (市区町村インデックス: 23区 + 多摩地域市部)
        → sc_{市区町村}/?page=N                      (1ページ30件。総件数 ÷ 30 でページ数算出)
          → /chintai/kaisha/kc_XXX_XXXXXXXXX/        (店舗詳細 = 取得即 yield)

    - 沿線・駅別一覧 (en_ / ek_) は使用しない (備考の指示)
    - SUUMO店舗ID (kc_XXX_XXXXXXXXX) で重複排除し、1店舗1行で出力する

注意 (robots.txt 準拠):
    「関連サイト」のリンク先は HTML 上では SUUMO のリダイレクタ
    /jj/chintai/common/FR901FK150 経由でしか露出せず、同パスは robots.txt で
    Disallow されているため、外部 URL への解決は行わない。
    本クローラーは「関連サイト名」とリダイレクタ URL をそのまま記録し、
    参考情報として問い合わせメールのドメイン (= 会社の独自ドメイン) を併記する。

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/suumo_5.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id suumo_5
"""

from __future__ import annotations

import math
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4  # noqa: E402

from src.framework.static import StaticCrawler  # noqa: E402
from src.const.schema import Schema  # noqa: E402

# 市区町村一覧ページ (起点 url からの相対パス)
_AREA_INDEX_PATH = "area/"

# /chintai/kaisha/tokyo/sc_shibuya/ のような市区町村別一覧
_SC_LIST_RE = re.compile(r"/chintai/kaisha/[a-z]+/(sc_[a-z0-9]+)/?$")
# /chintai/kaisha/kc_030_000731000/ のような店舗詳細
_DETAIL_RE = re.compile(r"/chintai/kaisha/(kc_\d{3}_\d+)/")

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_PER_PAGE = 30          # 一覧 1 ページあたりの件数
_MAX_PAGES = 200        # 市区町村あたりのページ数の安全上限
_MAX_ATTEMPTS = 3       # タイムアウト時のリトライ回数


def _norm(text: str | None) -> str:
    """全角スペースを半角に寄せて余分な空白を畳む。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text).replace("　", " ")).strip()


def _blank_if_hyphen(text: str) -> str:
    """SUUMO の未登録表記 (「-」「－」) を空文字に寄せる。"""
    return "" if text in {"-", "－", "ー", "―"} else text


class Suumo5Scraper(StaticCrawler):
    """SUUMO賃貸 不動産会社一覧（東京都） スクレイパー（一覧→詳細 / 取得即 yield）"""

    # アクセス集中でタイムアウトしやすいため 1 リクエストごとに待機する
    DELAY = 0.0          # 待機は _get() 側で一元管理する (二重待機を避ける)
    ITEM_DELAY = 0.0
    TIMEOUT = 30
    REQUEST_DELAY = 2.5  # 1 リクエストごとの待機秒数 (備考の「2〜3秒」)

    EXTRA_COLUMNS = [
        "SUUMO店舗ID",
        "FAX",
        "免許番号",
        "店舗の特徴",
        "関連サイト名",
        "関連サイトURL",
        "メールドメイン",
        "市区町村",
    ]

    # ------------------------------------------------------------------ 取得
    def _get(self, url: str) -> bs4.BeautifulSoup | None:
        """待機 + リトライ付きで HTML を取得する。

        get_soup() は CONTINUE_ON_ERROR=True のときタイムアウト等で None を返すため、
        None を失敗とみなして指数バックオフで再試行する。上限到達時は None を返す。
        """
        for attempt in range(_MAX_ATTEMPTS):
            if self.REQUEST_DELAY > 0 and not self._last_fetch_from_cache:
                time.sleep(self.REQUEST_DELAY)
            soup = self.get_soup(url)
            if soup is not None:
                return soup
            wait = min(2 ** attempt, 15)
            self.logger.warning(
                "取得失敗 (%d/%d) %s — %.0f 秒後に再試行", attempt + 1, _MAX_ATTEMPTS, url, wait
            )
            time.sleep(wait)
        self.logger.error("取得を %d 回試行しましたが失敗しました: %s", _MAX_ATTEMPTS, url)
        return None

    # ------------------------------------------------------------------ 本体
    def parse(self, url: str) -> Generator[dict, None, None]:
        root = url if url.endswith("/") else url + "/"

        municipalities = self._collect_municipalities(root)
        if not municipalities:
            self.logger.error("市区町村一覧を取得できませんでした: %s", root)
            return
        self.logger.info("市区町村数: %d", len(municipalities))

        seen: set[str] = set()
        for city_url, city_name in municipalities:
            for shop_id, detail_url in self._iter_shops(city_url):
                if shop_id in seen:
                    continue
                seen.add(shop_id)
                item = self._scrape_detail(detail_url, shop_id, city_name)
                if item:
                    yield item

    # --------------------------------------------------------- 市区町村一覧
    def _collect_municipalities(self, root: str) -> list[tuple[str, str]]:
        """/area/ から市区町村別一覧の (URL, 市区町村名) を収集する。"""
        index_url = urljoin(root, _AREA_INDEX_PATH)
        soup = self._get(index_url)
        if soup is None:
            return []

        result: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not _SC_LIST_RE.search(href.split("?")[0]):
                continue
            full = urljoin(index_url, href.split("?")[0])
            if full in seen:
                continue
            seen.add(full)
            result.append((full, _norm(a.get_text())))
        return result

    # --------------------------------------------------------- 一覧ページ送り
    def _iter_shops(self, city_url: str) -> Generator[tuple[str, str], None, None]:
        """市区町村別一覧を ?page=N で辿り、(店舗ID, 詳細URL) を順に返す。

        範囲外の page は 1 ページ目が返るため、総件数から算出したページ数で打ち切る。
        """
        first = self._get(city_url)
        if first is None:
            return

        total = self._extract_total(first)
        pages = min(math.ceil(total / _PER_PAGE), _MAX_PAGES) if total else 1
        self.logger.info("一覧: %s (%s件 / %sページ)", city_url, total or "?", pages)

        page_ids = self._extract_detail_ids(first, city_url)
        first_page_ids = {sid for sid, _ in page_ids}
        yield from page_ids

        for page in range(2, pages + 1):
            sep = "&" if "?" in city_url else "?"
            soup = self._get(f"{city_url}{sep}page={page}")
            if soup is None:
                continue
            page_ids = self._extract_detail_ids(soup, city_url)
            if not page_ids:
                break
            # 範囲外ページは 1 ページ目が返るので、同一内容なら打ち切る
            if {sid for sid, _ in page_ids} == first_page_ids:
                break
            yield from page_ids

    @staticmethod
    def _extract_total(soup: bs4.BeautifulSoup) -> int:
        node = soup.select_one(".pagination_set-hit")
        if node is None:
            return 0
        m = re.search(r"([\d,]+)", node.get_text(" ", strip=True))
        return int(m.group(1).replace(",", "")) if m else 0

    @staticmethod
    def _extract_detail_ids(
        soup: bs4.BeautifulSoup, base_url: str
    ) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            m = _DETAIL_RE.search(a.get("href") or "")
            if not m:
                continue
            shop_id = m.group(1)
            if shop_id in seen:
                continue
            seen.add(shop_id)
            found.append((shop_id, urljoin(base_url, f"/chintai/kaisha/{shop_id}/")))
        return found

    # ------------------------------------------------------------- 詳細ページ
    def _scrape_detail(self, detail_url: str, shop_id: str, city_name: str) -> dict | None:
        soup = self._get(detail_url)
        if soup is None:
            return None

        h1 = soup.select_one("h1")
        name = _norm(h1.get_text()) if h1 else ""
        if not name:
            self.logger.warning("店舗名を取得できませんでした: %s", detail_url)
            return None

        fields = self._parse_gaiyou(soup)

        address = _blank_if_hyphen(fields.get("所在地", ""))
        pref = ""
        m = _PREF_RE.match(address)
        if m:
            pref = m.group(1)
            address = address[len(pref):].strip()

        rel_names, rel_urls = self._parse_related_sites(soup, detail_url)

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.TEL: _blank_if_hyphen(fields.get("TEL", "")),
            Schema.TIME: _blank_if_hyphen(fields.get("営業時間", "")),
            Schema.HOLIDAY: _blank_if_hyphen(fields.get("定休日", "")),
            "SUUMO店舗ID": shop_id,
            "FAX": _blank_if_hyphen(fields.get("FAX", "")),
            "免許番号": _blank_if_hyphen(fields.get("免許番号", "")),
            "店舗の特徴": _blank_if_hyphen(fields.get("店舗の特徴", "")),
            "関連サイト名": rel_names,
            "関連サイトURL": rel_urls,
            "メールドメイン": self._parse_mail_domain(soup),
            "市区町村": city_name,
        }

    @staticmethod
    def _parse_gaiyou(soup: bs4.BeautifulSoup) -> dict[str, str]:
        """「店舗概要」テーブル (1 行に th/td が最大 2 組) をラベル → 値に展開する。"""
        fields: dict[str, str] = {}
        for table in soup.select("table.table_gaiyou"):
            for tr in table.select("tr"):
                cells = tr.find_all(["th", "td"], recursive=False)
                label = ""
                for cell in cells:
                    if cell.name == "th":
                        label = _norm(cell.get_text())
                    elif label:
                        fields.setdefault(label, _norm(cell.get_text(" ", strip=True)))
                        label = ""
        return fields

    @staticmethod
    def _parse_related_sites(
        soup: bs4.BeautifulSoup, detail_url: str
    ) -> tuple[str, str]:
        """「関連サイト」セルのリンク名と遷移先 URL を取り出す。

        リンク先は onclick 内の SUUMO リダイレクタ URL (FR901FK150) でしか露出せず、
        同パスは robots.txt で Disallow のため外部 URL への解決は行わない。
        """
        cell = None
        for table in soup.select("table.table_gaiyou"):
            for th in table.select("th"):
                if _norm(th.get_text()) == "関連サイト":
                    cell = th.find_next_sibling("td")
                    break
            if cell is not None:
                break
        if cell is None:
            return "", ""

        names: list[str] = []
        urls: list[str] = []
        for a in cell.select("a"):
            label = _norm(a.get_text())
            if not label:
                continue
            names.append(label)
            m = re.search(r"popUpMap2\(\s*'([^']+)'", a.get("onclick") or "")
            href = m.group(1) if m else (a.get("href") or "")
            if href and not href.startswith("javascript:"):
                urls.append(urljoin(detail_url, href))
        return " / ".join(names), " ".join(urls)

    @staticmethod
    def _parse_mail_domain(soup: bs4.BeautifulSoup) -> str:
        """問い合わせメールのドメイン欄から suumo.jp 以外 (= 会社の独自ドメイン) を拾う。"""
        node = soup.select_one(".mobile_settings-domain-body")
        if node is None:
            return ""
        domains = [
            d for d in re.split(r"[/\s]+", _norm(node.get_text(" ", strip=True)))
            if d and d != "suumo.jp"
        ]
        return " ".join(domains)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Suumo5Scraper()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://suumo.jp/chintai/kaisha/tokyo/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
