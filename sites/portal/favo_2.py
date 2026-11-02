"""
Favo富山版 (toyama.favo-web.jp) — 「プロ情報」掲載店舗スクレイパー

対象サイト: https://toyama.favo-web.jp/store/

取得フロー:
    サイトは WordPress。店舗は カスタム投稿タイプ `store` (全約82件)。
    1. 一覧ページ (引数 url = /store/) を 1 回取得し、カード (a[href*="/store/"]) の
       .archive_cateBar span から「掲載エリア」(富山市/高岡市…) を店舗URL別に控える。
       ※ 詳細ページ側のエリア表示は HTML コメントアウトされているため一覧からのみ取得可。
    2. カテゴリ名 (store_cate: グルメ/ビューティ/…) を /wp-json/wp/v2/store_cate で解決。
    3. /wp-json/wp/v2/store?per_page=50&page=N で店舗を列挙し、1 件ごとに詳細ページを
       get_soup() で取得 → div.store_data の dl (dt ラベル / dd 値) を解析して即 yield。

備考:
    - 名称は詳細「店舗情報」の「店名」。投稿タイトル (例:「ひざ・腰専門整体のプロ」) は
      キャッチ見出しなので名称には使わない (店名が無い場合のみフォールバック)。
    - 住所は「富山市…」のように都道府県を含まない。富山県版の地域サイトのため PREF は
      住所が都道府県で始まればそれを、そうでなければ「富山県」とする。
    - メールアドレスは掲載がある場合のみ (dl ラベル or mailto リンク)。調査時点では未掲載。
    - 店舗紹介文・PICK UP メニュー等の長文自由記述は著作権リスクのため取得しない。

実行方法:
    python scripts/sites/portal/favo_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id favo_2
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PER_PAGE = 50  # read timeout を避けるため 50 件以下でページング
_MAX_API_PAGES = 50  # 無限ループ防止の上限

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|石川|福井|"
    r"山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|徳島|"
    r"香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

# 店舗情報 dl のラベル → 出力カラム
_LABEL_MAP = {
    "住所": Schema.ADDR,
    "所在地": Schema.ADDR,
    "電話番号": Schema.TEL,
    "TEL": Schema.TEL,
    "営業時間": Schema.TIME,
    "定休日": Schema.HOLIDAY,
    "メール": Schema.EMAIL,
    "メールアドレス": Schema.EMAIL,
    "E-mail": Schema.EMAIL,
    "駐車場": "駐車場",
}

# store_sns 内リンクのドメイン → 出力カラム
_SNS_MAP = [
    ("instagram.com", Schema.INSTA),
    ("line.me", Schema.LINE),
    ("lin.ee", Schema.LINE),
    ("twitter.com", Schema.X),
    ("x.com", Schema.X),
    ("facebook.com", Schema.FB),
    ("tiktok.com", Schema.TIKTOK),
]


def _key(link: str) -> str:
    return link.rstrip("/")


class Favo2Scraper(StaticCrawler):
    """Favo富山版 プロ情報 スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["掲載エリア", "駐車場"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 1. 一覧ページから掲載エリアを取得
        area_map: dict[str, str] = {}
        soup = self.get_soup(url)
        if soup:
            for a in soup.select('a[href*="/store/"]'):
                span = a.select_one(".archive_cateBar span, .cateBar span")
                href = a.get("href", "")
                if span and href:
                    area_map.setdefault(_key(urljoin(url, href)), span.get_text(strip=True))
        self.logger.info(f"一覧の掲載エリア: {len(area_map)} 店舗")

        # 2. カテゴリ ID → 名称
        cate_names: dict[int, str] = {}
        cate_api = urljoin(url, "/wp-json/wp/v2/store_cate")
        resp = self.session.get(cate_api, params={"per_page": 100}, timeout=self.TIMEOUT)
        if resp.status_code == 200:
            cate_names = {c["id"]: c["name"] for c in resp.json()}
        else:
            self.logger.warning(f"store_cate 取得失敗: HTTP {resp.status_code}")

        # 3. 店舗を REST API で列挙し、詳細を 1 件ずつ取得して yield
        store_api = urljoin(url, "/wp-json/wp/v2/store")
        seen: set[str] = set()
        for page in range(1, _MAX_API_PAGES + 1):
            resp = self.session.get(
                store_api,
                params={"per_page": _PER_PAGE, "page": page, "_fields": "id,link,title,store_cate"},
                timeout=self.TIMEOUT,
            )
            if resp.status_code == 400:  # 範囲外ページ (rest_post_invalid_page_number)
                break
            resp.raise_for_status()
            posts = resp.json()
            if not posts:
                break
            for post in posts:
                link = post.get("link", "")
                if not link or _key(link) in seen:
                    continue
                seen.add(_key(link))
                item = self._parse_detail(link, post, cate_names, area_map)
                if item:
                    yield item
            total_pages = int(resp.headers.get("X-WP-TotalPages", page) or page)
            if page >= total_pages:
                break

    def _parse_detail(self, link: str, post: dict, cate_names: dict, area_map: dict) -> dict | None:
        soup = self.get_soup(link)
        if soup is None:
            self.logger.warning(f"詳細取得失敗: {link}")
            return None

        title = re.sub(r"\s+", " ", (post.get("title") or {}).get("rendered", "")).strip()
        cats = [cate_names.get(c, "") for c in post.get("store_cate") or []]
        item = {
            Schema.URL: link,
            Schema.NAME: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            Schema.EMAIL: "",
            Schema.HP: "",
            Schema.INSTA: "",
            Schema.LINE: "",
            Schema.X: "",
            Schema.FB: "",
            Schema.TIKTOK: "",
            Schema.CAT_SITE: "/".join(c for c in cats if c),
            "掲載エリア": area_map.get(_key(link), ""),
            "駐車場": "",
        }

        for dl in soup.select(".store_data dl"):
            dt, dd = dl.find("dt"), dl.find("dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)
            value = re.sub(r"\s+", " ", dd.get_text(" ", strip=True)).strip()
            if label in ("店名", "店舗名"):
                item[Schema.NAME] = value
            elif label in _LABEL_MAP and value and not item[_LABEL_MAP[label]]:
                item[_LABEL_MAP[label]] = value

        if not item[Schema.NAME]:
            item[Schema.NAME] = title

        for a in soup.select(".store_btn a[href]"):
            href = a["href"].strip()
            if a.get_text(strip=True) == "公式サイト" and href.startswith("http"):
                item[Schema.HP] = href
            elif href.startswith("tel:") and not item[Schema.TEL]:
                item[Schema.TEL] = href[4:].strip()
            elif href.startswith("mailto:") and not item[Schema.EMAIL]:
                item[Schema.EMAIL] = href[7:].split("?")[0].strip()

        for a in soup.select(".store_sns a[href]"):
            href = a["href"].strip()
            for domain, col in _SNS_MAP:
                if domain in href and not item[col]:
                    item[col] = href
                    break

        addr = item[Schema.ADDR]
        m = _PREF_PATTERN.match(addr)
        if m:
            item[Schema.PREF] = m.group(1)
            item[Schema.ADDR] = addr[m.end():].strip()
        elif addr:
            item[Schema.PREF] = "富山県"  # 富山県版の地域サイト (住所は市町村始まり)

        return item


if __name__ == "__main__":
    scraper = Favo2Scraper()
    scraper.execute("https://toyama.favo-web.jp/store/")
