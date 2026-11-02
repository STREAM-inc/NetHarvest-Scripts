"""
遺品整理の相談所 (ihinseiri-soudansho.co.jp) — 遺品整理業者ポータル 掲載業者スクレイパー

対象サイト: https://ihinseiri-soudansho.co.jp/search/

取得対象:
    - 関東・甲信・東海の 12 都県 (茨城・栃木・群馬・埼玉・千葉・東京・神奈川・山梨・長野・
      岐阜・静岡・愛知) の都道府県別一覧に掲載された業者
    - 取得項目: 名称 / 都道府県 / 郵便番号 / 住所 / 代表者名 / 従業員数 / 営業時間 / 定休日 /
      対応サービス / 対応エリア / 取得URL(詳細ページ)

取得フロー:
    1. 引数 url (/search/) から都道府県別一覧 /search/ihinseiri/{slug}/ を導出
       (東京都は tokyo_to、群馬県は gumma というスラッグ)
    2. 一覧 div.p-result__item h2 a から詳細 /search/{ID}/ を取得、
       ページ送りは a.next.page-numbers (?page=N。robots.txt で許可されているのは ?page= のみ。
       並び替え等のクエリ付き URL は robots.txt で禁止のため使わない)
    3. 詳細を 1 件取得するごとに即 yield (Pattern B)。複数都県に掲載される業者は ID で重複除外

注意:
    - 電話番号は全ページ共通のポータル代表番号 (0120-20-1349) のみで業者固有番号は無いため取得しない。
    - 一覧は「対応エリア」ベースのため所在地が対象外の県の業者も混ざる。
      所在地から判定した都道府県が 12 都県以外なら除外する (所在地が無い場合は掲載県で補完して残す)。
    - 特徴・業務内容・口コミなどの自由記述は著作権リスクのため取得しない。
    - 既定 UA は WAF で 403 になることがあるため、現行 Chrome 相当の UA を使う。

実行方法:
    python scripts/sites/service/ihinseiri_soudansho.py
    docker compose exec worker python /app/bin/run_flow.py --site-id ihinseiri_soudansho
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


# 備考で指定された 12 都県 (一覧スラッグ → 都道府県名)
_TARGET_PREFS = {
    "tokyo_to": "東京都",
    "kanagawa": "神奈川県",
    "saitama": "埼玉県",
    "chiba": "千葉県",
    "ibaraki": "茨城県",
    "tochigi": "栃木県",
    "gumma": "群馬県",
    "yamanashi": "山梨県",
    "nagano": "長野県",
    "gifu": "岐阜県",
    "shizuoka": "静岡県",
    "aichi": "愛知県",
}

_MAX_PAGES = 100

_PREF_RE = re.compile(
    r"^(北海道|東京都|京都府|大阪府|.{2,3}県)"
)
_POST_RE = re.compile(r"〒?\s*(\d{3})[-‐－ー−]?(\d{4})")


def _text(node) -> str:
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip() if node else ""


class IhinseiriSoudanshoScraper(StaticCrawler):
    """遺品整理の相談所 掲載業者スクレイパー (関東・甲信・東海 12 都県)"""

    DELAY = 3.0
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    EXTRA_COLUMNS = ["対応サービス", "対応エリア"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen = set()
        for slug, list_pref in _TARGET_PREFS.items():
            # 🔒 引数 url (/search/) をルートに都道府県別一覧を導出
            list_url = urljoin(url, f"ihinseiri/{slug}/")
            for _ in range(_MAX_PAGES):
                soup = self.get_soup(list_url)
                if soup is None:
                    self.logger.warning("一覧取得失敗: %s", list_url)
                    break
                links = soup.select("div.p-result__item h2.p-result__item-head a[href]")
                self.logger.info("%s %s: %d 件", list_pref, list_url, len(links))
                for a in links:
                    detail_url = urljoin(list_url, a["href"])
                    m = re.search(r"/search/(\d+)/?$", detail_url)
                    key = m.group(1) if m else detail_url
                    if key in seen:
                        continue
                    seen.add(key)
                    item = self._parse_detail(detail_url, list_pref)
                    if item:
                        yield item
                nxt = soup.select_one(".c-pagination__list a.next[href]") or soup.select_one("a.next.page-numbers[href]")
                if not nxt:
                    break
                list_url = urljoin(list_url, nxt["href"])

    def _parse_detail(self, detail_url: str, list_pref: str):
        soup = self.get_soup(detail_url)
        if soup is None:
            self.logger.warning("詳細取得失敗: %s", detail_url)
            return None

        # 会社情報 (p.b-company__item-ttl → div.b-company__item-txt)
        info = {}
        for row in soup.select("div.b-company__item"):
            label = _text(row.select_one(".b-company__item-ttl")).replace(" ", "")
            if label and label not in info:
                info[label] = _text(row.select_one(".b-company__item-txt"))

        # ヘッダーの「対応エリア:」「対応サービス:」ブロック
        head = {}
        for blk in soup.select("div.p-detail__head div.block"):
            ttl = re.sub(r"[\s:：]+", "", _text(blk.select_one("p.ttl")))
            if ttl and ttl not in head:
                head[ttl] = _text(blk.select_one(".txt"))

        name = info.get("会社名", "")
        if not name:
            h1 = soup.select_one("h1")
            name = _text(h1)
        if not name:
            title = soup.title.get_text(strip=True) if soup.title else ""
            name = re.sub(r"の料金・サービス.*$", "", title).strip()
        if not name:
            self.logger.warning("名称なし: %s", detail_url)
            return None

        addr_raw = info.get("所在地", "")
        post_code = ""
        m = _POST_RE.search(addr_raw)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
        addr = re.sub(r"^\s*〒?\s*\d{3}[-‐－ー−]?\d{4}\s*", "", addr_raw).strip()
        pm = _PREF_RE.match(addr)
        pref = pm.group(1) if pm else ""

        # 所在地が対象 12 都県以外の業者は除外 (所在地不明なら掲載県で補完)
        if pref and pref not in _TARGET_PREFS.values():
            self.logger.info("対象外の所在地のため除外: %s %s", pref, detail_url)
            return None
        if not pref:
            pref = list_pref

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.REP_NM: info.get("代表者名", "") or info.get("代表者", ""),
            Schema.EMP_NUM: info.get("従業員数", ""),
            Schema.TIME: info.get("営業時間", ""),
            Schema.HOLIDAY: info.get("定休日", ""),
            "対応サービス": head.get("対応サービス", ""),
            "対応エリア": head.get("対応エリア", ""),
        }


if __name__ == "__main__":
    scraper = IhinseiriSoudanshoScraper()
    scraper.execute("https://ihinseiri-soudansho.co.jp/search/")
