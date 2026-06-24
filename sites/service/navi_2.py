"""
便利屋さんNAVI (benriyanavi.com) — 全国の便利屋さん情報スクレイパー

取得対象:
    - トップページのナビから列挙できる全 47 都道府県 (東京は 23区内/23区外で 2 ページ)
      の /prefact/{romaji}.html に掲載される便利屋さんの一覧情報

取得フロー:
    1. ルート URL (https://benriyanavi.com/) を取得し、/prefact/*.html の都道府県ページ
       リンクを全て列挙する (ルート URL を起点に派生)
    2. 各都道府県ページの本文 (div.post) を解析。1 店舗 = <h3>店名</h3> +
       直後の <p>住所<br>代表者<br>TEL<br>HP</p> ブロック
    3. 1 店舗を解析するごとに即 yield する (一覧のみ・詳細ページ無し)

備考:
    - 各店舗の <p class="int"> は運営者/店舗による自由記述の紹介文 (プロース) のため、
      著作権リスクを避けて取得しない。
    - 住所先頭の 〒郵便番号、都道府県を分離して格納する。

実行方法:
    python scripts/sites/service/navi_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id navi_2
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREFACT_RE = re.compile(r"/prefact/[a-z0-9_]+\.html$", re.IGNORECASE)


class Navi2Scraper(StaticCrawler):
    """便利屋さんNAVI (benriyanavi.com) スクレイパー"""

    DELAY = 1.5

    # Schema に該当しないサイト固有カラムは無し。
    # 店舗紹介文 (p.int) は自由記述プロースのため著作権リスクで除外。
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        """url を唯一のルートとして都道府県ページを巡回し、店舗を逐次 yield する。"""
        soup = self.get_soup(url)
        if not soup:
            return

        pref_urls = self._collect_pref_urls(soup, url)
        self.logger.info("都道府県ページ数: %d", len(pref_urls))

        seen: set[tuple[str, str]] = set()
        for pref_url in pref_urls:
            self.logger.info("一覧ページ取得: %s", pref_url)
            page_soup = self.get_soup(pref_url)
            if not page_soup:
                continue

            page_pref = self._page_prefecture(page_soup)
            count = 0
            for record in self._parse_listing(page_soup, pref_url, page_pref):
                key = (record.get(Schema.NAME, ""), record.get(Schema.TEL, ""))
                if key in seen:
                    continue
                seen.add(key)
                self.total_items = len(seen)
                count += 1
                yield record

            self.logger.info("  → %d 件", count)
            time.sleep(self.DELAY)

    def _collect_pref_urls(self, soup, root_url: str) -> list[str]:
        """ルートページから /prefact/*.html リンクを列挙し、root の host に正規化する。"""
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            path = urlparse(href).path or href
            if not _PREFACT_RE.search(path):
                continue
            # ルート URL を起点に派生 (host/scheme を root に合わせる)
            absolute = urljoin(root_url, path)
            if absolute in seen:
                continue
            seen.add(absolute)
            urls.append(absolute)
        return urls

    def _page_prefecture(self, soup) -> str:
        """ページタイトル等から都道府県名を推定 (住所欠落時のフォールバック用)。"""
        title = soup.title.get_text(strip=True) if soup.title else ""
        m = _PREF_PATTERN.search(title)
        return m.group(1) if m else ""

    def _parse_listing(self, soup, page_url: str, page_pref: str):
        post = soup.select_one("div.post") or soup
        for h3 in post.find_all("h3"):
            info_p = self._next_info_p(h3)
            if info_p is None:
                continue

            name = h3.get_text(strip=True)
            if not name:
                continue

            item = self._build_record(name, info_p, page_url, page_pref)
            if item:
                yield item

    @staticmethod
    def _next_info_p(h3):
        """h3 直後の <p> 情報ブロックを返す。TEL/代表者 を含まなければ None。"""
        for sib in h3.next_siblings:
            tag = getattr(sib, "name", None)
            if tag == "h3":
                return None
            if tag == "p":
                txt = sib.get_text()
                if "TEL" in txt or "ＴＥＬ" in txt or "代表者" in txt:
                    return sib
                return None
        return None

    def _build_record(self, name: str, info_p, page_url: str, page_pref: str) -> dict | None:
        text = info_p.get_text("\n", strip=True)
        lines = [l.strip() for l in text.split("\n") if l.strip()]

        item: dict = {
            Schema.URL: page_url,
            Schema.NAME: name,
        }

        addr_line = ""
        for line in lines:
            if re.match(r"^(代表者|TEL|ＴＥＬ|HP|ＨＰ|FAX|ＦＡＸ)", line):
                continue
            addr_line = line
            break

        for line in lines:
            if line.startswith("代表者"):
                rep = re.sub(r"^代表者[\s　:：]*", "", line).strip()
                if rep:
                    item[Schema.REP_NM] = rep
            elif re.match(r"^(TEL|ＴＥＬ)", line):
                tel = re.sub(r"^(TEL|ＴＥＬ)[\s　:：]*", "", line).strip()
                if tel:
                    item[Schema.TEL] = tel

        if addr_line:
            addr = addr_line
            pm = _POST_RE.match(addr)
            if pm:
                item[Schema.POST_CODE] = pm.group(1)
                addr = addr[pm.end():].strip()
            item[Schema.ADDR] = addr
            prm = _PREF_PATTERN.search(addr)
            if prm:
                item[Schema.PREF] = prm.group(1)

        if not item.get(Schema.PREF) and page_pref:
            item[Schema.PREF] = page_pref

        a = info_p.find("a", href=True)
        if a:
            item[Schema.HP] = a["href"].strip()

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Navi2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://benriyanavi.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
