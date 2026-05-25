"""
住所でポン! 2012年版 — 電話帳ディレクトリ (jpon.xyz)

取得対象:
    - 2012年版電話帳 全47都道府県 × 市区町村 × 町字単位 の全レコード

取得フロー:
    1. トップ /2012/index.html から47都道府県の URL を収集
    2. 各都道府県 /2012/{pref_id}/index.html から市区町村 URL を収集
    3. 各市区町村 /2012/{pref_id}/{city_id}/index.html から町字 URL を収集
    4. 各町字 /2012/{pref_id}/{city_id}/{district_id}.html?all で全件を一括取得
       - 表示はマスク (03-3795-****) だが、a[href="/s/2012/{phone}"] に完全な電話番号
       - addressRegion / addressLocality は span の content 属性に完全値
       - 町名は h2 の階層 ("東京都 世田谷区 三宿 の電話帳") から抽出
       - span.entry = 転入 / span.exit = 転出 / 無印 = 現役

実行方法:
    python scripts/sites/portal/jpon.py
    python bin/run_flow.py --site-id jpon

注意:
    - サイトは過剰アクセスのレート制限を明示している。DELAY=3.0 推奨。
    - ステルス対策として ブラウザ風 Accept-Language / Accept-Encoding 等を付与。
    - 全国フル走査は 20-30万リクエスト規模。本番運用は段階的に。
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

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://jpon.xyz"
TOP_URL = "https://jpon.xyz/2012/index.html"

_URL_CHECKPOINT = _project_root / "output" / "jpon_urls.json"
_DONE_CHECKPOINT = _project_root / "output" / "jpon_done.txt"

# 表示文字列のスペース / タブ / 改行 / 全角スペースをまとめて正規化する
_WHITESPACE_RE = re.compile(r"\s+")

# /s/2012/{phone} 形式の href から完全な電話番号を取り出すための正規表現
_PHONE_HREF_RE = re.compile(r"/s/\d+/(\d[\d\-]+)")


def _clean(s) -> str:
    if s is None:
        return ""
    return _WHITESPACE_RE.sub(" ", str(s).replace("　", " ")).strip()


class JponScraper(StaticCrawler):
    """住所でポン! 2012年版 電話帳スクレイパー"""

    # サイト側で「過剰なアクセスは規制」と明示しているため保守的に設定
    DELAY = 3.0

    # ブラウザ風のヘッダーで bot 判定を回避（ステルス）
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )

    EXTRA_COLUMNS = ["市区町村", "町名", "エリアコード", "ステータス", "詳細URL"]

    def _setup(self):
        super()._setup()
        # ブラウザらしい追加ヘッダーを送る（StaticCrawler ベースのステルス対策）
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Sec-Ch-Ua": '"Chromium";v="125", "Not.A/Brand";v="24", "Google Chrome";v="125"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Referer": f"{BASE_URL}/",
        })

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 1-3. URL 収集 (チェックポイントがあればスキップ)
        if _URL_CHECKPOINT.exists():
            with open(_URL_CHECKPOINT, encoding="utf-8") as f:
                district_urls = json.load(f)
            self.logger.info("チェックポイントから町字URL %d件を読み込みました", len(district_urls))
        else:
            pref_urls = self._collect_prefecture_urls(url)
            self.logger.info("都道府県数: %d", len(pref_urls))

            city_urls: list[str] = []
            for pref_url in pref_urls:
                try:
                    city_urls.extend(self._collect_city_urls(pref_url))
                except Exception as e:
                    self.logger.warning("市区町村一覧取得失敗 %s: %s", pref_url, e)
                    continue
            self.logger.info("市区町村数: %d", len(city_urls))

            district_urls: list[str] = []
            for city_url in city_urls:
                try:
                    district_urls.extend(self._collect_district_urls(city_url))
                except Exception as e:
                    self.logger.warning("町字一覧取得失敗 %s: %s", city_url, e)
                    continue

            _URL_CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
            with open(_URL_CHECKPOINT, "w", encoding="utf-8") as f:
                json.dump(district_urls, f)
            self.logger.info("URLチェックポイント保存: %d件 → %s", len(district_urls), _URL_CHECKPOINT)

        # 完了済み町字URLをロードしてスキップ
        done_urls: set[str] = set()
        if _DONE_CHECKPOINT.exists() and _URL_CHECKPOINT.exists():
            with open(_DONE_CHECKPOINT, encoding="utf-8") as f:
                done_urls = {line.strip() for line in f if line.strip()}
            self.logger.info("完了済み %d件 をスキップします", len(done_urls))

        remaining = [u for u in district_urls if u not in done_urls]
        self.total_items = len(remaining)
        self.logger.info("町字数 (= 取得対象ページ数): 全 %d件 / 残り %d件", len(district_urls), len(remaining))

        # 4. 各町字 ?all で全件取得
        pending_done: list[str] = []
        for district_url in remaining:
            try:
                yield from self._scrape_district(district_url)
            except Exception as e:
                self.logger.warning("町字ページ取得失敗 %s: %s", district_url, e)

            pending_done.append(district_url)
            if len(pending_done) >= 100:
                self._flush_done(pending_done)
                pending_done.clear()

        if pending_done:
            self._flush_done(pending_done)

        # 全件処理完了 — チェックポイントを削除
        for p in (_URL_CHECKPOINT, _DONE_CHECKPOINT):
            if p.exists():
                p.unlink()
        self.logger.info("全件処理完了。チェックポイントをクリアしました")

    # -------------------------------------------------------------------------
    # 階層別ヘルパー
    # -------------------------------------------------------------------------

    @staticmethod
    def _flush_done(urls: list[str]) -> None:
        """処理済み町字URLをチェックポイントファイルに追記する"""
        with open(_DONE_CHECKPOINT, "a", encoding="utf-8") as f:
            f.write("\n".join(urls) + "\n")

    def _collect_prefecture_urls(self, top_url: str) -> list[str]:
        """トップ /2012/index.html から /2012/{pref_id}/index.html の URL を収集する"""
        soup = self.get_soup(top_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/2012/"][href$="/index.html"]'):
            href = a.get("href", "")
            # /2012/{pref_id}/index.html の形（深さ 2 階層）のみ拾う
            if not re.match(r"^/2012/\d+/index\.html$", href):
                continue
            full = urljoin(BASE_URL, href)
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    def _collect_city_urls(self, pref_url: str) -> list[str]:
        """都道府県 /2012/{pref}/index.html から /2012/{pref}/{city}/index.html の URL を収集する"""
        time.sleep(self.DELAY)
        soup = self.get_soup(pref_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/2012/"][href$="/index.html"]'):
            href = a.get("href", "")
            if not re.match(r"^/2012/\d+/\d+/index\.html$", href):
                continue
            full = urljoin(BASE_URL, href)
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    def _collect_district_urls(self, city_url: str) -> list[str]:
        """市区町村 /2012/{pref}/{city}/index.html から /2012/{pref}/{city}/{district}.html を収集する。

        ページ内のリンクは ?p=1 が付いた形 (/2012/27/4/1.html?p=1) になっているため、
        正規化して ?all 付きに揃える。
        """
        time.sleep(self.DELAY)
        soup = self.get_soup(city_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/2012/"]'):
            href = a.get("href", "")
            # /2012/{pref}/{city}/{district}.html のパターン
            m = re.match(r"^/2012/\d+/\d+/\d+\.html", href)
            if not m:
                continue
            # ?p=1 等のクエリを除去して ?all を付ける
            base_path = m.group(0)
            full = urljoin(BASE_URL, base_path) + "?all"
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    def _scrape_district(self, district_url: str) -> Generator[dict, None, None]:
        """町字ページ ?all から全レコードを yield する"""
        soup = self.get_soup(district_url)
        if soup is None:
            return

        # 階層情報を抽出: h2 = "住所でポン！ 2012年版 東京都 世田谷区  三宿 の電話帳"
        h2 = soup.select_one("h2")
        district_name = ""
        if h2:
            # "三宿 の電話帳" 部分を取り出す
            h2_text = _clean(h2.get_text(" ", strip=True))
            m = re.search(r"([^ 　]+)\s*の電話帳\s*$", h2_text)
            if m:
                district_name = m.group(1)

        # URL からエリアコード (例: 27-4-1) を抽出
        area_code = ""
        m = re.search(r"/2012/(\d+)/(\d+)/(\d+)\.html", district_url)
        if m:
            area_code = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        # 全 <tr itemtype="https://schema.org/Person"> を取得
        rows = soup.select('tr[itemtype="https://schema.org/Person"]')
        for tr in rows:
            try:
                item = self._parse_row(tr, district_url, district_name, area_code)
            except Exception as e:
                self.logger.warning("行解析失敗 %s: %s", district_url, e)
                continue
            if item and item.get(Schema.NAME):
                yield item

    def _parse_row(
        self,
        tr,
        source_url: str,
        district_name: str,
        area_code: str,
    ) -> dict | None:
        """1 レコード行 (<tr>) を辞書に変換する"""
        # 電話番号: 表示は ****でマスクされているが、a[href="/s/2012/{phone}"] に完全番号
        tel = ""
        detail_url = ""
        phone_a = tr.select_one('td.p a[href*="/s/"]')
        if phone_a:
            href = phone_a.get("href", "")
            m = _PHONE_HREF_RE.search(href)
            if m:
                tel = m.group(1)
            detail_url = urljoin(BASE_URL, href)

        # 名前
        name = ""
        name_span = tr.select_one('td.n [itemprop="name"]')
        if name_span:
            name = _clean(name_span.get_text())

        # 都道府県 (addressRegion content 属性)
        prefecture = ""
        region_el = tr.select_one('[itemprop="addressRegion"]')
        if region_el:
            prefecture = _clean(region_el.get("content") or region_el.get_text())

        # 市区町村 (addressLocality content 属性)
        city = ""
        locality_el = tr.select_one('[itemprop="addressLocality"]')
        if locality_el:
            city = _clean(locality_el.get("content") or locality_el.get_text())

        # ステータス: span.entry=転入 / span.exit=転出 / 無印=現役
        if tr.select_one("td.p span.entry"):
            status = "転入"
        elif tr.select_one("td.p span.exit"):
            status = "転出"
        else:
            status = "現役"

        # 住所: 市区町村 + 町名 (番地以下はサイト側で隠蔽されているため取得不能)
        addr = f"{city}{district_name}" if district_name else city

        return {
            Schema.NAME: name,
            Schema.TEL: tel,
            Schema.PREF: prefecture,
            Schema.ADDR: addr,
            Schema.URL: source_url,
            "市区町村": city,
            "町名": district_name,
            "エリアコード": area_code,
            "ステータス": status,
            "詳細URL": detail_url,
        }


def _recover_partial_csv(partial_path: Path) -> None:
    """中断された実行の一時CSV（ヘッダーなし）を復旧して最終CSVを生成する"""
    import csv
    from datetime import datetime
    from src.const.schema import Schema

    all_fieldnames = list(Schema.COLUMNS) + JponScraper.EXTRA_COLUMNS
    output_path = partial_path.parent / (
        f"{datetime.now().strftime('%Y%m%d')}_jpon_recovered_{partial_path.stem}.csv"
    )

    row_count = 0
    with open(partial_path, "r", encoding="utf-8", newline="") as f_in, \
         open(output_path, "w", encoding="utf-8-sig", newline="") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        writer.writerow(all_fieldnames)
        for row in reader:
            writer.writerow(row)
            row_count += 1

    print(f"復旧完了: {output_path}")
    print(f"行数: {row_count:,}件")


if __name__ == "__main__":
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="住所でポン! スクレイパー")
    parser.add_argument(
        "--recover",
        metavar="PARTIAL_CSV",
        help="中断された実行の一時CSVファイルを指定して最終CSVを復旧する",
    )
    args = parser.parse_args()

    if args.recover:
        _recover_partial_csv(Path(args.recover))
        sys.exit(0)

    scraper = JponScraper()
    scraper.execute(TOP_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
