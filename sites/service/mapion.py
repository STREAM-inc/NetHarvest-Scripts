"""
マピオン電話帳 「興信所・探偵事務所」 (mapion) — 大阪府・兵庫県・京都府 3府県スクレイパー

対象サイト: https://www.mapion.co.jp/phonebook/M10037/27/

取得対象:
    - マピオン電話帳のカテゴリ「興信所・探偵事務所」(M10037) に掲載されている
      大阪府 (27) / 兵庫県 (28) / 京都府 (26) の事業者
    - 取得カラムは 名称 / TEL / 住所 / 都道府県 の 4 列 (備考の指示どおり)

取得フロー:
    1. 引数 url からカテゴリのルート (…/phonebook/M10037/) を導出し、
       対象 3 府県の入口ページ (…/M10037/{27,28,26}/) を組み立てる。
       ※ 先頭は引数 url の府県コードそのもの。別ルート URL はハードコードしない。
    2. 府県ページから市区町村ページ (…/M10037/{5桁コード}/) のリンクを列挙する。
       京都府のように市区町村リンクを持たず府県ページに直接一覧が載るケースがあるため、
       府県ページ自体に一覧テーブルがあればそれも巡回対象に含める。
    3. 各一覧ページのページ送り (2 ページ目以降は末尾に `2.html` 等) を最後まで辿る。
       一覧の表 (table.list-table) から 店舗名 / TEL / 詳細ページ URL を取得する。
    4. 詳細ページを 1 件取得するたびに即 yield する
       (途中中断に強い Pattern B / 早期 yield)。住所は詳細ページにのみ掲載される。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。別 URL はハードコードしない。
    - 都道府県が対象 3 府県以外の事業者は除外する (備考の指示)。
    - 詳細ページが取得できなかった場合は一覧の 店舗名 / TEL と
      府県コード由来の都道府県だけで出力し (住所は空文字)、取りこぼしを防ぐ。
    - 説明文などの自由記述は取得しない (著作権リスク回避)。掲載も無い。

実行方法:
    # ローカルテスト
    python scripts/sites/service/mapion.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id mapion
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 対象 3 府県 (JIS コード → 都道府県名)。備考「3府県以外は対象外」に対応。
_TARGET_PREFS = {
    "27": "大阪府",
    "28": "兵庫県",
    "26": "京都府",
}

# 住所先頭から都道府県を切り出すためのパターン
# ※ `.+?[都道府県]` のような貪欲でないパターンは「京都府」を「京都」で誤マッチするため列挙する
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

# 〒123-4567 形式の郵便番号 (住所から切り落とすために使用)
_POST_PATTERN = re.compile(r"〒?\s*\d{3}-?\d{4}")

# ページ送りリンク (…/27100/2.html) からページ番号を取り出す
_PAGE_NO_PATTERN = re.compile(r"/(\d+)\.html$")

# 暴走防止のページ送り上限 (1 ページ 30 件なので 100 ページ = 3,000 件)
_MAX_PAGES = 100


class Mapion(StaticCrawler):
    """マピオン電話帳 興信所・探偵事務所 (大阪府・兵庫県・京都府) スクレイパー"""

    DELAY = 1.0

    def parse(self, url: str):
        # カテゴリのルート (…/phonebook/M10037/) を引数 url から導出する
        cat_root = self._category_root(url)
        cat_path = urlparse(cat_root).path  # 例: /phonebook/M10037/

        seen_details: set[str] = set()

        for pref_code, pref_name in _TARGET_PREFS.items():
            pref_url = urljoin(cat_root, f"{pref_code}/")

            for list_url in self._collect_list_urls(pref_url, cat_path):
                for page_url, soup in self._iter_list_pages(list_url):
                    for name, tel, detail_url in self._extract_rows(page_url, soup, cat_path):
                        if detail_url in seen_details:
                            continue
                        seen_details.add(detail_url)

                        try:
                            item = self._build_item(detail_url, name, tel, pref_name)
                        except Exception as e:  # 個別ページの失敗は握りつぶして継続
                            self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                            continue

                        if item:
                            yield item

    # ------------------------------------------------------------------ #
    # URL 導出
    # ------------------------------------------------------------------ #
    @staticmethod
    def _category_root(url: str) -> str:
        """引数 url (…/phonebook/M10037/27/) からカテゴリルート (…/phonebook/M10037/) を導出する。"""
        base = url if url.endswith("/") else url + "/"
        # 末尾が府県コード等の数字ディレクトリなら 1 階層上がカテゴリルート
        if re.search(r"/\d+/$", base):
            return base.rsplit("/", 2)[0] + "/"
        return base

    def _collect_list_urls(self, pref_url: str, cat_path: str) -> list[str]:
        """府県ページから一覧ページ (市区町村ページ) の URL を列挙する。

        京都府のように市区町村リンクを持たず府県ページ自体が一覧になっている
        ケースがあるため、府県ページに一覧テーブルがあれば府県ページも含める。
        """
        soup = self.get_soup(pref_url)
        if soup is None:
            return []

        list_urls: list[str] = []
        seen: set[str] = set()

        # 府県ページ自体が一覧を持つ場合 (例: 京都府)
        if soup.select_one("table.list-table"):
            list_urls.append(pref_url)
            seen.add(pref_url)

        # 市区町村ページ (…/M10037/{5桁コード}/) のリンク
        city_pattern = re.compile(r"^" + re.escape(cat_path) + r"(\d+)/$")
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            path = urlparse(urljoin(pref_url, href)).path
            if not city_pattern.match(path):
                continue
            city_url = urljoin(pref_url, href)
            if city_url == pref_url or city_url in seen:
                continue
            seen.add(city_url)
            list_urls.append(city_url)

        return list_urls

    def _iter_list_pages(self, list_url: str):
        """一覧ページとそのページ送り (2.html, 3.html …) を順に取得して (URL, soup) を返す。"""
        base = list_url if list_url.endswith("/") else list_url + "/"
        page = 1
        max_page = 1

        while page <= max_page and page <= _MAX_PAGES:
            page_url = base if page == 1 else urljoin(base, f"{page}.html")
            soup = self.get_soup(page_url)
            if soup is None:
                return

            yield page_url, soup

            # ページ送りリンクから最大ページ番号を更新 (ウィンドウ表示に備え毎ページ確認)
            for a in soup.select("a.pagination-link[href]"):
                m = _PAGE_NO_PATTERN.search(a.get("href") or "")
                if m:
                    max_page = max(max_page, int(m.group(1)))
            page += 1

    # ------------------------------------------------------------------ #
    # 抽出
    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract_rows(page_url: str, soup, cat_path: str):
        """一覧テーブルから (店舗名, TEL, 詳細ページURL) を取り出す。"""
        detail_pattern = re.compile(r"^" + re.escape(cat_path) + r"\d+/\d+/$")

        for table in soup.select("table.list-table"):
            for tr in table.select("tr"):
                a = tr.select_one("a[href]")
                if a is None:
                    continue
                detail_url = urljoin(page_url, a.get("href") or "")
                if not detail_pattern.match(urlparse(detail_url).path):
                    continue

                name = a.get_text(strip=True)

                # TEL は行内の td のうち電話番号形式のもの (番号列・名称列を除く)
                tel = ""
                for td in tr.find_all("td"):
                    text = td.get_text(strip=True)
                    if re.fullmatch(r"[0-9][0-9\-()]{8,}", text):
                        tel = text
                        break

                yield name, tel, detail_url

    def _build_item(self, detail_url: str, list_name: str, list_tel: str, pref_name: str) -> dict | None:
        """詳細ページから住所等を取得して 1 件分の dict を組み立てる。"""
        soup = self.get_soup(detail_url)

        rows: dict[str, str] = {}
        if soup is not None:
            table = soup.select_one("table.spot-table-basic")
            if table:
                for tr in table.select("tr"):
                    th = tr.find("th")
                    td = tr.find("td")
                    if not (th and td):
                        continue
                    label = th.get_text(strip=True)
                    if label and label not in rows:
                        rows[label] = td.get_text(" ", strip=True)

        # 名称: 詳細ページ優先、無ければ一覧の店舗名
        name = rows.get("名称", "").strip() or list_name.strip()
        if not name:
            return None

        # 住所: 「〒530-0041 大阪府大阪市北区…」→ 都道府県と市区町村以降に分解
        addr_raw = rows.get("住所", "")
        addr = ""
        pref = ""
        if addr_raw:
            cleaned = _POST_PATTERN.sub("", addr_raw).strip()
            m = _PREF_PATTERN.match(cleaned)
            if m:
                pref = m.group(1)
                addr = cleaned[m.end():].strip()
            else:
                addr = cleaned

        # 都道府県が読めない場合は巡回中の府県コード由来の名称で補完する
        if not pref:
            pref = pref_name

        # 3 府県以外は対象外 (備考の指示)
        if pref not in _TARGET_PREFS.values():
            return None

        # 電話番号: 詳細ページ優先、無ければ一覧の TEL
        tel = rows.get("電話番号", "").strip() or list_tel.strip()

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.TEL: tel,
            Schema.ADDR: addr,
            Schema.PREF: pref,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Mapion()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を唯一の起点とし、配下 URL は urljoin で派生させる。
    scraper.execute("https://www.mapion.co.jp/phonebook/M10037/27/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
