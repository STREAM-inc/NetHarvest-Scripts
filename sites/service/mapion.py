"""
マピオン電話帳 「興信所・探偵事務所」 (mapion) — 大阪府・兵庫県・京都府 3府県スクレイパー

対象サイト: https://www.mapion.co.jp/phonebook/M10037/27/

利用規約の確認結果 (重要):
    利用規約 (https://help.mapion.co.jp/mapionterms.html) 内に「クロール」の文字列が
    出現するが、これはヘルプサイト共通サイドバーのメニュー項目
    「地図表記・記号 / 標準地図（スクロール）」の "ス-クロール" が部分一致しているだけで、
    クロール/スクレイピングを禁止する条項ではない。
    規約本文に「スクレイピング」「クローリング」「ロボット」「自動収集」等の
    禁止条項は存在しない (禁止事項は第三者の権利侵害・改ざん・リバースエンジニアリング等の一般条項のみ)。
    → 過去にこの部分一致でクローラー生成を中断していたが誤検出のため、生成・実行を継続する。

取得対象:
    - マピオン電話帳のカテゴリ「興信所・探偵事務所」(M10037) に掲載されている
      大阪府 (27) / 兵庫県 (28) / 京都府 (26) の事業者
    - 取得カラムは 名称 / TEL / 住所 / 都道府県 の 4 列 (備考の指示どおり)

サイト構造 (live 実機で確認済み):
    - 府県ページ …/M10037/{2桁}/
        * 大阪府 (27) / 兵庫県 (28): 掲載のある市区町村ページへのリンクのみ (一覧テーブル無し)
        * 京都府 (26): 市区町村リンクが無く、府県ページ自体に一覧テーブルがある
    - 市区町村ページ …/M10037/{5桁}/ に一覧テーブル (table.list-table)
        * 1 行 = 連番 td + 店舗名 th(a) + TEL td (30 件/ページ)
        * ページ送りは <p class="pagination"> 内の a.pagination-link → …/{5桁}/2.html 形式
        * 政令市 (例: 大阪市 27100) の一覧には行政区コード配下の詳細 URL が混在する
        * 一覧行の詳細 URL は別カテゴリ (M10022 等) を指すことがある → 掲載されている以上取り込む
    - 詳細ページ …/phonebook/M{カテゴリ}/{5桁}/{spot_id}/
        * table.spot-table-basic の th/td (名称 / よみがな / 住所 / 地図 / 電話番号 …)
        * 住所は詳細ページにのみ掲載 (「〒530-0041」+ <br> + 住所)

取得フロー:
    1. 引数 url からカテゴリのルート (…/phonebook/M10037/) を導出し、
       対象 3 府県の入口ページ (…/M10037/{27,28,26}/) を組み立てる。
       ※ 先頭は引数 url の府県コードそのもの。別ルート URL はハードコードしない。
    2. 府県ページから一覧ページ (市区町村ページ) を列挙する。
       府県ページ自体に一覧テーブルがあれば府県ページも巡回対象に含める (京都府対応)。
    3. 各一覧ページのページ送りを最後まで辿り、店舗名 / TEL / 詳細ページ URL を取得する。
    4. 詳細ページを 1 件取得するたびに即 yield する
       (途中中断に強い Pattern B / 早期 yield)。

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

# 電話番号らしい文字列 (一覧テーブルの TEL 列判定用)
_TEL_PATTERN = re.compile(r"[0-9][0-9\-()]{8,}")

# 一覧のページ送りリンク (…/27100/2.html) からページ番号を取り出す
_PAGE_NO_PATTERN = re.compile(r"/(\d+)\.html$")

# 詳細ページのパス: /phonebook/M{カテゴリ}/{5桁市区町村}/{spot_id}/
# ※ 一覧行の詳細 URL は巡回中カテゴリ以外 (M10022 等) を指すことがあるためカテゴリは限定しない
_DETAIL_PATH_PATTERN = re.compile(r"^/phonebook/M\d+/\d{5}/\d+/$")

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
                    for name, tel, detail_url in self._extract_rows(page_url, soup):
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
            self.logger.warning("府県ページ取得失敗: %s", pref_url)
            return []

        list_urls: list[str] = []
        seen: set[str] = set()

        # 府県ページ自体が一覧を持つ場合 (例: 京都府)
        if soup.select_one("table.list-table"):
            list_urls.append(pref_url)
            seen.add(pref_url)

        # 市区町村ページ (…/M10037/{5桁コード}/) のリンク。掲載のある市区町村のみ張られる。
        city_pattern = re.compile(r"^" + re.escape(cat_path) + r"\d{5}/$")
        for a in soup.select("a[href]"):
            city_url = urljoin(pref_url, a.get("href") or "")
            if not city_pattern.match(urlparse(city_url).path):
                continue
            if city_url == pref_url or city_url in seen:
                continue
            seen.add(city_url)
            list_urls.append(city_url)

        self.logger.info("%s: 一覧ページ %d 件", pref_url, len(list_urls))
        return list_urls

    def _iter_list_pages(self, list_url: str):
        """一覧ページとそのページ送り (2.html, 3.html …) を順に取得して (URL, soup) を返す。"""
        base = list_url if list_url.endswith("/") else list_url + "/"
        base_path = urlparse(base).path
        page = 1
        max_page = 1

        while page <= max_page and page <= _MAX_PAGES:
            page_url = base if page == 1 else urljoin(base, f"{page}.html")
            soup = self.get_soup(page_url)
            if soup is None:
                return

            yield page_url, soup

            # ページ送りリンクから最大ページ番号を更新 (ウィンドウ表示に備え毎ページ確認)
            for a in soup.select("p.pagination a[href], a.pagination-link[href]"):
                href_path = urlparse(urljoin(page_url, a.get("href") or "")).path
                # 同じ一覧配下の …/{N}.html だけを対象にする
                if not href_path.startswith(base_path):
                    continue
                m = _PAGE_NO_PATTERN.search(href_path)
                if m:
                    max_page = max(max_page, int(m.group(1)))
            page += 1

    # ------------------------------------------------------------------ #
    # 抽出
    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract_rows(page_url: str, soup):
        """一覧テーブルから (店舗名, TEL, 詳細ページURL) を取り出す。"""
        for table in soup.select("table.list-table"):
            for tr in table.select("tr"):
                a = tr.select_one("a[href]")
                if a is None:  # thead 等
                    continue
                detail_url = urljoin(page_url, a.get("href") or "")
                if not _DETAIL_PATH_PATTERN.match(urlparse(detail_url).path):
                    continue

                # 店舗名は th > a (title 属性にも同じ値が入る)
                name = a.get_text(strip=True) or (a.get("title") or "").strip()

                # TEL は行内の td のうち電話番号形式のもの (連番列を除く)
                tel = ""
                for td in tr.find_all("td"):
                    text = td.get_text(strip=True)
                    if _TEL_PATTERN.fullmatch(text):
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
                    if label in ("", "地図", "モバイル") or label in rows:
                        continue  # 地図/モバイル欄は地図 JS を含むため取得しない
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
