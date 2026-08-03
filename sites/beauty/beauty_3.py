"""
ホットペッパービューティー — サロン営業リスト用スクレイパー

取得対象:
    - 全国のサロンを 4 系統すべて巡回して取得する:
        1. ヘアサロン           (トップ /        詳細 /slnH{9桁}/)
        2. ネイル・まつげサロン  (/nail/         詳細 /kr/slnH{9桁}/)
        3. リラクサロン          (/relax/        詳細 /kr/slnH{9桁}/)
        4. エステサロン          (/esthe/        詳細 /kr/slnH{9桁}/)
    - 各レコードに「掲載ジャンル」(Schema.CAT_SITE) を巡回系統から必ず値ありで付与する。

取得フロー:
    ジャンルトップ (BASE[/prefix])
      └─ サービスエリア (/[prefix]svcSX/)
            └─ 中エリア (/[prefix]svcSX/macYY/)
                  └─ サロン一覧 (/[prefix]svcSX/macYY/salon/ + PN{n}.html ページング)
                        └─ サロン詳細 (/slnH.../ または /kr/slnH.../)  ← 会社概要表
                              └─ 電話番号ページ (詳細/tel/)          ← TEL は別ページ

    一覧→詳細 (Pattern B): 詳細を 1 件取得するごとに即 yield する。
    ネイル/リラク/エステは詳細が同一 /kr/ を共有するため、salon_id で詳細をキャッシュし
    別ジャンルで再出現した際は掲載ジャンルだけ差し替えて再利用する (通信削減)。

    robots.txt 厳守: 予約系・会員系 (/client/, /CLP/ ...) には一切アクセスしない。
    公開のエリア/一覧/詳細/tel ページのみを巡回する。
    (Disallow の唯一の tel ページ /slnH000041808/tel/ は明示的にスキップする)

    著作権配慮: アクセス・道案内 / こだわり条件 / 備考 / その他 のような長文の
    自由記述 (プロース) は取得しない。構造化された短い値のみを取得する。

実行方法:
    # ローカルテスト
    python scripts/sites/beauty/beauty_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id beauty_3
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class Beauty3Scraper(StaticCrawler):
    """ホットペッパービューティー 全ジャンル (ヘア/ネイル/リラク/エステ) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "サロンID",
        "スタッフ数",
        "カット価格",
    ]

    # 一覧ページのハードリミット (無限ループ防止)。
    MAX_PAGES_PER_LIST = 400

    # 巡回する 4 系統。prefix は BASE からの相対ナビ接頭辞 (エリア/一覧のパスに付く)。
    # 詳細ページはヘアが /slnH.../、他は /kr/slnH.../ で共有される。
    GENRES = [
        {"key": "hair", "label": "ヘアサロン", "prefix": ""},
        {"key": "nail", "label": "ネイル・まつげサロン", "prefix": "nail/"},
        {"key": "relax", "label": "リラクサロン", "prefix": "relax/"},
        {"key": "esthe", "label": "エステサロン", "prefix": "esthe/"},
    ]

    # svc エリアコード抽出 (SA〜SZ)
    _SVC_CODE_RE = re.compile(r"/svc(S[A-Z])/")
    # サロン詳細URL (ヘア: /slnH.../ , キレイ系: /kr/slnH.../) — クエリは無視してルート化
    _SALON_ROOT_RE = re.compile(
        r"^(https?://beauty\.hotpepper\.jp/(?:kr/)?(slnH\d+)/)"
    )
    # robots.txt が Disallow している唯一の tel ページ
    _ROBOTS_DENY_TEL = "/slnH000041808/tel/"

    # パンくずの「○○検索トップ」から業種を抜く
    _GYOSHU_RE = re.compile(r"(.+?)検索トップ$")

    # 都道府県抽出 (住所先頭)
    _PREF_RE = re.compile(
        r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
        r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
        r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
        r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
        r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
    )

    def parse(self, url: str) -> Generator[dict, None, None]:
        """4 系統 × エリア × 一覧 を巡回してサロン情報を yield する。

        url は sites.yml の正規 URL (https://beauty.hotpepper.jp/) を唯一の起点とする。
        """
        base = url if url.endswith("/") else url + "/"

        self.seen: set[str] = set()               # dedup キー "genre_key:salon_id"
        self._detail_cache: dict[str, dict] = {}   # salon_id -> 掲載ジャンル抜きの詳細 dict

        for genre in self.GENRES:
            genre_base = urljoin(base, genre["prefix"])
            self.logger.info("=== ジャンル巡回開始: %s (%s) ===", genre["label"], genre_base)

            for area_url in self._collect_service_areas(genre_base):
                for mac_url in self._collect_mac_areas(area_url, genre["prefix"]):
                    list_url = urljoin(mac_url, "salon/")
                    for salon_root, salon_id in self._iter_salon_urls(list_url):
                        dedup_key = f"{genre['key']}:{salon_id}"
                        if dedup_key in self.seen:
                            continue
                        self.seen.add(dedup_key)

                        try:
                            item = self._build_item(salon_root, salon_id, genre["label"])
                        except Exception as e:  # 個別サロンのエラーは握りつぶして継続
                            self.logger.warning(
                                "詳細取得エラー (スキップ): %s — %s", salon_root, e
                            )
                            self.error_count += 1
                            continue
                        if item:
                            yield item

    # ------------------------------------------------------------------ #
    # エリア収集
    # ------------------------------------------------------------------ #
    def _collect_service_areas(self, genre_base: str) -> list[str]:
        """ジャンルトップから svc エリアコード (SA〜SI) を集め、大エリアURLを構築する。"""
        soup = self.get_soup(genre_base)
        codes: set[str] = set()
        if soup is not None:
            for a in soup.select("a[href]"):
                m = self._SVC_CODE_RE.search(a.get("href", ""))
                if m:
                    codes.add(m.group(1))
        if not codes:
            # フォールバック: 全国大エリア SA〜SI
            codes = {f"S{c}" for c in "ABCDEFGHI"}
        return [urljoin(genre_base, f"svc{c}/") for c in sorted(codes)]

    def _collect_mac_areas(self, area_url: str, prefix: str) -> list[str]:
        """サービスエリアページから中エリア (/[prefix]svcSX/macYY/) を収集"""
        soup = self.get_soup(area_url)
        if soup is None:
            return []
        pat = re.compile(rf"^/{re.escape(prefix)}svcS[A-Z]/mac[A-Za-z0-9]+/$")
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            path = urlsplit(href).path
            if pat.match(path):
                full = urljoin(area_url, path)
                if full not in seen:
                    seen.add(full)
                    urls.append(full)
        return urls

    def _iter_salon_urls(self, list_url: str) -> Generator[tuple[str, str], None, None]:
        """サロン一覧をページング (/salon/ → PN2.html → ...) して (詳細ルート, salon_id) を yield"""
        prev_ids: tuple[str, ...] | None = None
        page = 1
        while page <= self.MAX_PAGES_PER_LIST:
            page_url = list_url if page == 1 else urljoin(list_url, f"PN{page}.html")
            soup = self.get_soup(page_url)
            if soup is None:
                break

            roots: list[tuple[str, str]] = []
            seen_on_page: set[str] = set()
            for a in soup.select("a[href]"):
                m = self._SALON_ROOT_RE.match(a.get("href", "").strip())
                if m:
                    root, sid = m.group(1), m.group(2)
                    if sid not in seen_on_page:
                        seen_on_page.add(sid)
                        roots.append((root, sid))

            if not roots:
                break

            ids = tuple(sid for _, sid in roots)
            if ids == prev_ids:
                # 超過ページが最終ページへリダイレクトされ同じ結果を返した → 終了
                break
            prev_ids = ids

            for r in roots:
                yield r

            # 「次のページ」があるか: PN{page+1}.html リンク or 「次」テキスト
            html = str(soup)
            has_next = (f"PN{page + 1}.html" in html) or any(
                "次" in a.get_text(strip=True) for a in soup.select("a[href]")
            )
            if not has_next:
                break
            page += 1
        else:
            self.logger.warning("ページ上限(%d)到達: %s", self.MAX_PAGES_PER_LIST, list_url)

    # ------------------------------------------------------------------ #
    # 詳細ページ解析
    # ------------------------------------------------------------------ #
    def _build_item(self, salon_root: str, salon_id: str, genre_label: str) -> dict | None:
        """詳細をキャッシュ経由で取得し、掲載ジャンルを付与して返す。"""
        cached = self._detail_cache.get(salon_id)
        if cached is None:
            cached = self._scrape_detail(salon_root, salon_id)
            if cached is None:
                return None
            self._detail_cache[salon_id] = cached
        item = dict(cached)
        item[Schema.CAT_SITE] = genre_label  # 掲載ジャンル = 巡回系統 (必ず値あり)
        return item

    def _scrape_detail(self, url: str, salon_id: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url, "サロンID": salon_id}

        # 名称 (h1)
        h1 = soup.find("h1")
        if h1:
            data[Schema.NAME] = h1.get_text(" ", strip=True)

        # フリガナ: detailTitle の 【...】 部分
        title_el = soup.select_one(".detailTitle, [class*=detailTitle]")
        if title_el:
            kana_m = re.search(r"【([^】]+)】", title_el.get_text(" ", strip=True))
            if kana_m:
                data[Schema.NAME_KANA] = kana_m.group(1).strip()

        # 業種: パンくずの「○○検索トップ」
        gyoshu = self._extract_gyoshu(soup)
        if gyoshu:
            data[Schema.CAT_LV1] = gyoshu

        # サロンデータ表: 「住所」th を含む table を特定 (ヘア/kr 両レイアウト対応)
        table = self._find_data_table(soup)
        if table:
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(" ", strip=True).replace("\xa0", " ").strip()

                if label == "住所":
                    self._set_address(data, value)
                elif label == "営業時間":
                    data[Schema.TIME] = self._strip_promo(value)
                elif label == "定休日":
                    data[Schema.HOLIDAY] = self._strip_promo(value)
                elif label == "支払い方法":
                    data[Schema.PAYMENTS] = self._strip_promo(value)
                elif label == "スタッフ数":
                    data["スタッフ数"] = value
                elif label == "カット価格":
                    data["カット価格"] = self._strip_promo(value)
                elif label in ("お店のホームページ", "ホームページ", "URL"):
                    a = td.find("a", href=True)
                    href = a["href"].strip() if a else ""
                    # 外部HPは JS で隠すことがある (javascript:void(0)) → http(s) のみ採用
                    if href.startswith("http"):
                        data[Schema.HP] = href
                # アクセス・道案内 / こだわり条件 / 備考 / その他 は長文/宣伝の自由記述のため
                # 著作権リスクで除外する

        # 電話番号は別ページ (/tel/) から取得
        tel = self._scrape_tel(url)
        if tel:
            data[Schema.TEL] = tel

        if not data.get(Schema.NAME) and not data.get(Schema.ADDR):
            return None
        return data

    def _find_data_table(self, soup):
        """th に「住所」を持つ table を返す (ヘア/kr 両レイアウト対応)。"""
        for table in soup.find_all("table"):
            for th in table.find_all("th"):
                if th.get_text(strip=True) == "住所":
                    return table
        return None

    def _extract_gyoshu(self, soup) -> str:
        bc = soup.select_one("[class*=pankuz]")
        if not bc:
            return ""
        for el in bc.find_all(["a", "span", "li"]):
            m = self._GYOSHU_RE.match(el.get_text(strip=True))
            if m:
                return m.group(1).strip()
        return ""

    def _scrape_tel(self, salon_root: str) -> str:
        """サロンの電話番号ページ (詳細ルート/tel/) から番号を取得"""
        tel_url = urljoin(salon_root, "tel/")
        if urlsplit(tel_url).path == self._ROBOTS_DENY_TEL:
            return ""  # robots.txt が Disallow している唯一の tel ページ
        soup = self.get_soup(tel_url)
        if soup is None:
            return ""
        for tr in soup.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td and th.get_text(strip=True) == "電話番号":
                return td.get_text(" ", strip=True).replace("\xa0", " ").strip()
        return ""

    def _set_address(self, data: dict, address: str) -> None:
        """住所を都道府県と残りに分割して格納"""
        address = address.strip()
        m = self._PREF_RE.match(address)
        if m:
            data[Schema.PREF] = m.group(1)
        data[Schema.ADDR] = address

    @staticmethod
    def _strip_promo(value: str) -> str:
        """営業時間/定休日/支払い方法 等に紛れ込む SEO キーワード羅列を除去する。

        HPB は 【...】 / ［...］ / [...] の括弧内に「渋谷/パラジェル/...」のような
        スラッシュ区切りの検索キーワード列を詰め込む。これらの括弧ブロックのみ除去し、
        全角スペースも半角に整える。実際の値 (営業時間・定休日・決済手段・価格) は残す。
        """
        value = re.sub(r"【[^】]*】", "", value)
        value = re.sub(r"［[^］]*］", "", value)
        value = re.sub(r"\[[^\]]*\]", "", value)
        return re.sub(r"[ 　]+", " ", value).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Beauty3Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://beauty.hotpepper.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
