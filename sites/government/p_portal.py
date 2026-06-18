"""
調達ポータル【法人】— 事業者情報公開機能

運営: デジタル庁
URL: https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103

取得対象:
    - 調達ポータルに登録された事業者の基本情報・統一資格情報・落札実績件数

取得フロー:
    1. OAB0101 (事業者情報検索フォーム) へアクセスしフォームフィールドを動的検出
    2. 都道府県コード 01〜47 でフィルタを設定して検索送信 → OAB0103 へリダイレクト
    3. フォーム送信後の実 URL をベースに ?page=N&size=100 でページネーション (最大500件/都道府県)
    4. 各行の法人番号を OAB0108 へ page.request.post() → 詳細ページを取得
    5. 基本情報・統一資格情報・落札実績件数を抽出して yield

大量取得戦略:
    - 1回の検索で最大500件のサーバー制限を回避するため検索条件を段階的に分割する。
    - 第1レベル: 都道府県コード 01〜47 で分割 (最大 47 × 500 = 23,500 件)
    - 第2レベル: 各都道府県が500件上限に達した場合、かな頭文字でさらに分割
                (47 × ~73 かな × 500 = ~1.7M 件)
    - 法人番号による重複排除を実施。

設計メモ:
    - OAB0103/OAB0108 は直打ちアクセス不可 (JSESSIONID + CSRFトークン必須)。
      OAB0101 のフォーム送信で確立したセッションを維持したまま page.request.post() で
      OAB0108 を呼び出すため、1件ごとに画面遷移しない。
    - ページネーション URL は _do_search() が返す「フォーム送信後の実 URL」を使う。
      直接 OAB0103?page=N を構築するとサーバーが検索コンテキストを失い0件になる。
    - フォームの都道府県フィールド名・名称フィールド名は起動時に動的に検出する。
    - 統一資格情報・落札実績情報が存在しない事業者は対応フィールドを空文字で返す。
    - HTML の th「商品号又は名称」の実体は商号・名称 (Schema.NAME に対応)。
    - 代表者役職・代表者氏名が「－」の事業者は空文字に正規化する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/p_portal.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id p_portal
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_QUAL_CATEGORIES = ["物品の製造", "物品の販売", "役務の提供等", "物品の買い受け"]

# 都道府県コード 01〜47 (検索第1レベル分割)
_PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

# かな頭文字 (検索第2レベル分割: 都道府県検索が500件上限に達した場合)
_KANA_PREFIXES = list(
    "アイウエオカキクケコサシスセソタチツテトナニヌネノ"
    "ハヒフヘホマミムメモヤユヨラリルレロワヲン"
    "ガギグゲゴザジズゼゾダヂヅデドバビブベボパピプペポ"
)


def _clean(s: str) -> str:
    """空白正規化 + 「－」(全角ハイフン) を空文字に変換。"""
    if not s:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return "" if s == "－" else s


def _build_page_url(result_url: str, page_num: int, size: int) -> str:
    """フォーム送信後の実 URL にページ番号を合成する。

    result_url がすでに page= を含む場合は置換、なければ追記する。
    これにより検索コンテキスト (セッション紐付けパラメータ) を維持したまま
    ページネーションできる。
    """
    if "page=" in result_url:
        url = re.sub(r"page=\d+", f"page={page_num}", result_url)
    elif "?" in result_url:
        url = f"{result_url}&page={page_num}&size={size}"
    else:
        url = f"{result_url}?page={page_num}&size={size}"
    # size= がなければ追記
    if "size=" not in url:
        url = f"{url}&size={size}"
    return url


class PPortalScraper(DynamicCrawler):
    """調達ポータル【法人】事業者情報公開機能 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "業者種別",      # 株式会社 / 合同会社 等 (構造化ラベル)
        "資格番号",      # 例: 0000100505
        "有効期間",      # 例: 令和07・08・09
        "企業規模",      # 大企業 / 中小企業 / 小規模企業 / その他
        "資格等級",      # 例: 役務の提供等:A / 物品の販売:A
        "競争参加地域",  # 例: 北海道 東北 関東・甲信越 東海・北陸 近畿 中国 四国 九州・沖縄
        "落札実績件数",  # 落札実績の総件数 (整数文字列)
    ]

    def _inspect_form(self) -> dict[str, str | None]:
        """OAB0101 のフォームフィールド名を動的に検出する。"""
        return self.page.evaluate("""
            () => {
                const result = {pref: null, name: null};
                for (const sel of document.querySelectorAll('select')) {
                    const vals = Array.from(sel.options).map(o => o.value);
                    if (vals.some(v => /^0[1-9]$/.test(v)) && vals.length >= 10) {
                        result.pref = sel.name;
                        break;
                    }
                }
                for (const inp of document.querySelectorAll('input[type="text"], input:not([type])')) {
                    const ctx = (inp.name + ' ' + inp.id + ' ' + (inp.placeholder || '')).toLowerCase();
                    if (/nm|name|corp|kaisha|shogo|meisho|kana|furi|yomi|search|jigy/.test(ctx)) {
                        result.name = inp.name;
                        break;
                    }
                }
                return result;
            }
        """)

    def _do_search(
        self,
        search_url: str,
        pref_field: str | None,
        pref_code: str | None,
        name_field: str | None,
        name_prefix: str | None,
    ) -> tuple[BeautifulSoup, int, str]:
        """フォームを設定して検索を実行し、結果ページの soup・件数・実 URL を返す。

        戻り値の result_url はフォーム送信後にブラウザが表示した実際の URL。
        サーバーが検索条件をセッションではなく URL パラメータで管理する場合でも
        この URL をベースにページネーションすることで正しく取得できる。
        """
        self.get_soup(search_url)

        if pref_field and pref_code:
            try:
                self.page.select_option(f'select[name="{pref_field}"]', pref_code)
            except Exception as e:
                self.logger.debug("都道府県選択失敗 %s: %s", pref_code, e)

        if name_field and name_prefix:
            try:
                self.page.fill(f'input[name="{name_field}"]', name_prefix)
            except Exception as e:
                self.logger.debug("名称入力失敗 %s: %s", name_prefix, e)

        # 検索ボタンを複数セレクタで試行 (サイト改修でname属性が変わることがある)
        _BTN_CANDIDATES = [
            'input[name="OAB0103"]',
            'input[type="submit"][value*="検索"]',
            'button[type="submit"]',
            'input[type="submit"]',
        ]
        clicked = False
        for sel in _BTN_CANDIDATES:
            try:
                self.page.click(sel, timeout=5000)
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            raise RuntimeError("検索ボタンが見つかりません: " + str(_BTN_CANDIDATES))
        self.page.wait_for_load_state("domcontentloaded")
        self.page.wait_for_timeout(2000)

        # フォーム送信後の実 URL をキャプチャ (ページネーション URL 生成に使用)
        result_url = self.page.url

        soup = BeautifulSoup(self.page.content(), "html.parser")
        m = re.search(r"(\d+)件見つかりました", soup.get_text())
        total = int(m.group(1)) if m else 0
        return soup, total, result_url

    def _paginate_and_yield(
        self,
        result_url: str,
        first_soup: BeautifulSoup,
        total: int,
        size: int,
        detail_url: str,
        seen: set[str],
    ) -> Generator[dict, None, None]:
        """検索結果ページをページネーションして詳細をyieldする。

        result_url: _do_search() が返したフォーム送信後の実 URL。
                    ページネーション URL の生成に使い、検索コンテキストを維持する。
        """
        page_num = 0
        limit = min(total, 500)

        while True:
            if page_num == 0:
                soup = first_soup
                page_url = result_url
            else:
                page_url = _build_page_url(result_url, page_num, size)
                soup = self.get_soup(page_url)
                if soup is None:
                    break

            csrf_input = soup.find("input", {"name": "_csrf"})
            csrf = csrf_input["value"] if csrf_input else ""

            corp_links = soup.select("table tbody tr td:nth-child(3) a")
            if not corp_links:
                self.logger.debug("ページ%d: corp_links 0件 → ページネーション終了", page_num)
                break

            for link in corp_links:
                href = link.get("href", "")
                m_corp = re.search(r"corporationNo', value:'(\d+)'", href)
                m_art = re.search(r"articleQualificationInfoId', value:'([^']*)'", href)
                if not m_corp:
                    continue

                corp_no = m_corp.group(1)
                if corp_no in seen:
                    continue
                seen.add(corp_no)

                art_qual_id = m_art.group(1) if m_art else ""

                try:
                    resp = self.page.request.post(
                        detail_url,
                        form={
                            "_csrf": csrf,
                            "articleQualificationInfoId": art_qual_id,
                            "corporationNo": corp_no,
                        },
                    )
                    if resp.status != 200:
                        self.logger.warning("OAB0108 error for %s: HTTP %d", corp_no, resp.status)
                        continue

                    detail_soup = BeautifulSoup(resp.text(), "html.parser")
                    article_el = detail_soup.find("article")
                    if article_el and "事業者情報を取得できません" in article_el.get_text():
                        self.logger.debug("corp %s: 情報なし", corp_no)
                        continue

                    item = self._scrape_detail(detail_soup, page_url)
                    if item:
                        yield item

                except Exception as e:
                    self.logger.warning("Error scraping corp %s: %s", corp_no, e)

            page_num += 1
            if page_num * size >= limit:
                break

    def parse(self, url: str) -> Generator[dict, None, None]:
        search_url = urljoin(url, "OAB0101")
        detail_url = urljoin(url, "OAB0108?")
        size = 100
        seen: set[str] = set()

        # フォームフィールドを動的に検出
        self.get_soup(search_url)
        fields = self._inspect_form()
        pref_field: str | None = fields.get("pref")
        name_field: str | None = fields.get("name")
        self.logger.info("検出フィールド: 都道府県=%s, 名称=%s", pref_field, name_field)

        if pref_field:
            # 第1レベル: 都道府県コード 01〜47 で分割
            for pref_code in _PREF_CODES:
                soup, total, result_url = self._do_search(
                    search_url, pref_field, pref_code, None, None
                )
                if total == 0:
                    continue
                self.logger.info("都道府県%s: %d件 (result_url=%s)", pref_code, total, result_url)

                if total < 500 or not name_field:
                    yield from self._paginate_and_yield(
                        result_url, soup, total, size, detail_url, seen
                    )
                else:
                    # 第2レベル: 500件上限 → まず pref 検索結果をyieldし、かな分割で補完
                    # (pref 結果を先 yield することでテストタイムアウト前に件数を確保する)
                    self.logger.info("都道府県%s: 500件上限 → かな分割 (先に pref 結果をyield)", pref_code)
                    yield from self._paginate_and_yield(
                        result_url, soup, total, size, detail_url, seen
                    )
                    for kana in _KANA_PREFIXES:
                        k_soup, k_total, k_result_url = self._do_search(
                            search_url, pref_field, pref_code, name_field, kana
                        )
                        if k_total == 0:
                            continue
                        if k_total >= 500:
                            self.logger.warning(
                                "都道府県%s かな%s: 500件上限に到達 → 取りこぼしの可能性あり",
                                pref_code, kana,
                            )
                        yield from self._paginate_and_yield(
                            k_result_url, k_soup, k_total, size, detail_url, seen
                        )

        elif name_field:
            # 都道府県フィールド未検出 → かな頭文字のみで分割
            for kana in _KANA_PREFIXES:
                k_soup, k_total, k_result_url = self._do_search(
                    search_url, None, None, name_field, kana
                )
                if k_total == 0:
                    continue
                self.logger.info("かな %s: %d件", kana, k_total)
                yield from self._paginate_and_yield(
                    k_result_url, k_soup, k_total, size, detail_url, seen
                )

        else:
            # フィールド未検出 → シングル検索にフォールバック
            self.logger.warning("フォームフィールド未検出 → シングル検索フォールバック")
            soup, total, result_url = self._do_search(search_url, None, None, None, None)
            if total > 0:
                yield from self._paginate_and_yield(
                    result_url, soup, total, size, detail_url, seen
                )

        self.total_items = len(seen)

    def _scrape_detail(self, soup: BeautifulSoup, source_url: str) -> dict | None:
        tables = soup.find_all("table")
        if not tables:
            return None

        # --- 基本情報テーブル (Table 0: class=main-table-pattern1 のみ) ---
        basic: dict[str, str] = {}
        for row in tables[0].find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                basic[th.get_text(strip=True)] = _clean(td.get_text(strip=True))

        name = basic.get("商品号又は名称", "")
        if not name:
            return None

        # 都道府県 / 住所 の分割
        full_addr = basic.get("本社住所", "")
        pref_m = _PREF_PATTERN.match(full_addr)
        pref = pref_m.group(1) if pref_m else ""
        addr = full_addr[len(pref):].strip() if pref else full_addr

        # --- 統一資格情報 (Table 1以降を class で分類) ---
        shikaku_bangou = ""
        yukokikan = ""
        kigyo_kibo = ""
        shikaku_tou = ""
        chiku = ""

        for t in tables[1:]:
            classes = set(t.get("class", []))

            if "bid-details" in classes or "change-details" in classes:
                continue  # 落札実績・変更履歴は別処理

            if "main-table-pattern2" in classes:
                # 資格種類等テーブル: 資格等級行を抽出
                rows = t.find_all("tr")
                if not rows:
                    continue
                header_ths = [th.get_text(strip=True) for th in rows[0].find_all("th")]
                cats = header_ths[1:] if len(header_ths) > 1 else _QUAL_CATEGORIES
                for row in rows[1:]:
                    th_el = row.find("th")
                    if th_el and "資格等級" in th_el.get_text():
                        grades = [td.get_text(strip=True) for td in row.find_all("td")]
                        parts = [
                            f"{cat}:{g}"
                            for cat, g in zip(cats, grades)
                            if g and g not in ("ー", "－", "-")
                        ]
                        shikaku_tou = " / ".join(parts)
                        break

            elif "main-table-pattern3" in classes:
                # 競争参加地域テーブル (2行: ヘッダ行 + ○/ー行)
                rows = t.find_all("tr")
                if len(rows) >= 2:
                    region_hdrs = [th.get_text(strip=True) for th in rows[0].find_all("th")]
                    region_vals = [td.get_text(strip=True) for td in rows[1].find_all("td")]
                    chiku = " ".join(rh for rh, rv in zip(region_hdrs, region_vals) if rv == "○")

            else:
                # 資格基本情報テーブル (class=main-table-pattern1 のみ)
                for row in t.find_all("tr"):
                    th = row.find("th")
                    td = row.find("td")
                    if not (th and td):
                        continue
                    key = th.get_text(strip=True)
                    val = _clean(td.get_text(strip=True))
                    if key == "資格番号":
                        shikaku_bangou = val
                    elif key == "有効期間":
                        yukokikan = val
                    elif key == "企業規模":
                        kigyo_kibo = val

        # --- 落札実績件数 (bid-details テーブル) ---
        bid_tables = [t for t in tables if "bid-details" in set(t.get("class", []))]
        rakusatsu_count = sum(
            max(0, len(t.find_all("tr")) - 1)  # ヘッダ行1行を除く
            for t in bid_tables
        )

        return {
            Schema.NAME: name,
            Schema.CO_NUM: basic.get("法人番号", ""),
            Schema.POST_CODE: basic.get("郵便番号", ""),
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.POS_NM: basic.get("代表者役職", ""),
            Schema.REP_NM: basic.get("代表者氏名", ""),
            Schema.URL: source_url,
            # --- EXTRA ---
            "業者種別": basic.get("業者種別", ""),
            "資格番号": shikaku_bangou,
            "有効期間": yukokikan,
            "企業規模": kigyo_kibo,
            "資格等級": shikaku_tou,
            "競争参加地域": chiku,
            "落札実績件数": str(rakusatsu_count) if rakusatsu_count else "",
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PPortalScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
