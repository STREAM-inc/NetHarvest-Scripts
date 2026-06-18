"""
調達ポータル【個人】 — 事業者情報公開機能 (個人)

運営: デジタル庁
URL: https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103

取得対象:
    - 調達ポータルに登録された個人事業者の基本情報・統一資格情報・落札実績件数

取得フロー:
    1. OAB0103 (sites.yml url) を受け取り、urljoin で OAB0101 (検索フォーム) を導出
    2. 業者種別「個人」を選択し、都道府県コード 01〜47 でフィルタを設定して検索送信
       → OAB0103 へリダイレクト (list_url = url)
    3. OAB0103?page=N&size=50 をページネーション (最大500件 / 10ページ / 都道府県)
    4. 各行のリンクパラメータを OAB0108 へ page.request.post() → 詳細ページを取得
    5. 基本情報・統一資格情報・落札実績件数を抽出して yield

大量取得戦略:
    - 第1レベル: 都道府県コード 01〜47 で分割
    - 第2レベル: 各都道府県が500件上限に達した場合、かな頭文字でさらに分割
    - 識別子による重複排除を実施。
    - 各都道府県の初回検索結果は必ず _paginate_and_yield で処理する (500件上限でも捨てない)。
      500件上限の場合はその後かな分割で追加取得を試みる。
    - かな分割中に連続 _MAX_KANA_FAIL 回 get_soup 失敗 (テストタイムアウト等) が発生した場合は
      その都道府県の残りかなをスキップして次の都道府県へ進む。

設計メモ:
    - URL = OAB0103 (法人版 p_portal.py と同じ)。parse(url) は urljoin でフォーム URL を導出する。
    - OAB0103/OAB0108 は直打ちアクセス不可 (JSESSIONID + CSRFトークン必須)。
      OAB0101 のフォーム送信で確立したセッションを維持したまま page.request.post() で
      OAB0108 を呼び出すため、1件ごとに画面遷移しない。
    - 個人事業者は法人番号を持たないため CO_NUM は空文字になる場合がある。
    - HTML の th「屋号又は名称」「氏名」等は Schema.NAME に対応。
    - 代表者役職・代表者氏名が「－」の事業者は空文字に正規化する。
    - _do_search() は get_soup() が None を返した場合 (テストタイムアウト等) に即 (None, 0) を返す。
      これにより Playwright 操作が無効なページ上で継続しなくなる。

実行方法:
    # ローカルテスト
    python scripts/sites/government/p_portal_4.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id p_portal_4
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

# 法人を示すキーワード — 名称に含まれていたら個人クローラーの対象外
_CORP_PATTERN = re.compile(
    r"(株式会社|有限会社|合同会社|合資会社|合名会社|"
    r"一般社団法人|一般財団法人|公益社団法人|公益財団法人|"
    r"医療法人|NPO法人|特定非営利活動法人|学校法人|"
    r"社会福祉法人|宗教法人|農業協同組合|消費生活協同組合|"
    r"信用金庫|信用組合|独立行政法人|国立大学法人|地方独立行政法人)"
)

_QUAL_CATEGORIES = ["物品の製造", "物品の販売", "役務の提供等", "物品の買い受け"]

_PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

_KANA_PREFIXES = list(
    "アイウエオカキクケコサシスセソタチツテトナニヌネノ"
    "ハヒフヘホマミムメモヤユヨラリルレロワヲン"
    "ガギグゲゴザジズゼゾダヂヅデドバビブベボパピプペポ"
)

# かな分割中にこの回数連続で get_soup 失敗 → その都道府県の残りかなをスキップ
_MAX_KANA_FAIL = 3


def _clean(s: str) -> str:
    """空白正規化 + 「－」(全角ハイフン) を空文字に変換。"""
    if not s:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return "" if s == "－" else s


class PPortal4Scraper(DynamicCrawler):
    """調達ポータル【個人】 事業者情報公開機能 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "業者種別",      # 個人 等 (構造化ラベル)
        "資格番号",      # 例: 0000100505
        "有効期間",      # 例: 令和07・08・09
        "企業規模",      # 大企業 / 中小企業 / 小規模企業 / その他
        "資格等級",      # 例: 役務の提供等:A / 物品の販売:A
        "競争参加地域",  # 例: 北海道 東北 関東・甲信越 東海・北陸 近畿 中国 四国 九州・沖縄
        "落札実績件数",  # 落札実績の総件数 (整数文字列)
    ]

    def _inspect_form(self) -> dict[str, str | None]:
        """OAB0101 のフォームフィールド名を動的に検出する。個人ラジオボタンも検出。"""
        return self.page.evaluate("""
            () => {
                const result = {pref: null, name: null, indivSelector: null};

                for (const label of document.querySelectorAll('label')) {
                    const lt = label.textContent.trim();
                    if (/個人/.test(lt) && !/法人/.test(lt)) {
                        const forAttr = label.getAttribute('for');
                        const inp = forAttr
                            ? document.getElementById(forAttr)
                            : label.querySelector('input[type="radio"], input[type="checkbox"]');
                        if (inp && inp.name) {
                            result.indivSelector = `input[name="${inp.name}"][value="${inp.value}"]`;
                            break;
                        }
                    }
                }
                if (!result.indivSelector) {
                    for (const inp of document.querySelectorAll('input[type="radio"], input[type="checkbox"]')) {
                        const parent = inp.closest('td, li, span, div') || inp.parentElement;
                        const txt = parent ? parent.textContent.trim() : '';
                        if (/個人/.test(txt) && !/法人/.test(txt) && inp.name) {
                            result.indivSelector = `input[name="${inp.name}"][value="${inp.value}"]`;
                            break;
                        }
                    }
                }

                for (const sel of document.querySelectorAll('select')) {
                    const vals = Array.from(sel.options).map(o => o.value);
                    if (vals.some(v => /^0[1-9]$/.test(v)) && vals.length >= 10) {
                        result.pref = sel.name;
                        break;
                    }
                }
                for (const inp of document.querySelectorAll('input[type="text"], input:not([type])')) {
                    const ctx = (inp.name + ' ' + inp.id).toLowerCase();
                    if (/nm|name|corp|kaisha|shogo|meisho/.test(ctx)) {
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
        indiv_selector: str | None,
    ) -> tuple[BeautifulSoup | None, int]:
        """フォームを設定して検索を実行し、結果ページの soup と件数を返す。"""
        soup = self.get_soup(search_url)
        if soup is None:
            return None, 0

        # 個人ラジオは CSS で display:none のため check()/click() は可視待ちで30秒
        # タイムアウトする。よって DOM 操作(JS)で直接選択する。さらに法人種別
        # (corporationCla) を残すと「個人を選択した場合、法人種別は指定できません」
        # で検索が弾かれ 0件になるため、必ず全解除する。
        try:
            self.page.evaluate(
                """
                (sel) => {
                    // 1) 個人ラジオを選択 (ハンドラも発火させる)
                    let el = sel ? document.querySelector(sel) : null;
                    if (!el) {
                        for (const inp of document.querySelectorAll('input[type=radio]')) {
                            const p = inp.closest('td,li,span,div,label') || inp.parentElement;
                            const t = p ? p.textContent.trim() : '';
                            if (/個人/.test(t) && !/法人/.test(t)) { el = inp; break; }
                        }
                    }
                    if (el) {
                        el.checked = true;
                        if (el.click) el.click();
                        el.dispatchEvent(new Event('change', {bubbles:true}));
                    }
                    // 2) 法人種別チェックボックスを全解除 (ハンドラ後に確実に外す)
                    document.querySelectorAll('input[name*="corporationCla"]').forEach(cb => {
                        cb.checked = false;
                        cb.dispatchEvent(new Event('change', {bubbles:true}));
                    });
                }
                """,
                indiv_selector,
            )
        except Exception as e:
            self.logger.debug("個人選択/法人種別解除 失敗: %s", e)

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

        try:
            self.page.click('input[name="OAB0102"]')
        except Exception:
            try:
                self.page.click('input[type="submit"], button[type="submit"]')
            except Exception as e:
                self.logger.debug("送信ボタン未検出→JS submit: %s", e)
                self.page.evaluate("document.querySelector('form').submit()")
        self.page.wait_for_load_state("domcontentloaded")
        self.page.wait_for_timeout(2000)

        result_soup = BeautifulSoup(self.page.content(), "html.parser")
        text = result_soup.get_text()
        m = (
            re.search(r"(\d[\d,]*)\s*件見つかりました", text)
            or re.search(r"全\s*(\d[\d,]*)\s*件", text)
            or re.search(r"(\d[\d,]*)\s*件中", text)
            or re.search(r"(\d[\d,]*)\s*件のデータ", text)
        )
        if m:
            total = int(m.group(1).replace(",", ""))
        else:
            # 件数テキストが取得できなくても結果リンクがあれば1ページ分試みる
            has_links = bool(result_soup.select("table tbody tr td a"))
            total = 50 if has_links else 0
            if has_links:
                self.logger.debug("件数テキスト未検出だが結果リンクあり → total=50 で続行")
        return result_soup, total

    def _paginate_and_yield(
        self,
        list_url: str,
        first_soup: BeautifulSoup,
        total: int,
        size: int,
        detail_url: str,
        seen: set[str],
    ) -> Generator[dict, None, None]:
        """検索結果ページをページネーションして詳細をyieldする。"""
        page_num = 0
        # total=0 (件数テキスト未検出) でも最低1ページは試みる
        limit = min(total, 500) if total > 0 else size

        while True:
            if page_num == 0:
                soup = first_soup
            else:
                soup = self.get_soup(f"{list_url}?page={page_num}&size={size}")
                if soup is None:
                    break

            csrf_input = soup.find("input", {"name": "_csrf"})
            csrf = csrf_input["value"] if csrf_input else ""

            corp_links = (
                soup.select("table tbody tr td:nth-child(3) a")
                or soup.select("table tbody tr td:nth-child(2) a")
                or soup.select("table tbody tr td:nth-child(1) a")
                or soup.select("table tbody tr td a")
            )
            if not corp_links:
                break

            for link in corp_links:
                href = link.get("href", "")
                params = {m.group(1): m.group(2) for m in re.finditer(r"'([^']+)', value:'([^']*)'", href)}
                if not params:
                    continue

                entity_id = (
                    params.get("corporationNo")
                    or params.get("individualNo")
                    or next(iter(params.values()), "")
                )
                if not entity_id or entity_id in seen:
                    continue
                seen.add(entity_id)

                form_data = {"_csrf": csrf}
                form_data.update(params)

                try:
                    resp = self.page.request.post(detail_url, form=form_data)
                    if resp.status != 200:
                        self.logger.warning("OAB0108 error for %s: HTTP %d", entity_id, resp.status)
                        continue

                    detail_soup = BeautifulSoup(resp.text(), "html.parser")
                    article_el = detail_soup.find("article")
                    if article_el and "事業者情報を取得できません" in article_el.get_text():
                        self.logger.debug("entity %s: 情報なし", entity_id)
                        continue

                    item = self._scrape_detail(detail_soup, f"{list_url}?page={page_num}&size={size}")
                    if item:
                        yield item

                except Exception as e:
                    self.logger.warning("Error scraping entity %s: %s", entity_id, e)

            page_num += 1
            if page_num * size >= limit:
                break

    def parse(self, url: str) -> Generator[dict, None, None]:
        # url = OAB0103 (sites.yml の url)。検索フォームは OAB0101 を urljoin で導出。
        search_url = urljoin(url, "OAB0101")
        list_url = url  # OAB0103
        detail_url = urljoin(url, "OAB0108")
        size = 50
        seen: set[str] = set()

        init_soup = self.get_soup(search_url)
        if init_soup is None:
            self.logger.warning("OAB0101 へのアクセス失敗 → 終了")
            return

        fields = self._inspect_form()
        pref_field: str | None = fields.get("pref")
        name_field: str | None = fields.get("name")
        indiv_selector: str | None = fields.get("indivSelector")
        self.logger.info(
            "検出フィールド: 都道府県=%s, 名称=%s, 個人=%s",
            pref_field, name_field, indiv_selector,
        )

        if pref_field:
            for pref_code in _PREF_CODES:
                soup, total = self._do_search(
                    search_url, pref_field, pref_code, None, None, indiv_selector
                )
                if soup is None or total == 0:
                    continue
                self.logger.info("都道府県%s: %d件", pref_code, total)

                # ★ 500件上限でも初回結果は必ず処理する
                yield from self._paginate_and_yield(
                    list_url, soup, total, size, detail_url, seen
                )

                if total >= 500 and name_field:
                    # 500件上限 → かな頭文字で分割して追加取得。連続失敗 _MAX_KANA_FAIL 回で残りをスキップ。
                    self.logger.info("都道府県%s: 500件上限 → かな分割で追加取得", pref_code)
                    kana_fail = 0
                    for kana in _KANA_PREFIXES:
                        k_soup, k_total = self._do_search(
                            search_url, pref_field, pref_code, name_field, kana, indiv_selector
                        )
                        if k_soup is None:
                            kana_fail += 1
                            if kana_fail >= _MAX_KANA_FAIL:
                                self.logger.warning(
                                    "都道府県%s: かな検索連続%d回失敗 → 残りのかなをスキップ",
                                    pref_code, kana_fail,
                                )
                                break
                            continue
                        kana_fail = 0
                        if k_total == 0 and not k_soup.select("table tbody tr td a"):
                            continue
                        yield from self._paginate_and_yield(
                            list_url, k_soup, k_total, size, detail_url, seen
                        )

        elif name_field:
            kana_fail = 0
            for kana in _KANA_PREFIXES:
                k_soup, k_total = self._do_search(
                    search_url, None, None, name_field, kana, indiv_selector
                )
                if k_soup is None:
                    kana_fail += 1
                    if kana_fail >= _MAX_KANA_FAIL:
                        self.logger.warning("かな検索連続%d回失敗 → 停止", kana_fail)
                        break
                    continue
                kana_fail = 0
                if k_total == 0 and not k_soup.select("table tbody tr td a"):
                    continue
                self.logger.info("かな %s: %d件", kana, k_total)
                yield from self._paginate_and_yield(
                    list_url, k_soup, k_total, size, detail_url, seen
                )

        else:
            self.logger.warning("フォームフィールド未検出 → シングル検索フォールバック")
            soup, total = self._do_search(
                search_url, None, None, None, None, indiv_selector
            )
            if soup is not None and (total > 0 or soup.select("table tbody tr td a")):
                yield from self._paginate_and_yield(
                    list_url, soup, total, size, detail_url, seen
                )

        self.total_items = len(seen)

    def _scrape_detail(self, soup: BeautifulSoup, source_url: str) -> dict | None:
        tables = soup.find_all("table")
        if not tables:
            return None

        basic: dict[str, str] = {}
        for row in tables[0].find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                basic[th.get_text(strip=True)] = _clean(td.get_text(strip=True))

        name = (
            basic.get("商号又は名称")
            or basic.get("商品号又は名称")
            or basic.get("屋号又は名称")
            or basic.get("氏名")
            or ""
        )
        if not name:
            return None

        # 法人が検索結果に混入した場合はスキップ (業者種別優先 → 法人名フォールバック)
        gyosha_shubetsu = basic.get("業者種別", "")
        if gyosha_shubetsu and "個人" not in gyosha_shubetsu:
            self.logger.debug("業者種別が個人でない → スキップ: %s (%s)", name, gyosha_shubetsu)
            return None
        if _CORP_PATTERN.search(name):
            self.logger.debug("法人名を含む → スキップ: %s", name)
            return None

        full_addr = basic.get("本社住所", "")
        pref_m = _PREF_PATTERN.match(full_addr)
        pref = pref_m.group(1) if pref_m else ""
        addr = full_addr[len(pref):].strip() if pref else full_addr

        shikaku_bangou = ""
        yukokikan = ""
        kigyo_kibo = ""
        shikaku_tou = ""
        chiku = ""

        for t in tables[1:]:
            classes = set(t.get("class", []))

            if "bid-details" in classes or "change-details" in classes:
                continue

            if "main-table-pattern2" in classes:
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
                rows = t.find_all("tr")
                if len(rows) >= 2:
                    region_hdrs = [th.get_text(strip=True) for th in rows[0].find_all("th")]
                    region_vals = [td.get_text(strip=True) for td in rows[1].find_all("td")]
                    chiku = " ".join(rh for rh, rv in zip(region_hdrs, region_vals) if rv == "○")

            else:
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

        bid_tables = [t for t in tables if "bid-details" in set(t.get("class", []))]
        rakusatsu_count = sum(
            max(0, len(t.find_all("tr")) - 1)
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
            "業者種別": gyosha_shubetsu,
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

    scraper = PPortal4Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
