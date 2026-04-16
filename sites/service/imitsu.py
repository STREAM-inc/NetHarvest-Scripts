# scripts/sites/service/imitsu_sales_agent.py
"""
PRONIアイミツ — 営業代行会社スクレイパー
対象URL: https://imitsu.jp/ct-sales-agent/search/

取得対象 (会社情報セクション):
    - 会社名 / 設立年 / 代表者名 / 従業員数 / 売上高 / 決算月
    - 主要取引先 / 住所 (都道府県抽出) / 会社URL / 会社概要

取得対象 (サービス情報セクション):
    - 創業年 / 対応可能な業務 / 得意とする案件規模 / 業種 など

取得対象 (EXTRA):
    - 実績・事例（タイトル + 金額 + 種類）
    - 対応可能な業務（リスト）
    - 得意とする案件規模（リスト）

取得フロー:
    一覧ページ (pn=1〜N) → 詳細ページURL収集 → 各詳細ページでデータ取得

実行方法:
    # ローカルテスト（テストページ数で上限あり）
    python scripts/sites/service/imitsu.py

    # Prefect Flow 経由（全件）
    python bin/run_flow.py --site-id imitsu
"""

import re
import sys
import time
from pathlib import Path

# プロジェクトルートを sys.path に追加（ローカル直接実行対応）
# scripts/sites/service/xxx.py → .parent×4 でプロジェクトルートを取得
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://imitsu.jp"
LIST_URL = "https://imitsu.jp/ct-sales-agent/search/"

# 都道府県の正規表現パターン
_PREF_PATTERN = re.compile(
    r"^(北海道|(?:東京|大阪|京都|神奈川|愛知|兵庫|福岡|埼玉|千葉"
    r"|静岡|広島|宮城|茨城|新潟|栃木|群馬|長野|岐阜|福島|三重"
    r"|熊本|鹿児島|岡山|山口|愛媛|長崎|滋賀|奈良|沖縄|青森|岩手"
    r"|秋田|山形|富山|石川|福井|山梨|和歌山|鳥取|島根|香川|高知"
    r"|徳島|佐賀|大分|宮崎)都?道?府?県?)"
)


class ImitsuSalesAgentScraper(DynamicCrawler):
    """PRONIアイミツ 営業代行会社スクレイパー"""

    DELAY = 2.0  # サーバー負荷軽減（秒）
    EXTRA_COLUMNS = ["実績・事例", "対応可能な業務", "得意とする案件規模", "得意な領域", "IT・SaaSフラグ"]

    # テスト用：None で全ページ取得、整数でページ数上限
    _max_pages: int | None = None

    # ──────────────────────────────────────────────
    # 内部ユーティリティ
    # ──────────────────────────────────────────────

    def _get_soup(self, url: str, is_list_page: bool = False) -> BeautifulSoup:
        """Playwright でページを読み込み BeautifulSoup を返す（モーダル除去付き）"""
        if is_list_page:
            # 一覧ページ: JS描画完了を確実に待つため networkidle を使用
            self.page.goto(url, wait_until="networkidle")
            # カードが出現するまで最大10秒待機
            try:
                self.page.wait_for_selector("a.service-title-link", timeout=10000)
            except Exception:
                pass  # タイムアウトしても続行（ログは呼び出し元で判定）
            time.sleep(2.5)
        else:
            # 詳細ページ: domcontentloaded で十分
            self.page.goto(url, wait_until="domcontentloaded")
            time.sleep(1.5)

        # ポップアップ・オーバーレイを除去してスクレイピングを安定化
        try:
            self.page.evaluate("""
                document.querySelectorAll(
                    '[class*="modal"], [class*="overlay"], [class*="popup"], [class*="dialog"]'
                ).forEach(el => el.remove());
            """)
        except Exception:
            pass
        return BeautifulSoup(self.page.content(), "html.parser")

    @staticmethod
    def _text(el) -> str:
        """BeautifulSoup 要素からテキストを取得（None安全）"""
        return el.get_text(strip=True) if el else ""

    @staticmethod
    def _clean(text: str) -> str:
        """連続スペース・改行を半角スペース1つに正規化"""
        return re.sub(r"\s+", " ", text).strip()

    def _split_addr(self, raw: str) -> tuple[str, str]:
        """
        住所文字列を都道府県と市区町村以降に分割する。
        例: "大阪市北区豊崎3-15-5 TKビル2F" → ("大阪府", "大阪市北区豊崎3-15-5 TKビル2F")
            "東京都港区麻布台1-8-10" → ("東京都", "港区麻布台1-8-10")
        """
        addr = self._clean(raw)
        m = _PREF_PATTERN.match(addr)
        if m:
            return m.group(1), addr[m.end():].strip()
        return "", addr

    # ──────────────────────────────────────────────
    # メインのパース処理
    # ──────────────────────────────────────────────

    def parse(self, url: str):
        """
        一覧ページをページネーションしながら全詳細ページURL を収集し、
        各詳細ページから1件ずつデータを取得して yield する。
        """
        page = 1
        max_page = None
        scraped_urls: set[str] = set()

        while True:
            # ──── 一覧ページ取得 ────
            list_url = f"{LIST_URL}?pn={page}"
            self.logger.info("一覧ページ取得: pn=%d (累計取得済: %d件)", page, len(scraped_urls))

            try:
                soup = self._get_soup(list_url, is_list_page=True)
            except Exception as e:
                self.logger.warning("一覧ページ %d の取得に失敗しました: %s。スキップします。", page, e)
                page += 1
                continue

            # ──── 初回: 全件数・最終ページ数を取得 ────
            if page == 1:
                total = self._extract_total_count(soup)
                if total:
                    self.total_items = total
                    self.logger.info("全件数: %d件", total)
                max_page = self._extract_max_page(soup)
                self.logger.info("最終ページ: %d", max_page)

            # ──── 一覧カードから詳細URLを収集 ────
            # 実際の HTML: <div class="list-box"> > <a class="service-title-link">
            # フォールバック: div.list-box を問わず a.service-title-link を直接取得
            links = soup.select("div.list-box a.service-title-link")
            if not links:
                links = soup.select("a.service-title-link")
            if not links:
                self.logger.warning("pn=%d に案件カードがありません。ループを終了します。", page)
                break

            detail_urls = []
            for link in links:
                href = link.get("href", "").strip()
                if not href:
                    continue
                detail_url = href if href.startswith("http") else BASE_URL + href
                if detail_url not in scraped_urls:
                    detail_urls.append(detail_url)

            self.logger.info("pn=%d: %d 件のURLを検出", page, len(detail_urls))

            # ──── 各詳細ページをスクレイピング ────
            for detail_url in detail_urls:
                if detail_url in scraped_urls:
                    continue
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        scraped_urls.add(detail_url)
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)

            # ──── ページ終了判定 ────
            if self._max_pages and page >= self._max_pages:
                self.logger.info("テスト用ページ上限 (%d) に達しました。", self._max_pages)
                break

            if max_page and page >= max_page:
                self.logger.info(
                    "最終ページ (%d) に達しました。全 %d 件処理完了。", max_page, len(scraped_urls)
                )
                break

            page += 1

    # ──────────────────────────────────────────────
    # ページネーション・件数取得
    # ──────────────────────────────────────────────

    def _extract_total_count(self, soup) -> int:
        """
        <span class="service-count-number">690</span> から全件数を取得する。
        """
        el = soup.select_one("span.service-count-number")
        if el:
            try:
                return int(self._clean(el.get_text()))
            except ValueError:
                pass
        return 0

    def _extract_max_page(self, soup) -> int:
        """ページネーションから最大ページ数を取得する"""
        max_page = 1

        # <a class="page-numbers-item-link"> のテキストから最大値
        for a in soup.select("a.page-numbers-item-link"):
            try:
                num = int(self._text(a))
                if num > max_page:
                    max_page = num
            except ValueError:
                pass

        # 数字リンクが取れない場合は href の pn= パラメータから
        if max_page == 1:
            for a in soup.select("a[href*='pn=']"):
                m = re.search(r"pn=(\d+)", a.get("href", ""))
                if m:
                    num = int(m.group(1))
                    if num > max_page:
                        max_page = num

        return max_page

    # ──────────────────────────────────────────────
    # 詳細ページのスクレイピング
    # ──────────────────────────────────────────────

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """
        1件の詳細ページから情報を取得して dict を返す。
        取得失敗または必須項目なしの場合は None を返す。
        """
        self.logger.info("詳細ページ取得: %s", detail_url)
        if self.DELAY > 0:
            time.sleep(self.DELAY)

        soup = self._get_soup(detail_url)

        item: dict = {Schema.URL: detail_url}

        # ──── 会社名 ────
        # ページのタイトル h1 は「サービス名のサービス詳細」等になる場合があるため
        # supplier-information-section の「会社名」dt を優先する
        self._parse_supplier_info(soup, item)

        # 会社情報から名前が取れなければ h1 をフォールバックに使う
        if not item.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                item[Schema.NAME] = self._text(h1)

        if not item.get(Schema.NAME):
            self.logger.warning("会社名が取得できませんでした: %s", detail_url)
            return None

        # ──── サービス情報セクション ────
        self._parse_service_info(soup, item)

        # ──── 実績・事例セクション ────
        self._parse_achievements(soup, item)

        # ──── 得意な領域セクション ────
        self._parse_favorite_section(soup, item)

        return item

    # ──────────────────────────────────────────────
    # 会社情報セクション
    # ──────────────────────────────────────────────

    def _parse_supplier_info(self, soup, item: dict) -> None:
        """
        「会社情報」セクション (section.supplier-information-section) を解析する。

        HTML 構造:
            <dl class="supplier-information-item">
                <dt>会社名</dt>
                <dd>株式会社RISING INNOVATION</dd>
            </dl>

        取得フィールド:
            会社名, 設立年, 代表者名, 従業員数, 売上高, 決算月,
            主要取引先, 住所, 会社URL, 会社概要
        """
        section = soup.select_one("section.supplier-information-section")
        if not section:
            return

        for dl in section.select("dl.supplier-information-item"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if not dt or not dd:
                continue

            label = self._text(dt)
            value = self._clean(dd.get_text(separator=" "))

            if label == "会社名":
                item[Schema.NAME] = value

            elif label == "住所":
                pref, addr = self._split_addr(value)
                if pref:
                    item[Schema.PREF] = pref
                if addr:
                    item[Schema.ADDR] = addr

            elif label == "会社URL":
                a_tag = dd.select_one("a[href]")
                item[Schema.HP] = a_tag.get("href", "").strip() if a_tag else value

            elif label == "設立年":
                # "2020年" → そのまま格納
                item[Schema.OPEN_DATE] = value

            elif label == "代表者名":
                item[Schema.REP_NM] = value

            elif label == "従業員数":
                item[Schema.EMP_NUM] = value

            elif label == "資本金":
                item[Schema.CAP] = value

            elif label == "会社概要":
                item[Schema.LOB] = value

    # ──────────────────────────────────────────────
    # 得意な領域セクション
    # ──────────────────────────────────────────────

    # IT・SaaS関連と判断するキーワード一覧
    _IT_SAAS_KEYWORDS = ["SaaS", "saas", "IT",]

    def _parse_favorite_section(self, soup, item: dict) -> None:
        """
        「得意な領域」セクション (section.service-favorite-section) を解析する。

        HTML 構造:
            <section id="service-XXXXX-favorite" class="service-favorite-section">
                <p class="service-sec-text-contents">
                    弊社の得意領域は、<br>
                    ・HR領域及び採用領域<br>
                    ・SaaS領域<br>
                    ・IT領域<br>
                </p>
            </section>

        出力:
            「得意な領域」EXTRA: "HR領域及び採用領域、SaaS領域、IT領域" （「・」行から「〜〜領域」を含む行のみ）
            「IT・SaaSフラグ」EXTRA: キーワードが含まれる場合は "○"、含まない場合は ""
        """
        sections = soup.select(
            "section.service-favorite-section, section[id$='-favorite']"
        )
        if not sections:
            return

        all_domains: list[str] = []
        all_raw_texts: list[str] = []

        for section in sections:
            p = section.select_one("p.service-sec-text-contents")
            if not p:
                continue

            # <br> タグを改行として扱い、行単位で処理
            raw = p.get_text(separator="\n")
            all_raw_texts.append(raw)

            for line in raw.splitlines():
                # 「・」で始まり「領域」を含む行のみ抽出
                stripped = line.strip().lstrip("・\u30fb")  # ・（全角中黒）を除去
                if "領域" in stripped and stripped:
                    all_domains.append(stripped)

        if not all_domains and not all_raw_texts:
            return

        # 「得意な領域」カラム: 「〜〜領域」行のカンマ区切り（取れなければ生テキスト）
        if all_domains:
            item["得意な領域"] = "、".join(all_domains)
        else:
            item["得意な領域"] = self._clean(" ".join(all_raw_texts))

        # IT・SaaS キーワードのいずれかが含まれているかチェック
        full_text = " ".join(all_raw_texts)
        item["IT・SaaSフラグ"] = "○" if any(
            kw in full_text for kw in self._IT_SAAS_KEYWORDS
        ) else ""

    # ──────────────────────────────────────────────
    # サービス情報セクション
    # ──────────────────────────────────────────────

    def _parse_service_info(self, soup, item: dict) -> None:
        """
        「サービス情報」セクション (div.service-information-content) を解析する。

        HTML 構造:
            <div class="service-information-content">
                <dl>
                    <div class="service-information-content-list-item">
                        <dt>対応可能な業務</dt>
                        <dd>
                            <ul class="fixable-business-list">
                                <li class=" "><span>テレアポ代行</span></li>
                                <li class=" default-hidden"><span>…</span></li>
                            </ul>
                        </dd>
                    </div>
                    <div class="service-information-content-list-item">
                        <dt>創業年</dt>
                        <dd>2019年</dd>
                    </div>
                </dl>
            </div>

        取得フィールド:
            対応可能な業務, 得意とする案件規模, 創業年, 業種, サービスURL など
        """
        # 複数のサービスセクションが存在する場合があるため全て走査する
        content_divs = soup.select("div.service-information-content")

        for content in content_divs:
            for list_item in content.select("div.service-information-content-list-item"):
                dt = list_item.select_one("dt")
                dd = list_item.select_one("dd")
                if not dt or not dd:
                    continue

                label = self._text(dt)

                if label == "対応可能な業務":
                    # default-hidden クラスも含めて全 span を取得する
                    spans = dd.select("ul.fixable-business-list li span")
                    texts = [self._text(sp) for sp in spans if self._text(sp)]
                    if texts:
                        item["対応可能な業務"] = "、".join(texts)

                elif label == "得意とする案件規模":
                    spans = dd.select("ul.fixable-business-list li span")
                    texts = [self._text(sp) for sp in spans if self._text(sp)]
                    if texts:
                        item["得意とする案件規模"] = "、".join(texts)

                elif label in ("サービスURL", "サービス URL"):
                    a_tag = dd.select_one("a[href]")
                    if a_tag:
                        hp = a_tag.get("href", "").strip()
                        # 会社URLが未取得の場合に限りサービスURLで代替
                        if not item.get(Schema.HP) and hp:
                            item[Schema.HP] = hp

                elif label == "業種":
                    item[Schema.CAT_SITE] = self._clean(dd.get_text(separator=" / "))

                elif label == "創業年":
                    # 設立年が未取得の場合に限り代替利用
                    if not item.get(Schema.OPEN_DATE):
                        item[Schema.OPEN_DATE] = self._clean(dd.get_text())

    # ──────────────────────────────────────────────
    # 実績・事例セクション
    # ──────────────────────────────────────────────

    def _parse_achievements(self, soup, item: dict) -> None:
        """
        「実績・事例」セクション (section.service-achievement-section) を解析する。

        HTML 構造:
            <dl class="service-achievement-detail-list">
                <div class="service-achievement-detail-list-item">
                    <dt>金額</dt><dd>101万円～300万円</dd>
                </div>
                <div class="service-achievement-detail-list-item">
                    <dt>種類</dt><dd>テレアポ代行</dd>
                </div>
            </dl>

        出力形式（「実績・事例」EXTRA カラム）:
            "金額:101万円～300万円・種類:テレアポ代行 ｜ 金額:31万円～50万円・種類:テレアポ代行 ｜ …"
        """
        sections = soup.select(
            "section.service-achievement-section, section[id$='-achievement']"
        )
        if not sections:
            return

        entries = []
        seen: set[str] = set()  # 全フィールド文字列で重複除外

        for section in sections:
            for block in section.select("div.service-achievement-sm-block"):
                # 金額と種類を取得する
                kinagaku = ""
                shurui = ""
                for detail_item in block.select(
                    "dl.service-achievement-detail-list div.service-achievement-detail-list-item"
                ):
                    dt = detail_item.select_one("dt")
                    dd = detail_item.select_one("dd")
                    if not dt or not dd:
                        continue
                    label = self._text(dt)
                    value = self._text(dd)
                    if label == "金額":
                        kinagaku = value
                    elif label == "種類":
                        shurui = value

                if not kinagaku and not shurui:
                    continue

                entry = f"金額:{kinagaku}・種類:{shurui}"
                if entry not in seen:
                    seen.add(entry)
                    entries.append(entry)

        if entries:
            item["実績・事例"] = " ｜ ".join(entries)


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ImitsuSalesAgentScraper()
    scraper._max_pages = 1  # テスト: 2ページ分 (最大40件)
    scraper.execute(LIST_URL)

    print("\n" + "=" * 40)
    print("📊 実行結果サマリ")
    print("=" * 40)
    print(f"  出力ファイル:   {scraper.output_filepath}")
    print(f"  取得件数:       {scraper.item_count}")
    print(f"  観測カラム数:   {len(scraper.observed_columns)}")
    print(f"  観測カラム:     {scraper.observed_columns}")
    print("=" * 40)

    if scraper.output_filepath:
        print("\n CSV 先頭5行:")
        print("-" * 40)
        with open(scraper.output_filepath, encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if i >= 6:
                    break
                print(line.rstrip())