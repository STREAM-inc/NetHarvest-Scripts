# scripts/sites/service/scrape_biz_training.py
"""
比較ビズ — 営業代行会社スクレイパー
対象URL: https://www.biz.ne.jp/list/sales-outsourcing/

取得対象 (一覧ページ):
    - 会社名 / 取得URL / 代表者名 / 住所 (都道府県抽出) / HP
    - サービス種類 / 個人・法人対応 / 成果報酬制度 / 会社特色 / 会社規模 / 得意業界

取得フロー:
    一覧ページ (datacnt=0, 10, 20…) → 1ページあたり10件をそのまま yield

実行方法:
    # ローカルテスト（全件）
    python scripts/sites/service/scrape_biz_training.py

    # Prefect Flow 経由（全件）
    python bin/run_flow.py --site-id biz_sales_outsourcing
"""

import re
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加（ローカル直接実行対応）
# scripts/sites/service/xxx.py → .parent×4 でプロジェクトルートを取得
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.biz.ne.jp/list/sales-outsourcing/"
ITEMS_PER_PAGE = 10

# 都道府県の正規表現パターン
_PREF_PATTERN = re.compile(
    r"^(北海道|(?:東京|大阪|京都|神奈川|愛知|兵庫|福岡|埼玉|千葉"
    r"|静岡|広島|宮城|茨城|新潟|栃木|群馬|長野|岐阜|福島|三重"
    r"|熊本|鹿児島|岡山|山口|愛媛|長崎|滋賀|奈良|沖縄|青森|岩手"
    r"|秋田|山形|富山|石川|福井|山梨|和歌山|鳥取|島根|香川|高知"
    r"|徳島|佐賀|大分|宮崎)都?道?府?県?)"
)


class BizSalesOutsourcingScraper(StaticCrawler):
    """比較ビズ 営業代行会社スクレイパー"""

    DELAY = 1.5  # サーバー負荷軽減（秒）
    EXTRA_COLUMNS = [
        "サービス種類",
        "個人・法人対応",
        "成果報酬制度",
        "会社特色",
        "会社規模",
        "得意業界",
    ]

    # ──────────────────────────────────────────────
    # ユーティリティ
    # ──────────────────────────────────────────────

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
        例: "東京都渋谷区〇〇1-2-3" → ("東京都", "渋谷区〇〇1-2-3")
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
        一覧ページをページネーションしながら全件を yield する。

        ページネーション形式:
            https://www.biz.ne.jp/list/sales-outsourcing/?datacnt=0#list
            https://www.biz.ne.jp/list/sales-outsourcing/?datacnt=10#list
            ...
        """
        # ── 初回ページで総件数を取得 ──
        first_soup = self.get_soup(BASE_URL)
        total = self._extract_total_count(first_soup)
        if total > 0:
            self.total_items = total
            self.logger.info("全件数: %d件", total)

        # ── ページングループ ──
        offset = 0
        scraped_count = 0
        consecutive_empty = 0

        while True:
            page_url = f"{BASE_URL}?datacnt={offset}#list"
            self.logger.info(
                "一覧ページ取得: offset=%d (累計取得済: %d件)", offset, scraped_count
            )

            if offset == 0:
                soup = first_soup  # 初回は再取得しない
            else:
                try:
                    soup = self.get_soup(page_url)
                except Exception as e:
                    self.logger.warning(
                        "一覧ページ offset=%d の取得に失敗しました: %s。スキップします。", offset, e
                    )
                    offset += ITEMS_PER_PAGE
                    continue

            # ── 各ボックスをパース ──
            boxes = soup.select("li.box")
            if not boxes:
                consecutive_empty += 1
                self.logger.warning(
                    "offset=%d に企業カードがありません (%d回連続)", offset, consecutive_empty
                )
                if consecutive_empty >= 3:
                    self.logger.info("連続3回データなしのため終了します。")
                    break
                offset += ITEMS_PER_PAGE
                continue

            consecutive_empty = 0
            for box in boxes:
                item = self._parse_box(box, page_url)
                if item:
                    scraped_count += 1
                    yield item

            # ── 終了判定 ──
            if total > 0 and offset + ITEMS_PER_PAGE >= total:
                self.logger.info("全 %d 件の取得が完了しました。", scraped_count)
                break

            offset += ITEMS_PER_PAGE

    # ──────────────────────────────────────────────
    # 総件数の取得
    # ──────────────────────────────────────────────

    def _extract_total_count(self, soup) -> int:
        """
        <span class="count_total">XXX</span> から全件数を取得する。
        取得できない場合は 0 を返す（ページング終了まで continuation される）。
        """
        # パターン1: span.count_total
        el = soup.select_one("span.count_total")
        if el:
            try:
                return int(self._clean(el.get_text()))
            except ValueError:
                pass

        # パターン2: 「XXX件中」テキスト
        m = re.search(r"(\d+)\s*件中", soup.get_text())
        if m:
            return int(m.group(1))

        return 0

    # ──────────────────────────────────────────────
    # 企業カードのパース
    # ──────────────────────────────────────────────

    def _parse_box(self, box, page_url: str) -> dict | None:
        """
        <li class="box"> を解析して1件分の dict を返す。

        HTML 構造 (基本情報):
            <div class="right_box">
                <div class="re_tl"><h3>会社名（aタグなし）</h3></div>
                <div class="result_outline flex_in">
                    <div class="right">
                        <div class="tl"><a href="https://www.biz.ne.jp/goods/?cid=...">サービス説明</a></div>
                        <ul class="sub">
                            <li class="address"><span class="svg_icon map"></span><span>住所</span></li>
                        </ul>
                    </div>
                </div>
                <div class="compare_area js-scrollable">
                    <ul class="flex_in clum_6 ...">…</ul>
                </div>
            </div>

        HTML 構造 (追加情報):
            <ul class="flex_in clum_6 syncscroll dragscroll">
                <li><dl><dt>サービス種類</dt><dd>テレアポ代行<br>…</dd></dl></li>
                …
            </ul>

        注意: 会社名は <h3> に直接テキストが入る（<a> タグなし）
              詳細URLは result_outline 内の .tl a タグのhref
        """
        item: dict = {}

        try:
            # ──── 会社名 ────
            # <div class="re_tl"><h3>会社名</h3></div>
            name_h3 = box.select_one(".re_tl h3")
            if not name_h3:
                return None
            item[Schema.NAME] = self._text(name_h3)
            if not item[Schema.NAME]:
                return None

            # ──── 詳細ページURL ────
            # <div class="result_outline"> > <div class="right"> > <div class="tl"> > <a href="...">
            tl_a = box.select_one(".result_outline .tl a")
            if tl_a:
                item[Schema.URL] = tl_a.get("href", "").strip() or page_url
            else:
                item[Schema.URL] = page_url

            # ──── 住所 → 都道府県を分割 ────
            addr_li = box.select_one("li.address")
            if addr_li:
                # svg_icon を除いた span のテキストを取得
                spans = addr_li.select("span:not(.svg_icon)")
                raw_addr = self._text(spans[0]) if spans else ""
                if raw_addr:
                    pref, addr = self._split_addr(raw_addr)
                    if pref:
                        item[Schema.PREF] = pref
                    item[Schema.ADDR] = addr or raw_addr

            # ──── flex_in ul からサービス情報を取得 ────
            # div.compare_area > ul.flex_in
            flex_ul = box.select_one("div.compare_area ul.flex_in")
            if flex_ul:
                self._parse_flex_info(flex_ul, item)

        except Exception as e:
            self.logger.warning("ボックス解析エラー: %s (%s)", item.get(Schema.NAME, "不明"), e)

        return item if item.get(Schema.NAME) else None

    def _parse_flex_info(self, flex_ul, item: dict) -> None:
        """
        <ul class="flex_in"> 内の dl/dt/dd を解析して item に格納する。

        <br> で区切られた複数値は「、」で結合して格納する。
        """
        for li in flex_ul.select("li"):
            dt = li.select_one("dt")
            dd = li.select_one("dd")
            if not dt or not dd:
                continue

            label = self._text(dt)

            # <br> タグを区切りとして複数値を取得
            # get_text(separator="、") で <br> 位置を「、」に変換
            raw = dd.get_text(separator="、", strip=True)
            # 末尾の余分な「、」を除去、連続する「、」を正規化
            value = re.sub(r"[、,]+$", "", re.sub(r"[、,]{2,}", "、", raw)).strip()

            if not value:
                continue

            if label == "サービス種類":
                item["サービス種類"] = value
            elif label == "個人・法人対応":
                item["個人・法人対応"] = value
            elif label == "成果報酬制度":
                item["成果報酬制度"] = value
            elif label == "会社特色":
                item["会社特色"] = value
            elif label == "会社規模":
                # tooltip の余分なテキストを除去
                clean_val = re.sub(r"会社規模.*?」", "", value, flags=re.DOTALL).strip()
                item["会社規模"] = clean_val or value
            elif label == "得意業界":
                item["得意業界"] = value


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BizSalesOutsourcingScraper()
    scraper.execute(BASE_URL)

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
