"""
地方銀行フードセレクション 出展社一覧 (food-selection.com) スクレイパー

取得対象:
    - 開催年度 (2026年) の出展社 全 1,166 件
    - 会社名 / 都道府県 / 取扱商品カテゴリー / 公式HP / 初出展フラグ (備考の指定カラム)
    - 追加で 担当銀行・出展商品名・商品の分類・希望する販路先・業務用/小売用 を
      EXTRA カラムとして詳細ページから補完する

サイト構造 (Phase 1 調査結果):
    1. 一覧 https://www.food-selection.com/list.php
       - ページ送りは無く、1 ページに全出展社 (article.list_box × 1,166) を出力する。
         先頭の 1 ブロックだけは「1166件の結果」ヘッダで h4.list_ttl を持たないため除外。
       - 絞り込み (50音 kana[] / フリーワード word / 都道府県 area / 主催銀行 host /
         希望する販路先 / カテゴリー) は GET フォームだが、無指定の list.php が
         全件のスーパーセットになるため追加巡回は不要。
       - 一覧から取れる項目: 会社名 (span.name の直下テキスト)、都道府県 (span.city)、
         カテゴリー (span.cat 複数可・最大6)、公式HP (span.hp-btn の onclick window.open)、
         初出展 (span.first。hp-btn も first クラスを併せ持つので要除外)、
         出展商品名 (div.list_detail p.flx_item)。
    2. 詳細 detail.php?id=NNNNN — 1,166 件中 770 件のみリンクあり (残りは一覧情報のみ)。
       - 担当銀行 (span.bank) は詳細にしか無い。
       - カテゴリー・初出展・出展商品が一覧で空のとき詳細で補完できる。
       - 商品ごとの article.list_box > table.table_dashed に
         【商品の分類】【内容量】【保存温度帯】【業務用・小売用】【希望する販路先】
         【希望小売価格】の構造化ラベルがある。分類 / 販路先 / 業務用・小売用 を
         会社単位で重複除去して集約する。
    3. Static (requests + BeautifulSoup) で完結。JS レンダリング不要。

備考の遵守:
    - 指定された 会社名 / 都道府県 / 取扱商品カテゴリ / HP / 初出展フラグ を全て取得する。
    - ページ送り・絞り込みは存在しないため list.php 単体で全件を辿る。
    - 企業紹介文 (article.detail_box p.p) と商品説明文は長文の自由記述のため、
      著作権リスクを避けて取得しない。
    - 利用規約ページはサイト上に存在せず (privacy.html / youkou.html のみ)、
      スクレイピングを明示的に禁止する記述は確認されなかった。robots.txt は 404。

実行方法:
    python scripts/sites/corporate/food_selection_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id food_selection_2
"""

import copy
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# span.hp-btn の onclick="window.open('https://example.com/');return false;"
_HP_RE = re.compile(r"window\.open\(\s*['\"]([^'\"]+)['\"]")
# 一覧見出しの「1166件の結果」
_TOTAL_RE = re.compile(r"([0-9,]+)\s*件の結果")
# detail.php?id=29680
_ID_RE = re.compile(r"id=(\d+)")
# 詳細ページ商品テーブルの 【ラベル】値
_LABEL_RE = re.compile(r"^【([^】]+)】\s*(.*)$", re.S)
# 商品名が未登録のときのプレースホルダ
_PLACEHOLDER = "ただいま準備中です"
# 公式HP ボタンに「なし」等が入っている場合があるため URL らしさを判定する
_DOMAIN_RE = re.compile(r"^[\w.-]+\.[a-z]{2,}(?:[/:?#].*)?$", re.I)

# 会社単位に集約する商品テーブルのラベル -> EXTRA カラム名
_PRODUCT_LABELS = {
    "商品の分類": "商品の分類",
    "業務用・小売用": "業務用・小売用",
    "希望する販路先": "希望する販路先",
}


class FoodSelection2(StaticCrawler):
    """地方銀行フードセレクション 出展社一覧 スクレイパー"""

    DELAY = 0.3
    EXTRA_COLUMNS = [
        "初出展",
        "担当銀行",
        "出展商品",
        "出展商品数",
        "商品の分類",
        "業務用・小売用",
        "希望する販路先",
        "出展社ID",
    ]

    # ------------------------------------------------------------------
    # メイン処理: 一覧 1 ページ → (あれば) 詳細 → 1 件ずつ即 yield (Pattern B)
    # ------------------------------------------------------------------
    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            logger.error("一覧ページを取得できませんでした: %s", url)
            return

        m = _TOTAL_RE.search(soup.get_text(" ", strip=True))
        if m:
            self.total_items = int(m.group(1).replace(",", ""))
            logger.info("掲載件数: %s 件", self.total_items)

        boxes = soup.select("article.list_box")
        logger.info("一覧ブロック: %d 件 (ヘッダ含む)", len(boxes))

        seen: set[str] = set()
        for box in boxes:
            # 先頭の「N件の結果」ヘッダは h4.list_ttl を持たない
            if box.select_one("h4.list_ttl") is None:
                continue

            item = self._parse_list_box(box, url)
            if not item[Schema.NAME]:
                continue

            key = item["出展社ID"] or f"{item[Schema.NAME]}|{item[Schema.PREF]}"
            if key in seen:
                continue
            seen.add(key)

            detail_url = item[Schema.URL]
            if detail_url != url:
                self._enrich_from_detail(item, detail_url)

            yield item

    # ------------------------------------------------------------------
    # 一覧ブロックの解析
    # ------------------------------------------------------------------
    def _parse_list_box(self, box, list_url: str) -> dict:
        name = ""
        name_el = box.select_one("h4.list_ttl span.name")
        if name_el is not None:
            # span.city / span.first / span.hp-btn を除いた直下テキストが会社名
            clone = copy.copy(name_el)
            for sp in clone.select("span"):
                sp.decompose()
            name = clone.get_text(strip=True)

        city_el = box.select_one("h4.list_ttl span.city")
        pref = city_el.get_text(strip=True) if city_el is not None else ""

        cats = self._uniq(c.get_text(strip=True) for c in box.select("span.cat"))

        hp = ""
        hp_el = box.select_one("span.hp-btn")
        if hp_el is not None:
            m = _HP_RE.search(hp_el.get("onclick") or hp_el.get("onClick") or "")
            if m:
                hp = self._normalize_hp(m.group(1))

        # span.first は「初出展」バッジ。公式HP ボタンも first クラスを併せ持つため除外
        first_flag = ""
        for sp in box.select("h4.list_ttl span.first"):
            if "hp-btn" in (sp.get("class") or []):
                continue
            text = sp.get_text(strip=True)
            if text:
                first_flag = text
                break

        products = ""
        prod_el = box.select_one("div.list_detail p.flx_item")
        if prod_el is not None:
            text = prod_el.get_text(strip=True)
            if text and _PLACEHOLDER not in text:
                products = text

        page_url = list_url
        exhibitor_id = ""
        link = box.find("a", href=True)
        if link is not None and "detail.php" in link["href"]:
            page_url = urljoin(list_url, link["href"])
            m = _ID_RE.search(page_url)
            if m:
                exhibitor_id = m.group(1)

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.CAT_SITE: " / ".join(cats),
            Schema.HP: hp,
            Schema.URL: page_url,
            "初出展": first_flag,
            "担当銀行": "",
            "出展商品": products,
            "出展商品数": "",
            "商品の分類": "",
            "業務用・小売用": "",
            "希望する販路先": "",
            "出展社ID": exhibitor_id,
        }

    # ------------------------------------------------------------------
    # 詳細ページからの補完
    # ------------------------------------------------------------------
    def _enrich_from_detail(self, item: dict, detail_url: str) -> None:
        soup = self.get_soup(detail_url)
        if soup is None:
            logger.warning("詳細ページを取得できませんでした: %s", detail_url)
            return

        bank_el = soup.select_one("h2.sub_ttl span.bank")
        if bank_el is not None:
            item["担当銀行"] = re.sub(r"^担当銀行[：:]\s*", "", bank_el.get_text(strip=True))

        detail_box = soup.select_one("article.detail_box")

        if not item[Schema.CAT_SITE] and detail_box is not None:
            cats = self._uniq(c.get_text(strip=True) for c in detail_box.select("span.cat"))
            item[Schema.CAT_SITE] = " / ".join(cats)

        if not item["初出展"]:
            first_el = soup.select_one("h2.sub_ttl span.list_box span.first")
            if first_el is not None:
                item["初出展"] = first_el.get_text(strip=True)

        # 出展商品名 (詳細の目次リンク)
        prod_names = []
        if detail_box is not None:
            prod_names = self._uniq(
                a.get_text(strip=True) for a in detail_box.select("p.textlink a")
            )
        if prod_names:
            item["出展商品"] = "・".join(prod_names)

        # 商品ごとの構造化ラベル (article.list_box の table.table_dashed) を会社単位で集約
        product_boxes = soup.select("article.list_box")
        if product_boxes:
            item["出展商品数"] = str(len(product_boxes))

        collected: dict[str, list[str]] = {name: [] for name in _PRODUCT_LABELS.values()}
        for td in soup.select("article.list_box table.table_dashed td"):
            m = _LABEL_RE.match(td.get_text(" ", strip=True))
            if m is None:
                continue
            column = _PRODUCT_LABELS.get(m.group(1).strip())
            if column is None:
                continue
            for value in re.split(r"[　\s]+", m.group(2).strip()):
                value = value.strip()
                if value:
                    collected[column].append(value)

        for column, values in collected.items():
            if values:
                item[column] = " / ".join(self._uniq(values))

    # ------------------------------------------------------------------
    # ユーティリティ
    # ------------------------------------------------------------------
    @staticmethod
    def _uniq(values) -> list[str]:
        """空文字を除き、出現順を保ったまま重複を取り除く。"""
        result: list[str] = []
        for v in values:
            v = (v or "").strip()
            if v and v not in result:
                result.append(v)
        return result

    @staticmethod
    def _normalize_hp(value: str) -> str:
        """「なし」等の非 URL 値を除外し、スキーム無しのドメインには https:// を補う。"""
        value = (value or "").strip()
        if not value:
            return ""
        if re.match(r"^https?://", value, re.I):
            return value
        if _DOMAIN_RE.match(value):
            return f"https://{value}"
        return ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = FoodSelection2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.food-selection.com/list.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
