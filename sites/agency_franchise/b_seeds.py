"""
代理店募集.com (b-seeds.com) — 企業情報スクレイパー

取得対象:
    - 商材カテゴリ (サイドバー「商材で探す」配下 40 ターム) から
      各企業詳細ページ (WordPress 単一投稿) の企業情報テーブルを取得
    - 名称, 住所, 代表者, 設立, 事業内容, 商材

取得フロー:
    商材カテゴリ一覧 (親9 + 子31) → /business/product/{slug}/page/N/
      → .check-matter__list > a の詳細 URL 収集 (カテゴリ名を付与)
      → デデュープ (同一企業が複数カテゴリに出る場合は商材を","結合)
      → 詳細ページ table.detail-table を解析

実行方法:
    python scripts/sites/agency_franchise/b_seeds.py
    python bin/run_flow.py --site-id b_seeds
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://b-seeds.com"

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# サイドバー「商材で探す」モーダルから取得した 40 カテゴリ
# key = カテゴリ URL (slug), value = 表示名 (商材ラベル)
CATEGORIES: dict[str, str] = {
    # AI / IT / DX / SaaS
    "/business/product/aiit": "AI / IT / DX / SaaS",
    "/business/product/aiit/ai": "AI",
    "/business/product/aiit/dxefficiency": "DX・業務効率",
    "/business/product/aiit/iot": "IoT",
    "/business/product/aiit/security": "セキュリティ",
    "/business/product/aiit/payments": "決済システム",
    "/business/product/aiit/pc": "パソコン",
    # 通信 / 携帯 / 光回線
    "/business/product/telecom-type": "通信 / 携帯 / 光回線",
    "/business/product/telecom-type/fiber": "光回線",
    "/business/product/telecom-type/mobileline": "携帯・回線",
    "/business/product/telecom-type/wifi": "Wi-Fi",
    "/business/product/telecom-type/othertelecom": "その他通信関連",
    # 広告 / 集客 / Web制作
    "/business/product/marketing": "広告 / 集客 / Web制作",
    "/business/product/marketing/digitalseo": "デジタルマーケ / SEO / MEO",
    "/business/product/marketing/webdesign": "Web制作",
    "/business/product/marketing/leadgen": "集客・新規開拓",
    # 住宅・エネルギー
    "/business/product/energyhome": "住宅・エネルギー",
    "/business/product/energyhome/newpower": "新電力・ガス",
    "/business/product/energyhome/solar": "太陽光・蓄電池",
    # 金融・保険
    "/business/product/finance": "金融・保険",
    "/business/product/finance/insurance": "保険",
    "/business/product/finance/investment": "投資",
    # 美容 / 健康
    "/business/product/beautyhealth-type": "美容 / 健康",
    "/business/product/beautyhealth-type/cosmetics-beautyhealth-type": "化粧品",
    "/business/product/beautyhealth-type/healthproducts": "健康商材",
    # 飲料 / 食品
    "/business/product/food": "飲料 / 食品",
    "/business/product/food/beverage": "飲料・ウォーターサーバー",
    "/business/product/food/food-food": "食品",
    # その他商材
    "/business/product/others": "その他商材",
    "/business/product/others/service": "サービス",
    "/business/product/others/fashion": "ファッション",
    "/business/product/others/recruitment": "求人・採用",
    "/business/product/others/delivery": "デリバリー",
    "/business/product/others/ecoenergy": "エコ・省エネ",
    "/business/product/others/covid": "コロナ対策",
    "/business/product/others/automotive": "自動車関連",
    "/business/product/others/building": "建材・床材・屋根修理",
    "/business/product/others/costdown": "コスト削減・節税",
    "/business/product/others/subsidy-btob": "補助金・助成金",
    "/business/product/others/non": "その他",
}


class BSeedsScraper(StaticCrawler):
    """代理店募集.com 企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        # Step 1: 全カテゴリを巡回して 企業詳細 URL → 商材ラベル集合 を作る
        detail_to_products = self._collect_detail_urls()
        self.total_items = len(detail_to_products)
        self.logger.info("企業詳細URL 収集完了: %d 件", len(detail_to_products))

        # Step 2: 各詳細ページを取得
        for detail_url, product_labels in detail_to_products.items():
            try:
                item = self._scrape_detail(detail_url, product_labels)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得失敗: %s (%s)", detail_url, e)
                continue

    # ------------------------------------------------------------------
    # Step 1: カテゴリ別一覧を全ページネーションして詳細 URL + 商材ラベルを収集
    # ------------------------------------------------------------------
    def _collect_detail_urls(self) -> dict[str, list[str]]:
        """{detail_url: [商材ラベル, ...]} を返す (同一企業は複数ラベル統合)"""
        detail_to_products: dict[str, list[str]] = {}

        for slug, label in CATEGORIES.items():
            base = urljoin(BASE_URL, slug.rstrip("/") + "/")
            self.logger.info("カテゴリ走査: [%s] %s", label, base)

            page = 1
            while True:
                list_url = base if page == 1 else f"{base}page/{page}/"
                try:
                    soup = self.get_soup(list_url)
                except Exception as e:
                    self.logger.info("一覧ページなし (打切り): %s (%s)", list_url, e)
                    break

                items = soup.select("ul.check-matter li.check-matter__list")
                if not items:
                    break

                found_this_page = 0
                for li in items:
                    a = li.select_one('a[href^="https://b-seeds.com/"]')
                    if not a:
                        continue
                    href = a.get("href", "").strip()
                    # 自己参照/コンテンツ外を除外
                    if not href or "/request-list" in href or "/wp-content/" in href:
                        continue
                    # 商材詳細と思われる企業スラッグのみ対象
                    # (例: /wu-aiseminar, /ns-aiachool 等)
                    if "/business/" in href:
                        continue
                    labels = detail_to_products.setdefault(href, [])
                    if label not in labels:
                        labels.append(label)
                    found_this_page += 1

                if found_this_page == 0:
                    break
                # WordPress ページネーションは 20件/page 固定。20件未満なら最終
                if len(items) < 20:
                    break
                page += 1

        return detail_to_products

    # ------------------------------------------------------------------
    # Step 2: 詳細ページ (table.detail-table) から企業情報を抽出
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str, product_labels: list[str]) -> dict | None:
        soup = self.get_soup(url)

        # 企業名を含む最初の .detail-table を特定
        target_table = None
        for table in soup.select("table.detail-table"):
            if any(th.get_text(strip=True) == "企業名" for th in table.select("th")):
                target_table = table
                break
        if target_table is None:
            return None

        pairs: dict[str, str] = {}
        for tr in target_table.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            pairs[th.get_text(strip=True)] = td.get_text("\n", strip=True)

        name = pairs.get("企業名", "").strip()
        if not name:
            return None

        item = {
            Schema.URL: url,
            Schema.NAME: name,
        }

        self._set_address(item, pairs.get("所在地", ""))
        self._set_representative(item, pairs.get("代表者", ""))

        if pairs.get("設立"):
            item[Schema.OPEN_DATE] = pairs["設立"].strip()
        if pairs.get("事業内容"):
            item[Schema.LOB] = pairs["事業内容"].strip()

        if product_labels:
            item[Schema.CAT_SITE] = ",".join(product_labels)

        return item

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _set_address(self, item: dict, address: str) -> None:
        address = (address or "").strip()
        if not address:
            return
        zip_match = re.search(r"〒?\s*(\d{3}-?\d{4})", address)
        if zip_match:
            item[Schema.POST_CODE] = zip_match.group(1)
            address = address.replace(zip_match.group(0), "").strip()
        address = re.sub(r"\s+", " ", address).strip()
        pm = _PREF_PATTERN.match(address)
        if pm:
            item[Schema.PREF] = pm.group(1)
            item[Schema.ADDR] = address[pm.end():].strip()
        else:
            item[Schema.ADDR] = address

    def _set_representative(self, item: dict, rep_text: str) -> None:
        rep_text = (rep_text or "").strip()
        if not rep_text:
            return
        # 例: "代表取締役社長　白石 崇" / "代表理事　亀山 浩樹"
        m = re.match(
            r"^(代表取締役社長|代表取締役|代表理事|取締役|会長|社長|CEO|オーナー|理事長|代表)\s*(.+)$",
            rep_text,
        )
        if m:
            item[Schema.POS_NM] = m.group(1).strip()
            item[Schema.REP_NM] = m.group(2).strip()
        else:
            item[Schema.REP_NM] = rep_text


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BSeedsScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
