"""
DOMO NET (domonet.jp) — アルバイト・パート求人サイト スクレイパー

取得対象:
    - 詳細ページ (table.contents_single_table) の th/td ペア
        社名 / 事業内容 / 所在地 / URL / お問い合わせ先(TEL) /
        お仕事内容 / 給与 / 勤務地 / 時間・勤務日 / 休日・休暇 /
        最寄駅 / 資格 / 待遇 / 期間 / 応募方法 / 応募後のプロセス / その他
    - 一覧ページ (.searchList_Box h3) の 求人タイトル (h2 from detail)

取得フロー:
    ルート URL (https://domonet.jp/) → 5 地域 (/kanto /shizuoka /nagoya /kansai /pado) の /list
    → ページネーション (?page=N) → 詳細ページリンク収集 → 各詳細ページから th/td 抽出

実行方法:
    python scripts/sites/jobs/domonet.py
    python bin/run_flow.py --site-id domonet
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


_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_RE = re.compile(r"(?:TEL|電話|Tel)[\s:]*([\d\-()\s]{8,20})")

_REGION_PATHS = ["/kanto", "/shizuoka", "/nagoya", "/kansai", "/pado"]
_BASE = "https://domonet.jp"


class DomonetScraper(StaticCrawler):
    """DOMO NET アルバイト・パート求人 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "求人タイトル",
        "仕事内容",
        "給与",
        "勤務地",
        "勤務時間",
        "休日・休暇",
        "最寄駅",
        "資格",
        "待遇",
        "期間",
        "応募方法",
        "応募後のプロセス",
        "その他",
    ]

    def parse(self, url: str):
        """ルート URL → 5 地域 /list → ページング → 詳細ページ"""
        detail_urls = []
        for region in _REGION_PATHS:
            list_base = f"{_BASE}{region}/list"
            region_urls = self._collect_detail_urls(list_base)
            self.logger.info("地域 %s: 詳細URL %d 件", region, len(region_urls))
            detail_urls.extend(region_urls)

        # 重複排除
        seen = set()
        unique_urls = []
        for u in detail_urls:
            if u not in seen:
                seen.add(u)
                unique_urls.append(u)

        self.total_items = len(unique_urls)
        self.logger.info("全地域合計: ユニーク詳細URL %d 件", self.total_items)

        for detail_url in unique_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
                continue

    def _collect_detail_urls(self, list_base: str) -> list[str]:
        """ページネーションしながら詳細ページ URL を収集"""
        urls: list[str] = []
        page = 1
        max_pages_safety = 2000  # 暴走防止

        while page <= max_pages_safety:
            sep = "&" if "?" in list_base else "?"
            list_url = f"{list_base}{sep}page={page}"
            self.logger.info("一覧ページ取得: %s", list_url)
            soup = self.get_soup(list_url)
            if soup is None:
                break

            boxes = soup.select(".searchList_Box")
            if not boxes:
                break

            page_found = 0
            for box in boxes:
                a = box.select_one("h4 a[href]")
                if not a:
                    continue
                href = a.get("href", "").strip()
                if not href:
                    continue
                full = urljoin(_BASE, href)
                # クエリの bp=search は任意、保持してOK
                urls.append(full)
                page_found += 1

            if page_found == 0:
                break

            # 最終ページ判定: ページャに次ページリンクがなければ終了
            pager_next = soup.select_one(
                f'a[href*="page={page + 1}"]'
            )
            if not pager_next:
                break

            page += 1

        return urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから th/td ペアを抽出して Schema + EXTRA にマッピング"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item: dict = {
            Schema.URL: detail_url,
            Schema.CAT_SITE: "アルバイト・パート求人",
        }

        # 求人タイトル: h2 直下の最初の h2 (ヘッダーの位置に配置されている)
        # 本文エリア内の h2 を優先
        title_el = soup.select_one("#contentsBox h2") or soup.select_one("h2")
        if title_el:
            item["求人タイトル"] = title_el.get_text(strip=True)

        # 詳細テーブル: table.contents_single_table の th/td を収集
        pairs: dict[str, str] = {}
        tables = soup.select("table.contents_single_table")
        for table in tables:
            for row in table.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                # td はそのまま改行区切りで取得
                val = td.get_text("\n", strip=True)
                val = re.sub(r"\n{2,}", "\n", val).strip()
                if key and key not in pairs:
                    pairs[key] = val

        # --- Schema マッピング ---
        if "社名" in pairs:
            item[Schema.NAME] = pairs["社名"]

        if "事業内容" in pairs:
            item[Schema.LOB] = pairs["事業内容"]

        if "所在地" in pairs:
            addr_raw = pairs["所在地"].replace("\n", " ")
            # 郵便番号抽出
            m_post = _POST_RE.search(addr_raw)
            if m_post:
                item[Schema.POST_CODE] = m_post.group(1)
                addr_raw = _POST_RE.sub("", addr_raw, count=1).strip()
            # 都道府県抽出
            m_pref = _PREF_RE.search(addr_raw)
            if m_pref:
                item[Schema.PREF] = m_pref.group(1)
                item[Schema.ADDR] = addr_raw[m_pref.end():].strip() or addr_raw
            else:
                item[Schema.ADDR] = addr_raw

        if "URL" in pairs:
            # td の a[href] を優先、なければテキスト
            url_td = None
            for table in tables:
                for row in table.select("tr"):
                    th = row.select_one("th")
                    if th and th.get_text(strip=True) == "URL":
                        url_td = row.select_one("td")
                        break
                if url_td:
                    break
            if url_td:
                a = url_td.select_one("a[href]")
                hp = a.get("href", "").strip() if a else pairs["URL"].strip()
                if hp:
                    item[Schema.HP] = hp

        if "お問い合わせ先" in pairs:
            contact = pairs["お問い合わせ先"]
            m_tel = _TEL_RE.search(contact)
            if m_tel:
                tel = re.sub(r"[\s()]", "", m_tel.group(1)).strip("-")
                item[Schema.TEL] = tel

        # --- EXTRA_COLUMNS マッピング ---
        extra_map = {
            "お仕事内容": "仕事内容",
            "給与": "給与",
            "勤務地": "勤務地",
            "時間・勤務日": "勤務時間",
            "休日・休暇": "休日・休暇",
            "最寄駅": "最寄駅",
            "資格": "資格",
            "待遇": "待遇",
            "期間": "期間",
            "応募方法": "応募方法",
            "応募後のプロセス": "応募後のプロセス",
            "その他": "その他",
        }
        for src_key, dst_col in extra_map.items():
            if src_key in pairs:
                item[dst_col] = pairs[src_key]

        # NAME が取れなかった場合はスキップ
        if Schema.NAME not in item:
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DomonetScraper()
    scraper.execute("https://domonet.jp/")

    print("\n" + "=" * 60)
    print("📊 実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
