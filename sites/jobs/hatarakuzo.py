"""
はたらくぞドットコム (働くぞドットコム) — 福岡県の求人・転職情報スクレイパー

取得対象:
    - 福岡県の求人案件 (一覧 → 詳細)。お仕事データ・会社情報の構造化フィールドを取得する。
    - 「仕事内容」「事業内容」「応募資格」等の長文自由記述は著作権リスクのため取得しない。

取得フロー:
    1. /sheets/search/page~N を 1 ページ目から順に巡回 (1 ページ 20 件)
    2. 各一覧アイテムからカテゴリタグ (サイト定義業種) と詳細 URL を取得
    3. 詳細ページ (.box-item > .item-row) を解析し、1 件ずつ yield する (Pattern B)

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/hatarakuzo.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id hatarakuzo
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.hatarakuzo.com"
LIST_URL = f"{BASE_URL}/sheets/search"
ITEMS_PER_PAGE = 20

# 47 都道府県を先頭一致で抽出するためのパターン
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class HatarakuzoScraper(StaticCrawler):
    """はたらくぞドットコム 求人情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "職種",
        "雇用形態",
        "給与",
        "昇給・賞与",
        "交通手段",
        "特長",
        "屋内の受動喫煙対策",
        "福利厚生・諸手当",
        "応募方法",
        "会社名",
        "本社所在地",
        "年間売上高",
        "主要取引先",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 進捗表示用に総件数を推定 (最終ページ番号 × 1ページ件数)
        last_page = self._detect_last_page()
        if last_page:
            self.total_items = last_page * ITEMS_PER_PAGE
            self.logger.info("推定総ページ数: %d (推定 約%d 件)", last_page, self.total_items)

        page = 1
        while True:
            list_url = LIST_URL if page == 1 else f"{LIST_URL}/page~{page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            boxes = soup.select("div.items-box")
            if not boxes:
                break

            for box in boxes:
                a = box.find("a", href=True)
                if not a:
                    continue
                detail_url = urljoin(BASE_URL, a["href"].strip())

                # 一覧アイテムのカテゴリタグ (org) = サイト定義業種
                cat_site = ""
                for tag in box.select(".box-tag .tag-item"):
                    cls = tag.get("class") or []
                    # 雇用形態(blu)・NEW バッジ(gre) は除外し、業種タグを採用
                    if "blu" in cls or "gre" in cls:
                        continue
                    cat_site = tag.get_text(strip=True)
                    break

                try:
                    item = self._scrape_detail(detail_url, cat_site)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細ページ解析失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _detect_last_page(self) -> int | None:
        """巨大なページ番号を要求するとページャがクランプされ、末尾付近の
        ページ番号ウィンドウを返す挙動を利用して最終ページ番号を推定する。"""
        soup = self.get_soup(f"{LIST_URL}/page~9999")
        if soup is None:
            return None
        nums = []
        for a in soup.find_all("a", href=True):
            m = re.search(r"/page~(\d+)", a["href"])
            if m:
                nums.append(int(m.group(1)))
        return max(nums) if nums else None

    def _scrape_detail(self, url: str, cat_site: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 詳細ページのラベル→値マップを構築 (<br> はスペース結合)
        rows: dict[str, str] = {}
        for row in soup.select(".item-row"):
            label = row.select_one(".row-label")
            text = row.select_one(".row-text")
            if label and text:
                key = label.get_text(" ", strip=True)
                rows[key] = text.get_text(" ", strip=True)

        data: dict = {Schema.URL: url}
        if cat_site:
            data[Schema.CAT_SITE] = cat_site

        # --- Schema マッピング ---
        data[Schema.NAME] = rows.get("勤務先", "")

        address = rows.get("住所", "")
        m = _PREF_PATTERN.match(address)
        if m:
            data[Schema.PREF] = m.group(1)
            data[Schema.ADDR] = address[m.end():].strip()
        else:
            data[Schema.PREF] = ""
            data[Schema.ADDR] = address

        data[Schema.REP_NM] = rows.get("代表", "")
        data[Schema.EMP_NUM] = rows.get("従業員数", "")
        data[Schema.CAP] = rows.get("資本金", "")
        data[Schema.OPEN_DATE] = rows.get("設立", "")
        data[Schema.HOLIDAY] = rows.get("休日", "")
        data[Schema.TIME] = rows.get("勤務時間", "")

        # --- EXTRA_COLUMNS (構造化された短い値・列挙・数値のみ) ---
        data["職種"] = rows.get("職種", "")
        data["雇用形態"] = rows.get("雇用形態", "")
        data["給与"] = rows.get("給与", "")
        data["昇給・賞与"] = rows.get("昇給・賞与", "")
        data["交通手段"] = rows.get("交通手段", "")
        data["特長"] = rows.get("特長", "")
        data["屋内の受動喫煙対策"] = rows.get("屋内の受動喫煙対策", "")
        data["福利厚生・諸手当"] = rows.get("福利厚生・諸手当", "")
        data["応募方法"] = rows.get("応募方法", "")
        data["会社名"] = rows.get("会社名", "")
        data["本社所在地"] = rows.get("本社所在地", "")
        data["年間売上高"] = rows.get("年間売上高", "")
        data["主要取引先"] = rows.get("主要取引先", "")

        # 注: 「仕事内容」「事業内容(LOB)」「応募資格」は長文の自由記述のため
        #     著作権リスクを避けて取得しない。

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HatarakuzoScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
