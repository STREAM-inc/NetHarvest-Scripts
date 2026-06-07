# scripts/sites/realestate/hatomarksite.py
"""
ハトマークサイト (hatomarksite.com) — 全宅連 不動産会社検索スクレイパー

取得対象:
    全国47都道府県の宅地建物取引業者（不動産会社）の詳細情報

取得フロー:
    都道府県コード(01〜47)ごとに一覧ページをページネーション巡回 →
    各社の詳細ページ(agent/{11桁ID})を1件取得するごとに即 yield (Pattern B)

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/hatomarksite.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id hatomarksite
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 全宅連の一覧ベース URL（sites.yml の url が変わっても固定で組み立てる）
BASE = "https://www.hatomarksite.com/search/zentaku/agent"
LIMIT = 50  # 1ページあたり件数。read timeout を避けるため 50 以下に固定

# 都道府県コード 01〜47（JISコード）
PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

# 住所先頭の都道府県を切り出す正規表現
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
_TEL_PATTERN = re.compile(r"\[TEL\]\s*([0-9\-]+)")
_FAX_PATTERN = re.compile(r"\[FAX\]\s*([0-9\-]+)")


def _clean(text: str) -> str:
    """全角空白・連続空白を整理する。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("　", " ")).strip()


class HatomarksiteScraper(StaticCrawler):
    """ハトマークサイト 不動産会社検索 スクレイパー（全国47都道府県）"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "免許番号",
        "FAX",
        "所属団体名",
        "得意エリア",
        "取扱エリア",
        "交通",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """全都道府県を順に巡回し、各社の詳細を取得即 yield する。"""
        for pref_code in PREF_CODES:
            self.logger.info("=== 都道府県コード: %s ===", pref_code)
            yield from self._scrape_prefecture(pref_code)

    def _scrape_prefecture(self, pref_code: str) -> Generator[dict, None, None]:
        """1都道府県分: ページネーションを処理しながら詳細を取得即 yield。"""
        page = 1
        while True:
            list_url = (
                f"{BASE}/area/{pref_code}/list"
                f"?p_adr={pref_code}&limit={LIMIT}&orderby=addr_sort&page={page}"
            )
            self.logger.info("一覧ページ取得: 都道府県=%s page=%d", pref_code, page)

            soup = self.get_soup(list_url)
            if soup is None:
                break

            # 詳細ページリンクを収集（重複排除しつつ出現順を保持）
            detail_urls = []
            seen = set()
            for a in soup.select('a[href*="/agent/000"]'):
                href = a.get("href", "")
                m = re.search(r"/agent/(\d{8,})", href)
                if not m:
                    continue
                full = href if href.startswith("http") else f"https://www.hatomarksite.com{href}"
                if full not in seen:
                    seen.add(full)
                    detail_urls.append(full)

            if not detail_urls:
                # この都道府県は取得完了（または該当0件）
                break

            for detail_url in detail_urls:
                time.sleep(self.DELAY)
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)

            page += 1
            time.sleep(self.DELAY)

    def _scrape_detail(self, url: str) -> dict | None:
        """詳細ページ(agent/{ID})から会社情報を取得する。"""
        soup = self.get_soup(url)
        if soup is None:
            return None

        item = {Schema.URL: url}

        # 名称（h1 相当の .agent-name、なければ詳細テーブルの「商号」）
        name_el = soup.select_one(".agent-name")
        if name_el:
            item[Schema.NAME] = _clean(name_el.get_text())

        # detail-item ラベル → 直後の <p> が値
        fields = {}
        for label_p in soup.select("p.detail-item"):
            label = _clean(label_p.get_text())
            value_p = label_p.find_next_sibling("p")
            value = value_p.get_text(" ", strip=True) if value_p else ""
            fields[label] = _clean(value)

        # 商号（.agent-name が取れなかった場合の保険）
        if Schema.NAME not in item and fields.get("商号"):
            item[Schema.NAME] = fields["商号"]

        # 住所 → 郵便番号・都道府県・住所に分解
        addr_raw = fields.get("住所", "")
        if addr_raw:
            post_m = _POST_PATTERN.search(addr_raw)
            if post_m:
                item[Schema.POST_CODE] = post_m.group(1)
                addr_raw = _POST_PATTERN.sub("", addr_raw).strip()
            pref_m = _PREF_PATTERN.match(addr_raw)
            if pref_m:
                item[Schema.PREF] = pref_m.group(1)
                item[Schema.ADDR] = addr_raw[pref_m.end():].strip()
            else:
                item[Schema.ADDR] = addr_raw

        # 連絡先 → TEL / FAX
        contact = fields.get("連絡先", "")
        if contact:
            tel_m = _TEL_PATTERN.search(contact)
            if tel_m:
                item[Schema.TEL] = tel_m.group(1)
            fax_m = _FAX_PATTERN.search(contact)
            if fax_m:
                item["FAX"] = fax_m.group(1)

        # 代表者
        if fields.get("代表者"):
            item[Schema.REP_NM] = fields["代表者"]

        # 営業時間・定休日
        if fields.get("営業時間"):
            item[Schema.TIME] = fields["営業時間"]
        if fields.get("定休日"):
            item[Schema.HOLIDAY] = fields["定休日"]

        # 主な取扱物件 → サイト定義業種・ジャンル（構造化ラベル）
        if fields.get("主な取扱物件"):
            item[Schema.CAT_SITE] = fields["主な取扱物件"]

        # EXTRA（構造化された短いラベルのみ）
        if fields.get("免許番号"):
            item["免許番号"] = fields["免許番号"]
        if fields.get("所属団体名"):
            item["所属団体名"] = fields["所属団体名"]
        if fields.get("得意エリア"):
            item["得意エリア"] = fields["得意エリア"]
        if fields.get("取扱エリア"):
            item["取扱エリア"] = fields["取扱エリア"]
        if fields.get("交通"):
            item["交通"] = fields["交通"]

        # 名称が取れなければスキップ
        if Schema.NAME not in item:
            return None

        return item


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HatomarksiteScraper()
    scraper.execute(BASE + "/area")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
