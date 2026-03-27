# scripts/sites/portal/refonavi.py
"""
リフォーム評価ナビ (refonavi.or.jp) — リフォーム会社情報スクレイパー

取得対象:
    全国47都道府県のリフォーム会社詳細情報

取得フロー:
    都道府県一覧URL → ページネーション → 詳細ページリンク収集 → 各詳細ページからデータ取得

実行方法:
    # ローカルテスト
    python scripts/sites/portal/refonavi.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id refonavi
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

# 47都道府県スラッグ
PREFECTURES = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita",
    "yamagata", "fukushima", "ibaragi", "totigi", "gunma",
    "saitama", "chiba", "tokyo", "kanagawa", "niigata",
    "toyama", "ishikawa", "fukui", "yamanashi", "nagano",
    "gifu", "shizuoka", "aichi", "mie", "shiga",
    "kyoto", "osaka", "hyogo", "nara", "wakayama",
    "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka",
    "saga", "nagasaki", "kumamoto", "oita", "miyazaki",
    "kagoshima", "okinawa",
]

class RefonautiScraper(StaticCrawler):
    """リフォーム評価ナビ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["対応エリア", "FAX", "売上高"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """全都道府県を順にスクレイピング。

        Args:
            url: sites.yml で指定したベースURL（例: https://www.refonavi.or.jp）
        """
        # 末尾スラッシュを除いたベースURLを使用
        base_url = url.rstrip("/")
        for pref_slug in PREFECTURES:
            pref_url = f"{base_url}/search/{pref_slug}"
            self.logger.info("都道府県取得開始: %s", pref_url)
            yield from self._scrape_prefecture(base_url, pref_url)

    def _scrape_prefecture(self, base_url: str, pref_url: str) -> Generator[dict, None, None]:
        """1都道府県分: ページネーションを処理しながら詳細ページを取得"""
        page = 1
        while True:
            list_url = f"{pref_url}?limit=20&page={page}"
            self.logger.info("一覧ページ取得: page=%d", page)

            try:
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning("一覧ページ取得失敗: %s (%s)", list_url, e)
                break

            # 詳細ページリンクを収集
            grids = soup.select("div.grid_box.fit_colum2.main_grid")
            if not grids:
                break

            for grid in grids:
                link = grid.select_one("a[href^='/shop/']")
                if not link:
                    continue
                detail_url = base_url + link.get("href")
                time.sleep(self.DELAY)
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)

            # 次のページへ
            next_link = soup.select_one("div.pager a[rel='next']")
            if next_link:
                page += 1
                time.sleep(self.DELAY)
            else:
                break

    def _scrape_detail(self, url: str) -> dict | None:
        """詳細ページから会社情報を取得"""
        soup = self.get_soup(url)
        item = {Schema.URL: url}

        # --- th/td テーブルから情報を抽出 ---
        seen_addr = False
        seen_hp = False
        for tr in soup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue

            label = th.get_text(strip=True)
            value = " ".join(td.get_text(strip=True).split())

            if label == "リフォーム会社名":
                item[Schema.NAME] = value

            elif label == "住所" and not seen_addr:
                seen_addr = True
                # 「地図でみる」テキストを除去
                clean_addr = value.replace("地図でみる", "").strip()
                # 都道府県と住所を分離（"北海道 札幌市..."）
                parts = clean_addr.split(" ", 1)
                if len(parts) == 2:
                    item[Schema.PREF] = parts[0]
                    item[Schema.ADDR] = parts[1]
                else:
                    item[Schema.ADDR] = clean_addr

            elif label == "TEL":
                # TELが重複して連結されている場合（例: "03-1234-567803-1234-5678"）
                tel_raw = re.sub(r"[^\d\-]", "", value)
                # 最初の電話番号パターンだけ抽出（市外局番2-4桁、以降2ブロック）
                tel_match = re.match(r"(\d{2,4}(?:-\d{2,4}){1,2})", tel_raw)
                item[Schema.TEL] = tel_match.group(1) if tel_match else value

            elif label == "FAX":
                item["FAX"] = value

            elif label == "代表者名":
                # "代表取締役社長　藤原　加苗" のような形式を分割
                rep_raw = re.sub(r"[\s\xa0]+", " ", value).strip()
                # 役職と氏名を最初のスペースで分離
                parts = rep_raw.split(" ", 1)
                if len(parts) == 2:
                    item[Schema.POS_NM] = parts[0]
                    item[Schema.REP_NM] = parts[1]
                else:
                    item[Schema.REP_NM] = rep_raw

            elif label == "設立年月日":
                # "2011 年 09 月 26 日" → "2011-09-26"
                m = re.search(r"(\d{4})\s*年\s*(\d{2})\s*月\s*(\d{2})\s*日", value)
                item[Schema.OPEN_DATE] = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else value

            elif label == "資本金":
                item[Schema.CAP] = value

            elif label == "売上高・請負件数":
                item["売上高"] = value

            elif label == "従業員数":
                # "22 名(内、リフォーム担当10名)..." → "22 名"
                m = re.match(r"(\d+\s*名)", value)
                item[Schema.EMP_NUM] = m.group(1) if m else value

            elif label == "ホームページ" and not seen_hp:
                seen_hp = True
                item[Schema.HP] = value

            elif label == "対応エリア":
                # モーダルからエリアテキストを取得
                modal = soup.find(id="modal-1")
                if modal:
                    modal_text = modal.get_text(strip=True)
                    # 先頭の会社名を除去（最初の都道府県名以降を取得）
                    pref_match = re.search(
                        r"(北海道|青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|"
                        r"埼玉|千葉|東京|神奈川|新潟|富山|石川|福井|山梨|長野|"
                        r"岐阜|静岡|愛知|三重|滋賀|京都|大阪|兵庫|奈良|和歌山|"
                        r"鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|"
                        r"佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)",
                        modal_text,
                    )
                    if pref_match:
                        item["対応エリア"] = modal_text[pref_match.start():]
                    else:
                        item["対応エリア"] = modal_text

        # --- 事業内容: 会社説明文の最初の段落 ---
        lob_el = soup.select_one("p.limit_txt-pc_only")
        if lob_el:
            item[Schema.LOB] = " ".join(lob_el.get_text(strip=True).split())

        # --- 得意分野 → サイト定義業種・ジャンル ---
        specialty_div = soup.select_one("div.inner.pc_p0")
        if specialty_div:
            tags = specialty_div.select("p.button.small_fit")
            if tags:
                item[Schema.CAT_SITE] = " ".join(t.get_text(strip=True) for t in tags)

        # --- SNSリンク（最初の div.sns.float_box から取得）---
        sns_div = soup.select_one("div.sns.float_box")
        if sns_div:
            for a in sns_div.find_all("a", id=True):
                sns_id = a.get("id", "")
                href = a.get("href", "")
                if sns_id == "sp_instagram":
                    item[Schema.INSTA] = href
                elif sns_id == "sp_facebook":
                    item[Schema.FB] = href
                elif sns_id == "sp_x":
                    item[Schema.X] = href
                elif sns_id == "sp_line":
                    item[Schema.LINE] = href

        # 名称が取れなかった場合はスキップ
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

    scraper = RefonautiScraper()
    # テスト: 北海道のみ（全国実行は sites.yml 経由）
    scraper.execute("https://www.refonavi.or.jp")
