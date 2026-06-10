"""
ほっとサーチ (kyujin.hotsearch.jp) — ナイトワーク求人ポータル スクレイパー

取得対象:
    - キャバクラ／ガールズバー等のナイトワーク求人 (店舗単位)

取得フロー:
    一覧 (/s/, /s/p1, /s/p2 ...) から詳細リンク(/detail/N)を収集し、
    各詳細ページを 1 件ずつ取得して即 yield する (一覧→詳細 / Pattern B)。

ページネーション:
    1 ページ目 = /s/ , 2 ページ目以降 = /s/p{n-1} (パスベース)。
    ?p= / ?page= クエリは無視されるため使わない。

電話番号 (Schema.TEL):
    詳細ページ「募集方法」のテキストに電話番号が日本語と混在して埋め込まれている。
    ハイフン有無の両パターン (例: 03-5941-8639 / 08065950226) を正規表現で抽出する。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/kyujin.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kyujin
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

# 募集方法テキストから電話番号の塊だけをピンポイント抽出する。
#   - パターンB (ハイフンあり固定/携帯): 0\d{1,4}-\d{1,4}-\d{3,4}  例 03-5941-8639
#   - パターンA (ハイフンなし携帯/固定): 0\d{9,10}                  例 08065950226
_TEL_PATTERN = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}|0\d{9,10}")

# 都道府県の先頭マッチ (勤務地から PREF を切り出す)
_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|"
    r"岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)


def _clean(s) -> str:
    """空白 (全角 U+3000 / U+2003 含む) を 1 つにまとめてトリム。"""
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class KyujinScraper(StaticCrawler):
    """ほっとサーチ (kyujin.hotsearch.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル",   # 募集の見出し (短いヘッドライン)
        "職種",           # カウンターレディ / フロアレディ 等
        "給与",           # 体入時給・採用時給・給与システム (構造化された短文)
        "最寄り駅",       # アクセス
        "エリア",         # 掲載エリアタグ (吉祥寺 / 横浜駅 等)
        "雇用形態",       # アルバイト 等 (未記載の場合あり)
        "勤務条件",       # 週1からOK / 未経験者歓迎 等の条件タグ
        "特徴タグ",       # 即日勤務可能 / Wワーク歓迎 等の特徴タグ
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()
        page = 0  # 0 -> /s/ , 1 -> /s/p1 , 2 -> /s/p2 ...
        while True:
            list_url = urljoin(url, "s/") if page == 0 else urljoin(url, f"s/p{page}")
            soup = self.get_soup(list_url)
            if soup is None:
                break

            # 初回ページで総件数を進捗表示用に設定
            if page == 0:
                num_el = soup.select_one(".search_result_message .num, .search_result .num")
                if num_el:
                    digits = re.sub(r"[^\d]", "", num_el.get_text())
                    if digits:
                        self.total_items = int(digits)

            detail_urls: list[str] = []
            for a in soup.select("div.search_result_item a[href]"):
                href = a.get("href", "")
                m = re.search(r"/detail/(\d+)", href)
                if not m:
                    continue
                jid = m.group(1)
                if jid in seen_ids:
                    continue
                seen_ids.add(jid)
                detail_urls.append(urljoin(url, href))

            if not detail_urls:
                # これ以上アイテムが無い → 末尾に到達
                break

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # 個別ページの失敗で全体を止めない
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # --- 店名 (NAME) ---
        shop = soup.select_one("p.shop_name")
        if shop:
            data[Schema.NAME] = _clean(shop.get_text())

        # --- 求人タイトル (見出し / EXTRA) ---
        info = soup.select_one("div.recruit_detail_info")
        if info:
            h2 = info.select_one("h2")
            if h2:
                data["求人タイトル"] = _clean(h2.get_text())

        # NAME フォールバック
        if not data.get(Schema.NAME) and data.get("求人タイトル"):
            data[Schema.NAME] = data["求人タイトル"]
        if not data.get(Schema.NAME):
            return None

        # --- 業種ジャンル (CAT_SITE) — gyoshu_tag の末尾がジャンル ---
        gyoshu = [_clean(g.get_text()) for g in soup.select("span.gyoshu_tag") if _clean(g.get_text())]
        if gyoshu:
            data[Schema.CAT_SITE] = gyoshu[-1]

        # --- エリアタグ (EXTRA) ---
        areas = [_clean(a.get_text()) for a in soup.select("span.areainfo_tag") if _clean(a.get_text())]
        if areas:
            data["エリア"] = " / ".join(areas)

        # --- 特徴タグ (EXTRA) ---
        feats = [_clean(t.get_text()) for t in soup.select("span.tag") if _clean(t.get_text())]
        if feats:
            data["特徴タグ"] = " / ".join(feats)

        # --- 詳細テーブル (th/td) ---
        method_text = ""
        for tr in soup.select("table.table_detail tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            nkey = re.sub(r"\s+", "", th.get_text())  # 全角空白(給 与等)を除去
            val = _clean(td.get_text(" "))

            if "シフト" in nkey:
                data["勤務条件"] = val
            elif nkey.startswith("勤務時間"):
                data[Schema.TIME] = val
            elif "勤務地" in nkey:
                m = _PREF_PATTERN.match(val)
                if m:
                    data[Schema.PREF] = m.group(1)
                    data[Schema.ADDR] = val[m.end():].strip()
                else:
                    data[Schema.ADDR] = val
            elif "最寄" in nkey:
                data["最寄り駅"] = val
            elif "職種" in nkey:
                data["職種"] = val
            elif "雇用形態" in nkey:
                data["雇用形態"] = val
            elif "給与" in nkey:
                data["給与"] = val
            elif "休" in nkey:  # 休⽇・休暇 (⽇ は CJK 部首の異体字)
                data[Schema.HOLIDAY] = val
            elif "募集方法" in nkey:
                method_text = val

        # --- TEL: 募集方法テキストから電話番号の塊だけを抽出 ---
        tel_match = _TEL_PATTERN.search(method_text)
        data[Schema.TEL] = tel_match.group(0) if tel_match else ""

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = KyujinScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://kyujin.hotsearch.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
