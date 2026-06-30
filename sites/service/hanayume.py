"""
HANAYUME (ハナユメ) — 全国の結婚式場ポータル 式場情報スクレイパー

取得対象:
    - ハナユメ (hana-yume.net) に掲載されている全国の結婚式場 (詳細ページ
      /{hall_id}/) の構造化情報
    - 式場名 / 都道府県 / 住所 / 式場タイプ / エリア / 着席人数 / 駐車場

取得フロー:
    1. ルート URL (検索結果一覧: /search/hallList/?...) を起点に、`&page=N` で
       全ページ (1〜約13ページ / 全約649件 / 1ページ50件) を巡回する。
    2. 各一覧ページから式場詳細ページ ID (/{hall_id}/) を抽出する。
    3. 詳細ページを 1 件取得するたびに即 yield する
       (途中中断に強い Pattern B / 早期 yield)。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、ページ送り・詳細ページ URL は
      すべて `url` から派生させる (`f"{url}&page={n}"` / `urljoin(url, ...)`)。
      別 URL はハードコードしない。
    - 電話番号は全式場共通の「ハナユメ」相談ダイヤル (0120-791-317 ※ハナユメに繋がります)
      のみが掲載され、式場固有の番号は存在しないため Schema.TEL には載せない。
    - おすすめポイント・口コミ本文・挙式スタイル・収容人数・設備・支払い方法・最寄り駅／交通
      等は長文の自由記述 (プロース) であり、著作権リスク回避のため取得しない。
      取得するのは短い構造化ラベル (式場タイプ / エリア / 着席人数 / 駐車場) のみ。

実行方法:
    # ローカルテスト
    python scripts/sites/service/hanayume.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id hanayume
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


# 住所先頭から都道府県を切り出すためのパターン
_PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 式場詳細ページ ID (/{hall_id}/) を抽出するパターン
_HALL_ID_PATTERN = re.compile(r"^(?:https://hana-yume\.net)?/(\d+)/$")

# 一覧ページ上の総件数 (全N件)
_TOTAL_PATTERN = re.compile(r"全([0-9,]+)件")

# 詳細ページ SSR データブロック中の構造化フィールド
_PREFNAME_PATTERN = re.compile(r"'prefName':\s*'([^']*)'")
_STYLENAME_PATTERN = re.compile(r"'styleName':\s*'([^']*)'")
_REGIONNAME_PATTERN = re.compile(r"'regionName':\s*'([^']*)'")
# 着席人数: <span>着席：2名 ～ 140名</span>
_SEAT_PATTERN = re.compile(r"着席：([^<]+?)</span>")

# Schema に無いサイト固有の構造化項目 (いずれも短いラベル/タグ。自由記述は含めない)
_COL_AREA = "エリア"
_COL_SEAT = "着席人数"
_COL_PARKING = "駐車場"

# ページ巡回の安全上限 (全約13ページに対し十分な余裕)
_MAX_PAGES = 40


class Hanayume(StaticCrawler):
    """HANAYUME (ハナユメ) 全国の結婚式場 情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [_COL_AREA, _COL_SEAT, _COL_PARKING]

    def parse(self, url: str):
        seen: set[str] = set()
        total_set = False

        page = 1
        while page <= _MAX_PAGES:
            list_url = f"{url}&page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            # 総件数 (進捗表示用) を初回ページで設定
            if not total_set:
                m = _TOTAL_PATTERN.search(soup.get_text())
                if m:
                    try:
                        self.total_items = int(m.group(1).replace(",", ""))
                    except ValueError:
                        pass
                total_set = True

            # このページに含まれる式場詳細 ID を出現順で抽出 (重複除去)
            page_ids: list[str] = []
            for a in soup.find_all("a", href=True):
                m = _HALL_ID_PATTERN.match(a["href"])
                if not m:
                    continue
                hid = m.group(1)
                if hid not in seen:
                    seen.add(hid)
                    page_ids.append(hid)

            # 新規 ID が無ければ末尾ページに到達したとみなして終了
            if not page_ids:
                break

            # 詳細ページを 1 件取得するたびに即 yield (早期 yield / 途中中断に強い)
            for hid in page_ids:
                detail_url = urljoin(url, f"/{hid}/")
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:  # 個別式場の失敗は握りつぶして継続
                    self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                    continue

            page += 1

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        html = str(soup)
        item = {Schema.URL: detail_url}

        # 基本情報テーブル (式場名 を含む最初の table) を th/td 辞書化
        rows: dict[str, str] = {}
        table = None
        for t in soup.find_all("table"):
            if t.find("th") and "式場名" in t.get_text():
                table = t
                break
        if table:
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not (th and td):
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(" ", strip=True)
                if label and label not in rows:
                    rows[label] = value

        # 式場名 (テーブル優先、無ければ H1)
        name = rows.get("式場名", "")
        if not name:
            h1 = soup.find("h1")
            if h1:
                name = h1.get_text(strip=True)
        if name:
            item[Schema.NAME] = name

        # 所在地 → 都道府県 / 住所 ("地図を見る" プレフィックスを除去)
        addr_raw = rows.get("所在地", "").replace("地図を見る", "").strip()
        if addr_raw:
            item[Schema.ADDR] = addr_raw

        # 都道府県: SSR データブロックの prefName を優先、無ければ住所先頭から抽出
        pm = _PREFNAME_PATTERN.search(html)
        if pm and pm.group(1):
            item[Schema.PREF] = pm.group(1)
        else:
            mpref = _PREF_PATTERN.search(addr_raw)
            if mpref:
                item[Schema.PREF] = mpref.group(1)

        # 式場タイプ (専門式場・ゲストハウス / ホテル 等)
        sm = _STYLENAME_PATTERN.search(html)
        if sm and sm.group(1):
            item[Schema.CAT_SITE] = sm.group(1)

        # エリア (regionName)
        rm = _REGIONNAME_PATTERN.search(html)
        if rm and rm.group(1):
            item[_COL_AREA] = rm.group(1)

        # 着席人数 (例: 2名 ～ 140名)
        cm = _SEAT_PATTERN.search(html)
        if cm and cm.group(1).strip():
            item[_COL_SEAT] = cm.group(1).strip()

        # 駐車場 (短い構造化情報)
        parking = rows.get("駐車場", "")
        if parking:
            item[_COL_PARKING] = parking

        # NAME が取れなければ無効なページとして捨てる
        if not item.get(Schema.NAME):
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Hanayume()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を唯一の起点とし、ページ送り・詳細 URL は url から派生させる。
    scraper.execute(
        "https://hana-yume.net/search/hallList/?sg_hs%5Barea%5D=&sg_hs%5Border%5D=&sg_hs%5BsearchKind%5D="
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
