"""
小さなお葬式 (osohshiki) — 全国の葬儀場・斎場ポータル 斎場情報スクレイパー

取得対象:
    - 小さなお葬式 (osohshiki.jp) に掲載されている全国の葬儀場・斎場
      (詳細ページ /area/{pref}/.../sougijou/{id}/) の構造化情報
    - 斎場名 / 郵便番号 / 都道府県 / 住所 / 電話番号 /
      駐車場 / 併設火葬場 / 設備対応表 (ご安置中の面会・控室・会食室 等の 〇/― ラベル)

取得フロー:
    1. ルート URL から funeral-hall サイトマップ
       (/sitemap_xml/funeral-hall.xml) を導出して取得し、全斎場の詳細ページ
       URL (約 3,200 件) を列挙する。
       ※ サイト上に全斎場の一括リストページが無いため、サイトマップを起点とする。
    2. 列挙した詳細ページを 1 件取得するたびに即 yield する
       (途中中断に強い Pattern B / 早期 yield)。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。別 URL はハードコードしない。
    - 設備対応表は「〇 / ― / お問合せください」という短い構造化ラベルのみを取得し、
      概要・設備・アクセスの自由記述 PR 文や口コミ・FAQ・プラン表は取得しない
      (著作権リスク回避)。

実行方法:
    # ローカルテスト
    python scripts/sites/service/osohshiki.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id osohshiki
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

# 〒1234567 / 〒123-4567 形式の郵便番号 (7 桁、ハイフン任意)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3})-?(\d{4})")

# 設備ラベルから ※1/※2 注記と余分な空白を取り除く正規化
_NOTE_PATTERN = re.compile(r"\s*※\s*\d+\s*")


# Schema に無いサイト固有の構造化項目 (基本情報の短いラベル)
_COL_PARKING = "駐車場"
_COL_CREMATORY = "併設火葬場"

# 設備対応表の列 (備考で明示された 16 項目。値は 〇/―/お問合せください 等の短いラベル)
_FACILITY_COLS = [
    "ご安置中の面会",
    "付添い安置可",
    "宿泊",
    "控室",
    "会食室",
    "法要室",
    "お風呂・シャワー",
    "キッズ・託児所",
    "バリアフリー",
    "コンビニ（500m圏内）",
    "飲食店（500m圏内）",
    "宿泊ホテル（10km圏内）",
    "飲食持ち込み",
    "1日1組の貸し切り",
    "喪服のレンタル",
    "喪服の着付けサービス",
]


def _norm_label(text: str) -> str:
    """設備ラベルから ※注記・空白を除去して正規化する。"""
    text = _NOTE_PATTERN.sub("", text)
    return text.replace(" ", "").replace("　", "").strip()


class Osohshiki(StaticCrawler):
    """小さなお葬式 全国の葬儀場・斎場 情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [_COL_PARKING, _COL_CREMATORY] + _FACILITY_COLS

    def parse(self, url: str):
        # 1. サイトマップから全斎場の詳細ページ URL を列挙 (一括リストページが無いため)
        sitemap_url = urljoin(url, "/sitemap_xml/funeral-hall.xml")
        sitemap = self.get_soup(sitemap_url)
        if sitemap is None:
            return

        detail_urls = []
        seen = set()
        for loc in sitemap.find_all("loc"):
            href = loc.get_text(strip=True)
            if "/sougijou/" not in href:
                continue
            du = urljoin(url, href)
            if du not in seen:
                seen.add(du)
                detail_urls.append(du)

        self.total_items = len(detail_urls)

        # 2. 詳細ページを 1 件取得するたびに即 yield (早期 yield / 途中中断に強い)
        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:  # 個別斎場の失敗は握りつぶして継続
                self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                continue

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        # 斎場名 (H1)
        h1 = soup.select_one("h1")
        if h1:
            name = h1.get_text(" ", strip=True)
            if name:
                item[Schema.NAME] = name

        # 基本情報 (dl: dt.s-areaFuneralAccess__itemHead / dd)
        basic: dict[str, str] = {}
        for dt in soup.select("dt.s-areaFuneralAccess__itemHead"):
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            label = dt.get_text(strip=True)
            value = dd.get_text(" ", strip=True)
            if label and label not in basic:
                basic[label] = value

        # 所在地 → 郵便番号 / 都道府県 / 住所
        addr_raw = basic.get("所在地", "")
        if addr_raw:
            mp = _POST_PATTERN.search(addr_raw)
            if mp:
                item[Schema.POST_CODE] = f"{mp.group(1)}-{mp.group(2)}"
                addr = addr_raw[mp.end():]
            else:
                addr = addr_raw
            # 住所内の区切り空白を除去して連結
            addr = addr.replace(" ", "").replace("　", "").strip()
            if addr:
                item[Schema.ADDR] = addr
            mpref = _PREF_PATTERN.search(addr_raw)
            if mpref:
                item[Schema.PREF] = mpref.group(1)

        # 電話番号 (斎場ごとの問合せダイヤル。Pipeline が全角→半角・整形を行う)
        tel_raw = basic.get("電話番号", "")
        if tel_raw:
            mtel = re.search(r"0\d{1,4}-?\d{1,4}-?\d{3,4}", tel_raw)
            if mtel:
                item[Schema.TEL] = mtel.group(0)

        # 駐車場 / 併設火葬場 (短い構造化ラベル)
        if basic.get(_COL_PARKING):
            item[_COL_PARKING] = basic[_COL_PARKING]
        if basic.get(_COL_CREMATORY):
            item[_COL_CREMATORY] = basic[_COL_CREMATORY]

        # 設備対応表 (〇/―/お問合せ 等の短いラベルのみ。自由記述は取得しない)
        facility = soup.select_one("table.s-areaFuneralTable__table")
        if facility:
            for tr in facility.select("tr"):
                cells = tr.find_all(["th", "td"])
                # th/td が交互に並ぶ (1 行に複数ペア)
                for i in range(0, len(cells) - 1, 2):
                    label = _norm_label(cells[i].get_text(" ", strip=True))
                    value = cells[i + 1].get_text(" ", strip=True)
                    if label in _FACILITY_COLS and value:
                        item[label] = value

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

    scraper = Osohshiki()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を唯一の起点とし、配下 URL は urljoin で派生させる。
    scraper.execute("https://www.osohshiki.jp/area/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
