"""
トリムトリム【ホテル】 — 全国のペットホテル検索ポータル

取得対象:
    - 全国のペットホテル店舗情報 (約3,846件 / 全193ページ)

取得フロー:
    一覧ページ (?page=N) を全ページ巡回し、各カードから詳細ページURLを収集 →
    各詳細ページにアクセスして店舗情報・サービス内容・営業時間・動物取扱業情報を取得

実行方法:
    python scripts/sites/service/trimtrim_hotels.py
    python bin/run_flow.py --site-id trimtrim_hotels
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 都道府県判定用パターン (住所先頭から都道府県名を抽出)
_PREF_PATTERN = re.compile(
    r"^(北海道|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

DAY_LABEL_TO_SCHEMA = {
    "月曜日": Schema.TIME_MON,
    "火曜日": Schema.TIME_TUE,
    "水曜日": Schema.TIME_WED,
    "木曜日": Schema.TIME_THU,
    "金曜日": Schema.TIME_FRI,
    "土曜日": Schema.TIME_SAT,
    "日曜日": Schema.TIME_SUN,
}

BASE_URL = "https://trimtrim.jp"
LIST_URL = f"{BASE_URL}/hotels"


class TrimTrimHotelsScraper(StaticCrawler):
    """トリムトリム【ホテル】 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "ホテルID",
        "駐車場",
        "アクセス",
        "受付・予約",
        "施設特徴",
        "利用条件備考",
        "営業時間補足",
        "第一種動物取扱業の種別",
        "登録番号",
        "氏名又は名称",
        "事業所の名称",
        "事業所の所在地",
        "登録年月日",
        "登録の有効期間の末日",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 1ページ目から総ページ数 + 総件数を取得
        first_soup = self.get_soup(f"{LIST_URL}?page=1")
        if first_soup is None:
            self.logger.error("一覧1ページ目の取得に失敗しました")
            return

        total_pages = self._detect_total_pages(first_soup)
        total_count = self._detect_total_count(first_soup)
        self.total_items = total_count or (total_pages * 20)
        self.logger.info("総ページ数=%d, 総件数=%s", total_pages, total_count or "不明")

        seen_ids: set[str] = set()
        for page in range(1, total_pages + 1):
            soup = first_soup if page == 1 else self.get_soup(f"{LIST_URL}?page={page}")
            if soup is None:
                self.logger.warning("page=%d の取得に失敗しました。スキップ", page)
                continue

            detail_urls = self._collect_detail_urls(soup)
            self.logger.info("page=%d: %d 件のホテルを発見", page, len(detail_urls))

            for detail_url in detail_urls:
                m = re.search(r"/hotel-detail/(\d+)", detail_url)
                hotel_id = m.group(1) if m else detail_url
                if hotel_id in seen_ids:
                    continue
                seen_ids.add(hotel_id)
                try:
                    record = self._scrape_detail(detail_url)
                except Exception as e:
                    self.logger.warning("詳細ページ解析エラー (%s): %s", detail_url, e)
                    continue
                if record:
                    yield record

    # ---------------------------------------------------------------
    # 一覧ページ系
    # ---------------------------------------------------------------
    def _detect_total_pages(self, soup) -> int:
        max_page = 1
        for a in soup.select('a[href*="?page="]'):
            href = a.get("href", "")
            m = re.search(r"page=(\d+)", href)
            if m:
                max_page = max(max_page, int(m.group(1)))
        return max_page

    def _detect_total_count(self, soup) -> int | None:
        text = soup.get_text(" ", strip=True)
        m = re.search(r"全\s*([\d,]+)\s*件", text)
        if m:
            return int(m.group(1).replace(",", ""))
        return None

    def _collect_detail_urls(self, soup) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/hotel-detail/"]'):
            href = a.get("href", "")
            if not href:
                continue
            full = href if href.startswith("http") else f"{BASE_URL}{href}"
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    # ---------------------------------------------------------------
    # 詳細ページ
    # ---------------------------------------------------------------
    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        record: dict = {
            Schema.URL: url,
            Schema.CAT_SITE: "ペットホテル",
        }

        m = re.search(r"/hotel-detail/(\d+)", url)
        if m:
            record["ホテルID"] = m.group(1)

        # 名称・フリガナ: <h1>{name}</h1><p>{furigana}</p> (同一親の中)
        h1 = soup.find("h1")
        if h1:
            record[Schema.NAME] = h1.get_text(strip=True)
            sib_p = h1.find_next_sibling("p")
            if sib_p:
                record[Schema.NAME_KANA] = sib_p.get_text(strip=True)

        # 店舗情報セクション: ラベルが <div class="w-[72px] flex-shrink-0"> 内、値は次の兄弟 div
        for label_div in soup.select('div[class*="w-[72px]"][class*="flex-shrink-0"]'):
            label = label_div.get_text(strip=True)
            value_el = label_div.find_next_sibling()
            if not value_el:
                continue
            value = value_el.get_text(" ", strip=True)
            if not value:
                continue
            if label == "電話番号":
                record[Schema.TEL] = value
            elif label == "所在地":
                self._parse_address(value, record)
            elif label == "駐車場":
                record["駐車場"] = value
            elif label == "アクセス":
                record["アクセス"] = value

        # サービス内容のサブセクション (h3.w-24 + p.flex-1)
        for h3 in soup.select("h3"):
            cls = " ".join(h3.get("class") or [])
            if "w-24" not in cls:
                continue
            label = h3.get_text(strip=True)
            sib_p = h3.find_next_sibling("p")
            if not sib_p:
                continue
            value = sib_p.get_text(" ", strip=True)
            if not value:
                continue
            if label == "受付・予約":
                record["受付・予約"] = value
            elif label == "対応サービス":
                record[Schema.LOB] = value
            elif label == "施設特徴":
                record["施設特徴"] = value
            elif label == "決済・保険":
                record[Schema.PAYMENTS] = value

        # 営業時間 (曜日ごと): テキストノードから "{曜日}\n{値}" を抽出
        self._parse_business_hours(soup, record)

        # 利用条件備考
        notes_h2 = self._find_h2(soup, "利用条件備考")
        if notes_h2:
            container = notes_h2.parent
            if container:
                sib = container.find_next_sibling()
                if sib:
                    text = sib.get_text(" ", strip=True)
                    if text:
                        record["利用条件備考"] = text

        # 動物取扱業 登録情報 (dt/dd)
        for dt in soup.select("dt"):
            label = dt.get_text(strip=True)
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            value = dd.get_text(" ", strip=True)
            if not value:
                continue
            if label == "第一種動物取扱業の種別":
                record["第一種動物取扱業の種別"] = value
            elif label == "登録番号":
                record["登録番号"] = value
            elif label == "氏名又は名称":
                record["氏名又は名称"] = value
            elif label == "事業所の名称":
                record["事業所の名称"] = value
            elif label == "事業所の所在地":
                record["事業所の所在地"] = value
            elif label == "登録年月日":
                record["登録年月日"] = value
            elif label == "登録の有効期間の末日":
                record["登録の有効期間の末日"] = value
            elif label == "動物取扱責任者の名前":
                record[Schema.REP_NM] = value

        # 評価 / 口コミ件数
        for h2 in soup.find_all("h2"):
            txt = h2.get_text(strip=True)
            m_rate = re.match(r"^([\d.]+)の評価$", txt)
            if m_rate:
                record[Schema.SCORES] = m_rate.group(1)
                break
        for el in soup.find_all(["p", "span"]):
            txt = el.get_text(strip=True)
            m_rev = re.match(r"^(\d+)件の口コミ$", txt)
            if m_rev:
                record[Schema.REV_SCR] = m_rev.group(1)
                break

        # 名称が取れない場合は不正レコードとして除外
        if not record.get(Schema.NAME):
            return None

        return record

    # ---------------------------------------------------------------
    # ヘルパー
    # ---------------------------------------------------------------
    def _parse_address(self, value: str, record: dict) -> None:
        """所在地: "〒1234567東京都 大田区 ..." → POST_CODE, PREF, ADDR に分解"""
        text = value.strip()
        m = re.match(r"〒\s*(\d{3})-?(\d{4})\s*(.+)", text)
        if m:
            record[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
            text = m.group(3).strip()
        # 都道府県と市区町村以降に分割
        normalized = re.sub(r"\s+", "", text)  # 「東京都 大田区」→「東京都大田区」
        pm = _PREF_PATTERN.match(normalized)
        if pm:
            record[Schema.PREF] = pm.group(1)
            record[Schema.ADDR] = normalized[pm.end():]
        else:
            record[Schema.ADDR] = text

    def _parse_business_hours(self, soup, record: dict) -> None:
        """営業時間セクションから曜日ごとの時間帯と補足/定休日を抽出"""
        hours_h2 = self._find_h2(soup, "営業時間")
        if not hours_h2:
            return

        # 営業時間ブロック (h2 を含む祖先) のテキストを使って曜日→時刻を抽出
        # 安全のため h2 の祖先を辿りつつ、隣接するブロックまで含めて取得
        block = hours_h2
        while block.parent is not None and len(block.get_text(strip=True)) < 100:
            block = block.parent
            if block.name in {"section", "main", "body"}:
                break

        text = block.get_text("\n", strip=True)
        # 曜日と次の行を対応付け
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        for i, line in enumerate(lines):
            if line in DAY_LABEL_TO_SCHEMA and i + 1 < len(lines):
                record[DAY_LABEL_TO_SCHEMA[line]] = lines[i + 1]

        # 営業時間補足: "営業時間 9:00〜17:00..." のような注釈テキストを抽出
        notes = []
        for ln in lines:
            if re.search(r"営業時間\s*\d", ln) or "予約" in ln or "ご予約" in ln:
                notes.append(ln)
        if notes:
            record["営業時間補足"] = " / ".join(notes)
            # まとめて TIME 列にも入れる (営業時間サマリ)
            record[Schema.TIME] = notes[0]

        # 定休日
        for ln in lines:
            m = re.match(r"^定休日\s*[:：]?\s*(.+)$", ln)
            if m:
                record[Schema.HOLIDAY] = m.group(1).strip()
                break
            if "年中無休" in ln and Schema.HOLIDAY not in record:
                record[Schema.HOLIDAY] = "年中無休"
                break

    def _find_h2(self, soup, label: str):
        for h2 in soup.find_all("h2"):
            if h2.get_text(strip=True) == label:
                return h2
        return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = TrimTrimHotelsScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
