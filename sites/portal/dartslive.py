# scripts/sites/portal/dartslive.py
"""
DARTSLIVE SEARCH — ダーツライブ設置店舗スクレイパー

取得対象:
    - 全国の DARTSLIVE 設置店舗 (約 4,000 件 / 一覧 1 ページ 20 件)

取得フロー:
    一覧ページ (?page=N) で店舗名と店舗ID を拾い、店舗ごとに基本情報 API
    (/shop/shop-basicdata/) を 1 回呼んで住所・TEL・営業時間・定休日・最寄り駅・
    料金帯・SNS を取得する。詳細 HTML (/{locale}/shop/{id}) は 1 件 200KB 超と重く、
    定休日・SNS は JS 描画で HTML からは取れないため、API が失敗した店舗の
    フォールバックとしてのみ取得する。

設定メモ (「起動しても完走できない」対策):
    - ITEM_DELAY = 0 … 待機を execute() 側ではなく取得側 (_sleep) に寄せる。
      従来は DELAY=1.0 のアイテム待機 + 詳細HTML + API で 1 件約 3 秒 = 約 4,000 件で
      3 時間超となり、CSV が書き出される close() まで到達できなかった。
      API 主体 + DELAY=0.5 で 1 件あたり約 0.8 秒に短縮している。
    - get_soup() は CONTINUE_ON_ERROR=True のとき通信エラーで None を返す。素のまま
      使うと 1 回の瞬断で AttributeError となり parse() が丸ごと終了する (= 0 件) ため、
      上限付きリトライ付きの _get_soup() 経由で取得する。
    - 総ページ数は毎ページのページャから読み直す (固定値を持たない)。

実行方法:
    # ローカル実行 (全件)
    python scripts/sites/portal/dartslive.py

    # Prefect Flow 経由 (全件)
    python bin/run_flow.py --site-id dartslive
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 基本情報 API のパス (ホストは引数の url から urljoin で導出する)
API_PATH = "/shop/shop-basicdata/"

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# API の dayOfWeek コード (10 = 祝日)
_DOW = {1: "日曜日", 2: "月曜日", 3: "火曜日", 4: "水曜日", 5: "木曜日", 6: "金曜日", 7: "土曜日", 10: "祝日"}


def _clean(value) -> str:
    """None 安全に空白 (全角スペース・改行含む) を 1 個に潰して返す。"""
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _format_slot(day: dict) -> str:
    """1 日分の営業時間を "12:00 - 05:00" (0:00-0:00 なら "24時間営業") にする。"""
    slots: list[str] = []
    for i in ("1", "2"):
        open_h, close_h = day.get(f"openHour{i}"), day.get(f"closeHour{i}")
        if open_h is None or close_h is None:
            continue
        open_m, close_m = day.get(f"openMinutes{i}") or 0, day.get(f"closeMinutes{i}") or 0
        if open_h == close_h == 0 and open_m == close_m == 0:
            slots.append("24時間営業")
        else:
            slots.append(f"{open_h:02d}:{open_m:02d} - {close_h:02d}:{close_m:02d}")
    return " , ".join(slots)


class DartsliveScraper(StaticCrawler):
    """DARTSLIVE ダーツバー・ダーツ場店舗スクレイパー"""

    DELAY = 0.5          # 1 リクエストごとの待機 (取得側 _sleep() で消費する)
    ITEM_DELAY = 0       # execute() 側の二重待機を無効化 (待機は取得側に集約)
    TIMEOUT = 30
    EXTRA_COLUMNS = ["最寄り駅", "料金帯"]

    MAX_PAGES = 500      # 無限ループ防止の安全弁 (現状は約 200 ページ)
    MAX_RETRIES = 3      # 一覧 / API の取得リトライ上限

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------

    def parse(self, url: str):
        # 引数の url (= sites.yml の正規 URL) を唯一の起点とする
        root = re.sub(r"[?&]page=\d+", "", url)
        api_url = urljoin(root, API_PATH)
        locale = self._locale(root)              # 例: "jp"
        shop_prefix = f"/{locale}/shop/"

        seen: set[str] = set()
        last_page: int | None = None
        page = 1

        while page <= self.MAX_PAGES:
            soup = self._get_soup(f"{root}?page={page}")

            if page == 1:
                self._read_total(soup)
            found_last = self._read_last_page(soup)
            if found_last:
                last_page = max(last_page or 0, found_last)

            cards = self._collect_cards(soup, shop_prefix, seen)
            if not cards:
                self.logger.info("ページ %d: 店舗リンクなし。巡回を終了します", page)
                break

            for href, name in cards:
                detail_url = urljoin(root, href)
                shop_id = href.rsplit("/", 1)[-1]
                try:
                    item = self._build_item(detail_url, shop_id, name, api_url, locale)
                except requests.RequestException as e:
                    self.logger.warning("店舗の取得に失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            if last_page and page >= last_page:
                self.logger.info("最終ページ %d に到達しました", last_page)
                break
            page += 1

    # ------------------------------------------------------------------
    # 一覧ページ
    # ------------------------------------------------------------------

    @staticmethod
    def _locale(root: str) -> str:
        """起点 URL のパス先頭をロケールとして使う (例: /jp/shops/ → "jp")。"""
        segments = [s for s in urlparse(root).path.split("/") if s]
        return segments[0] if segments else "jp"

    def _sleep(self) -> None:
        if self.DELAY > 0:
            time.sleep(self.DELAY)

    def _get_soup(self, url: str):
        """get_soup() の None (通信エラー) を上限付きでリトライし、諦めたら例外にする。

        CONTINUE_ON_ERROR=True のとき get_soup() は None を返すため、戻り値をそのまま
        使うと瞬断 1 回で parse() 全体が落ちる。ここで吸収する。
        """
        for attempt in range(1, self.MAX_RETRIES + 1):
            self._sleep()
            soup = self.get_soup(url)
            if soup is not None:
                return soup
            wait = min(2 ** attempt, 10)
            self.logger.warning(
                "一覧ページの取得に失敗 (%d/%d): %s — %d 秒後に再試行",
                attempt, self.MAX_RETRIES, url, wait,
            )
            time.sleep(wait)
        raise RuntimeError(f"一覧ページを取得できませんでした: {url}")

    def _read_total(self, soup) -> None:
        """「検索結果 1-20件目/ 3,970件」から総件数を読み取り ETA 表示に使う。"""
        m = re.search(
            r"検索結果\s*[\d,]+-[\d,]+件目/\s*([\d,]+)件",
            soup.get_text(" ", strip=True),
        )
        if m:
            self.total_items = int(m.group(1).replace(",", ""))
            self.logger.info("総件数: %d 件", self.total_items)

    @staticmethod
    def _read_last_page(soup) -> int:
        """ページャのリンクから最終ページ番号を読む (見つからなければ 0)。"""
        last = 0
        for a in soup.select('a[href*="page="]'):
            m = re.search(r"page=(\d+)", a.get("href", ""))
            if m:
                last = max(last, int(m.group(1)))
        return last

    @staticmethod
    def _collect_cards(soup, shop_prefix: str, seen: set[str]) -> list[tuple[str, str]]:
        """一覧カードから (店舗ページのパス, 店舗名) を重複なしで取り出す。

        同じ店舗が PC 用 / スマホ用の 2 ブロックで出力されるため href で重複排除する。
        """
        cards: list[tuple[str, str]] = []
        for a in soup.select(f'a[href^="{shop_prefix}"]'):
            href = a.get("href", "").split("?")[0].rstrip("/")
            if not href or href in seen:
                continue
            heading = a.select_one("h2")
            name = heading.get_text(strip=True) if heading else ""
            if not name:
                continue
            seen.add(href)
            cards.append((href, name))
        return cards

    # ------------------------------------------------------------------
    # 店舗 1 件
    # ------------------------------------------------------------------

    def _build_item(self, detail_url: str, shop_id: str, name: str,
                    api_url: str, locale: str) -> dict | None:
        item: dict = {Schema.URL: detail_url, Schema.NAME: name}

        data = self._fetch_basicdata(api_url, shop_id, locale)
        if data is None:
            # API が使えない店舗だけ、重い詳細 HTML にフォールバックする
            self.logger.warning("基本情報 API が取得できないため詳細 HTML を使用: %s", detail_url)
            return self._scrape_detail(detail_url, item)

        self._fill_from_api(item, data)
        return item

    def _fetch_basicdata(self, api_url: str, shop_id: str, locale: str) -> dict | None:
        """基本情報 API を上限付きリトライで取得する。取得できなければ None。"""
        for attempt in range(1, self.MAX_RETRIES + 1):
            self._sleep()
            try:
                resp = self.session.get(
                    api_url,
                    params={"country_code": locale, "shop_enc_id": shop_id},
                    timeout=self.TIMEOUT,
                )
                resp.raise_for_status()
                payload = resp.json()
            except (requests.RequestException, ValueError) as e:
                self.logger.warning(
                    "API 取得に失敗 (%d/%d) shop_id=%s: %s", attempt, self.MAX_RETRIES, shop_id, e
                )
                time.sleep(min(2 ** attempt, 10))
                continue
            if not payload.get("success"):
                return None
            return payload.get("data") or None
        return None

    def _fill_from_api(self, item: dict, data: dict) -> None:
        shop = (data.get("basicdata") or {}).get("shop") or {}
        price_map = {
            t.get("type"): t.get("comment")
            for t in ((data.get("pricerange") or {}).get("type") or [])
        }

        addr = _clean(shop.get("address"))
        if addr:
            m = _PREF_RE.match(addr)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr[m.end():].strip()
            else:
                item[Schema.ADDR] = addr

        tel = _clean(shop.get("tel"))
        if tel:
            item[Schema.TEL] = tel

        hp = _clean(shop.get("homePageUrl"))
        if hp:
            item[Schema.HP] = hp

        hours = self._format_opentime(shop.get("opentime") or {})
        if hours:
            item[Schema.TIME] = hours

        holiday = self._format_closedday(shop.get("closedDay") or {})
        if holiday:
            item[Schema.HOLIDAY] = holiday

        stations = [_clean(s) for s in (shop.get("stationList") or [])]
        stations = [s for s in stations if s]
        if stations:
            item["最寄り駅"] = " / ".join(stations)

        budgets = shop.get("budgets") or {}
        price = price_map.get(budgets.get("range")) or _clean(budgets.get("freeText"))
        if price:
            item["料金帯"] = price

        sns = shop.get("sns") or {}
        twitter = _clean(sns.get("twitterAccount"))
        if twitter:
            item[Schema.X] = twitter
        instagram = _clean(sns.get("InstagramAccount"))
        if instagram:
            item[Schema.INSTA] = instagram

    @staticmethod
    def _format_opentime(opentime: dict) -> str:
        """API の曜日別営業時間を「毎日 12:00 - 05:00」「日曜日 … / 月曜日 …」形式にする。"""
        rows: list[tuple[str, str]] = []
        for day in opentime.get("dayOfWeekList") or []:
            label = _DOW.get(day.get("dayOfWeek"))
            slot = _format_slot(day)
            if label and slot:
                rows.append((label, slot))

        if not rows:
            text = ""
        elif len({slot for _, slot in rows}) == 1:
            text = f"毎日 {rows[0][1]}"
        else:
            text = " / ".join(f"{label} {slot}" for label, slot in rows)

        comment = _clean(opentime.get("comment"))
        return f"{text} {comment}".strip() if comment else text

    @staticmethod
    def _format_closedday(block: dict) -> str:
        closed = block.get("closedDay") or {}
        if closed.get("isOpen365"):
            text = "年中無休"
        else:
            parts = []
            day_of_month = closed.get("dayOfMonth")
            if day_of_month:
                parts.append(f"第{day_of_month}週")
            label = _DOW.get(closed.get("dayOfWeek"))
            if label:
                parts.append(label)
            text = "".join(parts)

        comment = _clean(block.get("comment"))
        return f"{text} {comment}".strip() if comment else text

    def _scrape_detail(self, url: str, item: dict) -> dict | None:
        """API フォールバック: 詳細 HTML から取れる範囲を埋める (定休日/SNS は JS 描画で不可)。"""
        soup = self._get_soup(url)

        addr_el = soup.select_one("p.address")
        if addr_el:
            full = addr_el.get_text(strip=True)
            m = _PREF_RE.match(full)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = full[m.end():].strip()
            else:
                item[Schema.ADDR] = full

        tel_el = soup.select_one("p.shop_telPhone")
        if tel_el:
            item[Schema.TEL] = tel_el.get_text(strip=True)

        hours = soup.select("ol#business-open-days li")
        if hours:
            item[Schema.TIME] = " / ".join(li.get_text(strip=True) for li in hours)

        for tr in soup.select("tbody.basicinfo-tbody tr"):
            th = tr.select_one("th h4")
            td = tr.select_one("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            if label == "最寄り駅":
                p = td.select_one("p")
                if p:
                    item["最寄り駅"] = _clean(p.get_text(" ", strip=True))
            elif label == "料金帯":
                p = td.select_one("p")
                if p:
                    item["料金帯"] = _clean(p.get_text(" ", strip=True))
            elif label == "店舗HP":
                a_el = td.select_one("a[href]")
                if a_el:
                    item[Schema.HP] = a_el["href"].strip()

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DartsliveScraper()
    scraper.execute("https://search.dartslive.com/jp/shops/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
