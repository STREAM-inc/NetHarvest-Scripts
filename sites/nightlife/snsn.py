"""
スナックブック (snsn.jp) — 全国のスナック/バー等ナイト店舗ポータル

取得対象:
    全国47都道府県のスナック・バー等の店舗情報。
    店舗名・カナ・住所・都道府県・電話番号・業種(サイト内ジャンル)・定休日・
    営業時間・支払方法、および平均予算・料金システム・座席数・卓数 (EXTRA)。

取得フロー (一覧 → 詳細, 早期 yield):
    1. ルート https://snsn.jp/ を起点に、47都道府県のランディングページ
       (例: https://snsn.jp/tokyo) を巡回。
    2. 各都道府県ページからエリアリンク (/{pref}/area/{id}) を抽出。
    3. 各エリアページを ?page=N でページ送りし、店舗カード (a.snk-card__link)
       から店舗詳細URL (/{pref}/{shop_slug}) を収集。URL で重複排除。
    4. 店舗詳細ページを取得し、1件ごとに即 yield する (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/snsn.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id snsn
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

# 都道府県スラッグ → 正式名称 (全国47)
PREF_SLUGS = {
    "hokkaido": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
    "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県", "ibaraki": "茨城県",
    "tochigi": "栃木県", "gunma": "群馬県", "saitama": "埼玉県", "chiba": "千葉県",
    "tokyo": "東京都", "kanagawa": "神奈川県", "niigata": "新潟県", "toyama": "富山県",
    "ishikawa": "石川県", "fukui": "福井県", "yamanashi": "山梨県", "nagano": "長野県",
    "gifu": "岐阜県", "shizuoka": "静岡県", "aichi": "愛知県", "mie": "三重県",
    "shiga": "滋賀県", "kyoto": "京都府", "osaka": "大阪府", "hyogo": "兵庫県",
    "nara": "奈良県", "wakayama": "和歌山県", "tottori": "鳥取県", "shimane": "島根県",
    "okayama": "岡山県", "hiroshima": "広島県", "yamaguchi": "山口県", "tokushima": "徳島県",
    "kagawa": "香川県", "ehime": "愛媛県", "kochi": "高知県", "fukuoka": "福岡県",
    "saga": "佐賀県", "nagasaki": "長崎県", "kumamoto": "熊本県", "oita": "大分県",
    "miyazaki": "宮崎県", "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}

# 料金システムとして拾う詳細テーブルのラベル (自由記述の長文は除外)
_PRICE_LABEL_KW = ("料金", "システム", "セット", "チャージ", "ボトル", "指名", "飲料", "ハウス")

# ラベルの装飾を落とすための接頭辞
_LABEL_PREFIX = re.compile(r"^(?:定休[:：]?|平均予算[:：]?|営業時間[:：]?|住所[:：]?|最寄駅[:：]?|目安)\s*")


class Snsn(StaticCrawler):
    """スナックブック スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["平均予算", "料金システム", "座席数", "卓数"]

    def parse(self, url: str):
        self._seen: set[str] = set()

        for slug, pref_name in PREF_SLUGS.items():
            pref_url = urljoin(url, slug)
            soup = self.get_soup(pref_url)
            if soup is None:
                continue

            # エリアリンク (/{slug}/area/{id}) を出現順で重複排除して抽出
            area_ids: list[str] = []
            seen_area: set[str] = set()
            for a in soup.select(f'a[href*="/{slug}/area/"]'):
                m = re.search(rf"/{re.escape(slug)}/area/(\d+)", a.get("href", ""))
                if m and m.group(1) not in seen_area:
                    seen_area.add(m.group(1))
                    area_ids.append(m.group(1))

            for area_id in area_ids:
                area_url = urljoin(url, f"{slug}/area/{area_id}")
                yield from self._crawl_area(area_url, slug, pref_name)

    def _crawl_area(self, area_url: str, slug: str, pref_name: str):
        """1エリアをページ送りしながら店舗詳細を即 yield する。"""
        page = 1
        max_page = 1
        while page <= max_page:
            list_url = area_url if page == 1 else f"{area_url}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            cards = soup.select("a.snk-card__link")
            if not cards:
                break

            # 表示中のページャリンクから最大ページ番号を更新 (ウィンドウ型ページャ対応・無限ループ防止)
            for a in soup.select('a[href*="page="]'):
                m = re.search(r"[?&]page=(\d+)", a.get("href", ""))
                if m:
                    max_page = max(max_page, int(m.group(1)))

            for a in cards:
                href = a.get("href")
                if not href:
                    continue
                detail_url = urljoin(area_url, href)
                if detail_url in self._seen:
                    continue
                self._seen.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url, slug, pref_name)
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, exc)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str, slug: str, pref_name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        basic = self._table_dict(soup, "snk-basic__table")
        detail = self._table_dict(soup, "snk-detail__table")

        # 店舗名 / カナ (例: "BAR YELLOW （イエロー）")
        name_raw = basic.get("店舗名", "").strip()
        name, kana = self._split_name_kana(name_raw)

        # 業種 (サイト内ジャンル)
        genre_el = soup.select_one(".genre")
        cat_site = genre_el.get_text(strip=True) if genre_el else ""
        if not cat_site and "地域/業種" in basic:
            parts = basic["地域/業種"].split("/")
            if len(parts) > 1:
                cat_site = re.sub(r"[（(].*", "", parts[1]).strip()

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref_name,
            Schema.ADDR: self._fv_text(soup, "address"),
            Schema.TEL: self._tel(soup),
            Schema.CAT_SITE: cat_site,
            Schema.TIME: basic.get("営業時間", "") or self._fv_text(soup, "hours"),
            Schema.HOLIDAY: basic.get("定休日", "") or self._fv_text(soup, "holiday"),
            Schema.PAYMENTS: basic.get("支払方法", ""),
            "平均予算": self._clean_prefix(
                basic.get("平均予算", "") or self._fv_text(soup, "budget")
            ),
            "料金システム": self._fee_system(detail),
        }

        seats, tables = self._seats_tables(detail.get("卓数・座席数", ""))
        item["座席数"] = seats
        item["卓数"] = tables
        return item

    # ---- helpers -------------------------------------------------------

    @staticmethod
    def _table_dict(soup, class_name: str) -> dict:
        """table.{class_name} の th→td テキストを辞書化。"""
        result: dict[str, str] = {}
        table = soup.select_one(f"table.{class_name}")
        if not table:
            return result
        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                key = th.get_text(strip=True)
                val = td.get_text(" ", strip=True)
                if key:
                    result[key] = val
        return result

    def _fv_text(self, soup, kind: str) -> str:
        el = soup.select_one(f".snk-fv__info-item.{kind}")
        if not el:
            return ""
        return self._clean_prefix(el.get_text(" ", strip=True))

    @staticmethod
    def _clean_prefix(text: str) -> str:
        return _LABEL_PREFIX.sub("", (text or "").strip()).strip()

    @staticmethod
    def _tel(soup) -> str:
        a = soup.select_one('a[href^="tel:"]')
        if a:
            return a.get("href", "").replace("tel:", "").strip()
        return ""

    @staticmethod
    def _split_name_kana(name_raw: str) -> tuple[str, str]:
        if not name_raw:
            return "", ""
        m = re.match(r"^(.*?)\s*[（(]([^（）()]+)[）)]\s*$", name_raw)
        if m:
            return m.group(1).strip(), m.group(2).strip()
        return name_raw, ""

    @staticmethod
    def _fee_system(detail: dict) -> str:
        parts = []
        for key, val in detail.items():
            if not val or len(val) > 40:
                continue
            if any(kw in key for kw in _PRICE_LABEL_KW):
                parts.append(f"{key}:{val}")
        return " / ".join(parts)

    @staticmethod
    def _seats_tables(raw: str) -> tuple[str, str]:
        raw = (raw or "").strip()
        if not raw:
            return "", ""
        seats, tables = [], []
        for part in re.split(r"[・、,/]", raw):
            part = part.strip()
            if not part:
                continue
            if "席" in part:
                seats.append(part)
            elif re.search(r"卓|台|ボックス|BOX|box", part):
                tables.append(part)
        # どちらにも分類できない場合は raw を座席数側に残す
        if not seats and not tables:
            return raw, ""
        return "・".join(seats), "・".join(tables)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Snsn()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://snsn.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
