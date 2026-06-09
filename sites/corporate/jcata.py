"""
一般社団法人 在日華人旅行業協会 (jcata.net) — 開催イベント一覧スクレイパー

取得対象:
    - 協会が公開する「開催予定のイベント」一覧（中日交流・観光関連イベント）
    - イベント名・開催日時・会場・住所・郵便番号・都道府県・電話番号・チケット料金・URL

取得フロー:
    Wix Events ウィジェットの一覧ページ (/event-list) を取得し、
    各イベントカード (data-hook="events-card") からイベント名・開催日・会場・
    詳細URL を抽出。続けて各詳細ページ (/event-details/...) を取得し、
    開催日時(フル)・会場住所・チケット料金を補完して 1 件ずつ即 yield する。

    ※ 協会には公開の会員企業ディレクトリが存在しない（ポートフォリオの
       「Member Unit Directory」は未登録のプレースホルダ）。構造化データとして
       公開されている一覧は本イベント一覧のみ。
    ※ イベント説明文 (event-description / about-section-text) は自由記述の
       プロース（著作権リスク）のため取得対象から除外。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/jcata.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jcata
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


BASE_URL = "https://www.jcata.net"
# 公開されている唯一の構造化一覧。トップ (sites.yml の url) には一覧が無いため固定で参照する。
EVENT_LIST_URL = f"{BASE_URL}/event-list"

# Wix Events の住所は英語表記 (例: "..., Osaka, 540-0008日本")。
# 住所中の英語県/都市トークンを日本語の都道府県へマップする（取得できなければ空文字）。
_PREF_MAP = {
    "Hokkaido": "北海道",
    "Aomori": "青森県", "Iwate": "岩手県", "Miyagi": "宮城県", "Akita": "秋田県",
    "Yamagata": "山形県", "Fukushima": "福島県",
    "Ibaraki": "茨城県", "Tochigi": "栃木県", "Gunma": "群馬県", "Saitama": "埼玉県",
    "Chiba": "千葉県", "Tokyo": "東京都", "Kanagawa": "神奈川県",
    "Niigata": "新潟県", "Toyama": "富山県", "Ishikawa": "石川県", "Fukui": "福井県",
    "Yamanashi": "山梨県", "Nagano": "長野県",
    "Gifu": "岐阜県", "Shizuoka": "静岡県", "Aichi": "愛知県", "Mie": "三重県",
    "Shiga": "滋賀県", "Kyoto": "京都府", "Osaka": "大阪府", "Hyogo": "兵庫県",
    "Nara": "奈良県", "Wakayama": "和歌山県",
    "Tottori": "鳥取県", "Shimane": "島根県", "Okayama": "岡山県",
    "Hiroshima": "広島県", "Yamaguchi": "山口県",
    "Tokushima": "徳島県", "Kagawa": "香川県", "Ehime": "愛媛県", "Kochi": "高知県",
    "Fukuoka": "福岡県", "Saga": "佐賀県", "Nagasaki": "長崎県", "Kumamoto": "熊本県",
    "Oita": "大分県", "Miyazaki": "宮崎県", "Kagoshima": "鹿児島県", "Okinawa": "沖縄県",
}

# 末尾が「日本」等の非ASCII語に直結すると \b が成立しないため境界指定は付けない
_POST_RE = re.compile(r"(\d{3}-\d{4})")

# 電話番号。市外局番(0始まり)で始まり 3 グループのもの。郵便番号(2グループ)や
# 日付(末尾2桁)とは桁構成が異なるため誤検出しにくい。
_TEL_RE = re.compile(r"(0\d{1,4}-\d{1,4}-\d{3,4})")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


def _hook_text(node, hook: str) -> str:
    """指定 data-hook 要素のテキストを返す（無ければ空文字）。"""
    if node is None:
        return ""
    el = node.select_one(f'[data-hook="{hook}"]')
    return _clean(el.get_text(" ", strip=True)) if el else ""


def _derive_pref(location: str) -> str:
    for token, pref in _PREF_MAP.items():
        if re.search(rf"\b{token}\b", location):
            return pref
    return ""


def _extract_tel(soup) -> str:
    """詳細ページから電話番号を抽出する。tel: リンクを優先し、無ければ本文から正規表現で探す。"""
    if soup is None:
        return ""
    # 1) tel: リンク（最も確実）
    for a in soup.select('a[href^="tel:"]'):
        m = _TEL_RE.search(a.get_text(" ", strip=True))
        if m:
            return m.group(1)
        digits = re.sub(r"[^\d+]", "", a.get("href", "")[len("tel:"):])
        if digits:
            return digits
    # 2) 本文テキストから抽出
    m = _TEL_RE.search(soup.get_text(" ", strip=True))
    return m.group(1) if m else ""


class JcataCrawler(StaticCrawler):
    """一般社団法人 在日華人旅行業協会 イベント一覧スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["開催日", "開催日時", "会場", "チケット料金"]

    def parse(self, url: str):
        # url (トップページ) は使わず、固定のイベント一覧ページを参照する
        soup = self.get_soup(EVENT_LIST_URL)
        if soup is None:
            return

        cards = soup.select('li[data-hook="events-card"]')
        self.total_items = len(cards)

        for card in cards:
            try:
                title_a = card.select_one('a[data-hook="title"]')
                name = _clean(title_a.get_text(" ", strip=True)) if title_a else _hook_text(card, "title")
                detail_url = ""
                if title_a and title_a.get("href"):
                    detail_url = urljoin(BASE_URL, title_a["href"])

                item = {
                    Schema.NAME: name,
                    Schema.URL: detail_url or EVENT_LIST_URL,
                    "開催日": _hook_text(card, "short-date"),
                    "会場": _hook_text(card, "short-location"),
                    "開催日時": "",
                    "チケット料金": "",
                    Schema.PREF: "",
                    Schema.POST_CODE: "",
                    Schema.ADDR: "",
                    Schema.TEL: "",
                }

                if detail_url:
                    item.update(self._scrape_detail(detail_url))

                yield item
            except Exception as e:
                self.logger.warning("イベントカードの解析に失敗: %s", e)
                continue

    def _scrape_detail(self, url: str) -> dict:
        """詳細ページから開催日時・住所・料金を補完する。"""
        out = {}
        soup = self.get_soup(url)
        if soup is None:
            return out

        # 名称（詳細側を優先・確実）
        title = _hook_text(soup, "event-title")
        if title:
            out[Schema.NAME] = title

        out["開催日時"] = _hook_text(soup, "event-full-date")

        location = _hook_text(soup, "event-full-location")
        if location:
            out[Schema.ADDR] = location
            # 会場名は先頭カンマまで（例: "大阪音乐厅, 1 Chome-1 ..." → "大阪音乐厅"）
            out["会場"] = location.split(",")[0].strip()
            m = _POST_RE.search(location)
            if m:
                out[Schema.POST_CODE] = m.group(1)
            pref = _derive_pref(location)
            if pref:
                out[Schema.PREF] = pref

        price = _hook_text(soup, "price")
        if price:
            out["チケット料金"] = price

        tel = _extract_tel(soup)
        if tel:
            out[Schema.TEL] = tel

        return out


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JcataCrawler()
    scraper.execute(BASE_URL + "/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
