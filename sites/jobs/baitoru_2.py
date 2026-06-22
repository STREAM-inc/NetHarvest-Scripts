# scripts/sites/jobs/baitoru_2.py
"""
(バイトル)　ほっともっと — バイトル キーワード検索「ほっともっと　アルバイト」から
ほっともっと各店舗の求人情報（勤務先名・住所・最寄り駅）を収集する。

取得対象:
    - バイトル キーワード検索ページ /kw/ほっともっと　アルバイト/ の全ページ（約49ページ/1,219件）
    - 各求人詳細ページ（短縮URLのリダイレクト先）から店舗情報を抽出

取得フロー:
    一覧ページ(25件/p) → 短縮URL(s.baitoru.com) → リダイレクト → 詳細ページ → 店舗情報抽出
    同一店舗（店舗名＋住所）は重複排除して1件のみ収録する

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/baitoru_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id baitoru_2
"""

import re
import sys
from pathlib import Path
from urllib.parse import urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

# キーワード検索パス（「ほっともっと　アルバイト」のURLエンコード済み）
_KW_PATH = "/kw/%E3%81%BB%E3%81%A3%E3%81%A8%E3%82%82%E3%81%A3%E3%81%A8%E3%80%80%E3%82%A2%E3%83%AB%E3%83%90%E3%82%A4%E3%83%88"

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_ITEMS_PER_PAGE = 25


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class BaitoruHottoScraper(StaticCrawler):
    """バイトル ほっともっと店舗情報スクレイパー

    全国のほっともっと求人をキーワード検索ページから収集し、
    各店舗（勤務先名・住所・最寄り駅）を重複排除して返す。
    """

    DELAY = 1.5
    EXTRA_COLUMNS = ["最寄り駅"]

    def parse(self, url: str):
        parsed = urlparse(url)
        # 引数 url のドメインからキーワード検索ベースURLを構築
        kw_base = f"{parsed.scheme}://{parsed.netloc}{_KW_PATH}"

        seen: set[str] = set()   # (店舗名|住所) 重複排除キー
        page = 1

        while True:
            # ページ1は末尾スラッシュあり、ページ2以降は /N（スラッシュなし）
            list_url = kw_base + "/" if page == 1 else kw_base + f"/{page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            if page == 1:
                # 総件数を取得して進捗表示を有効化（「N件/M件中」のM部分）
                count_match = re.search(r"(\d[\d,]+)\s*件中", soup.get_text())
                if count_match:
                    self.total_items = int(count_match.group(1).replace(",", ""))

            # 求人カード (div.pt02b) を収集
            cards = soup.select("div.pt02b")
            if not cards:
                break

            for card in cards:
                link = card.select_one("h2 a")
                if not link:
                    continue
                short_url = link.get("href", "")
                if not short_url:
                    continue

                try:
                    record = self._scrape_detail(short_url, seen)
                    if record:
                        yield record
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", short_url, e)
                    continue

            # 最終ページ判定: カード数が25未満なら終了
            if len(cards) < _ITEMS_PER_PAGE:
                break
            page += 1

    def _scrape_detail(self, short_url: str, seen: set) -> dict | None:
        # 短縮URL(s.baitoru.com) をリダイレクト追跡して実ページを取得
        resp = self.session.get(short_url, timeout=self.TIMEOUT)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        if "charset=" not in content_type.lower():
            resp.encoding = resp.apparent_encoding

        # リダイレクト後の実URLからトラッキングパラメータを除去
        detail_url = resp.url.split("?")[0].rstrip("/") + "/"
        soup = BeautifulSoup(resp.text, "html.parser")

        data: dict = {Schema.URL: detail_url}

        # 勤務地情報 (div.detail-basicInfo > dl.dl04)
        dl04 = soup.select_one("dl.dl04")
        if not dl04:
            return None

        for inner_dl in dl04.find_all("dl"):
            dt = inner_dl.select_one("dt")
            dd = inner_dl.select_one("dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)

            if label == "勤務先":
                data[Schema.NAME] = _clean(dd.get_text())
            elif label == "住所":
                li = inner_dl.select_one("li")
                addr = _clean(li.get_text() if li else dd.get_text())
                m = _PREF_RE.match(addr)
                if m:
                    data[Schema.PREF] = m.group(1)
                    data[Schema.ADDR] = addr[m.end():].strip()
                else:
                    data[Schema.ADDR] = addr
            elif label == "最寄駅":
                data["最寄り駅"] = _clean(dd.get_text())

        if not data.get(Schema.NAME):
            return None

        # 重複排除 (同一店舗が複数求人で掲載されることへの対応)
        key = data.get(Schema.NAME, "") + "|" + data.get(Schema.ADDR, "")
        if key in seen:
            return None
        seen.add(key)

        # 企業HP（会社情報セクションから取得）
        comp = soup.select_one("div.detail-companyInfo")
        if comp:
            for dl in comp.find_all("dl"):
                dt = dl.select_one("dt span")
                if dt and dt.get_text(strip=True) == "URL":
                    a = dl.select_one("dd a[href]")
                    if a:
                        data[Schema.HP] = a["href"]
                    break

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = BaitoruHottoScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.baitoru.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
