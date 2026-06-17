"""
調達ポータル — 事業者情報公開機能

運営: デジタル庁
URL: https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103

取得対象:
    - 調達ポータルに登録された事業者の基本情報・統一資格情報・落札実績件数

取得フロー:
    1. OAB0101 (事業者情報検索フォーム) へアクセス
    2. 検索ボタン押下 → OAB0100 (POSTハンドラ) → OAB0103 (検索結果一覧) へリダイレクト
    3. OAB0103?page=N&size=50 をページネーション (最大500件 / 10ページ)
    4. 各行の法人番号を OAB0108 へ page.request.post() → 詳細ページを取得
    5. 基本情報・統一資格情報・落札実績件数を抽出して yield

設計メモ:
    - OAB0103/OAB0108 は直打ちアクセス不可 (JSESSIONID + CSRFトークン必須)。
      OAB0101 のフォーム送信で確立したセッションを維持したまま page.request.post() で
      OAB0108 を呼び出すため、1件ごとに画面遷移しない。
    - 検索結果の上限はサーバー仕様で500件。全件取得には検索条件の分割が必要。
    - 統一資格情報・落札実績情報が存在しない事業者は対応フィールドを空文字で返す。
    - HTML の th「商品号又は名称」の実体は商号・名称 (Schema.NAME に対応)。
    - 代表者役職・代表者氏名が「－」の事業者は空文字に正規化する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/p_portal.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id p_portal
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_QUAL_CATEGORIES = ["物品の製造", "物品の販売", "役務の提供等", "物品の買い受け"]


def _clean(s: str) -> str:
    """空白正規化 + 「－」(全角ハイフン) を空文字に変換。"""
    if not s:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return "" if s == "－" else s


class PPortalScraper(DynamicCrawler):
    """調達ポータル 事業者情報公開機能 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "業者種別",      # 株式会社 / 合同会社 等 (構造化ラベル)
        "資格番号",      # 例: 0000100505
        "有効期間",      # 例: 令和07・08・09
        "企業規模",      # 大企業 / 中小企業 / 小規模企業 / その他
        "資格等級",      # 例: 役務の提供等:A / 物品の販売:A
        "競争参加地域",  # 例: 北海道 東北 関東・甲信越 東海・北陸 近畿 中国 四国 九州・沖縄
        "落札実績件数",  # 落札実績の総件数 (整数文字列)
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        search_url = urljoin(url, "OAB0101")
        detail_url = urljoin(url, "OAB0108?")
        size = 50

        # Step 1: OAB0101 フォームを開いて検索送信 → OAB0103 へリダイレクト
        self.get_soup(search_url)
        self.page.click('input[name="OAB0102"]')
        self.page.wait_for_load_state("domcontentloaded")
        self.page.wait_for_timeout(2000)

        # Step 2: 1ページ目の内容を取得して件数確定
        soup = BeautifulSoup(self.page.content(), "html.parser")
        count_text = soup.get_text()
        m = re.search(r"(\d+)件見つかりました", count_text)
        total = int(m.group(1)) if m else 500
        self.total_items = total
        self.logger.info("総件数: %d 件", total)

        # Step 3: ページネーション
        page_num = 0

        while True:
            if page_num > 0:
                soup = self.get_soup(f"{url}?page={page_num}&size={size}")
                if soup is None:
                    break

            # CSRF トークンを現ページから取得
            csrf_input = soup.find("input", {"name": "_csrf"})
            csrf = csrf_input["value"] if csrf_input else ""

            corp_links = soup.select("table tbody tr td:nth-child(3) a")
            if not corp_links:
                break

            for link in corp_links:
                href = link.get("href", "")
                m_corp = re.search(r"corporationNo', value:'(\d+)'", href)
                m_art = re.search(r"articleQualificationInfoId', value:'([^']*)'", href)
                if not m_corp:
                    continue

                corp_no = m_corp.group(1)
                art_qual_id = m_art.group(1) if m_art else ""

                try:
                    resp = self.page.request.post(
                        detail_url,
                        form={
                            "_csrf": csrf,
                            "articleQualificationInfoId": art_qual_id,
                            "corporationNo": corp_no,
                        },
                    )
                    if resp.status != 200:
                        self.logger.warning("OAB0108 error for %s: HTTP %d", corp_no, resp.status)
                        continue

                    detail_soup = BeautifulSoup(resp.text(), "html.parser")
                    article_el = detail_soup.find("article")
                    if article_el and "事業者情報を取得できません" in article_el.get_text():
                        self.logger.debug("corp %s: 情報なし", corp_no)
                        continue

                    item = self._scrape_detail(detail_soup, f"{url}?page={page_num}&size={size}")
                    if item:
                        yield item

                except Exception as e:
                    self.logger.warning("Error scraping corp %s: %s", corp_no, e)
                    continue

            page_num += 1
            if page_num * size >= total:
                break

    def _scrape_detail(self, soup: BeautifulSoup, source_url: str) -> dict | None:
        tables = soup.find_all("table")
        if not tables:
            return None

        # --- 基本情報テーブル (Table 0: class=main-table-pattern1 のみ) ---
        basic: dict[str, str] = {}
        for row in tables[0].find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                basic[th.get_text(strip=True)] = _clean(td.get_text(strip=True))

        name = basic.get("商品号又は名称", "")
        if not name:
            return None

        # 都道府県 / 住所 の分割
        full_addr = basic.get("本社住所", "")
        pref_m = _PREF_PATTERN.match(full_addr)
        pref = pref_m.group(1) if pref_m else ""
        addr = full_addr[len(pref):].strip() if pref else full_addr

        # --- 統一資格情報 (Table 1以降を class で分類) ---
        shikaku_bangou = ""
        yukokikan = ""
        kigyo_kibo = ""
        shikaku_tou = ""
        chiku = ""

        for t in tables[1:]:
            classes = set(t.get("class", []))

            if "bid-details" in classes or "change-details" in classes:
                continue  # 落札実績・変更履歴は別処理

            if "main-table-pattern2" in classes:
                # 資格種類等テーブル: 資格等級行を抽出
                rows = t.find_all("tr")
                if not rows:
                    continue
                header_ths = [th.get_text(strip=True) for th in rows[0].find_all("th")]
                cats = header_ths[1:] if len(header_ths) > 1 else _QUAL_CATEGORIES
                for row in rows[1:]:
                    th_el = row.find("th")
                    if th_el and "資格等級" in th_el.get_text():
                        grades = [td.get_text(strip=True) for td in row.find_all("td")]
                        parts = [
                            f"{cat}:{g}"
                            for cat, g in zip(cats, grades)
                            if g and g not in ("ー", "－", "-")
                        ]
                        shikaku_tou = " / ".join(parts)
                        break

            elif "main-table-pattern3" in classes:
                # 競争参加地域テーブル (2行: ヘッダ行 + ○/ー行)
                rows = t.find_all("tr")
                if len(rows) >= 2:
                    region_hdrs = [th.get_text(strip=True) for th in rows[0].find_all("th")]
                    region_vals = [td.get_text(strip=True) for td in rows[1].find_all("td")]
                    chiku = " ".join(rh for rh, rv in zip(region_hdrs, region_vals) if rv == "○")

            else:
                # 資格基本情報テーブル (class=main-table-pattern1 のみ)
                for row in t.find_all("tr"):
                    th = row.find("th")
                    td = row.find("td")
                    if not (th and td):
                        continue
                    key = th.get_text(strip=True)
                    val = _clean(td.get_text(strip=True))
                    if key == "資格番号":
                        shikaku_bangou = val
                    elif key == "有効期間":
                        yukokikan = val
                    elif key == "企業規模":
                        kigyo_kibo = val

        # --- 落札実績件数 (bid-details テーブル) ---
        bid_tables = [t for t in tables if "bid-details" in set(t.get("class", []))]
        rakusatsu_count = sum(
            max(0, len(t.find_all("tr")) - 1)  # ヘッダ行1行を除く
            for t in bid_tables
        )

        return {
            Schema.NAME: name,
            Schema.CO_NUM: basic.get("法人番号", ""),
            Schema.POST_CODE: basic.get("郵便番号", ""),
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.POS_NM: basic.get("代表者役職", ""),
            Schema.REP_NM: basic.get("代表者氏名", ""),
            Schema.URL: source_url,
            # --- EXTRA ---
            "業者種別": basic.get("業者種別", ""),
            "資格番号": shikaku_bangou,
            "有効期間": yukokikan,
            "企業規模": kigyo_kibo,
            "資格等級": shikaku_tou,
            "競争参加地域": chiku,
            "落札実績件数": str(rakusatsu_count) if rakusatsu_count else "",
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PPortalScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
