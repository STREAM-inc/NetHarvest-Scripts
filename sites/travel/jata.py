"""
日本旅行業協会（JATA）会員リスト — jata

取得対象:
    - 会員リスト検索 (/search/index.php): 正会員・協力会員。一覧の各社は
      detail.php?no=N の詳細ページを持ち、登録情報・所在地・代表者等を取得する。
    - 国内賛助会員リスト (/search/sanjyo.php): カナ行別の静的テーブル。
      会社名・住所・電話番号・FAX を取得する。

取得フロー:
    1. index.php に member_list=1(正会員)/2(協力会員) で POST 検索 → 一覧テーブル取得
    2. 各行の detail.php?no=N を GET し、1 件ごとに即 yield (Pattern B)
    3. sanjyo.php を GET し、賛助会員テーブルを 1 行ごとに即 yield

    ※ ルート URL は sites.yml の url (https://www.jata-net.or.jp/about/jata-about/about02/)。
      実データ URL は同一ホスト配下の /search/ を urljoin で派生させる。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/jata.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jata
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 住所先頭の都道府県を抽出
_PREF_PATTERN = re.compile(
    r"(北海道|東京都|(?:京都|大阪)府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|"
    r"熊本|大分|宮崎|鹿児島|沖縄)県)"
)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")


class Jata(StaticCrawler):
    """日本旅行業協会（JATA）会員リスト スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "会員種別",
        "登録日",
        "登録行政庁名",
        "登録種別",
        "登録番号",
        "旅公取協",
        "IATA加入",
        "ボンド保証制度",
        "重大事故支援システム",
        "ビル名",
        "FAX番号",
        "営業所数",
        "旅行業者代理業者数",
        "旅行業者代理業者の営業所数",
    ]

    # index.php 検索対象の会員種別 (9=その他 は結果 0 件のため除外)
    _MEMBER_TYPES = ["1", "2"]

    def parse(self, url: str):
        index_url = urljoin(url, "/search/index.php")
        sanjyo_url = urljoin(url, "/search/sanjyo.php")

        # --- 1) 会員リスト検索 (正会員 / 協力会員) → 詳細ページ ---
        for member_list in self._MEMBER_TYPES:
            soup = self._fetch_list(index_url, member_list)
            if soup is None:
                continue
            nos = []
            for a in soup.select('a[href*="detail.php?no="]'):
                m = re.search(r"no=(\d+)", a.get("href", ""))
                if m:
                    nos.append(m.group(1))
            # 重複除去 (出現順維持)
            seen = set()
            nos = [n for n in nos if not (n in seen or seen.add(n))]

            for no in nos:
                detail_url = urljoin(url, f"/search/detail.php?no={no}")
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # 個別失敗はスキップして継続
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

        # --- 2) 国内賛助会員リスト (静的テーブル) ---
        yield from self._parse_sanjyo(sanjyo_url)

    # ------------------------------------------------------------------
    def _fetch_list(self, index_url: str, member_list: str) -> bs4.BeautifulSoup | None:
        """index.php に POST 検索して結果一覧の soup を返す。"""
        try:
            resp = self.session.post(
                index_url,
                data={"member_list": member_list, "fetch_data": "検索"},
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
            if "charset=" not in resp.headers.get("Content-Type", "").lower():
                resp.encoding = resp.apparent_encoding
            return bs4.BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            self.logger.warning("一覧取得失敗 member_list=%s: %s", member_list, e)
            return None

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        kv: dict[str, bs4.Tag] = {}
        for tr in soup.select("table tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                if label:
                    kv[label] = cells[1]

        def txt(*keys: str) -> str:
            for k in keys:
                for label, cell in kv.items():
                    if label.startswith(k):
                        return cell.get_text(" ", strip=True)
            return ""

        name = txt("旅行業者名")
        if not name:
            return None

        # ホームページ (アンカー href 優先)
        hp = ""
        for label, cell in kv.items():
            if label.startswith("ホームページ"):
                a = cell.find("a", href=True)
                hp = a["href"].strip() if a else cell.get_text(strip=True)
                break

        return {
            Schema.NAME: name,
            Schema.PREF: txt("都道府県名"),
            Schema.POST_CODE: txt("郵便番号"),
            Schema.ADDR: txt("住所"),
            Schema.TEL: txt("電話番号"),
            Schema.REP_NM: txt("代表者名"),
            Schema.CAP: txt("資本金"),
            Schema.EMP_NUM: txt("旅行部門従事者数"),
            Schema.HP: hp,
            Schema.URL: detail_url,
            "会員種別": txt("会員種別"),
            "登録日": txt("登録日"),
            "登録行政庁名": txt("登録行政庁名"),
            "登録種別": txt("登録種別"),
            "登録番号": txt("登録番号"),
            "旅公取協": txt("旅公取協"),
            "IATA加入": txt("IATA加入"),
            "ボンド保証制度": txt("ボンド保証制度"),
            "重大事故支援システム": txt("重大事故支援システム"),
            "ビル名": txt("ビル名"),
            "FAX番号": txt("FAX番号"),
            "営業所数": txt("営業所数"),
            "旅行業者代理業者数": txt("旅行業者代理業者数"),
            "旅行業者代理業者の営業所数": txt("旅行業者代理業者の営業所数"),
        }

    def _parse_sanjyo(self, sanjyo_url: str):
        soup = self.get_soup(sanjyo_url)
        if soup is None:
            return
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                name = cells[0].get_text(" ", strip=True)
                if not name or "会社名" in name:  # ヘッダ行スキップ
                    continue

                # 住所セル: 〒post 都道府県+住所 / ビル名 / TEL: / FAX: を <br> 区切りで保持
                info_cell = cells[1]
                lines = [
                    t.strip()
                    for t in info_cell.get_text("\n", strip=True).split("\n")
                    if t.strip()
                ]

                post_code = pref = addr = building = tel = fax = ""
                addr_parts = []
                for ln in lines:
                    if ln.startswith("TEL"):
                        tel = ln.split(":", 1)[-1].strip() if ":" in ln else ln
                        continue
                    if ln.startswith("FAX"):
                        fax = ln.split(":", 1)[-1].strip() if ":" in ln else ln
                        continue
                    pm = _POST_PATTERN.search(ln)
                    if pm and not post_code:
                        post_code = pm.group(1)
                        ln = ln[pm.end():].strip()
                    prm = _PREF_PATTERN.match(ln)
                    if prm and not pref:
                        pref = prm.group(1)
                        ln = ln[prm.end():].strip()
                    if ln:
                        addr_parts.append(ln)
                if addr_parts:
                    addr = addr_parts[0]
                    if len(addr_parts) > 1:
                        building = " ".join(addr_parts[1:])

                # 会社名リンク (HP) があれば取得
                hp = ""
                a = cells[0].find("a", href=True)
                if a and a["href"].startswith("http"):
                    hp = a["href"].strip()

                yield {
                    Schema.NAME: name,
                    Schema.PREF: pref,
                    Schema.POST_CODE: post_code,
                    Schema.ADDR: addr,
                    Schema.TEL: tel,
                    Schema.HP: hp,
                    Schema.URL: sanjyo_url,
                    "会員種別": "国内賛助会員",
                    "ビル名": building,
                    "FAX番号": fax,
                }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Jata()
    # 🔒 sites.yml の url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://www.jata-net.or.jp/about/jata-about/about02/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
