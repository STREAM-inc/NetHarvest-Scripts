# scripts/sites/construction/partnershop.py
"""
タカラスタンダードパートナーショップ (partnershop.takara-standard.co.jp)
— タカラスタンダード認定リフォーム施工店 (パートナーショップ加盟店)

取得対象:
    - 北海道に所在する登録店舗のみ (備考指示による絞り込み)
      一覧の検索条件 search_type=location (=お店の所在地で探す) + prefecture=01 を使い、
      さらに詳細ページの住所 (addressRegion) が「北海道」であることを再確認して絞り込む。
    - 名称・都道府県・郵便番号・住所・TEL・公式サイト(HP)・営業時間・定休日
      + FAX / 対応部位 / 対応範囲 / こだわりポイント / 得意分野 / 対応エリア / 受賞実績

取得フロー:
    ルート(url = /shop) から検索一覧 URL を組み立てる
      → /shop/search?search_type=location&prefecture=01&page=N (10件/ページ・北海道は全13ページ)
      → 一覧カード (.c-shopcard) の詳細リンク /shop/{id} を抽出
      → 詳細ページを 1 件取得するごとに即 yield (Pattern B)
    詳細ページの JSON-LD (LocalBusiness) を主ソースとし、
    JSON-LD に無い項目 (営業時間・定休日・FAX 等) は定義リスト dl.c-table から補う。

注意:
    - 当サイトは User-Agent がブラウザ的でないと 403 を返す (robots.txt に Disallow は無し)。
      StaticCrawler 既定の UA でも 200 だが、確実性のため新しめの Chrome UA を明示指定する。
    - 「お店からの一言」「リフォーム事例」は自由記述の長文のため、著作権リスクを避けて取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/partnershop.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id partnershop
"""

import json
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


class PartnershopScraper(StaticCrawler):
    """タカラスタンダードパートナーショップ スクレイパー (北海道限定)"""

    DELAY = 1.0

    # 403 対策: ブラウザ相当の User-Agent を明示する
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    EXTRA_COLUMNS = [
        "FAX",
        "対応部位",
        "対応範囲",
        "こだわりポイント",
        "得意分野",
        "対応エリア",
        "受賞実績",
    ]

    # --- 備考: 北海道の登録店舗のみ抽出 ---
    TARGET_PREF_CODE = "01"
    TARGET_PREF_NAME = "北海道"

    # 一覧 1 ページあたりの件数 (data-pn-length と一致)
    PAGE_SIZE = 10
    # 想定外のページ送りで無限ループしないための上限
    MAX_PAGES = 200

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        ルート url (=/shop) を起点に、北海道の所在地検索一覧を巡回し詳細を 1 件ずつ yield する。

        Args:
            url: サイトのルート URL (例: https://partnershop.takara-standard.co.jp/shop)
        """
        search_base = f"{url.rstrip('/')}/search"
        seen_ids: set[str] = set()

        page = 1
        total_pages = None
        while page <= self.MAX_PAGES:
            list_url = (
                f"{search_base}?search_type=location"
                f"&prefecture={self.TARGET_PREF_CODE}&page={page}"
            )
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s", list_url)
                break

            # 総件数からページ数を確定 (初回のみ)
            if total_pages is None:
                pager = soup.select_one(".c-pagination")
                if pager is not None:
                    try:
                        total = int(pager.get("data-pn-total") or 0)
                        length = int(pager.get("data-pn-length") or self.PAGE_SIZE) or self.PAGE_SIZE
                        if total > 0:
                            total_pages = -(-total // length)  # 切り上げ
                            self.total_items = total
                            self.logger.info("北海道の掲載件数: %s件 (%sページ)", total, total_pages)
                    except (TypeError, ValueError):
                        total_pages = None

            detail_paths = self._extract_detail_paths(soup)
            if not detail_paths:
                self.logger.info("一覧にカードが無いため終了: %s", list_url)
                break

            new_on_page = 0
            for path in detail_paths:
                shop_id = path.rstrip("/").rsplit("/", 1)[-1]
                if shop_id in seen_ids:
                    continue
                seen_ids.add(shop_id)
                new_on_page += 1

                detail_url = urljoin(url, path)
                try:
                    row = self._parse_detail(detail_url)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細解析失敗: %s (%s)", detail_url, e)
                    continue
                if row:
                    yield row

            if new_on_page == 0:
                # 同じページが返り続けている (ページ送り不能) → 打ち切り
                self.logger.info("新規店舗が無いため終了: %s", list_url)
                break

            if total_pages is not None and page >= total_pages:
                break
            page += 1

    # ------------------------------------------------------------------
    # 一覧
    # ------------------------------------------------------------------
    def _extract_detail_paths(self, soup) -> list[str]:
        """一覧カードから詳細ページのパス (/shop/{id}) を重複なしで取り出す"""
        paths: list[str] = []
        for card in soup.select(".c-shopcard"):
            a = card.select_one("a.c-shopcard__ttl[href]")
            if a is None:
                a = card.select_one("a[href]")
            if a is None:
                continue
            href = (a.get("href") or "").strip()
            if not re.search(r"/shop/\d+", href):
                continue
            # /shop/{id}/case などのサブページは除外し、店舗 TOP に正規化する
            m = re.search(r"(/shop/\d+)", href)
            path = m.group(1)
            if path not in paths:
                paths.append(path)
        return paths

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            self.logger.warning("詳細ページ取得失敗: %s", detail_url)
            return None

        ld = self._local_business_ld(soup)
        address = ld.get("address") or {}

        # --- 名称 ---
        name = (ld.get("name") or "").strip()
        if not name:
            h1 = soup.select_one("h1.c-pagetitle__heading")
            name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        # --- 住所 (JSON-LD 優先、無ければ dl.c-table の「所在地」から) ---
        pref = (address.get("addressRegion") or "").strip()
        locality = (address.get("addressLocality") or "").strip()
        street = (address.get("streetAddress") or "").strip()
        post_code = (address.get("postalCode") or "").strip()

        table = self._table_fields(soup)
        if not pref:
            raw_addr = table.get("所在地", "")
            m = re.search(r"〒?\s*(\d{3}-?\d{4})", raw_addr)
            if m and not post_code:
                post_code = m.group(1)
            rest = re.sub(r"〒?\s*\d{3}-?\d{4}", "", raw_addr).strip()
            m2 = re.match(r"(北海道|東京都|(?:京都|大阪)府|.{2,3}県)\s*(.*)", rest)
            if m2:
                pref = m2.group(1)
                if not locality and not street:
                    street = m2.group(2).strip()

        # 備考: 北海道の登録店舗のみ抽出
        if pref and pref != self.TARGET_PREF_NAME:
            self.logger.info("北海道以外のためスキップ: %s (%s)", name, pref)
            return None

        addr = " ".join(x for x in (locality, street) if x).strip()

        # --- TEL ---
        tel = (ld.get("telephone") or "").strip()
        if not tel:
            tel_el = soup.select_one(".c-table__tel p.is-main span")
            tel = tel_el.get_text(strip=True) if tel_el else ""

        # --- 公式サイト (掲載が無い店舗では JSON-LD に url 自体が存在しない) ---
        hp = (ld.get("url") or "").strip()
        if not hp:
            hp_el = soup.select_one("dl.c-table a.c-table__link[href]")
            hp = hp_el["href"].strip() if hp_el else ""

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.TIME: table.get("営業時間", ""),
            Schema.HOLIDAY: table.get("定休日", ""),
            "FAX": table.get("FAX", ""),
            "対応部位": self._reform_parts(soup),
            "対応範囲": self._section_list(soup, "対応範囲"),
            "こだわりポイント": self._section_list(soup, "こだわりポイント"),
            "得意分野": self._section_list(soup, "得意分野"),
            "対応エリア": self._section_list(soup, "対応エリア"),
            "受賞実績": self._awards(soup),
        }

    # ------------------------------------------------------------------
    # パーツ抽出
    # ------------------------------------------------------------------
    @staticmethod
    def _local_business_ld(soup) -> dict:
        """JSON-LD の LocalBusiness を取り出す (無ければ空 dict)"""
        for tag in soup.select('script[type="application/ld+json"]'):
            raw = tag.string or tag.get_text() or ""
            if "LocalBusiness" not in raw:
                continue
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            candidates = data if isinstance(data, list) else [data]
            for item in candidates:
                if isinstance(item, dict) and item.get("@type") == "LocalBusiness":
                    return item
        return {}

    @staticmethod
    def _table_fields(soup) -> dict:
        """dl.c-table の <dt>ラベル</dt><dd>値</dd> を辞書化する

        ラベルは「公式<br>サイト」のように <br> を含むため、テキスト連結後に空白を除去して照合する。
        """
        fields: dict[str, str] = {}
        dl = soup.select_one("dl.c-table")
        if dl is None:
            return fields
        current_label = None
        for child in dl.find_all(["dt", "dd"], recursive=False):
            if child.name == "dt":
                current_label = re.sub(r"\s+", "", child.get_text(" ", strip=True))
            elif current_label:
                text = child.get_text("\n", strip=True)
                # MAP ボタン等のラベルを除去
                text = re.sub(r"\bMAP\b", "", text)
                text = re.sub(r"ネットで相談する", "", text)
                text = re.sub(r"[ \t　]+", " ", text)
                text = re.sub(r"\n{2,}", "\n", text).strip()
                fields[current_label] = text
                current_label = None
        return fields

    @staticmethod
    def _section_list(soup, heading: str) -> str:
        """見出し (lv2/lv3) に対応する ul.p-shopDetail__list の項目を「 / 」連結で返す

        見出しは「こだわり<br>ポイント」のように <br> が入るため空白除去して比較する。
        """
        target = re.sub(r"\s+", "", heading)
        for h in soup.select("h2[class*='p-shopDetail__heading']"):
            if re.sub(r"\s+", "", h.get_text(" ", strip=True)) != target:
                continue
            section = h.find_parent("section")
            if section is None:
                continue
            ul = section.select_one("ul.p-shopDetail__list")
            if ul is None:
                continue
            items = [li.get_text(" ", strip=True) for li in ul.select("li")]
            return " / ".join(x for x in items if x)
        return ""

    @staticmethod
    def _reform_parts(soup) -> str:
        """対応部位 (対応可能なもののみ。is-disabled は非対応なので除外)"""
        ul = soup.select_one("ul.p-shopDetail__reformpart")
        if ul is None:
            return ""
        parts = []
        for li in ul.find_all("li", recursive=False):
            if "is-disabled" in (li.get("class") or []):
                continue
            span = li.select_one("span")
            text = span.get_text(strip=True) if span else li.get_text(" ", strip=True)
            if text:
                parts.append(text)
        return " / ".join(parts)

    @staticmethod
    def _awards(soup) -> str:
        """受賞実績 (賞名＋コンテスト名/部門) を「 / 」連結で返す"""
        ul = soup.select_one("ul.p-shopDetail__awardlist")
        if ul is None:
            return ""
        awards = []
        for li in ul.select("li"):
            texts = [p.get_text(" ", strip=True) for p in li.select("p")]
            text = " ".join(t for t in texts if t)
            text = re.sub(r"\s+", " ", text).strip()
            if text:
                awards.append(text)
        return " / ".join(awards)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PartnershopScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://partnershop.takara-standard.co.jp/shop")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
