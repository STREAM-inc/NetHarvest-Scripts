"""
もえなび！ (moe-navi.jp) — メイド喫茶・コンカフェ・コスプレリフレ店舗情報スクレイパー

取得対象:
    - メイドカフェ / コンカフェ / コスプレリフレ / ガールズバー 等の店舗情報
    - 店名、ジャンル、住所、都道府県、電話番号、営業時間、定休日、公式HP
    - 席数・予約可否・貸切・団体・飲酒・禁煙・Wifi・英語メニュー・深夜営業
    - 平均予算、店舗メールアドレス、アクセス、特典・サービス、こんな人にオススメ

取得フロー:
    1. /shopsearch/ を起点にオフセット型ページネーション (/shopsearch/{offset}) を全ページ巡回
    2. 各一覧ページから .shoplist-item 配下の詳細リンク (/shop/{id}/) を収集
    3. 各店舗詳細ページから table.shop-info-table をラベル辞書化して全フィールド抽出

実行方法:
    python scripts/sites/nightlife/moe_navi.py
    python bin/run_flow.py --site-id moe_navi
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


BASE_URL = "https://www.moe-navi.jp"
LIST_PATH = "/shopsearch/"
ITEMS_PER_PAGE = 20

_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_PATTERN = re.compile(r"(0\d{1,4}[- ]?\d{1,4}[- ]?\d{3,4})")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _split_address(raw: str) -> tuple[str, str, str]:
    """住所文字列から (郵便番号, 都道府県, 住所以降) を返す。"""
    if not raw:
        return "", "", ""
    post = ""
    m = _POST_PATTERN.search(raw)
    if m:
        post = m.group(1)
        if "-" not in post:
            post = f"{post[:3]}-{post[3:]}"
    body = _POST_PATTERN.sub("", raw).strip()
    body = re.split(r"(?:大きな地図で見る|地図を印刷する|https?://)", body)[0].strip()
    pref = ""
    addr = body
    pm = _PREF_PATTERN.search(body)
    if pm:
        pref = pm.group(1)
        # 「兵庫県兵庫県神戸市...」のような重複表記に対応して都道府県末尾以降を採用
        tail = body[pm.end():]
        dup = _PREF_PATTERN.match(tail)
        if dup and dup.group(1) == pref:
            tail = tail[dup.end():]
        addr = tail.strip()
    return post, pref, addr


def _extract_tel(raw: str) -> str:
    """TEL セル内のテキストから電話番号部分のみ抽出する。"""
    if not raw:
        return ""
    m = _TEL_PATTERN.search(raw)
    return m.group(1) if m else ""


def _extract_payments(info: dict) -> str:
    """クレジットカード / QR コード決済行から支払い方法を合成する。"""
    payments = []
    if _clean(info.get("クレジットカード決済", "")) == "可":
        payments.append("クレジットカード")
    if _clean(info.get("QRコード決済", "")) == "可":
        payments.append("QRコード")
    return ", ".join(payments)


class MoeNaviScraper(StaticCrawler):
    """もえなび！ (moe-navi.jp) の店舗情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "アクセス",
        "平均予算",
        "店舗メールアドレス",
        "席数",
        "予約",
        "貸切",
        "団体利用",
        "飲酒",
        "禁煙・喫煙",
        "Wifi・無線LAN",
        "英語メニュー",
        "深夜営業",
        "クレジットカード決済",
        "QRコード決済",
        "こんな人にオススメ",
        "特典・サービス",
        "紹介文",
        "サブ紹介文",
    ]

    def parse(self, url: str):
        list_url_first = urljoin(BASE_URL, LIST_PATH)
        first = self.get_soup(list_url_first)
        if first is None:
            self.logger.error("一覧ページの取得に失敗: %s", list_url_first)
            return

        total_count = self._extract_total_count(first)
        self.total_items = total_count
        self.logger.info("総件数: %d 件 / %d 件ずつ取得", total_count or 0, ITEMS_PER_PAGE)

        seen: set[str] = set()

        # 1ページ目
        for detail_url in self._collect_detail_urls(first):
            if detail_url in seen:
                continue
            seen.add(detail_url)
            item = self._scrape_detail(detail_url)
            if item:
                yield item

        # 2ページ目以降 (/shopsearch/{offset})
        offset = ITEMS_PER_PAGE
        while True:
            page_url = urljoin(BASE_URL, f"{LIST_PATH}{offset}")
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("ページ取得失敗: %s", page_url)
                offset += ITEMS_PER_PAGE
                if total_count and offset >= total_count:
                    break
                continue

            detail_urls = self._collect_detail_urls(soup)
            if not detail_urls:
                # ページャーの末尾
                break

            new_found = 0
            for detail_url in detail_urls:
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                new_found += 1
                item = self._scrape_detail(detail_url)
                if item:
                    yield item

            if new_found == 0:
                # ページ内容が変わらないループを防ぐ
                break

            offset += ITEMS_PER_PAGE
            if total_count and offset >= total_count + ITEMS_PER_PAGE:
                break

    def _extract_total_count(self, soup) -> int:
        """'N件がヒット' の数字を抽出する。見つからなければ 0。"""
        node = soup.select_one(".dataTables_info")
        text = node.get_text(" ", strip=True) if node else soup.get_text(" ", strip=True)
        m = re.search(r"([\d,]+)\s*件がヒット", text)
        if m:
            return int(m.group(1).replace(",", ""))
        return 0

    def _collect_detail_urls(self, soup) -> list[str]:
        urls: list[str] = []
        for item in soup.select(".shoplist-item"):
            a = item.select_one(".shop-icon-wrapper a[href]")
            if not a:
                a = item.select_one('a[href*="/shop/"]')
            if not a:
                continue
            href = a.get("href", "").strip()
            if not href or "/job" in href:
                continue
            # /shop/NNN/ 形式のみ採用
            full = urljoin(BASE_URL, href)
            if re.search(r"/shop/\d+/?$", full):
                urls.append(full)
        return list(dict.fromkeys(urls))

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            self.logger.warning("詳細ページ取得失敗: %s", url)
            return None

        try:
            info: dict[str, str] = {}
            for table in soup.select("table.shop-info-table"):
                for tr in table.select("tr"):
                    th = tr.select_one("th")
                    td = tr.select_one("td")
                    if not th or not td:
                        continue
                    # th はアイコン等もあるので text で判定
                    key = _clean(th.get_text(" ", strip=True))
                    if not key:
                        continue
                    value = _clean(td.get_text(" ", strip=True))
                    # 既出キーは上書きしない (複数テーブル間の重複対策)
                    if key in info and info[key]:
                        continue
                    info[key] = value

            # 店名: H1 を優先、無ければ table の "店名"
            name_el = soup.select_one("h1.sitemeta") or soup.select_one("h3.shop-name")
            name = _clean(name_el.get_text(" ", strip=True)) if name_el else info.get("店名", "")

            # エリア (shop-sub-info-table 先頭 td)
            area = ""
            sub = soup.select_one("table.shop-sub-info-table td")
            if sub:
                area = _clean(sub.get_text(" ", strip=True))

            # 電話番号
            tel = _extract_tel(info.get("TEL", ""))

            # 住所分解
            post, pref, addr_rest = _split_address(info.get("住所", ""))

            # 公式HP: 行のアンカー優先、無ければテキスト
            hp = ""
            for table in soup.select("table.shop-info-table"):
                for tr in table.select("tr"):
                    th = tr.select_one("th")
                    if th and _clean(th.get_text()) in ("公式サイト（PC）", "公式サイト(PC)", "公式サイト", "公式HP"):
                        td = tr.select_one("td")
                        if td:
                            a = td.select_one("a[href^='http']")
                            hp = a.get("href", "").strip() if a else _clean(td.get_text())
                        break
                if hp:
                    break

            # メールアドレス: mailto 優先
            email = ""
            for table in soup.select("table.shop-info-table"):
                for tr in table.select("tr"):
                    th = tr.select_one("th")
                    if th and "メールアドレス" in _clean(th.get_text()):
                        td = tr.select_one("td")
                        if td:
                            ma = td.select_one("a[href^='mailto:']")
                            email = ma.get("href", "").replace("mailto:", "").strip() if ma else _clean(td.get_text())
                        break
                if email:
                    break

            # 平均予算: "3000円" → そのまま格納
            budget = info.get("平均予算(お一人様)", "") or info.get("平均予算", "")

            # 営業時間
            open_time = info.get("営業時間", "")

            # 紹介文 (shoplist-description) は一覧ページにしか無いため詳細ページでは空許容
            intro = ""
            desc_el = soup.select_one(".shop-description, .shop-main-description, [class*='shop-description']")
            if desc_el:
                intro = _clean(desc_el.get_text(" ", strip=True))[:500]

            sub_intro = ""
            sub_desc_el = soup.select_one(".shoplist-sub_description, .shop-sub-description")
            if sub_desc_el:
                sub_intro = _clean(sub_desc_el.get_text(" ", strip=True))[:200]

            item = {
                Schema.URL: url,
                Schema.NAME: name,
                Schema.PREF: pref,
                Schema.POST_CODE: post,
                Schema.ADDR: addr_rest,
                Schema.TEL: tel,
                Schema.CAT_SITE: info.get("ジャンル", ""),
                Schema.HP: hp,
                Schema.TIME: open_time,
                Schema.HOLIDAY: info.get("定休日", ""),
                Schema.PAYMENTS: _extract_payments(info),
                # EXTRA_COLUMNS
                "エリア": area,
                "アクセス": info.get("アクセス", ""),
                "平均予算": budget,
                "店舗メールアドレス": email,
                "席数": info.get("席数", ""),
                "予約": info.get("予約", ""),
                "貸切": info.get("貸切", ""),
                "団体利用": info.get("団体利用", ""),
                "飲酒": info.get("飲酒", ""),
                "禁煙・喫煙": info.get("禁煙・喫煙", ""),
                "Wifi・無線LAN": info.get("Wifi・無線LAN", ""),
                "英語メニュー": info.get("英語メニュー", ""),
                "深夜営業": info.get("深夜営業", ""),
                "クレジットカード決済": info.get("クレジットカード決済", ""),
                "QRコード決済": info.get("QRコード決済", ""),
                "こんな人にオススメ": info.get("こんな人にオススメ", ""),
                "特典・サービス": info.get("特典・サービス", ""),
                "紹介文": intro,
                "サブ紹介文": sub_intro,
            }
            return item
        except Exception as exc:
            self.logger.exception("詳細ページ解析失敗 %s: %s", url, exc)
            return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MoeNaviScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
