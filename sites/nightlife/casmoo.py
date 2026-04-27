"""
ウィリスト (キャスモー casmoo.jp) — 広島の夜のお店総合情報サイト スクレイパー

取得対象:
    - 広島エリアのキャバクラ/クラブ/ラウンジ/スナック/ガールズバー/コンセプトカフェ/
      セクキャバ/ホストクラブ/ボーイズバー/バー/メンズコンセプト/その他 全店舗の店舗詳細情報
    - 店名、ジャンル、住所、郵便番号、電話番号、営業時間、定休日、HP、Instagram、TikTok 等
    - 在籍キャスト、席数、システム、料金、本指名、場内指名、同伴、TAX、決済方法、注意事項 等

取得フロー:
    1. ジャンル別の一覧 (gsid=1..4 の 4 系統) を pn パラメータで全ページ巡回
    2. 各一覧ページから /shop/{id}/ 形式の店舗詳細リンクを収集
    3. 各店舗詳細ページから h1 + table.CustomShopInfoTable を辞書化して全フィールド抽出

実行方法:
    python scripts/sites/nightlife/casmoo.py
    python bin/run_flow.py --site-id casmoo
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


BASE_URL = "https://casmoo.jp"
LIST_URL_TMPL = (
    "https://casmoo.jp/shoplist.asp?mode=&pn={pn}&sort=&gsid={gsid}"
    "&regid=8&preid=34&areaid=3401"
)
GSID_LIST = [1, 2, 3, 4]
MAX_PAGES_PER_GSID = 20

_POST_PATTERN = re.compile(r"〒?\s*(\d{3})\s*[-‐−]?\s*(\d{4})")
_TEL_PATTERN = re.compile(r"(0\d{1,4}[-‐−]?\d{1,4}[-‐−]?\d{3,4})")
_SHOP_URL_PATTERN = re.compile(r"^/shop/\d+/?$")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _split_address(raw: str) -> tuple[str, str]:
    """住所文字列から (郵便番号, 住所以降) を返す。都道府県は固定 '広島県'。"""
    if not raw:
        return "", ""
    text = _clean(raw)
    text = re.sub(r"地図アプリで開く|地図を見る|大きな地図で見る", "", text).strip()
    post = ""
    m = _POST_PATTERN.search(text)
    if m:
        post = f"{m.group(1)}-{m.group(2)}"
        text = _POST_PATTERN.sub("", text, count=1).strip()
    return post, text


def _extract_tel(raw: str) -> str:
    if not raw:
        return ""
    m = _TEL_PATTERN.search(raw)
    return m.group(1) if m else ""


def _split_shop_name(h1_text: str, table_name: str) -> str:
    """H1 の '店名│...流川エリアの広島キャバクラ...' から店名部分のみ取り出す。"""
    if h1_text:
        for sep in ("│", "|"):
            if sep in h1_text:
                return _clean(h1_text.split(sep, 1)[0])
        return _clean(h1_text)
    # フォールバック: 店名セルは ruby 二重表示 ('Gran グラングラン') があるため、
    # 末尾の重複カナを軽く除去
    if table_name:
        cleaned = _clean(table_name)
        m = re.match(r"^(.*?)([ぁ-ヿ゠-ヿ぀-ゟー]+)\2$", cleaned)
        if m:
            return _clean(m.group(1) + m.group(2))
        return cleaned
    return ""


def _classify_social(href: str, current: dict) -> None:
    if not href:
        return
    h = href.lower()
    if "instagram.com" in h and not current.get(Schema.INSTA):
        current[Schema.INSTA] = href
    elif "tiktok.com" in h and not current.get(Schema.TIKTOK):
        current[Schema.TIKTOK] = href
    elif ("twitter.com" in h or "x.com" in h) and not current.get(Schema.X):
        current[Schema.X] = href
    elif "facebook.com" in h and not current.get(Schema.FB):
        current[Schema.FB] = href
    elif "line.me" in h and not current.get(Schema.LINE):
        current[Schema.LINE] = href


class CasmooScraper(StaticCrawler):
    """ウィリスト (キャスモー) 店舗情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "キャッチコピー",
        "在籍キャスト",
        "席数",
        "席説明",
        "システム",
        "延長料金",
        "VIPご利用料金",
        "本指名",
        "場内指名",
        "同伴",
        "TAX&サービス料",
        "飲み放題",
        "カラオケ",
        "クレジットカード",
        "電子マネー",
        "注意事項",
    ]

    def parse(self, url: str):
        seen: set[str] = set()
        total_estimate = 0

        # 1ページ目を先に踏んで全 gsid の総件数を控える
        first_soup = self.get_soup(LIST_URL_TMPL.format(pn=1, gsid=GSID_LIST[0]))
        if first_soup is not None:
            total_estimate = self._extract_total_count_from_genre_links(first_soup)
            if total_estimate:
                self.total_items = total_estimate
                self.logger.info("推定総件数: %d 件", total_estimate)

        for gsid in GSID_LIST:
            self.logger.info("ジャンル gsid=%d を巡回開始", gsid)
            for pn in range(1, MAX_PAGES_PER_GSID + 1):
                page_url = LIST_URL_TMPL.format(pn=pn, gsid=gsid)
                soup = self.get_soup(page_url)
                if soup is None:
                    self.logger.warning("一覧ページ取得失敗: %s", page_url)
                    break

                detail_urls = self._collect_detail_urls(soup)
                if not detail_urls:
                    self.logger.info("gsid=%d pn=%d: 店舗リンクなし。次ジャンルへ", gsid, pn)
                    break

                new_count = 0
                for detail_url in detail_urls:
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)
                    new_count += 1
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item

                if new_count == 0:
                    # 同じページが返り続ける場合の無限ループ防止
                    self.logger.info("gsid=%d pn=%d: 新規店舗なし。打ち切り", gsid, pn)
                    break

    def _extract_total_count_from_genre_links(self, soup) -> int:
        """ジャンル選択メニューの '(NNN)' 表記から全ジャンル合計を算出する。"""
        total = 0
        # SlideMenuGSLinkBox 内に '<gname>(NNN)' のテキストがある
        nodes = soup.select(".SlideMenuGSLinkBox dt, .SlideMenuGSLinkBox a, .SlideMenuGSLinkBox dd")
        text = " ".join(n.get_text(" ", strip=True) for n in nodes) if nodes else soup.get_text(" ", strip=True)
        # 大ジャンル表記: 'キャバクラ・ガールズバー・スナック・コンカフェ(161)'
        # ホストクラブ・ボーイズバー(38), セクキャバ(12), 飲食・美容・その他(3)
        for label in (
            "キャバクラ・ガールズバー・スナック・コンカフェ",
            "セクキャバ",
            "ホストクラブ・ボーイズバー",
            "飲食・美容・その他",
        ):
            m = re.search(re.escape(label) + r"\s*\(\s*(\d+)\s*\)", text)
            if m:
                total += int(m.group(1))
        return total

    def _collect_detail_urls(self, soup) -> list[str]:
        urls: list[str] = []
        for a in soup.select('a[href]'):
            href = a.get("href", "").strip()
            if not href:
                continue
            # 相対/絶対両対応
            if href.startswith("http"):
                if "casmoo.jp" not in href:
                    continue
                path = href.split("casmoo.jp", 1)[1]
            else:
                path = href
            if _SHOP_URL_PATTERN.match(path):
                full = urljoin(BASE_URL, path)
                if not full.endswith("/"):
                    full += "/"
                urls.append(full)
        return list(dict.fromkeys(urls))

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            self.logger.warning("詳細ページ取得失敗: %s", url)
            return None

        try:
            info: dict[str, str] = {}
            for table in soup.select("table.CustomShopInfoTable"):
                for tr in table.select("tr"):
                    th = tr.select_one("th")
                    td = tr.select_one("td")
                    if not th or not td:
                        continue
                    key = _clean(th.get_text(" ", strip=True))
                    if not key:
                        continue
                    value = _clean(td.get_text(" ", strip=True))
                    if key in info and info[key]:
                        continue
                    info[key] = value

            h1_el = soup.select_one("h1")
            name = _split_shop_name(
                h1_el.get_text(" ", strip=True) if h1_el else "",
                info.get("店名", ""),
            )

            h2_el = soup.select_one("h2")
            catch = _clean(h2_el.get_text(" ", strip=True)) if h2_el else ""

            post, addr_rest = _split_address(info.get("住所", ""))
            tel = _extract_tel(info.get("電話番号", ""))

            socials: dict[str, str] = {}
            for a in soup.select('a[href]'):
                _classify_social(a.get("href", ""), socials)

            # 外部 HP リンク (casmoo.jp 以外, SNS/地図系を除外)
            hp = ""
            for a in soup.select('a[href^="http"]'):
                href = a.get("href", "").strip()
                if not href or "casmoo.jp" in href:
                    continue
                if re.search(
                    r"instagram\.com|tiktok\.com|twitter\.com|x\.com|facebook\.com|line\.me|youtube\.com|youtu\.be|google\.com|googleapis|google\.co\.jp|maps\.app",
                    href,
                    re.IGNORECASE,
                ):
                    continue
                hp = href
                break

            payments: list[str] = []
            if _clean(info.get("クレジットカード", "")).startswith("利用可"):
                payments.append("クレジットカード")
            if _clean(info.get("電子マネー", "")).startswith("利用可"):
                payments.append("電子マネー")

            item = {
                Schema.URL: url,
                Schema.NAME: name,
                Schema.PREF: "広島県",
                Schema.POST_CODE: post,
                Schema.ADDR: addr_rest,
                Schema.TEL: tel,
                Schema.CAT_SITE: info.get("ジャンル", ""),
                Schema.HP: hp,
                Schema.INSTA: socials.get(Schema.INSTA, ""),
                Schema.TIKTOK: socials.get(Schema.TIKTOK, ""),
                Schema.X: socials.get(Schema.X, ""),
                Schema.FB: socials.get(Schema.FB, ""),
                Schema.LINE: socials.get(Schema.LINE, ""),
                Schema.TIME: info.get("営業時間", ""),
                Schema.HOLIDAY: info.get("定休日", ""),
                Schema.PAYMENTS: ", ".join(payments),
                # EXTRA_COLUMNS
                "キャッチコピー": catch,
                "在籍キャスト": info.get("在籍キャスト", ""),
                "席数": info.get("席数", ""),
                "席説明": info.get("席説明", ""),
                "システム": info.get("システム", ""),
                "延長料金": info.get("延長料金", ""),
                "VIPご利用料金": info.get("VIPご利用料金", ""),
                "本指名": info.get("本指名", ""),
                "場内指名": info.get("場内指名", ""),
                "同伴": info.get("同伴", ""),
                "TAX&サービス料": info.get("TAX&サービス料", ""),
                "飲み放題": info.get("飲み放題", ""),
                "カラオケ": info.get("カラオケ", ""),
                "クレジットカード": info.get("クレジットカード", ""),
                "電子マネー": info.get("電子マネー", ""),
                "注意事項": info.get("注意事項", ""),
            }
            return item
        except Exception as exc:
            self.logger.exception("詳細ページ解析失敗 %s: %s", url, exc)
            return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = CasmooScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
