# scripts/sites/nightlife/my_site.py

# ===== 【pip install -e . を実行していない場合のみ必要】===========
# 実行済みなら、この5行は丸ごと削除してOK。
# 実行方法: ターミナルでプロジェクトルートから `pip install -e .` または `uv pip install -e .`
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
# ================================================================

from src.framework.static import StaticCrawler   # 普通のサイトならこれ
from src.const.schema import Schema               # カラム名の定数


def extract_social_links(soup):
    social_links = {
        "HP": None,
        "X": None,
        "LINE公式": None,
        "Instagram": None,
        "TikTok": None,
        "YouTube": None,
    }
    for a_tag in soup.select(".shopSide__contents a.shopSide__contentsLink"):
        href = a_tag.get("href")
        if not href:
            continue
        icon = a_tag.select_one("div")
        icon_class = " ".join(icon.get("class", [])) if icon else ""
        if "--hp" in icon_class:
            social_links["HP"] = href
        elif "--x" in icon_class:
            social_links["X"] = href
        elif "--instagram" in icon_class:
            social_links["Instagram"] = href
        elif "--tikTok" in icon_class:
            social_links["TikTok"] = href
        elif "--youtube" in icon_class:
            social_links["YouTube"] = href
        elif "--line" in icon_class:
            social_links["LINE公式"] = href
    return social_links


def extract_phone_number(soup):
    # 一般的な国内電話番号（固定・携帯・IP・フリーダイヤル等）
    phone_pattern = re.compile(r"(?:\+81[-\s]?)?0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}")

    def normalize_phone(raw_text: str):
        digits = re.sub(r"\D", "", raw_text)
        if digits.startswith("81"):
            digits = "0" + digits[2:]
        return digits if re.fullmatch(r"0\d{9,10}", digits) else None

    # 最優先: 電話モーダル配下のtelリンク（ページ内の別用途リンク混入を避ける）
    tel_tag = soup.select_one('div[data-modal^="tel_"] a[href^="tel:"]')
    if not tel_tag:
        # 次点: 右カラム等にある通常のtelリンク
        tel_tag = soup.select_one("a.u-btn--pink.tel__phoneNumber[href^='tel:']") or soup.select_one('a[href^="tel:"]')
    if tel_tag:
        normalized = normalize_phone(tel_tag.get("href", "").replace("tel:", ""))
        if normalized:
            return normalized

    # 次点: 電話ラベル周辺テキスト（電話モーダル内を優先）
    candidates = []
    scoped_root = soup.select_one("div[data-modal^='tel_']") or soup
    for node in scoped_root.select("li, p, dt, dd, th, td, div, span"):
        text = node.get_text(" ", strip=True)
        if "TEL" in text.upper() or "電話" in text:
            candidates.append(text)
    for text in candidates:
        m = phone_pattern.search(text)
        if m:
            normalized = normalize_phone(m.group(0))
            if normalized:
                return normalized

    # 最終フォールバック: ページ全体
    m = phone_pattern.search(soup.get_text(" ", strip=True))
    if m:
        normalized = normalize_phone(m.group(0))
        if normalized:
            return normalized
    return None


class MinimumHostParaScraper(StaticCrawler):
    """ホスパラ（host-paradise.jp）掲載店舗情報スクレイパー"""

    DELAY = 1.0  # ← 待機秒数。1.0 = 1秒
    EXTRA_COLUMNS = ["TikTok", "YouTube"]

    def parse(self, url: str):
        # 1. HTMLを取得する（この1行で通信・リトライ・エラー処理は全部終わり）
        soup = self.get_soup(url)

        # 2. データを探す（CSSセレクタは対象サイトに合わせて変える）
        # 店名は詳細見出しを優先し、取れなければh1/dataLayerから補完
        name_tag = soup.select_one("h2.shopDetail__shopName")
        if name_tag and name_tag.get_text(strip=True):
            name = name_tag.get_text(strip=True)
        else:
            h1_tag = soup.select_one("h1.shop__name")
            if h1_tag and h1_tag.get_text(strip=True):
                name = re.split(r"[\(（\-]", h1_tag.get_text(" ", strip=True), maxsplit=1)[0].strip()
            else:
                name = None
                for script_tag in soup.find_all("script"):
                    script_text = script_tag.string or script_tag.get_text()
                    if not script_text:
                        continue
                    m = re.search(r"shop_name\s*:\s*['\"]([^'\"]+)['\"]", script_text)
                    if m:
                        name = m.group(1).strip()
                        break
        # 住所はアクセスマップ内を優先して取得
        addr_tag = soup.select_one("#accessMap .js-address") or soup.select_one("#accessMap p span")
        addr = addr_tag.get_text(strip=True) if addr_tag else None

        tel = extract_phone_number(soup)

        # SNS/公式HP情報
        social_links = extract_social_links(soup)
        
        yield {
                Schema.NAME: name,
                Schema.ADDR: addr,
                Schema.TEL: tel,
                Schema.HP: social_links["HP"],
                Schema.X: social_links["X"],
                Schema.INSTA: social_links["Instagram"],
                Schema.LINE: social_links["LINE公式"],
                "TikTok": social_links["TikTok"],
                "YouTube": social_links["YouTube"],
            }

# ============================================= 
#2. 一覧->詳細
# ============================================= 
class HostParadiseScraper(StaticCrawler):
    """ホスパラ（host-paradise.jp）掲載店舗情報スクレイパー"""
    DELAY = 1.0  # ← 待機秒数。1.0 = 1秒
    EXTRA_COLUMNS = ["TikTok", "YouTube"]

    def parse(self, url: str):
        # 1. 一覧ページから全店舗リンクを収集
        shop_links = self._collect_shop_links(url)
        self.total_items = len(shop_links)
        self.logger.info("店舗数: %d", self.total_items)
        for shop_link in shop_links:
            item = self._scrape_detail(shop_link)
            if item:
                yield item

# MinimumHostParaScraperの_parseを参考にして作成
    def _scrape_detail(self, url: str):
        soup = self.get_soup(url)
        # 店名は詳細見出しを優先し、取れなければh1/dataLayerから補完
        name_tag = soup.select_one("h2.shopDetail__shopName")
        if name_tag and name_tag.get_text(strip=True):
            name = name_tag.get_text(strip=True)
        else:
            h1_tag = soup.select_one("h1.shop__name")
            if h1_tag and h1_tag.get_text(strip=True):
                name = re.split(r"[\(（\-]", h1_tag.get_text(" ", strip=True), maxsplit=1)[0].strip()
            else:
                name = None

        # 住所はアクセスマップ内を優先して取得
        addr_tag = soup.select_one("#accessMap .js-address") or soup.select_one("#accessMap p span")
        addr = addr_tag.get_text(strip=True) if addr_tag else None

        tel = extract_phone_number(soup)
        
        # SNS/公式HP情報
        social_links = extract_social_links(soup)

        return {
            Schema.NAME: name,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: social_links["HP"],
            Schema.X: social_links["X"],
            Schema.INSTA: social_links["Instagram"],
            Schema.LINE: social_links["LINE公式"],
            "TikTok": social_links["TikTok"],
            "YouTube": social_links["YouTube"],
        }


    # 検索結果ページから詳細ページリンクを収集
    # 期待パターン: https://host-paradise.jp/{都道府県}/area{n}/{shop_id}/
    def _collect_shop_links_from_search_result(self, url: str) -> list[str]:
        soup = self.get_soup(url)
        shop_links = []
        for a_tag in soup.select("a[href]"):
            href = a_tag.get("href", "").strip()
            if not href:
                continue
            absolute_url = urljoin(url, href)
            parsed = urlparse(absolute_url)
            if parsed.netloc != "host-paradise.jp":
                continue
            if re.match(r"^/[a-z0-9_-]+/area\d+/\d+/?$", parsed.path):
                shop_links.append(f"{parsed.scheme}://{parsed.netloc}{parsed.path}")
        # 同ページ重複を削除
        return list(dict.fromkeys(shop_links))

    # 一覧ページからすべての店舗リンクを収集
    def _collect_shop_links(self, base_url: str) -> list[str]:
        base_url = base_url.rstrip("/")
        search_url = f"{base_url}/shop-search/result/"
        first_page_soup = self.get_soup(search_url)

        # ページ総数（例: "3ページ中、1~30件表示" から 3 を抽出）
        total_page_number = 1
        for p_tag in first_page_soup.select("p"):
            p_text = p_tag.get_text(" ", strip=True)
            m = re.search(r"(\d+)\s*ページ中", p_text)
            if m:
                total_page_number = int(m.group(1))
                break

        # 店舗リンクを収集（全ページ）
        shop_links = []
        shop_links.extend(self._collect_shop_links_from_search_result(search_url))
        for page_number in range(2, total_page_number + 1):
            shop_links.extend(self._collect_shop_links_from_search_result(f"{search_url}?page={page_number}"))

        # 全体重複を削除
        return list(dict.fromkeys(shop_links))

# ===== ここから下もコピペするだけ（実行用） =====
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    HostParadiseScraper().execute("https://host-paradise.jp/")
#    MinimumHostParaScraper().execute("https://host-paradise.jp/tokyo/area15/566/")