"""
ナイト_ナイトスタイル【ガールズバー(ガルバ)】 (nightstyle.jp /g7/) — ナイトワーク店舗情報スクレイパー

取得対象:
    - nightstyle.jp の業種「ガールズバー(ガルバ)」(/g7/) に掲載された全国の店舗情報

取得フロー:
    1. 業種一覧ページ (/g7/, /g7/2/, ...) を a.next_page で全ページ巡回
    2. 各ページの lazy_render/search_shop_list エンドポイントのJSを解析し店舗URLを収集
    3. 各店舗詳細ページ (/shop/{slug}/) を取得即 yield (1件ずつストリーム)

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/nightstyle_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id nightstyle_2
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# JS の .before("...") / .append("...") / .html("...") 引数を抽出
_JS_HTML_RE = re.compile(r'\.(?:before|append|html)\("((?:\\.|[^"\\])*)"\)', re.DOTALL)
# 店舗トップページのみ許可: /shop/<slug>/
_SHOP_TOP_RE = re.compile(r"^/shop/[^/]+/$")

_HEX2 = re.compile(r"^[0-9a-fA-F]{2}$")
_HEX4 = re.compile(r"^[0-9a-fA-F]{4}$")
_HEXU = re.compile(r"^[0-9a-fA-F]{1,6}$")

_POST_RE = re.compile(r"〒?\s*(\d{3}-\d{4})")
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _js_unescape(raw: str) -> str:
    """JSダブルクォート文字列内のエスケープを復元する。"""
    out = []
    i = 0
    n = len(raw)
    while i < n:
        c = raw[i]
        if c != "\\":
            out.append(c)
            i += 1
            continue
        i += 1
        if i >= n:
            out.append("\\")
            break
        c = raw[i]
        if c == "\n":
            i += 1
            continue
        if c == "\r":
            i += 2 if (i + 1 < n and raw[i + 1] == "\n") else 1
            continue
        if c == "n":
            out.append("\n"); i += 1; continue
        if c == "r":
            out.append("\r"); i += 1; continue
        if c == "t":
            out.append("\t"); i += 1; continue
        if c in ("\\", '"', "'", "/"):
            out.append(c); i += 1; continue
        if c == "x" and i + 2 < n:
            h = raw[i + 1: i + 3]
            if _HEX2.match(h):
                out.append(chr(int(h, 16))); i += 3; continue
        if c == "u":
            if i + 1 < n and raw[i + 1] == "{":
                j = raw.find("}", i + 2)
                if j != -1:
                    h = raw[i + 2: j]
                    if _HEXU.match(h):
                        out.append(chr(int(h, 16))); i = j + 1; continue
            if i + 4 < n:
                h = raw[i + 1: i + 5]
                if _HEX4.match(h):
                    out.append(chr(int(h, 16))); i += 5; continue
        out.append(c)
        i += 1
    return "".join(out)


def _extract_lazy_html(js_text: str) -> str:
    parts = _JS_HTML_RE.findall(js_text)
    if not parts:
        return ""
    return "".join(_js_unescape(p) for p in parts)


class Nightstyle2Scraper(StaticCrawler):
    """ナイト_ナイトスタイル【ガールズバー(ガルバ)】(nightstyle.jp /g7/) スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["エリア", "ブログ", "YouTube"]

    def parse(self, url: str):
        # 引数 url を唯一のルート(SSOT)として扱う。BASE はそのスキーム+ホストから導出する。
        parts = urlsplit(url)
        base = f"{parts.scheme}://{parts.netloc}"

        seen: set[str] = set()
        current = url

        while current:
            soup = self.get_soup(current)
            if soup is None:
                break

            # 業種一覧の店舗URLは lazy_render/search_shop_list のJS内にある
            lazy_a = soup.find(
                "a", href=re.compile(r"^/lazy_render/search_shop_list")
            )
            shop_urls: list[str] = []
            if lazy_a and lazy_a.get("href"):
                lazy_url = urljoin(base, lazy_a["href"])
                shop_urls = self._fetch_lazy_shops(lazy_url, current, base)

            for shop_url in shop_urls:
                if shop_url in seen:
                    continue
                seen.add(shop_url)
                try:
                    item = self._scrape_detail(shop_url)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細取得エラー: %s — %s", shop_url, e)
                    continue
                if item:
                    yield item  # 取得即 yield (バッファしない)

            # ページ送り: a.next_page (絶対URL)
            next_tag = soup.find("a", class_="next_page")
            next_href = next_tag.get("href") if next_tag else None
            if not next_href:
                break
            nxt = urljoin(current, next_href)
            if nxt == current:
                break
            current = nxt

    def _fetch_lazy_shops(self, lazy_url: str, referer: str, base: str) -> list[str]:
        """lazy_render エンドポイントのJSから店舗URLを返す。"""
        try:
            resp = self.session.get(
                lazy_url,
                headers={"Referer": referer, "X-Requested-With": "XMLHttpRequest"},
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
            resp.encoding = "utf-8"
        except Exception as e:  # noqa: BLE001
            self.logger.warning("lazy_render 取得失敗: %s — %s", lazy_url, e)
            return []

        lazy_html = _extract_lazy_html(resp.text)
        if not lazy_html:
            return []

        soup = BeautifulSoup(lazy_html, "html.parser")
        seen: set[str] = set()
        urls: list[str] = []
        for a in soup.select('a[href^="/shop/"]'):
            href = a.get("href", "")
            if not _SHOP_TOP_RE.match(href):
                continue
            full = urljoin(base, href)
            if full not in seen:
                seen.add(full)
                urls.append(full)
        return urls

    def _split_address(self, dd) -> tuple[str, str, str]:
        """住所 dd を 郵便番号 / 都道府県 / 住所 に分割する。

        構造: 〒xxx-xxxx <br> 都道府県+住所 <br><br> アクセス案内(プロース)。
        アクセス案内文は著作権リスクのため取得しない。
        """
        # <br> 区切りで行に分割
        lines: list[str] = []
        for piece in dd.stripped_strings:
            lines.append(piece.strip())
        post = ""
        pref = ""
        addr = ""
        for ln in lines:
            m = _POST_RE.search(ln)
            if m and not post:
                post = m.group(1)
                ln = _POST_RE.sub("", ln).strip()
            if not ln:
                continue
            pm = _PREF_RE.match(ln)
            if pm and not addr:
                pref = pm.group(1)
                addr = ln[pm.end():].strip()
                break
            if not addr and not post:
                # 郵便番号も都道府県も無い行(まれ): 先頭行を住所扱い
                addr = ln
                break
        return post, pref, addr

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        container = soup.find("div", class_="shop-address")
        if not container:
            return None

        # 名称 + カナ
        name, kana = "", ""
        p_tag = container.find("p")
        if p_tag:
            name = "".join(
                t for t in p_tag.contents if isinstance(t, str)
            ).strip()
            span = p_tag.find("span")
            kana = span.get_text(strip=True) if span else ""

        # dl から各フィールドを抽出
        info_dd = {}
        dl = container.find("dl")
        if dl:
            for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                info_dd[dt.get_text(strip=True)] = dd

        def _text(key: str) -> str:
            dd = info_dd.get(key)
            return dd.get_text(" ", strip=True) if dd else ""

        post, pref, addr = ("", "", "")
        if "住所" in info_dd:
            post, pref, addr = self._split_address(info_dd["住所"])

        # 業種 (リンク文字列を連結)
        cat = ""
        if "業種" in info_dd:
            anchors = info_dd["業種"].find_all("a")
            cat = (
                ", ".join(a.get_text(strip=True) for a in anchors)
                if anchors
                else info_dd["業種"].get_text(" ", strip=True)
            )

        # SNS / 公式サイト (店舗固有のものは div.sns-links 内に class 付きで存在)
        sns = {
            "instagram": "", "twitter": "", "facebook": "",
            "line": "", "tiktok": "", "youtube": "", "blog": "", "official": "",
        }
        sns_box = soup.find("div", class_="sns-links")
        if sns_box:
            for a in sns_box.find_all("a", href=True):
                classes = a.get("class") or []
                for key in sns:
                    if key in classes and not sns[key]:
                        sns[key] = a["href"].strip()
                        break

        return {
            Schema.URL:       url,
            Schema.NAME:      name,
            Schema.NAME_KANA: kana,
            Schema.POST_CODE: post,
            Schema.PREF:      pref,
            Schema.ADDR:      addr,
            Schema.TEL:       _text("電話番号"),
            Schema.CAT_SITE:  cat,
            Schema.TIME:      _text("営業時間"),
            Schema.HOLIDAY:   _text("定休日"),
            Schema.HP:        sns["official"],
            Schema.INSTA:     sns["instagram"],
            Schema.X:         sns["twitter"],
            Schema.FB:        sns["facebook"],
            Schema.LINE:      sns["line"],
            Schema.TIKTOK:    sns["tiktok"],
            "エリア":          _text("エリア"),
            "ブログ":          sns["blog"],
            "YouTube":        sns["youtube"],
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Nightstyle2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://nightstyle.jp/g7/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
