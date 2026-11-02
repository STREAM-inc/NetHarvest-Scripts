"""
ぐるっと会津 — 会津地方の地域ポータル 店舗・施設詳細

対象サイト: https://www.gurutto-aizu.com/

取得対象:
    - sitemap.xml に載る店舗・施設詳細 /detail/{id}/index.html (約370件)

取得フロー:
    {url}sitemap.xml → /detail/{id}/index.html のみ抽出 (access/menu/news/coupon/
    review/job/freepage 等のサブページは除外)
    → 詳細の基本情報テーブル (名称/電話/FAX/住所/公式URL/定休日 等)
      (公式HP型レイアウトで基本情報テーブルが無い店舗は /detail/{id}/company.html を参照)
    → 業種はパンくず (大カテゴリ) と h1 先頭の「{市町村}の{ジャンル}」から取得
    ニュース/求人/イベント/クーポン/口コミ等の記事ページは対象外。
    ぐるっと福島 (portal/gurutto_fukushima.py) と同一プラットフォーム。

実行方法:
    python scripts/sites/portal/gurutto_aizu.py
    docker compose exec worker python /app/bin/run_flow.py --site-id gurutto_aizu
"""

import re
import sys
import warnings
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema
from bs4 import XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

_DETAIL_RE = re.compile(r"/detail/(\d+)/index\.html$")
_TEL_RE = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")
_ZIP_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")
_KANA_RE = re.compile(r"[\(（]([ァ-ヶー・\s　]+)[\)）]\s*$")
_READING_RE = re.compile(r"[\(（]([ぁ-ゖー・\s　]+)[\)）]\s*$")
_GENRE_RE = re.compile(r"^\S+?[市町村郡]の(.+)$")
_HP_LABELS = ("公式URL", "公式サイト", "ホームページ", "URL")

_SNS_MAP = (
    ("instagram.com", Schema.INSTA),
    ("facebook.com", Schema.FB),
    ("twitter.com", Schema.X),
    ("x.com", Schema.X),
    ("line.me", Schema.LINE),
    ("lin.ee", Schema.LINE),
    ("tiktok.com", Schema.TIKTOK),
)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def _to_hira(text: str) -> str:
    return "".join(chr(ord(c) - 0x60) if "ァ" <= c <= "ヶ" else c for c in text)


def _short_name(name: str, kana: str = "") -> str:
    """「店名｜源泉かけ流しの…」「会津若松市の食事 …なら ｜ 店名（よみ）」のような
    SEO キーワード部分を落として店名部分を返す。"""
    parts = [p.strip() for p in re.split(r"\s*[｜|]\s*", name) if p.strip()]
    if not parts:
        return name
    pick = parts[0]
    kana_hira = re.sub(r"[\s　・]", "", _to_hira(kana))
    for part in parts:
        m = _READING_RE.search(part)
        if m and kana_hira and re.sub(r"[\s　・]", "", m.group(1)) == kana_hira:
            pick = part
            break
    else:
        if "の" in pick:
            plain = [p for p in parts[1:] if "の" not in p]
            if plain:
                pick = plain[0]
    m = _READING_RE.search(pick)
    if m and m.start() > 0:
        pick = pick[: m.start()].strip()
    return pick


class GuruttoAizuScraper(StaticCrawler):
    """ぐるっと会津 スクレイパー"""

    DELAY = 5.0  # robots.txt は * に Crawl-delay 指定なし (GPTBot=20)。低負荷で 5 秒
    EXTRA_COLUMNS = ["大カテゴリ", "FAX", "予約専用番号", "駐車場"]

    def parse(self, url: str):
        sitemap = self.get_soup(urljoin(url, "sitemap.xml"))
        if sitemap is None:
            return
        host = urlparse(url).netloc
        detail_urls: list[str] = []
        seen: set[str] = set()
        for loc in sitemap.find_all("loc"):
            loc_url = _clean(loc.get_text())
            if urlparse(loc_url).netloc != host or not _DETAIL_RE.search(loc_url):
                continue
            if loc_url not in seen:
                seen.add(loc_url)
                detail_urls.append(loc_url)
        self.total_items = len(detail_urls)

        for detail_url in detail_urls:
            item = self._parse_detail(detail_url)
            if item and item[Schema.NAME]:
                yield item

    # ------------------------------------------------------------------
    @staticmethod
    def _info_rows(soup) -> dict:
        rows: dict = {}
        for tr in soup.select("table tr"):
            th, td = tr.find("th"), tr.find("td")
            if th and td:
                label = _clean(th.get_text(" ", strip=True))
                if label and label not in rows:
                    rows[label] = td
        return rows

    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {
            Schema.URL: detail_url,
            Schema.NAME: "",
            Schema.NAME_KANA: "",
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.CAT_SITE: "",
            Schema.HP: "",
            Schema.HOLIDAY: "",
            Schema.TIME: "",
        }
        item.update({col: "" for col in (Schema.INSTA, Schema.FB, Schema.X, Schema.LINE, Schema.TIKTOK)})
        item.update({col: "" for col in self.EXTRA_COLUMNS})

        # パンくず: ぐるっと会津 > {大カテゴリ} > {店名}
        crumbs = [_clean(a.get_text(" ", strip=True))
                  for a in soup.select("#breadBox li:not(.home) a")]
        page_name = crumbs[-1] if crumbs else ""
        if len(crumbs) >= 2:
            item["大カテゴリ"] = crumbs[0]

        # h1: 「{市町村}の{ジャンル} {店名}」
        h1 = soup.select_one("h1.site__header-h1")
        if h1:
            h1_text = _clean(h1.get_text(" ", strip=True))
            if page_name and h1_text.endswith(page_name):
                h1_text = h1_text[: -len(page_name)].strip()
            m = _GENRE_RE.match(h1_text)
            if m:
                item[Schema.CAT_SITE] = _clean(m.group(1))
        if not item[Schema.CAT_SITE]:
            item[Schema.CAT_SITE] = item["大カテゴリ"]

        rows = self._info_rows(soup)
        if "住所" not in rows and soup.select_one('a[href$="/company.html"]'):
            csoup = self.get_soup(urljoin(detail_url, "company.html"))
            if csoup is not None:
                crows = self._info_rows(csoup)
                if crows:
                    rows, soup = crows, csoup

        def text(label: str) -> str:
            td = rows.get(label)
            return _clean(td.get_text(" ", strip=True)) if td is not None else ""

        # 名称・カナ
        name = text("名称")
        m = _KANA_RE.search(name)
        if m:
            item[Schema.NAME_KANA] = _clean(m.group(1))
            name = _clean(name[: m.start()])
        if text("フリガナ"):
            item[Schema.NAME_KANA] = text("フリガナ")
        item[Schema.NAME] = _short_name(name or page_name, item[Schema.NAME_KANA])

        # 住所・郵便番号 (「アクセス」リンクを除去)
        addr_td = rows.get("住所") or rows.get("所在地")
        if addr_td is not None:
            for a in addr_td.find_all("a"):
                a.decompose()
            addr = _clean(addr_td.get_text(" ", strip=True))
            m = _ZIP_RE.search(addr)
            if m:
                item[Schema.POST_CODE] = m.group(1)
                addr = _clean(addr[m.end():])
            item[Schema.ADDR] = addr
            m = _PREF_RE.match(addr)
            if m:
                item[Schema.PREF] = m.group(1)

        # 電話 (予約専用番号とお問合せ番号が併記されることがある)
        tel_txt = text("電話") or text("TEL") or text("電話番号")
        if tel_txt:
            reserve = re.search(r"予約専用[：:]\s*(" + _TEL_RE.pattern + ")", tel_txt)
            inquiry = re.search(r"お問合せ[：:]\s*(" + _TEL_RE.pattern + ")", tel_txt)
            if reserve:
                item["予約専用番号"] = reserve.group(1)
            if inquiry:
                item[Schema.TEL] = inquiry.group(1)
            elif not reserve:
                m = _TEL_RE.search(tel_txt)
                item[Schema.TEL] = m.group(0) if m else ""
        fax = _TEL_RE.search(text("FAX"))
        item["FAX"] = fax.group(0) if fax else ""

        for label in _HP_LABELS:
            hp_td = rows.get(label)
            if hp_td is None:
                continue
            a = hp_td.find("a", href=re.compile(r"^https?://"))
            hp = a["href"] if a else text(label)
            if hp.startswith("http"):
                item[Schema.HP] = hp
                break

        item[Schema.HOLIDAY] = text("定休日") or text("休館日") or text("休業日")
        item[Schema.TIME] = text("営業時間")
        item["駐車場"] = text("駐車場")

        # SNS (店舗ナビ内の公式SNSリンク / テーブル内リンク)
        for a in soup.select(".l-navi__sns-list a[href], td a[href]"):
            href = a["href"]
            for key, col in _SNS_MAP:
                if key in urlparse(href).netloc and not item[col]:
                    item[col] = href
        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = GuruttoAizuScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.gurutto-aizu.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
