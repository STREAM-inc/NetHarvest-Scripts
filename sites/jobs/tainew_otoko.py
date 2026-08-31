# https://tainew-otoko.com/用
"""
メンズ体入 (tainew-otoko.com) — キャバクラボーイ・黒服求人スクレイパー (再調査版)

既存 tainew_otoko が 0 件になった件の再調査を踏まえた改訂版。

再調査 (2026-08-31) の結果:
    - サイト・sitemap.xml とも HTTP 200 で静的取得可能 (WAF なし / robots.txt は全許可)
    - 利用規約 (/menu/terms/) にスクレイピング・クローリングの明示的禁止条項なし
    - sitemap.xml の /shop/view/ が最も網羅的 (367件)。
      一覧ページ (/shoplist/*) から辿れるのは 351件で sitemap の部分集合
    - 詳細ページの構造 (h1.shopName / th-td テーブル / div.shopViewWrap) は現行も有効

旧版からの主な修正:
    1. sitemap URL を引数 url から派生させる (ハードコードを廃止)
    2. 住所・TEL が空でもレコードを破棄せず出力する
       (旧版は 住所/TEL いずれか欠落で return None → 取りこぼし。実測 TEL 欠落 約10%)
    3. DELAY を 1.0 → 0.3 に短縮 (367件で 8分超 → 実行時間超過による 0 件化を回避)
    4. 募集職種 / エリア / 最寄り駅 / 採用担当 を EXTRA_COLUMNS として追加

取得対象:
    店舗名 / 名称_カナ / 都道府県 / 住所 / TEL / サイト定義業種 / 定休日 / HP / SNS
    + EXTRA: 募集職種 / エリア / 最寄り駅 / 採用担当

実行方法:
    python scripts/sites/nightlife/tainew_otoko_2.py
    python bin/run_flow.py --site-id tainew_otoko_2
"""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup, Tag

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_PATTERN = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}|0\d{9,10}")
_TEL_NOTE_RE = re.compile(r"[（(]受付時間[^）)]*[）)]")
_KANA_TEXT_RE = re.compile(r"^[ぁ-ゟ゠-ヿー\s・･ー－\-]+$")
# 給与テーブルの td は必ず給与形態で始まる (例: "時給 2,000円 以上")
_SALARY_RE = re.compile(r"^(時給|月給|日給|年収|週給|完全歩合|歩合給)")
# 給与テーブルと同じ th/td 形式で並ぶ募集条件・応募情報のラベル (職種ではない)
_NON_JOB_LABELS = frozenset({
    "時間", "休日", "資格", "待遇", "応募方法", "担当", "TEL", "住所",
    "最寄り駅", "店舗URL", "SNS", "給与", "勤務地", "備考",
})

# HP 候補から除外するホスト (SNS・地図・自社ドメイン・画像ストレージ)
_HP_SKIP_HOSTS = (
    "instagram.com",
    "line.me",
    "lin.ee",
    "twitter.com",
    "x.com",
    "facebook.com",
    "tiktok.com",
    "maps.google.com",
    "google.com",
    "goo.gl",
    "youtube.com",
    "youtu.be",
    "tainew-otoko.com",
    "tainew.com",
    "storage.googleapis.com",
    "luline.jp",
)


class TainewOtoko2Scraper(StaticCrawler):
    """メンズ体入 スクレイパー (再調査版)"""

    DELAY = 0.3
    EXTRA_COLUMNS: list[str] = ["募集職種", "エリア", "最寄り駅", "採用担当"]

    _SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    _SHOP_PATH_RE = re.compile(r"^/shop/view/[^/]+/?$")

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        """sitemap.xml から店舗詳細URLを列挙し、1件取得ごとに即 yield する。"""
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("対象店舗URL数: %d", self.total_items)

        seen: set[tuple[str, str, str]] = set()
        saved = skipped = failed = 0

        for index, shop_url in enumerate(shop_urls, start=1):
            try:
                soup = self.get_soup(shop_url)
            except Exception as e:  # noqa: BLE001 — 1件の失敗で全体を止めない
                failed += 1
                self.logger.warning("詳細取得失敗: %s (%s)", shop_url, e)
                continue
            if soup is None:
                failed += 1
                continue

            record = self._parse_shop_page(shop_url, soup)
            if record is None:
                failed += 1
                self.logger.warning("店舗名が取得できずスキップ: %s", shop_url)
                continue

            key = self._shop_key(record)
            if key in seen:
                skipped += 1
                self.logger.info("重複スキップ: %s", record[Schema.NAME])
                continue
            seen.add(key)
            saved += 1
            self.logger.info(
                "取得OK: %d/%d 残り%d件 店舗=%s",
                index,
                self.total_items,
                self.total_items - index,
                record[Schema.NAME],
            )
            yield record

        self.logger.info(
            "完了: 候補%d件 取得%d件 重複%d件 失敗%d件",
            self.total_items,
            saved,
            skipped,
            failed,
        )

    # ------------------------------------------------------------------ #
    # 列挙
    # ------------------------------------------------------------------ #
    def _collect_shop_urls(self, url: str) -> list[str]:
        """引数 url を起点に sitemap.xml から /shop/view/ を収集する。"""
        # 引数がすでに店舗詳細URLの場合はそれ1件だけを対象にする (単体テスト用)
        if self._SHOP_PATH_RE.match(urlparse(url).path):
            return [url]

        sitemap_url = url if url.endswith(".xml") else urljoin(url, "/sitemap.xml")
        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception as e:  # noqa: BLE001
            self.logger.warning("サイトマップ取得失敗: %s (%s)", sitemap_url, e)
            return []

        host = urlparse(url).netloc or urlparse(sitemap_url).netloc
        shop_urls: list[str] = []
        for node in root.findall(".//sm:loc", self._SITEMAP_NS):
            loc = (node.text or "").strip()
            if not loc:
                continue
            parsed = urlparse(loc)
            if parsed.netloc != host:
                continue
            if not self._SHOP_PATH_RE.match(parsed.path):
                continue
            shop_urls.append(loc if loc.endswith("/") else loc + "/")
        return list(dict.fromkeys(shop_urls))

    # ------------------------------------------------------------------ #
    # 詳細ページ解析
    # ------------------------------------------------------------------ #
    def _parse_shop_page(self, shop_url: str, soup: BeautifulSoup) -> dict | None:
        name, kana = self._extract_name_kana(soup)
        if not name:
            return None
        if not kana:
            kana = self._extract_kana_from_meta(soup, name)

        labels = self._extract_labels(soup)
        address = self._extract_address(labels.get("住所"))
        pref, addr_body = self._split_pref(address)
        sns = self._extract_sns(soup, labels.get("SNS"))

        return {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: self._extract_tel(soup, labels),
            Schema.CAT_SITE: self._extract_cat_site(soup, shop_url),
            Schema.HOLIDAY: self._text_of(labels.get("休日")),
            Schema.HP: self._extract_hp(soup, labels.get("店舗URL")),
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.LINE: sns["line"],
            Schema.TIKTOK: sns["tiktok"],
            "募集職種": self._extract_job_types(soup),
            "エリア": self._extract_area(soup, shop_url),
            "最寄り駅": self._text_of(labels.get("最寄り駅")),
            "採用担当": self._text_of(labels.get("担当")),
        }

    def _extract_labels(self, soup: BeautifulSoup) -> dict[str, Tag]:
        """応募フォーム以外の th/td テーブルからラベル→td のマップを作る。"""
        labels: dict[str, Tag] = {}
        for table in soup.find_all("table"):
            classes = table.get("class") or []
            if any("send-form-table" in c for c in classes):
                continue
            for tr in table.find_all("tr"):
                th, td = tr.find("th"), tr.find("td")
                if th is None or td is None:
                    continue
                label = self._clean(th.get_text(strip=True))
                if label and label not in labels:
                    labels[label] = td
        return labels

    def _extract_name_kana(self, soup: BeautifulSoup) -> tuple[str, str]:
        h1 = soup.select_one("h1.shopName")
        if h1 is None:
            return "", ""
        kana_el = h1.find("span")
        kana = self._clean(kana_el.get_text(" ", strip=True)) if kana_el else ""
        if kana_el is not None:
            kana_el.extract()
        name = self._clean(h1.get_text(" ", strip=True))
        return name, kana if _KANA_TEXT_RE.match(kana) else ""

    def _extract_kana_from_meta(self, soup: BeautifulSoup, name: str) -> str:
        """h1 の span が空の店舗向け: meta keywords の2番目がカナ表記。"""
        meta = soup.find("meta", attrs={"name": "keywords"})
        if meta is None or not meta.get("content"):
            return ""
        parts = [self._clean(p) for p in meta["content"].split(",") if p.strip()]
        for part in parts[1:4]:
            if part and part != name and _KANA_TEXT_RE.match(part):
                return part
        return ""

    def _extract_address(self, td: Tag | None) -> str:
        if td is None:
            return ""
        map_link = td.select_one("a.mapLink, a.map_link")
        if map_link is not None:
            address = self._clean(map_link.get_text(" ", strip=True))
            if address:
                return address
        return self._clean(td.get_text(" ", strip=True))

    def _split_pref(self, address: str) -> tuple[str, str]:
        match = _PREF_PATTERN.match(address or "")
        if not match:
            return "", address
        return match.group(1), address[match.end():].strip()

    def _extract_tel(self, soup: BeautifulSoup, labels: dict[str, Tag]) -> str:
        td = labels.get("TEL")
        if td is not None:
            text = _TEL_NOTE_RE.sub(" ", td.get_text(" ", strip=True))
            match = _TEL_PATTERN.search(text.replace("ー", "-").replace("−", "-"))
            if match:
                return match.group(0)
        wrap = soup.select_one("div.shopViewWrap, motion.shopViewWrap") or soup
        for anchor in wrap.select('a[href^="tel:"]'):
            match = _TEL_PATTERN.search(anchor["href"].replace("tel:", "").strip())
            if match:
                return match.group(0)
        return ""

    def _extract_cat_site(self, soup: BeautifulSoup, shop_url: str) -> str:
        """shopFirstView の業種タグ (/shoplist/type/N/) を採用。姉妹サイトのリンクは除外。"""
        section = soup.select_one("section.shopFirstView, .shopFirstView") or soup
        host = urlparse(shop_url).netloc
        for anchor in section.find_all("a", href=True):
            if "/shoplist/type/" not in anchor["href"]:
                continue
            if urlparse(urljoin(shop_url, anchor["href"])).netloc != host:
                continue
            text = self._clean(anchor.get_text(strip=True))
            if text:
                return text
        return ""

    def _extract_area(self, soup: BeautifulSoup, shop_url: str) -> str:
        """掲載エリア名 (/shoplist/area/N/) を採用。"""
        section = soup.select_one("section.shopFirstView, .shopFirstView") or soup
        host = urlparse(shop_url).netloc
        for anchor in section.find_all("a", href=True):
            if "/shoplist/area/" not in anchor["href"]:
                continue
            if urlparse(urljoin(shop_url, anchor["href"])).netloc != host:
                continue
            text = self._clean(anchor.get_text(strip=True))
            # 「草加のボーイ・黒服求人」のような見出しリンクではなくタグ表記を優先
            if text and "求人" not in text:
                return text
        return ""

    def _extract_job_types(self, soup: BeautifulSoup) -> str:
        """給与テーブルの th (職種名) を ' / ' 連結。PC/SP 重複は除去。"""
        names: list[str] = []
        for table in soup.find_all("table"):
            classes = table.get("class") or []
            if any("send-form-table" in c for c in classes):
                continue
            for tr in table.find_all("tr"):
                th, td = tr.find("th"), tr.find("td")
                if th is None or td is None:
                    continue
                label = self._clean(th.get_text(strip=True))
                if not label or label in _NON_JOB_LABELS or label in names:
                    continue
                # 給与形態で始まる td を持つ行だけを職種行とみなす
                if _SALARY_RE.match(self._clean(td.get_text(" ", strip=True))):
                    names.append(label)
        return " / ".join(names)

    def _extract_hp(self, soup: BeautifulSoup, td: Tag | None) -> str:
        if td is not None:
            anchor = td.find("a", href=True)
            candidate = anchor["href"].strip() if anchor else self._clean(td.get_text(strip=True))
            if candidate.startswith("http") and not self._is_excluded_hp(candidate):
                return candidate
        wrap = soup.select_one("div.shopViewWrap, motion.shopViewWrap")
        if wrap is None:
            return ""
        for anchor in wrap.find_all("a", href=True):
            href = anchor["href"].strip()
            if href.startswith("http") and not self._is_excluded_hp(href):
                return href
        return ""

    def _is_excluded_hp(self, href: str) -> bool:
        host = urlparse(href).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return any(skip in host for skip in _HP_SKIP_HOSTS)

    def _extract_sns(self, soup: BeautifulSoup, sns_td: Tag | None) -> dict[str, str]:
        result = {"insta": "", "x": "", "fb": "", "line": "", "tiktok": ""}
        scopes: list[Tag] = []
        if sns_td is not None:
            scopes.append(sns_td)
        wrap = soup.select_one("div.shopViewWrap, motion.shopViewWrap")
        if wrap is not None:
            scopes.append(wrap)

        for scope in scopes:
            for anchor in scope.find_all("a", href=True):
                href = anchor["href"].strip()
                if not href.startswith("http"):
                    continue
                host = urlparse(href).netloc.lower()
                if host.startswith("www."):
                    host = host[4:]
                if "instagram.com" in host and not result["insta"]:
                    result["insta"] = href
                elif ("twitter.com" in host or host == "x.com") and not result["x"]:
                    result["x"] = href
                elif "facebook.com" in host and not result["fb"]:
                    result["fb"] = href
                elif ("line.me" in host or "lin.ee" in host) and not result["line"]:
                    result["line"] = href
                elif "tiktok.com" in host and not result["tiktok"]:
                    result["tiktok"] = href
        return result

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #
    def _text_of(self, td: Tag | None) -> str:
        return self._clean(td.get_text(" ", strip=True)) if td is not None else ""

    def _shop_key(self, record: dict) -> tuple[str, str, str]:
        return (
            record.get(Schema.NAME, ""),
            record.get(Schema.ADDR, ""),
            record.get(Schema.TEL, ""),
        )

    @staticmethod
    def _clean(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").replace("　", " ")).strip()


if __name__ == "__main__":
    scraper = TainewOtoko2Scraper()
    scraper.execute("https://tainew-otoko.com/shoplist/search/?salary_type=0_1&word=")
