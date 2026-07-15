"""
AIR函館 for Night (air-h.jp) — 函館のナイトビジネス店舗情報スクレイパー

取得対象:
    - 店舗検索ページ (/water/shop-search/) に掲載される全店舗 (推定 約26件、ページ送り無し)
    - 店舗名 / カナ / 都道府県 / 住所 / TEL / 種別(サイト定義業種) / 営業時間 / 定休日 /
      支払方法(カード) / 店舗HP / SNS(LINE/Instagram/X/Facebook/TikTok)
    - サイト固有(EXTRA): エリア / キャスト衣装 / 座席・卓数 / カラオケ / VIP /
      料金システム / 求人業務 / 求人資格 / 求人給与 / 求人待遇 / 求人時間 / 担当者

取得フロー:
    1. 引数 url (店舗検索ページ) を取得し、各店舗カード (a.listcard) から
       詳細URL (/water/shop/{id}/) と一覧項目 (名称/エリア/種別/住所) を収集
    2. 詳細ページを 1件取得するごとに即 yield (一覧のカード情報とマージ)
    3. 詳細ページのセクション見出し h1 (料金システム/店舗情報/求人内容/応募) の
       直後テーブルを th/td 辞書化して各フィールドを抽出

除外フィールド (著作権リスク — 自由記述プロースのため):
    - 店舗情報の「備考」(新型コロナ対応等の長文)
    - 求人内容の「内容」(仕事内容の説明文)
    - 求人の「休日」(シフト説明が長文になり得るため)

利用規約 (https://air-h.jp/contract/) 第5条(禁止事項) にスクレイピング/クローリングの
明示的禁止は無い (収集禁止は「他のユーザーに関する個人情報」に限定)。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/air_for_night.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id air_for_night
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup, Tag

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_PATTERN = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")

# EXTRA カラム名 (Schema に該当しないサイト固有の構造化情報)
_COL_AREA = "エリア"
_COL_COSTUME = "キャスト衣装"
_COL_SEATS = "座席・卓数"
_COL_KARAOKE = "カラオケ"
_COL_VIP = "VIP"
_COL_PRICE = "料金システム"
_COL_JOB = "求人業務"
_COL_QUALIFY = "求人資格"
_COL_SALARY = "求人給与"
_COL_BENEFIT = "求人待遇"
_COL_JOB_TIME = "求人時間"
_COL_CONTACT = "担当者"


class AirForNightScraper(StaticCrawler):
    """AIR函館 for Night スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = [
        _COL_AREA,
        _COL_COSTUME,
        _COL_SEATS,
        _COL_KARAOKE,
        _COL_VIP,
        _COL_PRICE,
        _COL_JOB,
        _COL_QUALIFY,
        _COL_SALARY,
        _COL_BENEFIT,
        _COL_JOB_TIME,
        _COL_CONTACT,
    ]

    # ------------------------------------------------------------------ #
    # メインフロー (引数 url を唯一のルートとして使用)
    # ------------------------------------------------------------------ #

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        cards = soup.select("a.listcard")
        self.total_items = len(cards)
        self.logger.info("店舗カード: %d件", len(cards))

        seen: set[str] = set()
        for card in cards:
            href = card.get("href")
            if not href:
                continue
            detail_url = urljoin(url, href)
            if detail_url in seen:
                continue
            seen.add(detail_url)

            list_info = self._parse_card(card)
            try:
                record = self._scrape_detail(detail_url, list_info)
            except Exception as e:  # 個別店舗の失敗は握りつぶして続行
                self.logger.warning("詳細取得失敗: %s (%s)", detail_url, e)
                continue
            if record:
                self.logger.info(
                    "取得: %s (%s)",
                    record.get(Schema.NAME) or "?",
                    record.get(_COL_AREA) or "",
                )
                yield record

    # ------------------------------------------------------------------ #
    # 一覧カードの解析
    # ------------------------------------------------------------------ #

    def _parse_card(self, card: Tag) -> dict:
        info: dict[str, str] = {"name": "", "area": "", "genre": "", "addr": ""}
        h2 = card.find("h2")
        if h2:
            info["name"] = self._clean(h2.get_text(" "))
        for p in card.select("div.txac p"):
            txt = self._clean(p.get_text(" "))
            if txt.startswith("エリア:"):
                info["area"] = txt.split(":", 1)[1].strip()
            elif txt.startswith("種別:"):
                info["genre"] = txt.split(":", 1)[1].strip()
            elif "fss" in (p.get("class") or []):
                info["addr"] = txt
        return info

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #

    def _scrape_detail(self, url: str, list_info: dict) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name, kana = self._name_kana(soup)
        name = name or list_info.get("name", "")
        if not name:
            return None

        shop_info = self._section_dict(soup, "店舗情報")
        apply_info = self._section_dict(soup, "応募")
        recruit_info = self._section_dict(soup, "求人内容")

        # 住所: 一覧カード優先 (北海道 プレフィックス付きで安定)、無ければ応募住所
        addr = list_info.get("addr", "") or apply_info.get("住所", "")
        pref = self._pref(addr)

        contact = self._contact_links(soup)

        record = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: contact["tel"] or self._first_tel(apply_info.get("店舗TEL", "")),
            Schema.CAT_SITE: list_info.get("genre", ""),
            Schema.TIME: shop_info.get("営業時間", ""),
            Schema.HOLIDAY: shop_info.get("定休日", ""),
            Schema.PAYMENTS: shop_info.get("カード", ""),
            Schema.HP: contact["hp"],
            Schema.LINE: contact["line"] or apply_info.get("LINEID", ""),
            Schema.INSTA: contact["insta"],
            Schema.X: contact["x"],
            Schema.FB: contact["fb"],
            Schema.TIKTOK: contact["tiktok"],
            # EXTRA
            _COL_AREA: list_info.get("area", ""),
            _COL_COSTUME: shop_info.get("キャスト衣装", ""),
            _COL_SEATS: shop_info.get("座席・卓数", ""),
            _COL_KARAOKE: shop_info.get("カラオケ", ""),
            _COL_VIP: shop_info.get("VIP", ""),
            _COL_PRICE: self._pricing_text(soup),
            _COL_JOB: recruit_info.get("業務", ""),
            _COL_QUALIFY: recruit_info.get("資格", ""),
            _COL_SALARY: recruit_info.get("給与", ""),
            _COL_BENEFIT: recruit_info.get("待遇", ""),
            _COL_JOB_TIME: recruit_info.get("時間", ""),
            _COL_CONTACT: apply_info.get("担当者", ""),
        }
        return record

    # ------------------------------------------------------------------ #
    # 詳細ページ ヘルパ
    # ------------------------------------------------------------------ #

    def _name_kana(self, soup: BeautifulSoup) -> tuple[str, str]:
        """セクション見出し以外の h1 (店舗名) から名称とカナを取得。"""
        for h1 in soup.find_all("h1"):
            cls = h1.get("class") or []
            if "pgttl" in cls or "fsll" in cls:
                continue
            italics = h1.find_all("i")
            if len(italics) >= 2:
                return self._clean(italics[0].get_text(" ")), self._clean(italics[1].get_text(" "))
            if italics:
                return self._clean(italics[0].get_text(" ")), ""
            return self._clean(h1.get_text(" ")), ""
        return "", ""

    def _section_dict(self, soup: BeautifulSoup, title: str) -> dict[str, str]:
        """セクション見出し h1 (title) の直後テーブルを th/td 辞書化。

        ラベルの全角スペース(　)・空白を除去したキーで格納する
        (例: '業　務' → '業務', '住　所' → '住所', 'LINE ID' → 'LINEID')。
        """
        heading = None
        for h1 in soup.find_all("h1"):
            if self._clean(h1.get_text(" ")) == title:
                heading = h1
                break
        if heading is None:
            return {}
        table = heading.find_next("table")
        if table is None:
            return {}
        return self._table_dict(table)

    def _table_dict(self, table: Tag) -> dict[str, str]:
        info: dict[str, str] = {}
        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = re.sub(r"[\s　]", "", th.get_text())
            value = self._clean(td.get_text(" "))
            if key and key not in info:
                info[key] = value
        return info

    def _pricing_text(self, soup: BeautifulSoup) -> str:
        """料金システムテーブルを 'ラベル：値 / …' の構造化テキストにまとめる。"""
        table = None
        for h1 in soup.find_all("h1"):
            # 料金セクションの見出しは店舗により「料金システム」「料金プラン」等ゆれる
            if re.match(r"料金(システム|プラン|表)?$", self._clean(h1.get_text(" "))):
                table = h1.find_next("table")
                break
        if table is None:
            return ""
        parts: list[str] = []
        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = self._clean(th.get_text(" "))
            value = self._clean(td.get_text(" "))
            if label and value:
                parts.append(f"{label}：{value}")
        return " / ".join(parts)

    def _contact_links(self, soup: BeautifulSoup) -> dict[str, str]:
        """店舗連絡先ボックス (div.p16.bgcbase) 内のアンカーのみから TEL/HP/SNS を判定。

        フッター等サイト共通の SNS (air_hakodate) 混入を避けるため範囲を限定する。
        """
        out = {"tel": "", "hp": "", "line": "", "insta": "", "x": "", "fb": "", "tiktok": ""}
        for box in soup.select("div.p16.bgcbase"):
            for a in box.find_all("a", href=True):
                href = a["href"].strip()
                if href.lower().startswith("tel:"):
                    if not out["tel"]:
                        out["tel"] = self._first_tel(href)
                    continue
                href = self._clean_url(href)
                low = href.lower()
                if not low.startswith("http"):
                    continue
                if "instagram.com" in low and not out["insta"]:
                    out["insta"] = href
                elif ("line.me" in low or "lin.ee" in low) and not out["line"]:
                    out["line"] = href
                elif ("x.com" in low or "twitter.com" in low) and not out["x"]:
                    out["x"] = href
                elif "facebook.com" in low and not out["fb"]:
                    out["fb"] = href
                elif "tiktok.com" in low and not out["tiktok"]:
                    out["tiktok"] = href
                elif not out["hp"]:
                    out["hp"] = href
        return out

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #

    @staticmethod
    def _clean_url(href: str) -> str:
        """'https://facebook.com/https://www.facebook.com/...' の様な二重連結を補正。"""
        idx = href.rfind("http")
        return href[idx:] if idx > 0 else href

    @staticmethod
    def _pref(address: str) -> str:
        m = _PREF_PATTERN.search(address)
        return m.group(1) if m else ""

    @staticmethod
    def _first_tel(text: str) -> str:
        if not text:
            return ""
        m = _TEL_PATTERN.search(text)
        return m.group(0) if m else ""

    @staticmethod
    def _clean(value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\xa0", " ").replace("　", " ")).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = AirForNightScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://air-h.jp/water/shop-search/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
