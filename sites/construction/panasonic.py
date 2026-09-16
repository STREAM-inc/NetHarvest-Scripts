# scripts/sites/construction/panasonic.py
"""
PanasonicリフォームClub (reform-club.panasonic.com) — パナソニック推奨リフォーム会社の加盟店一覧

取得対象:
    - 全国47都道府県の PanasonicリフォームClub 加盟店 (リフォーム会社)
    - 名称・都道府県・郵便番号・住所・TEL・掲載電話番号・FAX・HP・メールアドレス・
      営業時間・定休日・市区町村・旧店舗名・運営会社・各種許可番号・登録資格

取得フロー:
    ルート(url = 都道府県別ページ) を取得
      → ページ内フッターの都道府県ナビ (a[href*="pref.php?id="]) から全47都道府県ページを導出
      → 各都道府県ページの .shop-list-city > .shop-list-wrap を解析 (ページネーション無し)
      → 店舗ごとに詳細ページ (shop.php?id=N) を取得してメール/営業時間/定休日等を補完し
        1 件ずつ即 yield

備考:
    - 一覧ページに住所・TEL・FAX・許可番号まで揃っており、詳細ページは補完用。
    - 一覧見出しの電話番号はフリーダイヤル(0120)のことがあるため、
      Schema.TEL には店舗情報欄の「電話」を、Schema.PHONE には見出しの掲載番号を入れる。
    - 店舗紹介文・スタッフコメント・資格の説明文は自由記述のため取得しない (著作権配慮)。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/panasonic.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id panasonic
"""

import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class PanasonicReformClubScraper(StaticCrawler):
    """PanasonicリフォームClub 加盟店スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "市区町村",
        "旧店舗名",
        "FAX",
        "運営会社",
        "建設業許可",
        "建築士事務所登録",
        "宅地建物取引業免許番号",
        "登録・資格",
        "店舗ID",
    ]

    # 一覧 <ul class="shop-list-detail-info"> / 詳細 <ul class="shop-detail-info"> の
    # 「ラベル ：値」形式を EXTRA カラムへ振り分けるための対応表 (ラベルは表記揺れあり)
    _INFO_LABELS = [
        ("建設業許可", "建設業許可"),
        ("建築士事務所", "建築士事務所登録"),
        ("宅地建物", "宅地建物取引業免許番号"),
        ("運営会社", "運営会社"),
    ]

    def parse(self, url: str):
        """
        ルート url (都道府県別ページ) を起点に、全47都道府県の加盟店を巡回する。

        Args:
            url: 都道府県別ページ URL
                 (例: https://reform-club.panasonic.com/shop/pref.php?id=hokkaido)
        """
        root_soup = self.get_soup(url)
        if root_soup is None:
            self.logger.error("ルートページ取得失敗: %s", url)
            return

        # ルートページのフッターナビから全都道府県ページ (slug, 都道府県名, URL) を導出する。
        # ルート自身の都道府県も必ずこの一覧に含まれる。
        prefectures = self._collect_prefecture_pages(root_soup, url)
        self.logger.info("都道府県ページ: %d 件", len(prefectures))

        for pref_name, pref_url in prefectures:
            # ルートページは取得済みだが get_soup 側にキャッシュがあるため再取得で問題ない
            pref_soup = self.get_soup(pref_url)
            if pref_soup is None:
                self.logger.warning("都道府県ページ取得失敗: %s", pref_url)
                continue

            for city_block in pref_soup.select(".shop-list-city"):
                city_el = city_block.select_one(".shopList-title")
                city = city_el.get_text(strip=True) if city_el else ""

                for card in city_block.select(".shop-list-wrap"):
                    try:
                        row = self._parse_card(card, pref_url, pref_name, city)
                    except Exception as e:  # noqa: BLE001
                        self.logger.warning("店舗カード解析失敗: %s (%s)", pref_url, e)
                        continue
                    if row:
                        yield row

    # ------------------------------------------------------------------ 一覧

    def _collect_prefecture_pages(self, soup, url: str) -> list[tuple[str, str]]:
        """ルートページのナビから (都道府県名, 都道府県ページURL) を重複なしで収集する"""
        pages: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="pref.php?id="]'):
            name = a.get_text(strip=True)
            href = a.get("href") or ""
            if not name or not href:
                continue
            pref_url = urljoin(url, href)
            if pref_url in seen:
                continue
            seen.add(pref_url)
            pages.append((name, pref_url))
        if not pages:
            # ナビが取れない場合でも、少なくともルートページ自身は巡回する
            h1 = soup.select_one("h1")
            name = h1.get_text(strip=True).replace("のリフォーム会社", "").strip() if h1 else ""
            pages.append((name, url))
        return pages

    def _parse_card(self, card, page_url: str, pref_name: str, city: str) -> dict | None:
        """一覧の 1 枚の .shop-list-wrap から行データを構築し、詳細ページで補完する"""
        title_a = card.select_one(".shop-list-title a")
        name = title_a.get_text(strip=True) if title_a else ""
        if not name:
            title_el = card.select_one(".shop-list-title")
            name = title_el.get_text(strip=True) if title_el else ""
        if not name:
            return None

        detail_href = title_a.get("href") if title_a else ""
        detail_url = urljoin(page_url, detail_href) if detail_href else ""
        shop_id = self._extract_shop_id(detail_url)

        # 見出しの掲載電話番号 (フリーダイヤルのことがある)
        headline_tel = self._text(card.select_one(".shop-list-tel"))
        old_name = self._clean_old_name(card.select_one(".shop-list-old-name"))

        post_code = self._text(card.select_one(".shop-list-post"))
        full_addr = self._text(card.select_one("#shop-list-add"))
        if not full_addr:
            addr_el = card.select_one(".shop-list-address")
            if addr_el:
                full_addr = self._text(addr_el).replace("〒", "").replace(post_code, "").strip()

        # 店舗情報欄の「電話」「FAX」
        tel = ""
        fax = ""
        for block in card.select(".shop-list-tel-info-block"):
            label = self._text(block.select_one("dt"))
            value = self._text(block.select_one("dd"))
            if "電話" in label:
                tel = value
            elif "FAX" in label.upper():
                fax = value

        info = self._parse_info_list(card.select(".shop-list-detail-info li"))
        licenses = self._collect_licenses(card)
        hp = self._pick_hp(card)

        row = {
            Schema.URL: detail_url or page_url,
            Schema.NAME: name,
            Schema.PREF: pref_name,
            Schema.POST_CODE: post_code,
            Schema.ADDR: self._strip_pref(full_addr, pref_name),
            Schema.TEL: tel or headline_tel,
            Schema.PHONE: headline_tel,
            Schema.CAT_SITE: "リフォーム会社",
            Schema.HP: hp,
            Schema.EMAIL: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            "市区町村": city,
            "旧店舗名": old_name,
            "FAX": fax,
            "運営会社": info.get("運営会社", ""),
            "建設業許可": info.get("建設業許可", ""),
            "建築士事務所登録": info.get("建築士事務所登録", ""),
            "宅地建物取引業免許番号": info.get("宅地建物取引業免許番号", ""),
            "登録・資格": licenses,
            "店舗ID": shop_id,
        }

        if detail_url:
            self._enrich_from_detail(row, detail_url)
        return row

    # ------------------------------------------------------------------ 詳細

    def _enrich_from_detail(self, row: dict, detail_url: str) -> None:
        """詳細ページ (shop.php?id=N) からメール・営業時間・定休日等を補完する"""
        soup = self.get_soup(detail_url)
        if soup is None:
            self.logger.warning("詳細ページ取得失敗: %s", detail_url)
            return

        h1 = soup.select_one("h1")
        if h1 and h1.get_text(strip=True):
            row[Schema.NAME] = h1.get_text(strip=True)

        # 営業時間 / 定休日 (掲載していない店舗もある)
        time_el = soup.select_one(".shop-head-info-time")
        if time_el:
            row[Schema.TIME] = self._strip_label(self._text(time_el), "営業時間")
        for p in soup.select(".shop-head-info"):
            text = self._text(p)
            if text.startswith("定休日"):
                row[Schema.HOLIDAY] = self._strip_label(text, "定休日")
                break

        if not row.get("旧店舗名"):
            row["旧店舗名"] = self._clean_old_name(soup.select_one(".shop-head-old-name"))

        # 店舗情報セクション (住所 / 電話 / FAX / Eメール)
        for block in soup.select(".shop-detail-contents"):
            label = self._text(block.select_one(".shop-detail-contents-head"))
            detail_el = block.select_one(".shop-detail-contents-detail")
            if not label or detail_el is None:
                continue
            if "Eメール" in label or "メール" in label:
                mail_a = detail_el.select_one('a[href^="mailto:"]')
                row[Schema.EMAIL] = (
                    mail_a["href"].replace("mailto:", "").strip()
                    if mail_a
                    else self._text(detail_el)
                )
            elif "電話" in label:
                value = self._text(detail_el)
                if value:
                    row[Schema.TEL] = value
            elif "FAX" in label.upper():
                value = self._text(detail_el)
                if value:
                    row["FAX"] = value
            elif "住所" in label:
                post = self._text(detail_el.select_one("#shop_post"))
                addr = self._text(detail_el.select_one("#shop_address"))
                if post:
                    row[Schema.POST_CODE] = post
                if addr:
                    row[Schema.ADDR] = self._strip_pref(addr, row.get(Schema.PREF, ""))

        info = self._parse_info_list(soup.select(".shop-detail-info li"))
        for key, value in info.items():
            if value and not row.get(key):
                row[key] = value

        if not row.get("登録・資格"):
            row["登録・資格"] = self._collect_licenses(soup)
        if not row.get(Schema.HP):
            row[Schema.HP] = self._pick_hp(soup)

    # ------------------------------------------------------------------ 共通

    @staticmethod
    def _text(el) -> str:
        """要素のテキストを空白正規化して返す (None 安全)"""
        if el is None:
            return ""
        return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()

    @staticmethod
    def _strip_label(text: str, label: str) -> str:
        """「営業時間 9:00～17:30」→「9:00～17:30」のようにラベルを除去する"""
        return re.sub(rf"^{re.escape(label)}[\s：:]*", "", text).strip()

    def _clean_old_name(self, el) -> str:
        """「（旧店舗名　リファイン西野）」→「リファイン西野」に整形する"""
        text = self._text(el).strip("（）()")
        return self._strip_label(text, "旧店舗名")

    @staticmethod
    def _strip_pref(addr: str, pref_name: str) -> str:
        """Schema.ADDR は市区町村以降のため、先頭の都道府県名を落とす"""
        if pref_name and addr.startswith(pref_name):
            return addr[len(pref_name):].strip()
        return addr

    @staticmethod
    def _extract_shop_id(detail_url: str) -> str:
        """shop.php?id=301122 → "301122" """
        if not detail_url:
            return ""
        return (parse_qs(urlparse(detail_url).query).get("id") or [""])[0]

    def _parse_info_list(self, li_elements) -> dict:
        """「建設業許可 ：北海道知事許可…」形式の <li> 群を EXTRA カラム名の dict にする"""
        result: dict[str, str] = {}
        for li in li_elements:
            text = self._text(li)
            if not text:
                continue
            for keyword, column in self._INFO_LABELS:
                if keyword not in text:
                    continue
                # 「ラベル ：値」/「ラベル：値」の両方に対応。区切りが無ければ全文を値とする
                parts = re.split(r"[：:]", text, maxsplit=1)
                value = parts[1].strip() if len(parts) == 2 else text
                if value and not result.get(column):
                    result[column] = value
                break
        return result

    def _collect_licenses(self, scope) -> str:
        """登録・資格アイコンの alt (短いラベル) を「/」区切りで連結する"""
        labels = []
        for img in scope.select("img.license-icon"):
            alt = (img.get("alt") or "").strip()
            if alt and alt not in labels:
                labels.append(alt)
        return " / ".join(labels)

    def _pick_hp(self, scope) -> str:
        """「お店のホームページ」リンクを取り出し、計測用 utm パラメータを除去する"""
        for a in scope.select("a.shop-contact-link[href]"):
            href = a["href"].strip()
            if not href.lower().startswith("http"):
                continue
            text = self._text(a)
            if "ホームページ" not in text and not a.select_one("span.web"):
                continue
            return self._strip_utm(href)
        return ""

    @staticmethod
    def _strip_utm(href: str) -> str:
        """サイトが付与する utm_* 計測パラメータを取り除いた URL を返す"""
        parsed = urlparse(href)
        if not parsed.query:
            return href
        kept = [
            (k, v)
            for k, v in parse_qs(parsed.query, keep_blank_values=True).items()
            if not k.lower().startswith("utm_")
        ]
        query = urlencode([(k, vv) for k, vs in kept for vv in vs])
        return urlunparse(parsed._replace(query=query))


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PanasonicReformClubScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://reform-club.panasonic.com/shop/pref.php?id=hokkaido")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
