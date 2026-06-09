# scripts/sites/jobs/pokepara_tainew.py

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

# ===== NetHarvest プロジェクトルートを import パスに追加 =====
root_path = Path(__file__).resolve().parent.parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class PokeparaTainewScraper(StaticCrawler):
    """
    ポケパラ体入 求人情報スクレイパー

    方針:
        - StaticCrawler のまま
        - 5秒に1回取得
        - 詳細URLを全件集めてから処理するのではなく、
          一覧ページで見つけた詳細URLをすぐ詳細取得して yield する
        - そのため、実行開始後すぐに Pipeline 側へデータが流れやすい
        - エラーはできるだけスキップして継続
    """

    DELAY = 5.0
    CONTINUE_ON_ERROR = True

    EXTRA_COLUMNS = [
        "エリア",
        "給与",
        "体入時給",
        "入店時給",
        "職種",
        "衣装",
        "勤務日",
        "アクセス",
        "資格",
        "メール",
        "LINE",
    ]

    SITE_ROOT = "https://www.pokepara-tainew.jp/"

    DETAIL_PATTERN = re.compile(
        r"^https?://(?:www\.)?pokepara-tainew\.jp/.+/shop\d+/?$"
    )

    PHONE_PATTERN = re.compile(
        r"(?:0\d{1,4}-\d{1,4}-\d{3,4}|0\d{9,10})"
    )

    EMAIL_PATTERN = re.compile(
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    )

    AREA_URLS = [
        "https://www.pokepara-tainew.jp/hokkaido/",
        "https://www.pokepara-tainew.jp/tohoku/",
        "https://www.pokepara-tainew.jp/kanto/",
        "https://www.pokepara-tainew.jp/tokai/",
        "https://www.pokepara-tainew.jp/kansai/",
        "https://www.pokepara-tainew.jp/shizuoka/",
        "https://www.pokepara-tainew.jp/nn/",
        "https://www.pokepara-tainew.jp/hokuriku/",
        "https://www.pokepara-tainew.jp/chugoku/",
        "https://www.pokepara-tainew.jp/shikoku/",
        "https://www.pokepara-tainew.jp/kyushu/",
        "https://www.pokepara-tainew.jp/okinawa/",
    ]

    PREF_MAP = {
        "tokyo": "東京都",
        "kanagawa": "神奈川県",
        "chiba": "千葉県",
        "saitama": "埼玉県",
        "aichi": "愛知県",
        "osaka": "大阪府",
        "fukuoka": "福岡県",
        "hokkaido": "北海道",
        "miyagi": "宮城県",
        "shizuoka": "静岡県",
        "kyoto": "京都府",
        "hyogo": "兵庫県",
        "hiroshima": "広島県",
    }

    STOP_LABELS = [
        "給与",
        "職種",
        "衣装",
        "勤務地",
        "勤務日",
        "アクセス",
        "営業時間",
        "定休日",
        "資格",
        "TEL",
        "WEB",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        逐次処理版:
            一覧ページ取得
            → 詳細URL発見
            → すぐ詳細ページ取得
            → すぐ yield
        """
        seen_detail_urls: set[str] = set()
        list_urls = self._build_start_list_urls(url)

        processed_detail_count = 0
        self.logger.info("開始一覧URL: %d 件", len(list_urls))

        for base_url in list_urls:
            page = 1

            while True:
                page_url = self._build_page_url(base_url, page)

                self.logger.info(
                    "一覧ページ処理中: base=%s page=%s url=%s",
                    base_url,
                    page,
                    page_url,
                )

                soup = self.get_soup(page_url)
                if soup is None:
                    self.logger.warning("一覧ページ取得失敗。次のエリアへ進みます: %s", page_url)
                    break

                detail_urls = self._extract_detail_urls_from_soup(
                    soup=soup,
                    base_url=page_url,
                    seen_detail_urls=seen_detail_urls,
                )

                self.logger.info(
                    "一覧ページ取得完了: page=%s details_on_page=%s total_seen=%s",
                    page,
                    len(detail_urls),
                    len(seen_detail_urls),
                )

                if not detail_urls:
                    self.logger.info("詳細URLなしのためページ巡回終了: %s", page_url)
                    break

                for detail_url in detail_urls:
                    try:
                        item = self._scrape_detail(detail_url)
                        if item:
                            processed_detail_count += 1
                            self.logger.info("✅ yield: %s 件目 / %s", processed_detail_count, detail_url)
                            yield item
                    except Exception as exc:
                        self.logger.warning("詳細ページをスキップ: %s (%s)", detail_url, exc)
                        continue

                page += 1

        self.logger.info("parse終了: yield済み=%s 件", processed_detail_count)

    def _build_start_list_urls(self, start_url: str) -> list[str]:
        normalized_url = self._normalize_list_url(start_url)

        if self.DETAIL_PATTERN.fullmatch(normalized_url):
            return [normalized_url]

        if normalized_url.rstrip("/") == self.SITE_ROOT.rstrip("/"):
            return [self._normalize_list_url(u) for u in self.AREA_URLS]

        return [normalized_url]

    def _build_page_url(self, base_url: str, page: int) -> str:
        base_url = self._normalize_list_url(base_url)
        if page == 1:
            return base_url
        return urljoin(base_url, f"page/{page}/")

    def _extract_detail_urls_from_soup(self, soup, base_url: str, seen_detail_urls: set[str]) -> list[str]:
        urls: list[str] = []

        for link in soup.select("a[href*='/shop']"):
            href = (link.get("href") or "").strip()
            if not href:
                continue

            detail_url = self._normalize_url(urljoin(base_url, href))

            if not self.DETAIL_PATTERN.fullmatch(detail_url):
                continue

            if detail_url in seen_detail_urls:
                continue

            seen_detail_urls.add(detail_url)
            urls.append(detail_url)

        return urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        self.logger.info("詳細取得開始: %s", detail_url)

        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        full_text = self._get_full_text(soup)

        raw_name = self._clean_text(soup.select_one("h1"))
        title = self._clean_text(soup.select_one("title"))
        name = self._normalize_shop_name(raw_name or title)

        pref = self._extract_pref_from_url(detail_url)
        salary = self._extract_by_label(full_text, "給与")
        trial_wage = self._extract_wage(salary, "体入時給")
        regular_wage = self._extract_wage(salary, "入店時給")
        job_type = self._extract_by_label(full_text, "職種")
        costume = self._extract_by_label(full_text, "衣装")
        address = self._extract_by_label(full_text, "勤務地")
        work_day = self._extract_by_label(full_text, "勤務日")
        access = self._extract_by_label(full_text, "アクセス")
        business_hours = self._extract_by_label(full_text, "営業時間")
        holiday = self._extract_by_label(full_text, "定休日")
        qualification = self._extract_by_label(full_text, "資格")

        if "→MAP" in address:
            address = address.split("→MAP")[0].strip()

        tel = self._extract_tel(soup, full_text)
        email = self._extract_email(full_text)
        line_url = self._extract_line_url(soup)
        hp = self._extract_hp(soup, detail_url)
        area, genre = self._extract_area_and_genre(title)

        if not name and not any([address, tel, salary]):
            self.logger.warning("主要項目が取得できないためスキップ: %s", detail_url)
            return None

        self.logger.info("詳細取得完了: name=%s tel=%s", name or "-", tel or "-")

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.URL: detail_url,
            Schema.CAT_SITE: genre,
            Schema.TIME: business_hours,
            Schema.HOLIDAY: holiday,
            "エリア": area,
            "給与": salary,
            "体入時給": trial_wage,
            "入店時給": regular_wage,
            "職種": job_type,
            "衣装": costume,
            "勤務日": work_day,
            "アクセス": access,
            "資格": qualification,
            "メール": email,
            "LINE": line_url,
        }

    def _normalize_shop_name(self, text: str) -> str:
        value = self._clean_value(text)
        if not value:
            return ""

        for sep in ["の求人", "求人", " - ", "｜", "|"]:
            if sep in value:
                value = value.split(sep)[0].strip()

        for sep in ["・", "･", "/", "／"]:
            if sep in value:
                value = value.split(sep)[0].strip()

        return value

    def _extract_by_label(self, full_text: str, label: str) -> str:
        stop = "|".join(map(re.escape, self.STOP_LABELS))
        pattern = rf"(?:^|\n){re.escape(label)}\s*\n?(.+?)(?=\n(?:{stop})\s*(?:\n|$)|$)"
        match = re.search(pattern, full_text, flags=re.DOTALL)

        if not match:
            return ""

        value = match.group(1)
        cut_words = ["応募特典", "求人の特徴", "店内写真", "女の子の声", "ブログ", "スタッフ紹介", "関連エリア", "メニュー"]

        for word in cut_words:
            if word in value:
                value = value.split(word)[0]

        return self._clean_value(value)

    def _extract_tel(self, soup, full_text: str) -> str:
        tel_link = soup.select_one("a[href^='tel:']")
        if tel_link:
            return tel_link.get("href", "").replace("tel:", "").strip()

        tel_match = self.PHONE_PATTERN.search(full_text)
        if tel_match:
            return tel_match.group(0)

        return ""

    def _extract_email(self, full_text: str) -> str:
        email_match = self.EMAIL_PATTERN.search(full_text)
        if email_match:
            return email_match.group(0)
        return ""

    def _extract_line_url(self, soup) -> str:
        line_link = soup.select_one("a[href*='line.me'], a[href*='lin.ee']")
        if line_link:
            return line_link.get("href", "").strip()
        return ""

    def _extract_hp(self, soup, detail_url: str) -> str:
        for link in soup.select("a[href]"):
            text = self._clean_text(link)
            href = (link.get("href") or "").strip()
            if not href:
                continue
            if "ポケパラお店ページ" in text or "お店の宣伝ページ" in text:
                return urljoin(detail_url, href)
        return ""

    def _extract_area_and_genre(self, title: str) -> tuple[str, str]:
        if not title or " - " not in title:
            return "", ""

        tail = title.split(" - ", 1)[1]
        if "/" not in tail:
            return "", ""

        area, genre = [value.strip() for value in tail.split("/", 1)]
        genre = genre.replace("｜ポケパラ体入", "").strip()

        return area, genre

    def _extract_pref_from_url(self, target_url: str) -> str:
        path_parts = [part for part in urlparse(target_url).path.split("/") if part]
        if not path_parts:
            return ""

        if path_parts[0] in self.PREF_MAP:
            return self.PREF_MAP.get(path_parts[0], "")

        for part in path_parts:
            if part in self.PREF_MAP:
                return self.PREF_MAP[part]

        return ""

    def _extract_wage(self, salary: str, label: str) -> str:
        match = re.search(rf"{label}\s*([0-9,]+)\s*円?", salary)
        if not match:
            return ""
        return match.group(1).replace(",", "")

    def _clean_text(self, node) -> str:
        if not node:
            return ""
        return " ".join(node.stripped_strings).strip()

    def _clean_value(self, value: str) -> str:
        return re.sub(r"\s+", " ", value or "").strip()

    def _get_full_text(self, soup) -> str:
        return "\n".join(line.strip() for line in soup.get_text("\n").splitlines() if line.strip())

    def _normalize_url(self, raw_url: str) -> str:
        parsed = urlparse(raw_url.strip())
        path = parsed.path if parsed.path.endswith("/") else f"{parsed.path}/"
        return f"{parsed.scheme}://{parsed.netloc}{path}"

    def _normalize_list_url(self, raw_url: str) -> str:
        normalized = self._normalize_url(raw_url)
        parsed = urlparse(normalized)
        path = re.sub(r"/page/\d+/?$", "/", parsed.path)
        return f"{parsed.scheme}://{parsed.netloc}{path}"


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    crawler = PokeparaTainewScraper()

    # 東京だけテスト
    # crawler.execute("https://www.pokepara-tainew.jp/tokyo/")

    # 全国で実行する場合
    crawler.execute("https://www.pokepara-tainew.jp/")

    # 地方ブロックだけ実行する場合
    # crawler.execute("https://www.pokepara-tainew.jp/kanto/")
