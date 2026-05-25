# scripts/sites/jobs/pokepara_tainew.py

import re
import sys
from collections import deque
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

# ===== NetHarvest プロジェクトルートを import パスに追加 =====
root_path = Path(__file__).resolve().parent.parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class PokeparaTainewScraper(StaticCrawler):
    """
    ポケパラ体入 求人情報スクレイパー

    件数をできるだけ多く取る方針:
        1. 地方ブロックURLを明示的に巡回
        2. 各一覧ページ内の shop 詳細URLを収集
        3. 一覧ページ内の「エリア」「業種」「駅」などの一覧リンクもキューに追加
        4. /page/N/ と ?page=N の両方を試す
        5. 詳細URLは set でユニーク化
    """

    DELAY = 2.0

    # 巡回量を増やしたい場合はここを上げる
    MAX_LIST_PAGES = 20000
    MAX_EMPTY_PAGES_IN_ROW = 2

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

    # トップから拾い漏れることがあるため、地方ブロックを明示
    SEED_LIST_URLS = [
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
        # 都道府県直下も保険で入れる
        "https://www.pokepara-tainew.jp/tokyo/",
        "https://www.pokepara-tainew.jp/kanagawa/",
        "https://www.pokepara-tainew.jp/chiba/",
        "https://www.pokepara-tainew.jp/saitama/",
        "https://www.pokepara-tainew.jp/aichi/",
        "https://www.pokepara-tainew.jp/osaka/",
        "https://www.pokepara-tainew.jp/fukuoka/",
    ]

    PREF_MAP = {
        "hokkaido": "北海道",
        "aomori": "青森県",
        "iwate": "岩手県",
        "miyagi": "宮城県",
        "akita": "秋田県",
        "yamagata": "山形県",
        "fukushima": "福島県",
        "ibaraki": "茨城県",
        "tochigi": "栃木県",
        "gunma": "群馬県",
        "saitama": "埼玉県",
        "chiba": "千葉県",
        "tokyo": "東京都",
        "kanagawa": "神奈川県",
        "niigata": "新潟県",
        "toyama": "富山県",
        "ishikawa": "石川県",
        "fukui": "福井県",
        "yamanashi": "山梨県",
        "nagano": "長野県",
        "gifu": "岐阜県",
        "shizuoka": "静岡県",
        "aichi": "愛知県",
        "mie": "三重県",
        "shiga": "滋賀県",
        "kyoto": "京都府",
        "osaka": "大阪府",
        "hyogo": "兵庫県",
        "nara": "奈良県",
        "wakayama": "和歌山県",
        "tottori": "鳥取県",
        "shimane": "島根県",
        "okayama": "岡山県",
        "hiroshima": "広島県",
        "yamaguchi": "山口県",
        "tokushima": "徳島県",
        "kagawa": "香川県",
        "ehime": "愛媛県",
        "kochi": "高知県",
        "fukuoka": "福岡県",
        "saga": "佐賀県",
        "nagasaki": "長崎県",
        "kumamoto": "熊本県",
        "oita": "大分県",
        "miyazaki": "宮崎県",
        "kagoshima": "鹿児島県",
        "okinawa": "沖縄県",
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

    EXCLUDE_LIST_PATH_KEYWORDS = [
        "/shop/",
        "/gal/",
        "/girls/",
        "/blog/",
        "/voice/",
        "/movie/",
        "/news/",
        "/ranking/",
        "/review/",
        "/diary/",
        "/staff/",
        "/cast/",
        "/photo/",
        "/apply/",
        "/contact/",
        "/company/",
        "/privacy/",
        "/rule/",
        "/terms/",
        "/help/",
        "/about/",
        "/contents/",
        "/column/",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """一覧ページから詳細URLを集め、各詳細ページを1件ずつ取得する"""
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)

        self.logger.info("詳細URL: %d 件", len(detail_urls))

        if not detail_urls:
            raise RuntimeError(f"詳細URLを取得できませんでした: {url}")

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as exc:
                self.logger.warning("スキップ: %s (%s)", detail_url, exc)
                continue

    def _collect_detail_urls(self, start_url: str) -> list[str]:
        """
        詳細URLをできるだけ広く収集する。

        重要:
            - 全国トップ指定なら SEED_LIST_URLS を起点にする
            - 一覧ページ中の一覧リンクも追加する
            - ページネーションも追加する
            - shop 詳細URLはユニーク化する
        """
        normalized_input_url = self._normalize_url(start_url)

        if self.DETAIL_PATTERN.fullmatch(normalized_input_url):
            self.logger.info("詳細URL直接指定モード: %s", normalized_input_url)
            return [normalized_input_url]

        seed_urls = self._build_seed_urls(normalized_input_url)

        queue = deque(seed_urls)
        seen_list_urls: set[str] = set()
        seen_detail_urls: set[str] = set()
        detail_urls: list[str] = []

        processed_list_pages = 0

        while queue:
            if processed_list_pages >= self.MAX_LIST_PAGES:
                self.logger.warning("MAX_LIST_PAGES到達: %s", self.MAX_LIST_PAGES)
                break

            list_url = self._normalize_list_url(queue.popleft())

            if list_url in seen_list_urls:
                continue

            if not self._is_valid_list_url(list_url):
                continue

            seen_list_urls.add(list_url)
            processed_list_pages += 1

            self.logger.info(
                "一覧ページ処理中: %s / queue=%s / details=%s / url=%s",
                processed_list_pages,
                len(queue),
                len(detail_urls),
                list_url,
            )

            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s", list_url)
                continue

            # 1. 詳細URL収集
            new_detail_count = self._extract_detail_urls_from_soup(
                soup=soup,
                base_url=list_url,
                seen_detail_urls=seen_detail_urls,
                detail_urls=detail_urls,
            )

            # 2. 一覧リンクを追加
            new_list_count = self._enqueue_list_urls_from_soup(
                soup=soup,
                base_url=list_url,
                queue=queue,
                seen_list_urls=seen_list_urls,
            )

            # 3. ページネーション候補を追加
            self._enqueue_pagination_urls(
                current_url=list_url,
                queue=queue,
                seen_list_urls=seen_list_urls,
            )

            self.logger.info(
                "一覧ページ完了: new_details=%s new_lists=%s total_details=%s url=%s",
                new_detail_count,
                new_list_count,
                len(detail_urls),
                list_url,
            )

        return detail_urls

    def _build_seed_urls(self, normalized_input_url: str) -> list[str]:
        """開始URLから初期キューを作る"""
        if normalized_input_url.rstrip("/") == self.SITE_ROOT.rstrip("/"):
            return [self._normalize_list_url(u) for u in self.SEED_LIST_URLS]

        return [self._normalize_list_url(normalized_input_url)]

    def _extract_detail_urls_from_soup(
        self,
        soup,
        base_url: str,
        seen_detail_urls: set[str],
        detail_urls: list[str],
    ) -> int:
        """soup内のshop詳細URLを収集する"""
        new_count = 0

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
            detail_urls.append(detail_url)
            new_count += 1

        return new_count

    def _enqueue_list_urls_from_soup(
        self,
        soup,
        base_url: str,
        queue: deque,
        seen_list_urls: set[str],
    ) -> int:
        """一覧ページ内にある別の一覧URLをキューへ追加する"""
        new_count = 0

        for link in soup.select("a[href]"):
            href = (link.get("href") or "").strip()
            if not href:
                continue

            absolute_url = self._normalize_list_url(urljoin(base_url, href))

            if absolute_url in seen_list_urls:
                continue

            if not self._is_valid_list_url(absolute_url):
                continue

            queue.append(absolute_url)
            new_count += 1

        return new_count

    def _enqueue_pagination_urls(
        self,
        current_url: str,
        queue: deque,
        seen_list_urls: set[str],
    ) -> None:
        """ページネーションURL候補をキューへ追加する"""
        page_num = self._extract_page_number(current_url)

        # 1ページ処理時は2ページ目を追加、2ページ以降は次ページを追加
        next_page = page_num + 1

        for candidate_url in self._build_page_urls(current_url, next_page):
            candidate_url = self._normalize_list_url(candidate_url)

            if candidate_url in seen_list_urls:
                continue

            if not self._is_valid_list_url(candidate_url):
                continue

            queue.append(candidate_url)

    def _build_page_urls(self, base_url: str, page: int) -> list[str]:
        """ページネーションURL候補を作る"""
        base_url = self._remove_page_part(self._normalize_list_url(base_url))

        return [
            urljoin(base_url, f"page/{page}/"),
            self._set_query_param(base_url, "page", str(page)),
            self._set_query_param(base_url, "p", str(page)),
        ]

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから1件分の求人情報を取得する"""
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
            # ===== NetHarvest 標準 Schema =====
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.URL: detail_url,
            Schema.CAT_SITE: genre,
            Schema.TIME: business_hours,
            Schema.HOLIDAY: holiday,

            # ===== サイト固有カラム =====
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

    def _is_valid_list_url(self, raw_url: str) -> bool:
        """一覧ページとして巡回してよいURLか判定する"""
        parsed = urlparse(raw_url)

        if parsed.scheme not in ("http", "https"):
            return False

        if parsed.netloc not in ("www.pokepara-tainew.jp", "pokepara-tainew.jp"):
            return False

        path = parsed.path

        if not path or path == "/":
            return True

        for keyword in self.EXCLUDE_LIST_PATH_KEYWORDS:
            if keyword in path:
                return False

        # 静的ファイルっぽいものを除外
        if re.search(r"\.(jpg|jpeg|png|gif|webp|svg|css|js|pdf|zip)$", path, re.I):
            return False

        # 一覧として有効そうなパスのみ残す
        # 例: /tokyo/, /tokyo/m2/a10023/, /tokyo/g1/, /tokyo/m2/a10023/g1/, /tokyo/m9/a10035/s93/
        if re.search(r"/(m\d+|a\d+|g\d+|s\d+)/?", path):
            return True

        first_part = path.strip("/").split("/")[0]
        if first_part in self.PREF_MAP:
            return True

        block_names = [
            "kanto",
            "tokai",
            "kansai",
            "tohoku",
            "hokuriku",
            "chugoku",
            "shikoku",
            "kyushu",
            "okinawa",
            "nn",
            "shizuoka",
            "hokkaido",
        ]
        if first_part in block_names:
            return True

        return False

    def _normalize_shop_name(self, text: str) -> str:
        """
        例:
            Gelande・ゲレンデの求人 - 仙台駅西口/ラウンジ
        を
            Gelande
        にする
        """
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
        """テキストからラベルに続く値を抽出する"""
        stop = "|".join(map(re.escape, self.STOP_LABELS))
        pattern = rf"(?:^|\n){re.escape(label)}\s*\n?(.+?)(?=\n(?:{stop})\s*(?:\n|$)|$)"
        match = re.search(pattern, full_text, flags=re.DOTALL)

        if not match:
            return ""

        value = match.group(1)

        cut_words = [
            "応募特典",
            "求人の特徴",
            "店内写真",
            "女の子の声",
            "ブログ",
            "スタッフ紹介",
            "関連エリア",
            "メニュー",
        ]

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
        """titleからエリア・業種を推定する"""
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

    def _extract_page_number(self, raw_url: str) -> int:
        """URLから現在ページ番号を推定する"""
        parsed = urlparse(raw_url)

        match = re.search(r"/page/(\d+)/?$", parsed.path)
        if match:
            return int(match.group(1))

        query = parse_qs(parsed.query)
        for key in ("page", "p"):
            values = query.get(key)
            if values and values[0].isdigit():
                return int(values[0])

        return 1

    def _remove_page_part(self, raw_url: str) -> str:
        """ページ番号部分を除いたベースURLを返す"""
        parsed = urlparse(raw_url)
        path = re.sub(r"/page/\d+/?$", "/", parsed.path)

        query = parse_qs(parsed.query)
        query.pop("page", None)
        query.pop("p", None)

        new_query = urlencode(query, doseq=True)

        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            path,
            parsed.params,
            new_query,
            "",
        ))

    def _set_query_param(self, raw_url: str, key: str, value: str) -> str:
        parsed = urlparse(raw_url)
        query = parse_qs(parsed.query)
        query[key] = [value]

        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query, doseq=True),
            "",
        ))

    def _clean_text(self, node) -> str:
        if not node:
            return ""
        return " ".join(node.stripped_strings).strip()

    def _clean_value(self, value: str) -> str:
        return re.sub(r"\s+", " ", value or "").strip()

    def _get_full_text(self, soup) -> str:
        return "\n".join(
            line.strip()
            for line in soup.get_text("\n").splitlines()
            if line.strip()
        )

    def _normalize_url(self, raw_url: str) -> str:
        parsed = urlparse(raw_url.strip())
        path = parsed.path if parsed.path.endswith("/") else f"{parsed.path}/"
        query = f"?{parsed.query}" if parsed.query else ""
        return f"{parsed.scheme}://{parsed.netloc}{path}{query}"

    def _normalize_list_url(self, raw_url: str) -> str:
        normalized = self._normalize_url(raw_url)
        parsed = urlparse(normalized)
        path = re.sub(r"/page/\d+/?$", "/", parsed.path)
        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            path,
            parsed.params,
            parsed.query,
            "",
        ))


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    crawler = PokeparaTainewScraper()

    # 全国取得
    #crawler.execute("https://www.pokepara-tainew.jp/")

    # 地域だけテストする場合
    crawler.execute("https://www.pokepara-tainew.jp/kanto/")

    # 都道府県だけテストする場合
    #crawler.execute("https://www.pokepara-tainew.jp/tokyo/")

    # 詳細1件だけテストする場合
    # crawler.execute("https://www.pokepara-tainew.jp/tokyo/m5/a10008/shop19933/")
