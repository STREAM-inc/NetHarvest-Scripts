"""
iiYEAH!を建てる — 工務店・ハウスメーカー情報スクレイパー（安定版）
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

_BASE_URL = "https://iiyeah-tateru.jp"

# 都道府県（完全列挙）
PREFS = [
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
]

_PREF_RE = re.compile(rf"^({'|'.join(PREFS)})")


class IiYeahTateruScraper(DynamicCrawler):
    DELAY = 2.0

    EXTRA_COLUMNS = [
        "会社名",
        "メール",
        "建設許可番号",
        "建築士事務所登録番号",
        "免許・許可・登録等",
        "保証・保険等",
        "取扱い工法",
        "参考価格",
        "対応エリア",
    ]

    def parse(self, url: str):
        collected_urls = []
        seen_urls = set()

        page_num = 1
        total_pages = 999  # 動的取得前提

        while page_num <= total_pages:
            page_url = f"{url}?page={page_num}"
            self.logger.info(f"一覧取得: {page_url}")

            soup = self.get_soup(page_url)
            if not soup:
                self.logger.warning("soup取得失敗")
                break

            # ページ数取得
            if page_num == 1:
                max_page = 1
                for a in soup.select("a[href*='page=']"):
                    m = re.search(r"page=(\d+)", a.get("href", ""))
                    if m:
                        max_page = max(max_page, int(m.group(1)))
                total_pages = max_page
                self.logger.info(f"総ページ数: {total_pages}")

            links_found = 0

            for a in soup.select("a[href]"):
                href = a.get("href", "")

                # /shops/ID をゆるく拾う
                m = re.search(r"/shops/(\d+)", href)
                if not m:
                    continue

                full_url = urljoin(_BASE_URL, f"/shops/{m.group(1)}")

                if full_url not in seen_urls:
                    seen_urls.add(full_url)
                    collected_urls.append(full_url)
                    links_found += 1

            if links_found == 0:
                self.logger.warning("リンク0件 → 終了")
                break

            page_num += 1
            time.sleep(self.DELAY)

        self.logger.info(f"詳細URL数: {len(collected_urls)}")
        self.total_items = len(collected_urls)

        # 詳細取得
        for detail_url in collected_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning(f"失敗: {detail_url} {e}")

            time.sleep(self.DELAY)

    def _scrape_detail(self, url: str):
        soup = self.get_soup(url)
        if not soup:
            return None

        data = {Schema.URL: url}

        # =========================
        # ① 会社名（最優先）
        # =========================
        name = None

        # パターン1: h1
        h1 = soup.select_one("h1")
        if h1:
            name = h1.get_text(strip=True)

        # パターン2: title fallback
        if not name:
            title = soup.title.string if soup.title else ""
            if title:
                name = title.split("|")[0].strip()

        if name:
            data[Schema.NAME] = name
        else:
            self.logger.warning(f"NAME取得失敗: {url}")
            return None

        # =========================
        # ② ラベル＋値構造（divベース）
        # =========================
        rows = soup.select("div")

        for row in rows:
            text = row.get_text(" ", strip=True)

            # 明らかに短すぎるものは除外
            if len(text) < 4:
                continue

            # よくある「ラベル：値」形式
            if "：" in text:
                parts = text.split("：", 1)
                key = parts[0].strip()
                val = parts[1].strip()

                self._map_field(data, key, val)

        # =========================
        # ③ テーブル形式 fallback
        # =========================
        for tr in soup.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")

            if th and td:
                key = th.get_text(" ", strip=True)
                val = td.get_text(" ", strip=True)
                self._map_field(data, key, val)

        # =========================
        # ④ SNS
        # =========================
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not href:
                continue

            if "instagram.com" in href:
                data.setdefault(Schema.INSTA, href)
            elif "facebook.com" in href:
                data.setdefault(Schema.FB, href)
            elif "line.me" in href:
                data.setdefault(Schema.LINE, href)
            elif "twitter.com" in href or "x.com" in href:
                data.setdefault(Schema.X, href)
            elif "tiktok.com" in href:
                data.setdefault(Schema.TIKTOK, href)

        return data

    def _map_field(self, data, key, val):
        if not val:
            return

        if key == "名称":
            data.setdefault(Schema.NAME, val)

        elif "会社名" in key:
            data.setdefault("会社名", val)

        elif "所在地" in key or "住所" in key:
            m = _PREF_RE.match(val)
            if m:
                data[Schema.PREF] = m.group(1)
                data[Schema.ADDR] = val[m.end() :].strip()
            else:
                data[Schema.ADDR] = val

        elif "代表者" in key:
            data.setdefault(Schema.REP_NM, val)

        elif "設立" in key:
            data.setdefault(Schema.OPEN_DATE, val)

        elif "電話" in key or "TEL" in key.upper():
            data.setdefault(Schema.TEL, val)

        elif "メール" in key:
            data.setdefault("メール", val)

        elif "営業時間" in key:
            data.setdefault(Schema.TIME, val)

        elif "定休日" in key:
            data.setdefault(Schema.HOLIDAY, val)

        elif "事業内容" in key:
            data.setdefault(Schema.LOB, val)

        elif "ホームページ" in key or key == "HP":
            data.setdefault(Schema.HP, val)

        elif key == "建設許可番号":
            data.setdefault("建設許可番号", val)

        elif key == "建築士事務所登録番号":
            data.setdefault("建築士事務所登録番号", val)

        elif key == "免許・許可・登録等":
            data.setdefault("免許・許可・登録等", val)

        elif "保証" in key or "保険" in key:
            data.setdefault("保証・保険等", val)

        elif "工法" in key:
            data.setdefault("取扱い工法", val)

        elif "価格" in key:
            data.setdefault("参考価格", val)

        elif "対応エリア" in key or "施工エリア" in key:
            data.setdefault("対応エリア", val)

        elif "資本金" in key:
            data.setdefault(Schema.CAP, val)

        elif "従業員" in key or "スタッフ" in key:
            data.setdefault(Schema.EMP_NUM, val)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    scraper = IiYeahTateruScraper()
    scraper.execute("https://iiyeah-tateru.jp/shops")

    print(f"出力: {scraper.output_filepath}")
    print(f"件数: {scraper.item_count}")
