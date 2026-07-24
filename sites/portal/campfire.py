"""
CAMPFIRE (キャンプファイヤー) — クラウドファンディングのプロジェクト & オーナー情報

取得対象:
    - プロジェクト検索一覧 (https://camp-fire.jp/projects/search) の各プロジェクト
    - 各プロジェクトのオーナープロフィール (/profile/{slug}) の所在地情報

取得フロー (一覧 → プロジェクト詳細 → プロフィール):
    1. 検索一覧を ?page=N でページ送りし、.card-wrapper から
       プロジェクト名・ID・カテゴリ・支援状況を取得
    2. プロジェクト詳細ページからオーナーの /profile/{slug} リンクを取得
    3. プロフィールページからオーナー名・在住国・現在地(=都道府県)・
       出身国・出身地・投稿プロジェクト数を取得
    各プロジェクトを取得するたびに即 yield する (Pattern B)。

備考:
    - オーナープロフィールの説明文 (p.readmore) は自由記述プロース (企業名が
      混ざる場合もある) だが、著作権リスクのため取得しない。
    - 現在地 / 出身地 が「未設定」の場合は空文字にする。

実行方法:
    # ローカルテスト
    python scripts/sites/portal/campfire.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id campfire
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

# 一覧が尽きても稀に空ページが挟まる事故を避けるため、明示的な上限を設ける
_MAX_PAGES = 500


class Campfire(StaticCrawler):
    """CAMPFIRE (キャンプファイヤー) スクレイパー"""

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    DELAY = 1.5
    EXTRA_COLUMNS = [
        "プロジェクトID",
        "オーナー名",
        "オーナーID",
        "オーナープロフィールURL",
        "在住国",
        "出身国",
        "出身地",
        "投稿プロジェクト数",
        "達成率",
        "現在の支援総額",
        "支援者数",
        "残り日数",
        "ブランド",
    ]

    def parse(self, url: str):
        page = 1
        while page <= _MAX_PAGES:
            soup = self.get_soup(f"{url}?page={page}")
            if soup is None:
                break
            cards = soup.select(".card-wrapper")
            if not cards:
                break

            for card in cards:
                try:
                    item = self._parse_card(card, url)
                except Exception as e:  # 個別カードのエラーは握って続行
                    self.logger.warning("カード解析に失敗しました: %s", e)
                    continue
                if item:
                    yield item

            page += 1

    # ------------------------------------------------------------------
    # 一覧カード + 詳細/プロフィールの合成
    # ------------------------------------------------------------------
    def _parse_card(self, card, root_url: str) -> dict | None:
        anchor = card.select_one("a.card[href]") or card.select_one("a[href]")
        if anchor is None:
            return None

        href = anchor.get("href", "")
        project_url = urljoin(root_url, href.split("?")[0])

        name = (anchor.get("data-gtm-data-name") or "").strip()
        if not name:
            name_el = card.select_one("h2.name")
            if name_el:
                name = name_el.get_text(strip=True)

        category = anchor.get("data-gtm-data-category", "") or ""
        category = re.sub(r"^category_", "", category).strip()
        brand = (anchor.get("data-gtm-data-brand") or "").strip()
        project_id = (anchor.get("data-gtm-data-id") or "").strip()

        item = {
            Schema.NAME: name,
            Schema.URL: project_url,
            Schema.PREF: "",
            Schema.CAT_SITE: category,
            "プロジェクトID": project_id,
            "オーナー名": "",
            "オーナーID": "",
            "オーナープロフィールURL": "",
            "在住国": "",
            "出身国": "",
            "出身地": "",
            "投稿プロジェクト数": "",
            "達成率": self._text_num(card, ".success-rate-sp"),
            "現在の支援総額": self._first_number(card, ".footer-item.total"),
            "支援者数": self._first_number(card, ".footer-item.rest"),
            "残り日数": self._first_number(card, ".footer-item.per"),
            "ブランド": brand,
        }

        # プロジェクト詳細 → オーナープロフィールへ辿って所在地を補完
        profile_url = self._find_profile_url(project_url, root_url)
        if profile_url:
            item["オーナープロフィールURL"] = profile_url
            item["オーナーID"] = profile_url.rstrip("/").rsplit("/", 1)[-1]
            self._enrich_from_profile(item, profile_url)

        return item

    def _find_profile_url(self, project_url: str, root_url: str) -> str:
        soup = self.get_soup(project_url)
        if soup is None:
            return ""
        a = soup.select_one('a.user-display-name-wrap[href^="/profile/"]')
        if a is None:
            a = soup.select_one('a[href^="/profile/"]')
        if a is None:
            return ""
        return urljoin(root_url, a.get("href", "").split("?")[0])

    def _enrich_from_profile(self, item: dict, profile_url: str) -> None:
        soup = self.get_soup(profile_url)
        if soup is None:
            return

        name_el = soup.select_one(".username h1") or soup.select_one("h1")
        if name_el:
            item["オーナー名"] = name_el.get_text(strip=True)

        # 「これまでに N件のプロジェクトを投稿しています」から件数を抽出
        for h2 in soup.select("h2"):
            t = h2.get_text(strip=True)
            if "プロジェクト" in t:
                m = re.search(r"([0-9,]+)\s*件", t)
                if m:
                    item["投稿プロジェクト数"] = m.group(1).replace(",", "")
                break

        # ul.pref.clearfix 内の「ラベル：値」を収集 (在住国/現在地/出身国/出身地)
        labels = {}
        for ul in soup.select("ul.pref"):
            for li in ul.select("li"):
                txt = li.get_text(strip=True)
                m = re.match(r"(.+?)[：:]\s*(.*)$", txt)
                if m:
                    labels[m.group(1).strip()] = self._clean_loc(m.group(2).strip())

        item["在住国"] = labels.get("在住国", "")
        item["出身国"] = labels.get("出身国", "")
        item["出身地"] = labels.get("出身地", "")
        # 現在地 = オーナーの都道府県として PREF に採用
        item[Schema.PREF] = labels.get("現在地", "")

    # ------------------------------------------------------------------
    # ヘルパ
    # ------------------------------------------------------------------
    @staticmethod
    def _clean_loc(value: str) -> str:
        return "" if value in ("", "未設定", "未登録") else value

    @staticmethod
    def _text_num(node, selector: str) -> str:
        el = node.select_one(selector)
        return el.get_text(strip=True) if el else ""

    @staticmethod
    def _first_number(node, selector: str) -> str:
        el = node.select_one(selector)
        if not el:
            return ""
        m = re.search(r"[\d,]+", el.get_text(" ", strip=True))
        return m.group(0).replace(",", "") if m else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Campfire()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://camp-fire.jp/projects/search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
