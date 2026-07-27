"""
CAMPFIRE (キャンプファイヤー) — 国内最大級のクラウドファンディングプラットフォーム

取得対象:
    - 公開中/終了済みの全プロジェクト (実行者=店舗/事業者ごとの構造化情報)
    - 事業者名(=実行者)・プロジェクト名・カテゴリ・都道府県・達成率・
      詳細ページタイトル・掲載日(掲載開始日)・掲載終了日・
      目標金額・支援総額・募集方式、および実行者に紐づく外部URL
      (公式サイト / X / Instagram / Facebook / TikTok / LINE / YouTube 等)

都道府県 / 達成率 / 詳細ページタイトルについて:
    - 都道府県: プロジェクトヒーロー内 .other-info-lists の prefecture= リンク文言を採用。
      地域を設定していない (主に古い) プロジェクトには存在しないため、その場合は空。
      検索モーダル (.region-lists) の全県ナビとは別物なので混同しない。
    - 達成率: p.percentage 内の数値 (例 "8" → "8%")。無い場合は支援総額/目標金額から算出。
    - 詳細ページタイトル: <title> の文言 (例 "○○ - CAMPFIRE (キャンプファイヤー)")。

取得フロー (Static / 取得即 yield = Pattern B):
    1. ルート url (= sites.yml の url, https://camp-fire.jp/) から /sitemap.xml を派生
    2. サイトマップインデックスから projects.N.xml (プロジェクト一覧) を列挙
    3. 各 projects.N.xml から /projects/{id}/view のプロジェクト URL を取得
    4. プロジェクトページを 1 件取得するごとに <script type="application/ld+json">
       (Project / BreadcrumbList) と実行者リンクをパースして即 yield する

掲載日について:
    - ld+json Project の additionalProperty.startDate を「掲載日(掲載開始日)」として直接取得。
      掲載終了日 (endDate) も併せて保持する (掲載期間 = startDate〜endDate)。

取得できないフィールド (出典に存在しないため除外):
    - 電話番号 / 住所 / 代表者名 / 資本金 / 従業員数:
      プロジェクトページには特定商取引法に基づく事業者情報の掲示が無く、実行者の多くが
      個人のため、これらの企業属性は出典に存在しない。
    - プロジェクト説明文 (description): 自由記述プロースのため著作権配慮で保存対象外。

規約:
    - 利用規約 (https://camp-fire.jp/term) にスクレイピング/クローリングを明示的に
      禁止する条項は無い (第27条の一般的な設備不正利用禁止のみ)。
    - robots.txt は /projects/*/backers/ 等を Disallow するが、/projects/{id}/view
      (公開プロジェクトページ) と /sitemap* は許可されている。

実行方法:
    # ローカルテスト
    python scripts/sites/portal/campfire_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id campfire_2
"""

import json
import re
import sys
import warnings
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import XMLParsedAsHTMLWarning

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# サイトマップ (XML) を共通の get_soup (html.parser) で読むため出る警告を抑制。
# <loc> の抽出のみで用途上問題ないため無視する。
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# サイトマップインデックス内でプロジェクト一覧サブサイトマップを判定するパターン
_PROJECT_SITEMAP = re.compile(r"/sitemap/projects\.\d+\.xml", re.I)
# プロジェクト詳細 URL (/projects/{id}/view) を判定するパターン
_PROJECT_URL = re.compile(r"/projects/(\d+)/view", re.I)

# 実行者リンクを SNS 種別に振り分けるためのホスト判定
_SNS_HOSTS = {
    "x": ("x.com", "twitter.com"),
    "insta": ("instagram.com",),
    "fb": ("facebook.com",),
    "tiktok": ("tiktok.com",),
    "line": ("line.me",),
    "youtube": ("youtube.com", "youtu.be"),
}

# CAMPFIRE 公式アカウント / 共有ボタン等 (実行者本人の URL ではない) を除外するパターン
_OFFICIAL_RE = re.compile(
    r"campfirejp|campfire_jp|campfire\.co\.jp|/sharer|intent/tweet|"
    r"/share|/plugins/|help\.camp-fire|community\.camp-fire",
    re.I,
)


class Campfire2Scraper(StaticCrawler):
    """CAMPFIRE (キャンプファイヤー) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "プロジェクトID",
        "プロジェクト名",
        "詳細ページタイトル",  # <title> 文言
        "達成率",              # p.percentage の数値 (例 "8%")
        "掲載日",          # 掲載開始日 (ld+json startDate)
        "掲載終了日",      # 掲載期間の終了日 (ld+json endDate)
        "目標金額",
        "支援総額",
        "募集方式",        # All-or-Nothing / All-In
        "YouTube",
        "プロフィールURL", # 実行者プロフィール (/profile/{slug})
        "ユーザー関連URL", # 実行者に紐づく外部URLを全て連結
    ]

    # ------------------------------------------------------------------ #
    # メインフロー
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()

        # 1. サイトマップインデックス (root url から派生) → プロジェクト一覧サブサイトマップ
        index_url = urljoin(url, "/sitemap.xml")
        sitemap_urls = self._extract_locs(index_url, _PROJECT_SITEMAP)
        if not sitemap_urls:
            self.logger.warning("プロジェクト用サイトマップが見つかりません: %s", index_url)
            return
        self.logger.info("プロジェクト用サイトマップ数: %d", len(sitemap_urls))

        # 2. 各サブサイトマップ → プロジェクト URL → 取得即 yield
        for sm_url in sitemap_urls:
            for project_url in self._extract_project_urls(sm_url, url):
                m = _PROJECT_URL.search(project_url)
                pid = m.group(1) if m else ""
                if pid and pid in seen_ids:
                    continue
                if pid:
                    seen_ids.add(pid)
                try:
                    record = self._scrape_detail(project_url, pid)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細取得失敗 %s: %s", project_url, e)
                    continue
                if record:
                    yield record

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, url: str, pid: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        project, breadcrumb = self._extract_ld_json(soup)
        if not project:
            # 削除済み / 非公開 / エラーページ (Project ld+json 無し) はスキップ
            return None

        props = {
            p.get("name"): p.get("value")
            for p in project.get("additionalProperty", []) or []
            if isinstance(p, dict)
        }

        # 実行者名 (=店舗/事業者名)。無ければプロジェクト名で代替。
        sponsor = project.get("sponsor") or {}
        name = ""
        if isinstance(sponsor, dict):
            name = self._clean(sponsor.get("name"))
        project_name = self._clean(project.get("name"))
        if not name:
            name = project_name
        if not name:
            return None

        # カテゴリ: パンくずの category= リンクの name
        category = self._category_from_breadcrumb(breadcrumb)

        # 都道府県: プロジェクトヒーロー内の prefecture= リンク文言 (検索ナビとは別)
        prefecture = self._prefecture(soup)
        # 達成率: p.percentage の数値。無ければ 支援総額/目標金額 から算出
        rate = self._achievement_rate(soup, props)
        # 詳細ページタイトル (<title>)
        page_title = self._clean(soup.title.get_text() if soup.title else "")

        # 実行者に紐づく外部リンクを分類
        sns = self._collect_owner_links(soup)
        profile_url = self._profile_url(soup, url)

        record = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: prefecture,
            Schema.CAT_SITE: category,
            Schema.HP: sns.get("hp", ""),
            Schema.X: sns.get("x", ""),
            Schema.INSTA: sns.get("insta", ""),
            Schema.FB: sns.get("fb", ""),
            Schema.TIKTOK: sns.get("tiktok", ""),
            Schema.LINE: sns.get("line", ""),
            # --- EXTRA ---
            "プロジェクトID": pid,
            "プロジェクト名": project_name,
            "詳細ページタイトル": page_title,
            "達成率": rate,
            "掲載日": self._clean(props.get("startDate")),
            "掲載終了日": self._clean(props.get("endDate")),
            "目標金額": props.get("fundingGoal", ""),
            "支援総額": props.get("amountRaised", ""),
            "募集方式": self._clean(props.get("fundingType")),
            "YouTube": sns.get("youtube", ""),
            "プロフィールURL": profile_url,
            "ユーザー関連URL": " | ".join(sns.get("all", [])),
        }
        return record

    # ------------------------------------------------------------------ #
    # ヘルパー
    # ------------------------------------------------------------------ #
    def _extract_locs(self, sitemap_url: str, pattern: re.Pattern) -> list[str]:
        """サイトマップ (XML) を取得し、<loc> のうち pattern に一致する URL を返す。"""
        soup = self.get_soup(sitemap_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for loc in soup.find_all("loc"):
            href = loc.get_text(strip=True)
            if pattern.search(href) and href not in seen:
                seen.add(href)
                urls.append(href)
        return urls

    def _extract_project_urls(self, sitemap_url: str, root: str) -> list[str]:
        """プロジェクト一覧サブサイトマップから /projects/{id}/view の URL を返す。"""
        soup = self.get_soup(sitemap_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for loc in soup.find_all("loc"):
            href = loc.get_text(strip=True)
            if _PROJECT_URL.search(href):
                absolute = urljoin(root, href)
                if absolute not in seen:
                    seen.add(absolute)
                    urls.append(absolute)
        return urls

    @staticmethod
    def _extract_ld_json(soup) -> tuple[dict | None, dict | None]:
        """ページ内の ld+json から Project と BreadcrumbList を取り出す。"""
        project = None
        breadcrumb = None
        for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = tag.string or tag.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            candidates = data if isinstance(data, list) else [data]
            for d in candidates:
                if not isinstance(d, dict):
                    continue
                t = d.get("@type")
                if t == "Project" and project is None:
                    project = d
                elif t == "BreadcrumbList" and breadcrumb is None:
                    breadcrumb = d
        return project, breadcrumb

    @staticmethod
    def _category_from_breadcrumb(breadcrumb: dict | None) -> str:
        """パンくずの category= を指す要素の name をカテゴリとして返す。"""
        if not breadcrumb:
            return ""
        for el in breadcrumb.get("itemListElement", []) or []:
            if not isinstance(el, dict):
                continue
            item = el.get("item") or ""
            if "category=" in str(item):
                return Campfire2Scraper._clean(el.get("name"))
        return ""

    @staticmethod
    def _prefecture(soup) -> str:
        """プロジェクト固有の都道府県 (.other-info-lists の prefecture= リンク) を返す。

        検索モーダル (.region-lists) の全県ナビには prefecture= リンクが47件並ぶため、
        ヒーロー内 .other-info-lists に限定してプロジェクト本来の地域のみを採る。
        地域未設定のプロジェクトには存在しないため、その場合は空文字。
        """
        a = soup.select_one('.other-info-lists a[href*="prefecture="]')
        if a:
            return Campfire2Scraper._clean(a.get_text())
        return ""

    @staticmethod
    def _achievement_rate(soup, props: dict) -> str:
        """達成率を返す。p.percentage の数値優先、無ければ 支援総額/目標金額 から算出。"""
        el = soup.select_one("p.percentage span") or soup.select_one("p.percentage")
        if el:
            text = Campfire2Scraper._clean(el.get_text())
            m = re.search(r"\d[\d,]*", text)
            if m:
                return m.group(0).replace(",", "") + "%"
        # フォールバック: ld+json の金額から算出
        try:
            goal = float(props.get("fundingGoal") or 0)
            raised = float(props.get("amountRaised") or 0)
            if goal > 0:
                return f"{round(raised / goal * 100)}%"
        except (TypeError, ValueError):
            pass
        return ""

    def _collect_owner_links(self, soup) -> dict:
        """実行者(オーナー)カードの外部リンクを SNS 種別に振り分けて返す。

        オーナーカードのリンクは `a.text[target="_blank"]` で描画される。
        CAMPFIRE 公式アカウント・共有ボタンは除外し、SNS はホスト名で分類する。
        """
        result: dict = {"all": []}
        seen: set[str] = set()
        for a in soup.select('a.text[href][target="_blank"]'):
            href = (a.get("href") or "").strip()
            if not href or href in seen:
                continue
            host = urlparse(href).netloc.lower()
            # CAMPFIRE 内部リンク / 公式 / 共有ボタンは除外
            if "camp-fire.jp" in host or _OFFICIAL_RE.search(href):
                continue
            seen.add(href)
            result["all"].append(href)

            matched_sns = False
            for key, hosts in _SNS_HOSTS.items():
                if any(host == h or host.endswith("." + h) for h in hosts):
                    result.setdefault(key, href)  # 先頭のものを採用
                    matched_sns = True
                    break
            # SNS でない外部リンクは公式サイト(HP)の第一候補とする
            if not matched_sns:
                result.setdefault("hp", href)
        return result

    @staticmethod
    def _profile_url(soup, root: str) -> str:
        """実行者プロフィール (/profile/{slug}) の絶対 URL を返す。"""
        for a in soup.select('a[href^="/profile/"], a[href*="/profile/"]'):
            href = a.get("href") or ""
            m = re.search(r"/profile/[A-Za-z0-9_\-]+", href)
            if m:
                return urljoin(root, m.group(0))
        return ""

    @staticmethod
    def _clean(value) -> str:
        """空白を畳んで前後を除去する (構造化値の整形のみ)。"""
        if value is None:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Campfire2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://camp-fire.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
