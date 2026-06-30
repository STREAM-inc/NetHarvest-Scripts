"""
クラウドワークス (CrowdWorks) — クラウドソーシング クライアント(企業)情報スクレイパー

取得対象:
    - 仕事一覧 (/public/jobs) に掲載されている各仕事のクライアント (発注者) 情報
    - 各クライアントの企業ページ (/public/employers/{user_id}) に記載された
      構造化された会社概要 (都道府県・仕事カテゴリ・実績・評価・公開URL 等)

取得フロー:
    1. 仕事一覧ページ (?page=N) の #vue-container[data] に埋め込まれた JSON を解析
    2. 各仕事の client.user_id を取り出し (重複は除外)
    3. クライアントごとに企業ページ /public/employers/{user_id} を取得し
       #vue-container[data] の JSON から会社概要を抽出して即 yield

備考対応:
    - 「会社概要 / 概要が登録されていません。ここで習得する。匿名性が高い」という方針に従い、
      各仕事 → クライアント情報 → 企業ページ の順に辿って会社概要を取得する。
    - クラウドワークスは匿名性が高く、TEL・住所・代表者名は公開されない。
      取得できるのは都道府県・カテゴリ・実績/評価などの構造化情報のみ。
    - company_profile.description (会社紹介文) や口コミ本文は「自由記述プロース」のため
      著作権リスク回避として取得しない (備考に明示的な許可が無いため除外)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/crowdworks.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id crowdworks
"""

import json
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class Crowdworks(StaticCrawler):
    """クラウドワークス クライアント(企業)情報スクレイパー"""

    DELAY = 1.5

    # 仕事一覧 / PR枠で client を保持しうるリストキー
    _LIST_KEYS = ("job_offers", "pr_diamond", "pr_platinum", "pr_gold", "recommendation")

    EXTRA_COLUMNS = [
        "ユーザーID",        # client.user_id / employer page id (数値コード)
        "ユーザー名",        # username (ハンドル)。display_name と異なる場合がある
        "アカウント登録日",  # created_at を YYYY-MM-DD に整形
        "本人確認",          # identity_verified -> 済 / 未
        "認定クライアント",  # is_certified_employer -> 認定 / 通常
        "公式アカウント",    # is_official_account -> 公式 / 通常
        "募集実績数",        # job_offer_achievement_count
        "プロジェクト完了率",  # project_finished_data.rate (%)
        "完了プロジェクト数",  # project_finished_data.total_finished_count
    ]

    def parse(self, url: str):
        self._seen: set[int] = set()
        page = 1
        while True:
            list_url = self._with_page(url, page)
            soup = self.get_soup(list_url)
            if soup is None:
                break

            data = self._vue_data(soup)
            if not data:
                break
            search = data.get("searchResult", {})

            # 進捗表示用に総件数 (掲載中の仕事数) を初回に設定
            if page == 1:
                total = (search.get("page") or {}).get("total_entries")
                if total:
                    self.total_items = total

            # 当ページの全リストから client.user_id を抽出
            user_ids = []
            for key in self._LIST_KEYS:
                for row in search.get(key) or []:
                    uid = (row.get("client") or {}).get("user_id")
                    if uid:
                        user_ids.append((uid, (row.get("client") or {}).get("username")))

            if not user_ids:
                break

            for uid, username in user_ids:
                if uid in self._seen:
                    continue
                self._seen.add(uid)
                try:
                    record = self._scrape_employer(url, uid, username)
                except Exception as e:  # 個別エラーはログして継続
                    self.logger.warning("企業ページ取得失敗 user_id=%s: %s", uid, e)
                    continue
                if record:
                    yield record

            # ページネーション: 最終ページに達したら終了
            page_info = search.get("page") or {}
            current = page_info.get("current_page", page)
            total_page = page_info.get("total_page")
            if total_page and current >= total_page:
                break
            page += 1

    def _scrape_employer(self, root_url: str, user_id: int, username: str | None) -> dict | None:
        """企業ページ /public/employers/{user_id} を取得して会社概要を構築する。"""
        employer_url = urljoin(root_url, f"/public/employers/{user_id}")
        soup = self.get_soup(employer_url)
        if soup is None:
            return None
        data = self._vue_data(soup)
        if not data:
            return None

        eu = (data.get("employer_profile_json") or {}).get("employer_user") or {}
        summary = data.get("employer_profile_summary_json") or {}

        # 表示名 (会社名/クライアント名)。無ければ public_employer_page_json から補完
        name = eu.get("display_name")
        if not name:
            name = ((data.get("public_employer_page_json") or {}).get("employer_user") or {}).get("display_name")

        feedback = eu.get("feedback") or {}
        pfd = eu.get("project_finished_data") or {}
        company_profile = summary.get("company_profile") or {}
        categories = [c.get("name", "") for c in (summary.get("job_categories") or []) if c.get("name")]

        record = {
            Schema.URL: employer_url,
            Schema.NAME: name or "",
            Schema.PREF: eu.get("prefecture_name") or "",
            # company_profile.url が登録されていれば会社HP として採用 (多くは null)
            Schema.HP: company_profile.get("url") or "",
            # 仕事カテゴリ (サイト定義ジャンル)
            Schema.CAT_SITE: " / ".join(categories),
            # 評価 (口コミ採点 / 件数)
            Schema.SCORES: self._fmt_num(feedback.get("average_score")),
            Schema.REV_SCR: self._fmt_num(feedback.get("total_count")),
            # --- EXTRA ---
            "ユーザーID": str(user_id),
            "ユーザー名": username or "",
            "アカウント登録日": self._fmt_date(eu.get("created_at")),
            "本人確認": "済" if eu.get("identity_verified") else "未",
            "認定クライアント": "認定" if eu.get("is_certified_employer") else "通常",
            "公式アカウント": "公式" if eu.get("is_official_account") else "通常",
            "募集実績数": self._fmt_num(eu.get("job_offer_achievement_count")),
            "プロジェクト完了率": self._fmt_num(pfd.get("rate")),
            "完了プロジェクト数": self._fmt_num(pfd.get("total_finished_count")),
        }
        return record

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------
    @staticmethod
    def _vue_data(soup) -> dict | None:
        """#vue-container[data] に埋め込まれた JSON を取り出す。"""
        el = soup.select_one("#vue-container")
        if not el or not el.get("data"):
            return None
        try:
            return json.loads(el["data"])
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _with_page(url: str, page: int) -> str:
        """ルート url にページ番号を付与する (既存クエリ ?ref= 等は保持)。"""
        parts = urlparse(url)
        qs = parse_qs(parts.query)
        qs["page"] = [str(page)]
        new_query = urlencode(qs, doseq=True)
        return urlunparse(parts._replace(query=new_query))

    @staticmethod
    def _fmt_date(value: str | None) -> str:
        """ISO8601 (2022-06-27T16:49:28+09:00) -> 2022-06-27。"""
        if not value:
            return ""
        return str(value).split("T", 1)[0]

    @staticmethod
    def _fmt_num(value) -> str:
        if value is None:
            return ""
        return str(value)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Crowdworks()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://crowdworks.jp/public/jobs?ref=public_header")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
