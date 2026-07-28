"""
Makuake (マクアケ) — 応援購入サービスの実行者(企業/クリエイター)情報

取得対象:
    - Makuake の「すべてのプロジェクト」(/discover/all) に掲載された各プロジェクトの
      実行者(オーナー = 企業/クリエイター)のプロフィール・実績サマリ。
    - 1 実行者につき 1 行 (同一実行者は id で重複排除)。

取得フロー (備考「プロジェクトを開いてからこのようにやる必要ある」を反映):
    1. プロジェクト一覧 API (api.makuake.com/v2/projects) をページング取得。
    2. 各プロジェクトの公開ページ (project url) を開き、
       <a class="owner-info_name" href="/member/index/{id}/"> から実行者 id を取得。
       あわせて同ページの実行者 SNS ブロック (div.owner-contact 内の
       <a class="owner-contact_icon" href="..."> ) から X / Instagram / Facebook の
       URL を取得する (SNS は実行者 API には含まれず、プロジェクトページにのみ存在するため)。
    3. 実行者サマリ API (/v2/member/{id}, /v2/member/{id}/rate) で企業名・実績を取得。
    4. 重複を除外して 1 件ずつ即 yield (Pattern B)。

    ※ 一覧ページ・実行者ページとも Vue SPA で HTML には一覧データが無いため、
      サイトのフロントが利用している公開 JSON API を使用する
      (clubjt / onecareer / jobway 等と同じ方針)。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/makuake.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id makuake
"""

import logging
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)


def _classify_sns(href: str) -> str | None:
    """SNS リンクの URL からどの SNS かを判定して Schema カラム名を返す。"""
    low = href.lower()
    if "twitter.com" in low or "x.com/" in low or low.rstrip("/").endswith("x.com"):
        return Schema.X
    if "instagram.com" in low:
        return Schema.INSTA
    if "facebook.com" in low:
        return Schema.FB
    return None

# フロントが利用する公開 API のベース (www.makuake.com の SPA バックエンド)
_API_BASE = "https://api.makuake.com"
_PER_PAGE = 50  # read timeout 回避のため小さめ
# プロジェクトページ内の実行者リンク /member/index/{id}/
_MEMBER_ID_RE = re.compile(r"/member/index/(\d+)/")


class Makuake(StaticCrawler):
    """Makuake (マクアケ) 実行者スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "実行者ID",
        "総合評価",
        "評価件数",
        "応援購入総額",
        "プロジェクト数",
        "サポーター数",
        "認定クリエイター",
    ]

    def _api_get(self, path: str, params: dict | None = None):
        """公開 JSON API を叩いて dict を返す (失敗時 None)。"""
        api_url = f"{_API_BASE}{path}"
        try:
            resp = self.session.get(
                api_url,
                params=params,
                timeout=self.TIMEOUT,
                headers={
                    "Origin": "https://www.makuake.com",
                    "Referer": "https://www.makuake.com/",
                    "Accept": "application/json",
                },
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:  # noqa: BLE001 - 個別失敗はスキップして継続
            logger.warning("API 取得失敗: %s — %s", api_url, e)
            return None

    def _extract_owner(self, project_url: str) -> tuple[str | None, dict]:
        """プロジェクトページを開いて実行者(オーナー) id と SNS を取得する。

        戻り値: (owner_id または None, {Schema.X/INSTA/FB: url} の SNS dict)
        SNS は実行者 API に無く、プロジェクトページの div.owner-contact 内の
        <a class="owner-contact_icon" href="..."> にのみ存在する。
        """
        soup = self.get_soup(project_url)
        if soup is None:
            return None, {}

        # 実行者 id
        a = soup.select_one('a.owner-info_name[href*="/member/index/"]') or soup.select_one(
            'a[href*="/member/index/"]'
        )
        owner_id = None
        if a:
            m = _MEMBER_ID_RE.search(a.get("href", ""))
            owner_id = m.group(1) if m else None

        # 実行者 SNS (X / Instagram / Facebook)
        sns: dict = {}
        for link in soup.select('a.owner-contact_icon[href]'):
            href = (link.get("href") or "").strip()
            if not href:
                continue
            col = _classify_sns(href)
            if col and col not in sns:
                sns[col] = href

        return owner_id, sns

    def _build_member_row(self, owner_id: str, sns: dict | None = None) -> dict | None:
        """実行者サマリ API と SNS から 1 行分のデータを組み立てる。"""
        member = self._api_get(f"/v2/member/{owner_id}")
        if not member or not member.get("user"):
            return None
        user = member["user"]
        name = (user.get("name") or "").strip()
        if not name:
            return None

        rate = self._api_get(f"/v2/member/{owner_id}/rate") or {}
        rate_count = rate.get("count") or 0
        rate_avg = rate.get("total_evaluation") or 0

        row = {
            Schema.NAME: name,
            Schema.URL: f"https://www.makuake.com/member/index/{owner_id}/",
            Schema.X: (sns or {}).get(Schema.X, ""),
            Schema.INSTA: (sns or {}).get(Schema.INSTA, ""),
            Schema.FB: (sns or {}).get(Schema.FB, ""),
            "実行者ID": str(owner_id),
            "総合評価": f"{round(float(rate_avg), 1)}" if rate_count else "",
            "評価件数": str(rate_count) if rate_count else "",
            "応援購入総額": str(member.get("total_collected_money", "") or ""),
            "プロジェクト数": str(member.get("projects_count", "") or ""),
            "サポーター数": str(member.get("total_supporters_count", "") or ""),
            "認定クリエイター": "はい" if user.get("is_selected_creator") else "いいえ",
        }
        return row

    def parse(self, url: str):
        # url は sites.yml と同じ https://www.makuake.com/discover/all (起点)。
        # プロジェクト一覧はフロントの公開 API で取得し、各実行者ページの url は id から派生させる。
        seen: set[str] = set()
        page = 1
        while True:
            data = self._api_get(
                "/v2/projects", params={"page": page, "per_page": _PER_PAGE}
            )
            if not data:
                break
            projects = data.get("projects") or []
            if not projects:
                break

            # 進捗表示用: 総プロジェクト数を上限として設定 (実行者は重複除外されるため概算)
            if self.total_items == 0:
                total = (data.get("pagination") or {}).get("total")
                if total:
                    self.total_items = int(total)

            for proj in projects:
                project_url = proj.get("url")
                if not project_url:
                    continue
                try:
                    owner_id, sns = self._extract_owner(project_url)
                    if not owner_id or owner_id in seen:
                        continue
                    seen.add(owner_id)
                    row = self._build_member_row(owner_id, sns)
                    if row:
                        yield row
                except Exception as e:  # noqa: BLE001 - 個別アイテムのエラーは握って継続
                    logger.warning("プロジェクト処理失敗: %s — %s", project_url, e)
                    continue

            pagination = data.get("pagination") or {}
            total = pagination.get("total")
            if total is not None and page * _PER_PAGE >= int(total):
                break
            page += 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Makuake()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.makuake.com/discover/all")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
