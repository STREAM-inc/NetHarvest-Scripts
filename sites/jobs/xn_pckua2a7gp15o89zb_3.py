"""
求人ボックス【外国人人材】 (求人ボックス.com / kyujinbox) — 外国人を正社員採用できる企業の営業リスト生成

取得対象:
    - 「在留資格 / 日本語能力試験 (JLPT) / 永住権 / 日本語検定」等、外国人採用のシグナルとなる
      キーワードでヒットし、かつ雇用形態が「正社員」の求人。
    - 各求人カードに埋め込まれた構造化 JSON (data-func-show-arg) から、会社名・勤務地・
      雇用形態・給与・特徴タグ・元求人URL 等を取得する。

取得フロー (Pattern B: 詳細取得ごとに即 yield):
    root(=sites.yml url) から派生した各シグナルキーワードの検索ページ
      f"{root}{keyword}の仕事" を ?pg=N でページ送りしながら巡回。
    カードの JSON をパースし、正社員のみ・uniqueId でデデュープして 1 件ずつ yield する。

備考対応:
    - 「外国人を正社員として採用できる企業」= 雇用形態フィルタ (正社員) + 外国人採用シグナル
      キーワードでの検索。フィルタは parse() 内に実装。
    - 仕事内容 / 雇用形態 / 給与 等の求人情報を EXTRA_COLUMNS に付与 (備考で明示的に要求されたため
      仕事内容スニペットも含む)。勤務時間は求人詳細ページに行かなくても検索結果カードの
      スニペット文中 (例:「勤務時間は09:00～18:00で…」) に含まれるため、xx:yy~xx:yy 形式で
      正規表現抽出する。

robots.txt: /api/, /jb/(詳細), /apply* 等は Disallow。検索結果ページ (/{kw}の仕事) は許可対象。
            本クローラは許可された検索結果ページのみを取得する。

実行方法:
    python scripts/sites/jobs/xn_pckua2a7gp15o89zb_3.py
    docker compose exec worker python /app/bin/run_flow.py --site-id xn_pckua2a7gp15o89zb_3
"""

import json
import logging
import re
import sys
from pathlib import Path
from urllib.parse import quote, urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 都道府県 (workArea 先頭トークンの判定用)
_PREFS = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    "東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    "滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|"
    "香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(rf"^({_PREFS})")

# 勤務時間 (例「勤務時間は09:00～18:00で…」)。スニペット文中から xx:yy~xx:yy 形式で抽出。
_WORK_TIME_RE = re.compile(r"(\d{1,2}:\d{2})\s*[~〜～\-－]\s*(\d{1,2}:\d{2})")


class KyujinBoxForeigner(StaticCrawler):
    """求人ボックス【外国人人材】 スクレイパー (外国人×正社員の営業リスト)"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "給与",
        "勤務地",
        "勤務時間",
        "特徴タグ",
        "更新日",
        "ヒットキーワード",
        "仕事内容",
    ]

    # 外国人採用のシグナルとなる検索キーワード (備考の JLPT / 在留資格 / 永住権 等に対応)
    KEYWORDS = ["在留資格", "日本語能力試験", "永住権", "日本語検定", "JLPT"]

    # キーワードごとのページ送り上限 (25件/ページ)。営業リストの規模と巡回時間の折衷。
    MAX_PAGES_PER_KEYWORD = 40

    def parse(self, url: str):
        seen = set()

        for keyword in self.KEYWORDS:
            # root(=引数 url) から検索URLを派生。別URLはハードコードしない。
            search_url = urljoin(url, quote(f"{keyword}の仕事"))

            for pg in range(1, self.MAX_PAGES_PER_KEYWORD + 1):
                page_url = search_url if pg == 1 else f"{search_url}?pg={pg}"
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                cards = soup.select(".p-result_card")
                if not cards:
                    break

                for card in cards:
                    try:
                        item = self._parse_card(card, keyword)
                    except Exception as e:  # noqa: BLE001 - 個別カードの失敗はスキップして継続
                        logger.warning("カード解析失敗 (%s pg=%d): %s", keyword, pg, e)
                        continue

                    if item is None:
                        continue

                    uid = item.pop("_uid", None)
                    if uid and uid in seen:
                        continue
                    if uid:
                        seen.add(uid)

                    yield item

    def _parse_card(self, card, keyword: str) -> dict | None:
        link = card.select_one("a.p-result_title_link")
        if not link or not link.get("data-func-show-arg"):
            return None

        outer = json.loads(link["data-func-show-arg"])
        data = json.loads(outer["json"])

        # 正社員フィルタ (「正社員」「正社員 / アルバイト・パート」等を許容)
        employ = (data.get("employType") or "").strip()
        if "正社員" not in employ:
            return None

        company = (data.get("company") or data.get("siteName") or "").strip()
        if not company:
            return None

        work_area = (data.get("workArea") or "").strip()
        pref, addr = "", work_area
        m = _PREF_RE.match(work_area)
        if m:
            pref = m.group(1)
            addr = work_area[m.end():].strip()

        tags = data.get("allFeatureTags") or []
        updated = (data.get("updatedAt") or "")[:10]  # YYYY-MM-DD

        # 仕事内容スニペット (備考で明示要求されたため取得)
        lines_el = card.select_one(".p-result_lines")
        job_desc = lines_el.get_text(" ", strip=True) if lines_el else ""

        # 勤務時間はスニペット文中から xx:yy~xx:yy 形式で抽出 (詳細ページ不要)
        work_time = ""
        tm = _WORK_TIME_RE.search(job_desc)
        if tm:
            work_time = f"{tm.group(1)}~{tm.group(2)}"

        return {
            "_uid": data.get("uniqueId"),
            Schema.NAME: company,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.URL: (data.get("url") or "").strip(),
            "求人タイトル": (data.get("formatTitle") or data.get("title") or "").strip(),
            "雇用形態": employ,
            "給与": (data.get("payment") or "").strip(),
            "勤務地": work_area,
            "勤務時間": work_time,
            "特徴タグ": " / ".join(tags),
            "更新日": updated,
            "ヒットキーワード": keyword,
            "仕事内容": job_desc,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KyujinBoxForeigner()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://xn--pckua2a7gp15o89zb.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
