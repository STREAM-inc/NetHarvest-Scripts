"""
サンラーズ (lancers) — ランサーズ フリーランス・法人ランサー ディレクトリ スクレイパー

取得対象:
    - ランサーズ (lancers.jp) のランサー検索 (/profile/search) に掲載されている
      フリーランス・法人ランサーのプロフィール構造化情報
    - 名称 (個人名/法人名) / 職種 / ニックネーム / ランク / 実績・満足率・完了率・
      リピーター数 / 稼働状況 / 稼働単価 / 得意なカテゴリ・業種・スキル /
      ランサーズ登録日 / 24時間以内メッセージ返信率 / インボイス対応 / 認証状況

取得フロー:
    1. ルート URL から /profile/search を派生し、?page=N で全ページを巡回する。
    2. 各ページの 15 件のランサーについて、リストから 名称/職種/ニックネーム/
       各種実績メトリクスを取得し、詳細プロフィール (/profile/{nickname}) を
       1 件取得するたびに即 yield する (Pattern B / 早期 yield)。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。別 URL はハードコードしない。
    - キャッチコピー・自己PR・パッケージ (提案) タイトルは自由記述の宣伝文 (プロース)
      であり、著作権リスク回避のため取得しない。
    - プロフィールには住所・電話番号・都道府県が掲載されないため
      Schema.ADDR / Schema.TEL / Schema.PREF は取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/service/lancers.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id lancers
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 連続する空白 (改行・全角含む) を 1 つの半角スペースに畳む
_WS = re.compile(r"\s+")

# EXTRA カラム名 (いずれも短い構造化ラベル・数値・日付。自由記述は含めない)
_COL_NICKNAME = "ニックネーム"
_COL_RANK = "ランク"
_COL_ACHIEVEMENTS = "実績数"
_COL_SATISFACTION = "満足率"
_COL_COMPLETION = "完了率"
_COL_REPEATER = "リピーター数"
_COL_AVAILABILITY = "稼働状況"
_COL_RATE = "稼働単価"
_COL_CATEGORY = "得意なカテゴリ"
_COL_INDUSTRY = "得意な業種"
_COL_SKILL = "得意なスキル"
_COL_REG_DATE = "ランサーズ登録日"
_COL_REPLY_RATE = "24時間以内メッセージ返信率"
_COL_INVOICE = "インボイス対応"
_COL_VERIFY = "認証状況"

# 詳細ページ dt ラベル (前方一致) → EXTRA カラム名
_DETAIL_LABELS = [
    ("稼働時間の目安", _COL_AVAILABILITY),
    ("稼働単価の目安", _COL_RATE),
    ("得意なカテゴリ", _COL_CATEGORY),
    ("得意な業種", _COL_INDUSTRY),
    ("得意なスキル", _COL_SKILL),
    ("登録日", _COL_REG_DATE),
    ("24時間以内のメッセージ返信率", _COL_REPLY_RATE),
]


class Lancers(StaticCrawler):
    """サンラーズ (ランサーズ) ランサーディレクトリ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        _COL_NICKNAME,
        _COL_RANK,
        _COL_ACHIEVEMENTS,
        _COL_SATISFACTION,
        _COL_COMPLETION,
        _COL_REPEATER,
        _COL_AVAILABILITY,
        _COL_RATE,
        _COL_CATEGORY,
        _COL_INDUSTRY,
        _COL_SKILL,
        _COL_REG_DATE,
        _COL_REPLY_RATE,
        _COL_INVOICE,
        _COL_VERIFY,
    ]

    @staticmethod
    def _clean(text: str) -> str:
        return _WS.sub(" ", text or "").strip()

    @staticmethod
    def _metric(status_text: str) -> str:
        """'実績 722' / '満足率 99 %' 等から数値部分を抽出する。"""
        m = re.search(r"([\d,]+\s*(?:%|人|件)?)", status_text)
        return m.group(1).replace(" ", "") if m else ""

    def parse(self, url: str):
        list_url = urljoin(url, "profile/search")
        page = 1
        while True:
            page_url = f"{list_url}?page={page}"
            soup = self.get_soup(page_url)
            items = soup.select(".p-search-talent__media.js-search-talent__item")
            if not items:
                break

            for item in items:
                try:
                    row = self._parse_list_item(item, url)
                    if not row:
                        continue
                    detail_url = row.pop("_detail_url", None)
                    if detail_url:
                        try:
                            row.update(self._scrape_detail(detail_url))
                        except Exception as exc:  # noqa: BLE001
                            logger.warning("詳細取得失敗 %s: %s", detail_url, exc)
                    yield row
                except Exception as exc:  # noqa: BLE001
                    logger.warning("リストアイテム解析失敗 (page=%s): %s", page, exc)
                    continue

            page += 1

    def _parse_list_item(self, item, url: str) -> dict | None:
        link = item.select_one(".p-search-talent__media-title-link")
        if not link:
            return None
        href = link.get("href")
        detail_url = urljoin(url, href) if href else None

        # 名称 (ニックネーム span を除いた本体テキスト)
        nick_el = link.select_one(".p-search-talent__media-title-nickname")
        nickname = ""
        if nick_el:
            nickname = nick_el.get_text(strip=True).strip("()（）")
            nick_el.extract()
        name = self._clean(link.get_text(" ", strip=True))

        # 職種 (サイト定義ジャンル)
        job_el = item.select_one(".c-badge-job__text")
        job = job_el.get_text(strip=True) if job_el else ""

        row = {
            Schema.NAME: name,
            Schema.URL: detail_url or url,
            Schema.CAT_SITE: job,
            _COL_NICKNAME: nickname,
            _COL_ACHIEVEMENTS: "",
            _COL_SATISFACTION: "",
            _COL_COMPLETION: "",
            _COL_REPEATER: "",
            "_detail_url": detail_url,
        }

        # 実績メトリクス (外部実績 --external は除外)
        for st in item.select(".p-search-talent__media-status"):
            classes = st.get("class") or []
            if "p-search-talent__media-status--external" in classes:
                continue
            txt = self._clean(st.get_text(" ", strip=True))
            if txt.startswith("実績"):
                row[_COL_ACHIEVEMENTS] = self._metric(txt)
            elif txt.startswith("満足率"):
                row[_COL_SATISFACTION] = self._metric(txt)
            elif txt.startswith("完了率"):
                row[_COL_COMPLETION] = self._metric(txt)
            elif txt.startswith("リピーター"):
                row[_COL_REPEATER] = self._metric(txt)

        return row

    def _scrape_detail(self, detail_url: str) -> dict:
        soup = self.get_soup(detail_url)
        out: dict = {
            _COL_RANK: "",
            _COL_AVAILABILITY: "",
            _COL_RATE: "",
            _COL_CATEGORY: "",
            _COL_INDUSTRY: "",
            _COL_SKILL: "",
            _COL_REG_DATE: "",
            _COL_REPLY_RATE: "",
            _COL_INVOICE: "",
            _COL_VERIFY: "",
        }

        # 名称 (詳細ページの h1 が最も正確)
        h1 = soup.select_one("h1")
        if h1:
            out[Schema.NAME] = self._clean(h1.get_text(" ", strip=True))

        # ランク ('ランク 認定ランサー' → '認定ランサー')
        rank_el = soup.select_one(".p-profile-lancer-badge__text")
        if rank_el:
            rank = self._clean(rank_el.get_text(" ", strip=True))
            out[_COL_RANK] = re.sub(r"^ランク\s*", "", rank)

        # インボイス対応
        inv_el = soup.select_one(".c-badge-invoice__text")
        if inv_el:
            out[_COL_INVOICE] = self._clean(inv_el.get_text(strip=True))

        # 認証状況 (本人確認 / 機密保持確認 / 電話確認 / ランサーズチェック)
        verify = [
            self._clean(li.get_text(" ", strip=True))
            for li in soup.select(".p-profile-header__status-list .p-profile-header__status")
        ]
        out[_COL_VERIFY] = " / ".join(v for v in verify if v)

        # dt/dd 形式の構造化フィールド (前方一致ラベル)
        for dl in soup.select("dl"):
            for dt in dl.select("dt"):
                label = self._clean(dt.get_text(" ", strip=True))
                col = next((c for pre, c in _DETAIL_LABELS if label.startswith(pre)), None)
                if not col:
                    continue
                dd = dt.find_next_sibling("dd")
                if dd is not None:
                    out[col] = self._clean(dd.get_text(" ", strip=True))

        return out


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Lancers()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.lancers.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
