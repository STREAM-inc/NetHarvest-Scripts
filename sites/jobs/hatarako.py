"""
はたらこねっと (hatarako.net) — 派遣・求人情報スクレイパー

取得対象:
    - エリア別求人一覧（例: 東京都 https://www.hatarako.net/tokyo/）に掲載される
      各求人（1求人=1行）。派遣会社/掲載企業・勤務先名・勤務地・職種・雇用形態・
      給与・勤務期間・勤務時間・休日を求人詳細ページから収集する。
      派遣会社（詳細ヘッダの「派遣会社：…」）と勤務先名（「勤務先の情報」の
      「勤務先：…」）は、露出している求人のみ値が入る。

取得フロー:
    一覧ページ (…/page{N}/) の article ブロックから求人詳細URL(/job/{id}/)を取得
    → 各詳細ページを開き即 yield（Pattern B: list → detail、取得即 yield）
    → 次ページへ。求人が無くなる or 404 で終了。

備考の反映:
    呼び出し時の備考は詳細リンク1件のサンプル項目（職種/給与/勤務期間/勤務時間/
    休日・休暇）を列挙したもので、明示的な絞り込み条件（エリア/期間フィルタ等）は
    無い。よって parse() 側の追加フィルタは実装しない。起点 url が /tokyo/ である
    ため対象は東京都に限定される。
    仕事内容・応募資格・会社メッセージ・福利厚生などの長文自由記述（プロース）は
    著作権リスク回避のため取得対象から除外する。

実行方法:
    python scripts/sites/jobs/hatarako.py
    docker compose exec worker python /app/bin/run_flow.py --site-id hatarako
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 暴走防止のセーフティ上限（東京都は約2万件≒1,100ページ規模）。
MAX_PAGES = 3000
# 会社名らしさの判定に使う法人格キーワード。
_CO_KEYWORD = re.compile(
    r"(株式会社|有限会社|合同会社|合資会社|合名会社|一般社団|一般財団|"
    r"公益社団|公益財団|医療法人|社会福祉法人|学校法人|協同組合|組合|"
    r"会社|法人|Inc|Corp|Co\.|LLC)"
)
_JOB_ID_RE = re.compile(r"/job/(\d+)/")
# 求人詳細タイトル末尾の「の{雇用形態}の求人・募集情報」から雇用形態を抜く。
# 直前セグメントに「の」を含まないため [^の] で確実に雇用形態のみを掴む。
_EMPLOY_RE = re.compile(r"の([^の]+?)の求人・募集情報")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class HatarakoScraper(StaticCrawler):
    """はたらこねっと 求人スクレイパー（hatarako.net）"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "雇用形態", "給与", "勤務期間", "業界", "募集No", "派遣会社", "勤務先名",
    ]

    # ------------------------------------------------------------------ #
    # 一覧巡回（Pattern B: 詳細を取得即 yield）
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        # URL一貫性ルール: 引数 url を唯一のルート（起点）とし、ページURLを派生。
        seen: set[str] = set()
        page = 1
        while page <= MAX_PAGES:
            list_url = self._page_url(url, page)
            soup = self.get_soup(list_url)
            if soup is None:
                break
            detail_urls = self._list_detail_urls(soup, list_url)
            if not detail_urls:
                break  # このページに求人が無い → 巡回終了
            for detail_url in detail_urls:
                if detail_url in seen:
                    continue  # 別ページで既出（注目枠など）は重複排除
                seen.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue
                if item and item.get(Schema.NAME):
                    yield item
            page += 1

    @staticmethod
    def _page_url(url: str, page: int) -> str:
        """起点 url からページ url を派生する（…/{area}/page{N}/）。"""
        parts = urlsplit(url)
        base_path = parts.path.rstrip("/")  # 例: /tokyo
        path = f"{base_path}/" if page <= 1 else f"{base_path}/page{page}/"
        return urlunsplit((parts.scheme, parts.netloc, path, parts.query, ""))

    @staticmethod
    def _list_detail_urls(soup, list_url: str) -> list[str]:
        """一覧ページの主要求人（article ブロック）から詳細URLを抽出する。

        サイドバーの「おすすめ」求人を拾わないよう、メインの求人カード
        (article) 内の /job/{id}/ リンクのみを対象にする。
        """
        urls: list[str] = []
        seen: set[str] = set()
        for art in soup.select("article"):
            a = art.select_one('a[href*="/job/"]')
            if not a:
                continue
            m = _JOB_ID_RE.search(a.get("href", ""))
            if not m:
                continue
            detail = urljoin(list_url, f"/job/{m.group(1)}/")
            if detail not in seen:
                seen.add(detail)
                urls.append(detail)
        return urls

    # ------------------------------------------------------------------ #
    # 求人詳細ページから情報を抽出
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 募集No（求人ID）
        m = _JOB_ID_RE.search(url)
        if m:
            data["募集No"] = m.group(1)

        # パンくず: [TOP, 都道府県, 市区町村, 大業種, 職種, (会社名)]
        crumbs = [
            _clean(a.get_text()) for a in soup.select(".common-breadcrumb a")
        ]
        crumbs = [c for c in crumbs if c]

        title = _clean(soup.title.get_text()) if soup.title else ""
        title_head = title.split("｜")[0]

        # 会社名（派遣会社/掲載企業）
        data[Schema.NAME] = self._company_name(crumbs, title_head)

        # 派遣会社（詳細ヘッダの「派遣会社：{会社名 支店}」）。ページにより
        # span.span01 / span.span02 のいずれかに入る。派遣求人のみ露出し、
        # 直接雇用求人は「求人会社：…」となるため空。
        for sp in soup.select("span.span01, span.span02"):
            t = _clean(sp.get_text(" "))
            if t.startswith("派遣会社："):
                haken = t[len("派遣会社："):].strip()
                if haken:
                    data["派遣会社"] = haken
                break

        # 勤務先名（「勤務先の情報」th「勤務先：{勤務先名}」内の span.span01）。
        # 勤務先公開の求人のみ露出（非公開なら空）。
        for th in soup.find_all("th"):
            t = th.get_text(strip=True)
            if t.startswith("勤務先："):
                sp = th.select_one("span.span01")
                val = _clean(sp.get_text()) if sp else t[len("勤務先："):].strip()
                if val:
                    data["勤務先名"] = val
                break

        # 雇用形態（タイトル末尾「の{雇用形態}の求人・募集情報」）
        em = _EMPLOY_RE.search(title_head)
        if em:
            data["雇用形態"] = em.group(1).strip()

        # 求人サマリ（dl.dl01 の dt/dd）
        summary = {}
        for dl in soup.select("dl.dl01"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if dt and dd:
                summary[dt.get_text(strip=True)] = _clean(dd.get_text(" "))

        # 勤務地: 「東京都 / 渋谷区」→ PREF / ADDR
        work = summary.get("勤務地", "")
        if work:
            segs = [s.strip() for s in re.split(r"[／/]", work) if s.strip()]
            if segs:
                data[Schema.PREF] = segs[0]
                data[Schema.ADDR] = " ".join(segs[1:])

        # 職種・業界: 仕事内容 dd「職種： X 業界： Y」から構造化部分のみ抽出
        job_kind = summary.get("仕事内容", "")
        jm = re.search(r"職種[：:]\s*(.+?)(?:\s*業界[：:]|$)", job_kind)
        if jm:
            data[Schema.CAT_SITE] = jm.group(1).strip()
        gm = re.search(r"業界[：:]\s*(.+)$", job_kind)
        if gm:
            data["業界"] = gm.group(1).strip()

        # 給与・勤務期間・勤務時間・休日（備考で明示された構造化フィールド）
        if summary.get("給与"):
            data["給与"] = summary["給与"]
        if summary.get("勤務期間"):
            data["勤務期間"] = summary["勤務期間"]
        if summary.get("勤務時間"):
            data[Schema.TIME] = summary["勤務時間"]
        if summary.get("休日・休暇"):
            data[Schema.HOLIDAY] = summary["休日・休暇"]

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _company_name(crumbs: list[str], title_head: str) -> str:
        """会社名を決定する。

        パンくずが 6 段以上（TOP/都道府県/市区町村/大業種/職種/会社名）なら
        末尾を会社名として採用（例: ヒューマンリソシア株式会社（行政・自治体））。
        そうでない場合はタイトル先頭「{会社名}（{求人名}）の…」の先頭括弧まで。
        """
        if len(crumbs) >= 6 and _CO_KEYWORD.search(crumbs[-1]):
            return crumbs[-1]
        # タイトル先頭の会社名（最初の全角/半角括弧の手前まで）
        name = re.split(r"[（(]", title_head, maxsplit=1)[0].strip()
        if name:
            return name
        return crumbs[-1] if crumbs else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HatarakoScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.hatarako.net/tokyo/?prelink=alltopip")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
