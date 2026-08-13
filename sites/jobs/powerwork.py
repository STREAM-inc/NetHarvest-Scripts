"""
パワーワーク (POWER WORK) — 建設・運送・警備・資源循環などの求人情報サイト (powerwork.jp)

取得対象:
    - 全国の市区町村別求人一覧 (/zenkoku/DC{5桁市区町村コード}) に掲載された求人票。
      掲載企業名・応募用TEL・住所(営業所/面接地)・担当者名・事業内容・募集職種・
      更新日・求人URL を取得する。

取得フロー:
    1. 引数 url (起点の市区町村ページ) をまず巡回し、詳細を1件取得するごとに即 yield する。
    2. 続いて全国インデックス (/zenkoku) から全市区町村ページのリンクを収集し、
       起点以外を順に巡回する。
    3. 一覧は 30件/ページで ?page=N のページ送り。「次のページ」リンクが無ければ終了。
       掲載0件の市区町村は求人リンクが無いのでスキップして次へ進む。
    4. 同一求人が複数の市区町村ページに掲載されるため、求人ID (/kyujin/{ID}) で重複除去する。

    名称 (Schema.NAME) は詳細ページ企業情報テーブルの「掲載企業名（広告主）」を採用する。
    長文の自由記述 (仕事内容・その他・待遇・応募資格・応募の流れ本文・キャッチコピー) は
    著作権リスクのため取得しない。担当者名のみ「応募の流れ」欄から氏名部分を抽出する。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/powerwork.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id powerwork
"""

import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_JOB_ID_RE = re.compile(r"/kyujin/(\d+)")
_AREA_HREF_RE = re.compile(r"/zenkoku/DC\d+")
_DATE_RE = re.compile(r"(\d{4})[/年](\d{1,2})[/月](\d{1,2})")
# 「応募の流れ」欄の担当者表記: 「担当/大石」「担当者：山田」「ご担当 佐藤」など
_STAFF_RE = re.compile(r"担当者?\s*[/／:：]?\s*([^\s、。（(/／]{1,12})")
_MAX_PAGE = 300  # 無限ループ防止の上限 (1エリア最大 300ページ)

# 詳細ページ table.mod-table1 の見出し → EXTRA カラム名
_EXTRA_LABELS = {
    "雇用形態": "雇用形態",
    "給与": "給与",
    "勤務地域": "勤務地域",
    "管理番号": "管理番号",
}


def _clean(text: str) -> str:
    """空白・改行を1スペースに正規化する。"""
    return re.sub(r"\s+", " ", text or "").strip()


class PowerWorkCrawler(StaticCrawler):
    """パワーワーク スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "募集職種",
        "掲載日",
        "求人ID",
        "担当者名",
        "電話番号2",
        "雇用形態",
        "給与",
        "勤務地域",
        "管理番号",
    ]

    def parse(self, url: str):
        seen_ids: set[str] = set()

        # 1. 起点の市区町村ページ (sites.yml の url) をまず巡回する
        yield from self._crawl_area(url, seen_ids)

        # 2. 全国インデックスから全市区町村ページを収集して巡回する
        index_url = urllib.parse.urljoin(url, "/zenkoku")
        index_soup = self.get_soup(index_url)
        if index_soup is None:
            return

        start = url.split("?")[0].rstrip("/")
        area_urls: list[str] = []
        for a in index_soup.select('a[href*="/zenkoku/DC"]'):
            href = a.get("href") or ""
            if not _AREA_HREF_RE.search(href):
                continue
            area_url = urllib.parse.urljoin(index_url, href).split("?")[0].rstrip("/")
            if area_url == start or area_url in area_urls:
                continue
            area_urls.append(area_url)

        self.logger.info("全国インデックスから %d 件の市区町村ページを取得", len(area_urls))

        for area_url in area_urls:
            yield from self._crawl_area(area_url, seen_ids)

    def _crawl_area(self, area_url: str, seen_ids: set[str]):
        """1市区町村ページを全ページ巡回し、未取得の求人詳細を1件ずつ yield する。"""
        base = area_url.split("?")[0]
        page = 1

        while page <= _MAX_PAGE:
            page_url = base if page == 1 else f"{base}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                return

            job_ids: list[str] = []
            for a in soup.select('.mod-jobResultBox a[href*="/kyujin/"]'):
                m = _JOB_ID_RE.search(a.get("href") or "")
                if m and m.group(1) not in job_ids:
                    job_ids.append(m.group(1))

            # 掲載0件の市区町村はここで終了 (スキップして次のエリアへ)
            if not job_ids:
                return

            for job_id in job_ids:
                if job_id in seen_ids:
                    continue
                seen_ids.add(job_id)
                detail_url = urllib.parse.urljoin(page_url, f"/kyujin/{job_id}")
                try:
                    item = self._scrape_detail(detail_url, job_id)
                except Exception as e:  # noqa: BLE001 — 1件の失敗で全体を止めない
                    self.logger.warning("詳細取得に失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            # 「次のページ」リンクが無ければ最終ページ
            has_next = any(
                "次のページ" in _clean(a.get_text())
                for a in soup.select(".mod-pagination-wrap a[href]")
            )
            if not has_next:
                return
            page += 1

    def _scrape_detail(self, detail_url: str, job_id: str) -> dict | None:
        """求人詳細ページから1件分のデータを取得する。"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        # 求人情報テーブル + 企業情報テーブルの th/td を1つの辞書にまとめる
        fields: dict[str, str] = {}
        for tr in soup.select("table.mod-table1 tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = _clean(th.get_text())
            if label and label not in fields:
                fields[label] = _clean(td.get_text(" "))

        # 名称: 「掲載企業名（広告主）」 / 無ければ h1 の「企業名｜職種」前半
        h1 = soup.select_one("h1")
        h1_text = _clean(h1.get_text(" ")) if h1 else ""
        h1_parts = [p.strip() for p in h1_text.split("｜")] if h1_text else []
        company = ""
        for label, value in fields.items():
            if label.startswith("掲載企業名"):
                company = value
                break
        if not company and h1_parts:
            company = h1_parts[0]
        if not company:
            return None
        # 「株式会社　起工業」のような全角スペース区切りを詰める
        company = company.replace("　", " ").strip()

        # 募集職種: h1 の「企業名｜職種」後半 / 無ければ title のフォールバック
        job_title = h1_parts[1] if len(h1_parts) > 1 else ""
        if not job_title and soup.title:
            t = _clean(soup.title.get_text()).split("｜")
            job_title = t[1] if len(t) > 1 else ""
        job_title = re.sub(r"（ID：\d+）$", "", job_title).strip()

        # 住所 (営業所/面接地) → 都道府県を分離
        address = fields.get("住所", "")
        pref = ""
        m = _PREF_PATTERN.match(address)
        if m:
            pref = m.group(1)

        # 応募用TEL: 電話応募モーダルの tel: リンク (電話番号1 / 電話番号2)
        tels: list[str] = []
        for a in soup.select('a[href^="tel:"]'):
            text = _clean(a.get_text())
            num = text if re.fullmatch(r"[\d\-()+ ]+", text or "") else (a.get("href") or "")[4:]
            num = num.strip()
            if num and num not in tels:
                tels.append(num)

        # 担当者名: 「応募の流れ」欄に記載がある場合のみ
        staff = ""
        flow = fields.get("応募の流れ", "")
        if flow:
            sm = _STAFF_RE.search(flow)
            if sm:
                candidate = sm.group(1).strip()
                # 「者」「制」等の助詞・単独記号を除外
                if candidate and not re.fullmatch(r"[はがのをにでとやも者]+", candidate):
                    staff = candidate

        # 掲載日 (更新日): 「2026/04/02 （…133日経過…）」から日付のみ抽出
        update_raw = fields.get("更新日", "")
        posted = ""
        dm = _DATE_RE.search(update_raw)
        if dm:
            posted = f"{dm.group(1)}/{int(dm.group(2)):02d}/{int(dm.group(3)):02d}"

        item = {
            Schema.NAME: company,
            Schema.URL: detail_url,
            Schema.TEL: tels[0] if tels else "",
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.LOB: fields.get("事業内容", ""),
            Schema.HP: fields.get("HPアドレス", ""),
            Schema.TIME: fields.get("勤務時間", ""),
            Schema.HOLIDAY: fields.get("休日", ""),
            "募集職種": job_title,
            "掲載日": posted,
            "求人ID": job_id,
            "担当者名": staff,
            "電話番号2": tels[1] if len(tels) > 1 else "",
        }
        for label, column in _EXTRA_LABELS.items():
            item[column] = fields.get(label, "")

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PowerWorkCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://powerwork.jp/zenkoku/DC13101")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
