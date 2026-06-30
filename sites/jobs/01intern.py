"""
ゼロワンインターン (01intern.com) — 長期インターン求人を掲載する企業情報

取得対象:
    - 求人一覧 (job/list) に掲載されている募集企業の企業情報を「企業単位」で取得する。
      同一企業が複数求人を出しているため、企業ID (corporation id) で重複排除し、
      1企業1レコードとして出力する。

取得フロー:
    一覧ページ (job/list.html?...&page=N) を巡回
      → 各求人カード (section.i-job-item) の画像 URL から企業ID を抽出して重複排除
      → 企業ページ (/corporation/{id}) から企業情報 (社名・業種・代表者・設立・
        従業員数・所在地・企業HP) を取得
      → 代表求人の詳細ページ (/job/{id}.html) から連絡先 (TEL)・電話受付時間 を補完
      → 1企業ごとに即 yield (Pattern B)

備考:
    - 連絡先 (TEL) は求人詳細ページにのみ掲載され、出現率は約 2/3。
    - 企業HP は企業ページにのみ掲載 (出現率は企業による)。
    - 任せたい仕事/給与/応募資格/選考フロー 等の自由記述プロースは著作権リスクのため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/01intern.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id 01intern
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
# 求人カードの画像 URL に企業ID が埋まっている (例: .../corporaions/2731/xxx.jpg)
_CORP_ID_RE = re.compile(r"corpora\w+?/(\d+)")


class ZeroOneInternCrawler(StaticCrawler):
    """ゼロワンインターン 募集企業スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["電話受付時間"]

    def parse(self, url: str):
        seen: set[str] = set()
        page = 1
        while True:
            page_url = url if page == 1 else f"{url}&page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break
            cards = soup.select("section.i-job-item")
            if not cards:
                break

            for card in cards:
                try:
                    m = _CORP_ID_RE.search(str(card))
                    corp_id = m.group(1) if m else None

                    job_link = card.select_one('a[href*="/job/"]')
                    job_href = (
                        urllib.parse.urljoin(url, job_link["href"])
                        if job_link and job_link.get("href")
                        else ""
                    )

                    # 企業IDが画像から取れない場合は求人詳細から取得する (フォールバック)
                    job_soup = None
                    if corp_id is None and job_href:
                        job_soup = self.get_soup(job_href)
                        if job_soup is not None:
                            corp_a = job_soup.select_one('a[href*="/corporation/"]')
                            if corp_a and corp_a.get("href"):
                                cm = re.search(r"/corporation/(\d+)", corp_a["href"])
                                corp_id = cm.group(1) if cm else None

                    if not corp_id or corp_id in seen:
                        continue
                    seen.add(corp_id)

                    record = self._scrape_company(corp_id, job_href, url, job_soup)
                    if record:
                        yield record
                except Exception as e:
                    self.logger.warning(f"page {page}: card skip — {e}")
                    continue

            page += 1

    def _scrape_company(
        self, corp_id: str, job_href: str, root_url: str, job_soup=None
    ) -> dict | None:
        """企業ページ + 代表求人詳細から企業情報レコードを構築する。"""
        corp_url = urllib.parse.urljoin(root_url, f"/corporation/{corp_id}")
        corp_soup = self.get_soup(corp_url)
        if corp_soup is None:
            return None

        kv = self._dt_dd_map(corp_soup)
        name = kv.get("社名", "")
        if not name:
            return None  # NAME は必須

        # 所在地 → 都道府県 + 住所
        addr_raw = kv.get("所在地", "")
        pref = ""
        addr = addr_raw
        pm = _PREF_PATTERN.match(addr_raw)
        if pm:
            pref = pm.group(1)
            addr = addr_raw[pm.end():].strip()

        # 企業HP は dd 内のリンク href を優先
        hp = ""
        for dt in corp_soup.find_all("dt"):
            if dt.get_text(strip=True) == "企業ホームページ":
                dd = dt.find_next_sibling("dd")
                if dd:
                    a = dd.find("a", href=True)
                    hp = a["href"] if a else dd.get_text(strip=True)
                break

        # TEL・電話受付時間 は求人詳細ページから補完
        tel = ""
        reception = ""
        if job_soup is None and job_href:
            job_soup = self.get_soup(job_href)
        if job_soup is not None:
            jkv = self._dt_dd_map(job_soup)
            tel = jkv.get("連絡先", "")
            reception = jkv.get("電話受付時間", "")

        return {
            Schema.URL: corp_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: kv.get("代表者", ""),
            Schema.EMP_NUM: kv.get("従業員数", ""),
            Schema.OPEN_DATE: kv.get("設立", ""),
            Schema.CAT_SITE: kv.get("業種", ""),
            Schema.HP: hp,
            "電話受付時間": reception,
        }

    @staticmethod
    def _dt_dd_map(soup) -> dict:
        """dt → 直後の dd のテキストを辞書化する (ラベルは一意な前提)。"""
        result: dict[str, str] = {}
        for dt in soup.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if dd is None:
                continue
            label = dt.get_text(strip=True)
            if label and label not in result:
                result[label] = dd.get_text(" ", strip=True)
        return result


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ZeroOneInternCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute(
        "https://01intern.com/job/list.html?areas=999999999&areas=13&areas=14&areas=12&areas=11&areas=27&areas=26&areas=28&areas=8&areas=1&areas=3&areas=4&areas=10&areas=9&areas=19&areas=15&areas=20&areas=16&areas=17&areas=21&areas=22&areas=23&areas=24&areas=25&areas=29&areas=30&areas=32&areas=33&areas=34&areas=35&areas=37&areas=38&areas=39&areas=40&areas=41&areas=42&areas=43&areas=44&areas=46&areas=47&areas=48"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
