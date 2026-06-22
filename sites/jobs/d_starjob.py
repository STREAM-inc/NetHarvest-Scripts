"""
ディースターNET (d-starjob.com) — 求人情報スクレイパー (StaticCrawler 版)

取得対象:
    各求人案件の構造化情報
    (社名・勤務地都道府県・会社所在地・電話番号・休日・事業内容 ＋
     エリア区分・雇用形態・職種・給与・勤務時間・勤務地・求人の特徴・
     事業許可番号・掲載期間・求人ID)

取得フロー (一覧 → 詳細, Pattern B = 詳細1件ごとに即 yield):
    1. このサイトには全件一覧が無く「地方ごと」に選択して検索する必要がある。
       一覧は /list.html?district=N で取得する。
         - district=1 : 西日本 (大阪・兵庫・滋賀 …)
         - district=2 : 東日本 (神奈川・茨城・栃木 …)
       両 district の oid は重複しないため、両方を巡回することで全国を網羅する。
    2. 各 district を ?p=1,2,3,… でページ送り (20件/ページ)。
       div.result_list が 0 件になったページで当該 district の巡回を終了する。
    3. 各 result_list から詳細URL (/detail/index/oid/{oid}.html) を取り出し、
       詳細ページを取得 → 1件パースした直後にその場で yield する
       (全件バッファせず早期 yield。テスト実行のタイムアウト/504 を回避)。
    4. 詳細ページの <table> 内 <th>ラベル</th><td>値</td> を全テーブル横断で
       ラベル→値の辞書にまとめ、最初に現れた非空値を採用してマッピングする。

★ StaticCrawler を使う理由:
    完全サーバーサイドレンダリングで、一覧・詳細とも初期 HTML に全データが含まれる。
    requests (実ブラウザ風 UA) で 200 取得可能、JS レンダリング不要。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/d_starjob.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id d_starjob
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

# 地方選択 (このサイトは全件一覧が無く district 単位で検索する)。
# 西日本 / 東日本の両方を巡回して全国を網羅する。
_DISTRICTS = {
    1: "西日本",
    2: "東日本",
}

# 異常時 (result_list が永遠に空にならない) の無限ループ保険。
# 全国 約 38,732 件 (20件/ページ) ≒ 約 1,937 ページ。district ごとに上限を設ける。
_MAX_PAGES = 3000

# 都道府県抽出パターン (勤務地テキストの先頭から)
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 電話番号らしき先頭の番号のみを取り出す (後続の案内文を捨てる)。
# TEL の全角→半角正規化は Pipeline が自動処理するため、ここでは抽出のみ行う。
_TEL_PATTERN = re.compile(r"(0\d{1,3}[-\(]?\d{1,4}[-\)]?\d{2,4})")


class DStarJobScraper(StaticCrawler):
    """ディースターNET 求人スクレイパー (d-starjob.com)"""

    DELAY = 1.5
    # Schema に無いサイト固有カラム。いずれも構造化された短いラベル/タグ/コード/日付であり、
    # 自由記述プロース (仕事内容・仕事の特徴・待遇・備考 等) は著作権リスクのため含めない。
    EXTRA_COLUMNS = [
        "エリア区分",
        "雇用形態",
        "職種",
        "給与",
        "勤務時間",
        "勤務地",
        "求人の特徴",
        "事業許可番号",
        "掲載期間",
        "求人ID",
    ]

    def parse(self, url: str):
        # url は sites.yml の正規ルート (https://d-starjob.com/)。
        # 一覧・詳細 URL はすべてこの url から派生させる (別 URL をハードコードしない)。
        list_base = urljoin(url, "list.html")

        # 進捗表示用に全国総件数を初期推定としてセット (取得できなければ未設定のまま)。
        self.total_items = self._fetch_total(f"{list_base}?district=1")

        seen_oids: set[str] = set()

        for district, area_label in _DISTRICTS.items():
            page = 1
            while page <= _MAX_PAGES:
                list_url = f"{list_base}?district={district}&p={page}"
                soup = self.get_soup(list_url)
                if soup is None:
                    break

                cards = soup.select("div.result_list")
                if not cards:
                    # 当該 district はこれ以上ページが無い
                    break

                for card in cards:
                    link = card.select_one('a[href*="/detail/index/oid/"]')
                    if not link or not link.get("href"):
                        continue
                    detail_url = urljoin(url, link["href"])

                    m = re.search(r"/oid/(\d+)", detail_url)
                    oid = m.group(1) if m else detail_url
                    if oid in seen_oids:
                        continue
                    seen_oids.add(oid)

                    try:
                        item = self._scrape_detail(detail_url, area_label)
                    except Exception as e:  # 個別案件の失敗は握りつぶして継続
                        self.logger.warning("詳細パース失敗 (スキップ): %s — %s", detail_url, e)
                        continue

                    if item:
                        # 詳細1件取得ごとに即 yield (全件バッファしない)
                        yield item

                page += 1

    def _fetch_total(self, list_url: str) -> int | None:
        """一覧ページの総件数表示 (span.salmon_pink) を進捗推定用に取得する。"""
        try:
            soup = self.get_soup(list_url)
            if soup is None:
                return None
            el = soup.select_one("span.salmon_pink")
            if el:
                digits = re.sub(r"[^\d]", "", el.get_text())
                if digits:
                    return int(digits)
        except Exception:
            pass
        return None

    def _scrape_detail(self, detail_url: str, area_label: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        # 全テーブル横断で <th>ラベル</th><td>値</td> を辞書化 (最初の非空値を採用)。
        labels: dict[str, str] = {}
        for tr in soup.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = td.get_text(" ", strip=True)
            if key and val and not labels.get(key):
                labels[key] = val

        # 必須フィールド (社名) が無ければ無効ページとして除外
        name = labels.get("社名", "")
        if not name:
            return None

        work_place = self._clean_workplace(labels.get("勤務地", ""))
        pref = self._extract_pref(work_place or labels.get("所在地", ""))

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: labels.get("所在地", ""),
            Schema.TEL: self._extract_tel(labels.get("電話番号", "")),
            Schema.HOLIDAY: labels.get("休日", ""),
            Schema.LOB: labels.get("事業内容", ""),
            # --- EXTRA ---
            "エリア区分": area_label,
            "雇用形態": labels.get("雇用形態", ""),
            "職種": labels.get("職種", ""),
            "給与": labels.get("給与", ""),
            "勤務時間": labels.get("勤務時間", ""),
            "勤務地": work_place,
            "求人の特徴": labels.get("求人の特徴", ""),
            "事業許可番号": labels.get("事業許可番号", ""),
            "掲載期間": labels.get("掲載期間", ""),
            "求人ID": labels.get("求人ID", ""),
        }
        return item

    @staticmethod
    def _clean_workplace(text: str) -> str:
        """勤務地テキストから先頭の所在地表現のみ抽出し、後続の自由記述
        (【受動喫煙措置について】【募集情報】等) を除去する。"""
        if not text:
            return ""
        # 最初の "【" 以降 (受動喫煙措置・募集情報などのプロース) を切り落とす
        head = text.split("【", 1)[0].strip()
        return head

    @staticmethod
    def _extract_pref(text: str) -> str:
        if not text:
            return ""
        m = _PREF_PATTERN.search(text)
        return m.group(1) if m else ""

    @staticmethod
    def _extract_tel(text: str) -> str:
        if not text:
            return ""
        m = _TEL_PATTERN.search(text)
        return m.group(1) if m else ""


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = DStarJobScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://d-starjob.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
