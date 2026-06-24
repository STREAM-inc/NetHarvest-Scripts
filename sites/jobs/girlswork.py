"""
がーるずわーく (girlswork) — ガールズバー&コンカフェ求人情報ポータル

取得対象:
    - 求人検索 (/job-search/) に掲載された店舗求人の詳細情報

取得フロー:
    1. /job-search/ (引数 url から派生) を 1 ページ目として取得
    2. 各ページの .single_job から詳細ページ (/job/{id}/) のリンクを抽出
    3. 詳細ページを 1 件取得するごとに即 yield (Pattern B)
    4. /job-search/page/{n}/ で次ページへ。アイテムが無くなったら終了

メモ:
    - WordPress 製の静的 HTML サイト (StaticCrawler)。
    - 詳細ページ (/job/{id}/) は Referer ヘッダが無いと 403 を返すため、
      prepare() で同一オリジンの Referer をセッションに付与する。
    - 一覧/詳細の店舗紹介文・キャッチコピー・応募方法は自由記述の長文のため
      著作権リスクを避けて取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/girlswork.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id girlswork
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

# 都道府県の先頭マッチ
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_PATTERN = re.compile(r"0\d{1,3}[-(]?\d{1,4}[-)]?\d{3,4}")
_LINE_PATTERN = re.compile(r"@[A-Za-z0-9_.\-]{2,}")


class GirlsWork(StaticCrawler):
    """がーるずわーく スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "年齢層",
        "給与",
        "勤務時間",
        "最寄駅",
        "職種",
        "待遇",
        "担当",
        "雰囲気",
    ]

    def prepare(self):
        # 詳細ページ (/job/{id}/) は Referer が無いと 403 になるため付与する
        if self.session is not None:
            self.session.headers.update({"Referer": "https://girlswork.jp/"})

    def parse(self, url: str):
        # 引数 url を唯一のルートとし、求人検索の一覧 URL を派生させる
        list_base = urljoin(url, "/job-search/")

        page = 1
        while True:
            page_url = list_base if page == 1 else urljoin(url, f"/job-search/page/{page}/")
            soup = self.get_soup(page_url)
            if soup is None:
                break

            blocks = soup.select(".single_job")
            if not blocks:
                break

            # 1 ページ目で総ページ数から件数を概算 (進捗表示用)
            if page == 1:
                pages = [int(m) for m in re.findall(r"/job-search/page/(\d+)/", str(soup))]
                max_page = max(pages) if pages else 1
                self.total_items = max_page * len(blocks)

            detail_urls = []
            seen = set()
            for block in blocks:
                a = block.select_one('a[href*="/job/"]')
                if not a or not a.get("href"):
                    continue
                href = urljoin(url, a["href"])
                if href in seen:
                    continue
                seen.add(href)
                detail_urls.append(href)

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細ページの取得に失敗: %s — %s", detail_url, e)
                    continue

            page += 1

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        # .single_job_tbl 内の <h3>ラベル</h3><p|div>値</p|div> を収集
        fields = {}
        for tbl in soup.select(".single_job_tbl"):
            for h3 in tbl.find_all("h3"):
                label = h3.get_text(strip=True)
                if not label or label in fields:
                    continue
                sib = h3.find_next_sibling(["p", "div"])
                if sib is not None:
                    fields[label] = sib

        def text(label: str) -> str:
            el = fields.get(label)
            if el is None:
                return ""
            return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()

        # 名称: 店舗名 → なければページ上部の h2
        name = text("店舗名")
        if not name:
            h2 = soup.select_one(".single_job_top h2")
            name = h2.get_text(strip=True) if h2 else ""

        # 住所 / 郵便番号 / 都道府県
        post_code = pref = addr = ""
        addr_el = fields.get("住所")
        if addr_el is not None:
            for a in addr_el.select("a"):  # GOOGLE MAP リンクを除去
                a.decompose()
            raw = addr_el.get_text("\n", strip=True)
            m = _POST_PATTERN.search(raw)
            if m:
                post_code = m.group(1)
            lines = [ln.strip() for ln in raw.split("\n") if ln.strip()]
            addr_lines = [ln for ln in lines if not re.fullmatch(r"〒?\s*\d{3}-?\d{4}", ln)]
            full_addr = " ".join(addr_lines)
            full_addr = re.sub(r"^〒?\s*\d{3}-?\d{4}\s*", "", full_addr).strip()
            pm = _PREF_PATTERN.match(full_addr)
            if pm:
                pref = pm.group(1)
                addr = full_addr[pm.end():].strip()
            else:
                addr = full_addr

        # 電話番号 (LINE ID が同居することがあるので分離)
        tel_raw = text("電話番号")
        tel_m = _TEL_PATTERN.search(tel_raw)
        tel = tel_m.group(0) if tel_m else ""

        # LINE アカウント (電話番号欄・担当欄に @id が載ることがある)
        line = ""
        for src in (tel_raw, text("応募方法")):
            lm = _LINE_PATTERN.search(src)
            if lm:
                line = lm.group(0)
                break

        item = {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HOLIDAY: text("休日"),
            Schema.CAT_SITE: text("業種"),
            Schema.LINE: line,
            # --- EXTRA_COLUMNS ---
            "エリア": text("エリア"),
            "年齢層": text("年齢層"),
            "給与": text("給与"),
            "勤務時間": text("勤務時間"),
            "最寄駅": text("最寄駅"),
            "職種": text("職種"),
            "待遇": text("待遇"),
            "担当": text("担当"),
            "雰囲気": text("雰囲気"),
        }
        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = GirlsWork()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://girlswork.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
