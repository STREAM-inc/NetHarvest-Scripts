"""
ビズリーチ — 掲載企業 企業情報スクレイパー

取得対象 (一覧=サイトマップ → 詳細ページで完結):
    - 企業名 / 詳細URL
    - 設立年月日 / 代表者名 / 資本金 / 従業員数 / 本社所在地 (都道府県分割)
    - 売上高 / 営業利益 / 平均年収 / 平均年齢 / 年間休日 / 上場区分 (EXTRA)

取得フロー:
    企業一覧ページや company インデックスは存在しない。
    robots.txt が公開しているサイトマップ `sitemap_company_view.txt`
    (プレーンテキスト, 1行1 URL, 約12,500社) を唯一の列挙ソースとして使用する。
    各企業の詳細ページ /company/view/{id}/ を 1 件取得するたびに即 yield する
    (Pattern B: 取得即 yield なので途中 break しても無駄な通信が起きない)。

    会社概要ブロック内の 【ラベル】値 形式から構造化フィールドのみを抽出する。
    「事業内容」「当社について」「中期経営計画」など自由記述の長文プロースは
    著作権リスク回避のため取得しない (Schema.LOB / DESCRIPTION も同様に除外)。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/bizreach_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bizreach_2
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 企業一覧の列挙ソース (robots.txt が公開しているプレーンテキストサイトマップ)。
# ルート URL から派生させる (urljoin) ため、別ルートのハードコードはしない。
_SITEMAP_PATH = "/sitemap_company_view.txt"

# 詳細ページが実在企業を持たない (削除済み等) 場合のプレースホルダ見出し
_PLACEHOLDER_NAME = "企業情報"

# 都道府県 (住所の先頭から都道府県を分割するため)
_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(_PREF)

# 会社概要内の 【ラベル】値 ペア抽出
_KV_RE = re.compile(r"【([^】]+)】([^【]*)")

# 【ラベル】→ Schema 定数 (構造化された短い値のみ)
_SCHEMA_LABELS = {
    "設立": Schema.OPEN_DATE,
    "設立年月日": Schema.OPEN_DATE,
    "創業": Schema.OPEN_DATE,
    "創立": Schema.OPEN_DATE,
    "代表者": Schema.REP_NM,
    "代表取締役": Schema.REP_NM,
    "代表": Schema.REP_NM,
    "資本金": Schema.CAP,
    "従業員数": Schema.EMP_NUM,
    "連結従業員数": Schema.EMP_NUM,
    "社員数": Schema.EMP_NUM,
    "本社所在地": Schema.ADDR,
    "所在地": Schema.ADDR,
    "本社": Schema.ADDR,
}

# 【ラベル】→ EXTRA カラム名 (短い数値・区分のみ。プロースは含めない)
_EXTRA_LABELS = {
    "売上高": "売上高",
    "営業利益": "営業利益",
    "平均年収": "平均年収",
    "社員平均年収": "平均年収",
    "平均年齢": "平均年齢",
    "年間休日": "年間休日",
    "上場": "上場区分",
    "上場区分": "上場区分",
}


def _clean(s) -> str:
    if s is None:
        return ""
    # 全角スペースは保持しつつ連続空白を整理
    return re.sub(r"[ \t　]+", " ", str(s).replace("\r", "")).strip()


class BizreachCompanyScraper(StaticCrawler):
    """ビズリーチ 掲載企業 企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["売上高", "営業利益", "平均年収", "平均年齢", "年間休日", "上場区分"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # ルート URL (= sites.yml の url) からサイトマップ URL を派生させる
        sitemap_url = urljoin(url, _SITEMAP_PATH)
        sitemap_soup = self.get_soup(sitemap_url)
        if sitemap_soup is None:
            self.logger.error("サイトマップ取得失敗: %s", sitemap_url)
            return

        # プレーンテキスト (1行1 URL)。get_soup の text からURLを抽出する
        raw = sitemap_soup.get_text("\n")
        detail_urls = [
            line.strip()
            for line in raw.splitlines()
            if re.search(r"/company/view/\d+/?$", line.strip())
        ]
        # 重複排除しつつ順序維持
        seen: set[str] = set()
        detail_urls = [u for u in detail_urls if not (u in seen or seen.add(u))]

        self.total_items = len(detail_urls)
        self.logger.info("企業詳細URL %d 件をサイトマップから取得", self.total_items)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception as e:  # 個別エラーはスキップして継続
                self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                continue
            if item:
                yield item

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        h1 = soup.find("h1")
        name = _clean(h1.get_text(" ", strip=True)) if h1 else ""
        # 実在企業を持たないプレースホルダはスキップ
        if not name or name == _PLACEHOLDER_NAME:
            return None

        item = {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.OPEN_DATE: "",
            Schema.REP_NM: "",
            Schema.CAP: "",
            Schema.EMP_NUM: "",
            Schema.PREF: "",
            Schema.ADDR: "",
        }
        for col in self.EXTRA_COLUMNS:
            item[col] = ""

        # 会社概要ブロックを特定
        heading = soup.find(
            ["h2", "h3"], string=lambda x: x and "会社概要" in x
        )
        container = heading.find_parent(["section", "div"]) if heading else None
        overview_text = container.get_text("\n", strip=True) if container else ""

        for m in _KV_RE.finditer(overview_text):
            label = m.group(1).strip()
            # 値は次の 【 まで。最初の行のみ・■セクション見出し以降は捨てて
            # 自由記述プロースの混入を防ぐ
            value = _clean(m.group(2).split("\n")[0].split("■")[0])
            if not value:
                continue
            if label in _SCHEMA_LABELS:
                key = _SCHEMA_LABELS[label]
                if not item.get(key):
                    item[key] = value
            elif label in _EXTRA_LABELS:
                col = _EXTRA_LABELS[label]
                if not item.get(col):
                    item[col] = value

        # 住所から都道府県を分割
        addr = item.get(Schema.ADDR, "")
        if addr:
            pm = _PREF_RE.search(addr)
            if pm:
                item[Schema.PREF] = pm.group(0)
                item[Schema.ADDR] = addr[pm.start():].strip()

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BizreachCompanyScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.bizreach.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
