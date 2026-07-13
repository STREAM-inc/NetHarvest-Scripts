"""
一般社団法人 全国警備業協会（AJSSA）会員名簿 — 全国加盟業者検索

取得対象:
    - 全国警備業協会 (kameiin.ajssa.or.jp) の加盟業者（全都道府県 + その他・全件）
    - 会員名 / 都道府県 / 住所 / 電話番号 / 業種（警備業務種別）

取得フロー:
    Rails (ransack) 製の検索サイト。検索結果は table.table-striped に列挙される。
      列: 会員名 / 都道府県 / 住所 / 業種(単字コード) / 電話番号
    都道府県は q[prefecture_id_eq] で絞り込む。ルート URL の placeholder
    (選んでください) では絞り込まれないため、prefecture_id を 1〜48
    (47都道府県 + 48=その他) と順に切り替えて全国分を巡回する。
    各都道府県はさらに ?page=N (1ページ 100件) でページ送りされるので、
    データ行が無くなるまでページを進める。
    行を 1 件ずつ即 yield する (Pattern B)。

    ※ ルート URL は引数 url を唯一の起点とし、prefecture_id / page のみ
      差し替えて派生させる (SSOT = sites.yml)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_33.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_33
"""

import logging
import sys
from pathlib import Path
from urllib.parse import urlencode, urlparse, urlunparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 業種セル内の単字コード → 警備業務種別 (検索フォームの option ラベルに準拠)
_BIZ = {
    "施": "施設警備",
    "交": "交通誘導警備",
    "貴": "貴重品運搬警備",
    "身": "身辺警備",
    "機": "機械警備",
    "核": "核燃警備",
    "空": "空港警備",
    "ホ": "ホームセキュリティ",
    "保": "保安警備",
}

# prefecture_id: 1〜47=都道府県 (1=北海道 … 47=沖縄県), 48=その他。
# フォームの <select name="q[prefecture_id_eq]"> は placeholder を除き 1〜48 の
# 48 option を持つ (実サイトで確認済み) ため、この範囲で全国分を網羅できる。
_PREFECTURE_IDS = list(range(1, 49))
# 1 都道府県あたりの安全弁 (最大ページ数)。全国最大でも数ページ規模。
_MAX_PAGES = 60


class Ajssa33(StaticCrawler):
    """全国警備業協会 (AJSSA) 会員名簿 スクレイパー — 全国分"""

    DELAY = 1.5
    # 全カラムが Schema に対応するため EXTRA_COLUMNS は無し。
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str):
        for pref_id in _PREFECTURE_IDS:
            for page in range(1, _MAX_PAGES + 1):
                page_url = self._build_url(url, pref_id, page)
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                table = soup.select_one("table.table-striped")
                rows = table.find_all("tr") if table else []
                # 先頭行はヘッダ (会員名/都道府県/住所/業種/電話番号)
                data_rows = [r for r in rows if r.find("td")]
                if not data_rows:
                    # この都道府県のページ末尾 (または該当0件) → 次の都道府県へ
                    break

                for row in data_rows:
                    try:
                        item = self._parse_row(row, page_url)
                        if item:
                            yield item
                    except Exception as e:  # 個別行のエラーはスキップして継続
                        logger.warning("行の解析に失敗しskip: %s", e)
                        continue

                # ページが満杯 (100件) でなければ最終ページ → 次の都道府県へ
                if len(data_rows) < 100:
                    break

    @staticmethod
    def _build_url(url: str, pref_id: int, page: int) -> str:
        """引数 url を起点に prefecture_id / page のみ差し替えた URL を組み立てる。"""
        parts = urlparse(url)
        params = {
            "utf8": "✓",
            "q[suppliername_cont]": "",
            "q[bussinessold1_or_bussinessold2_or_bussinessold3_or_bussinessold4_or_"
            "bussinessold5_or_bussinessold6_or_bussinessold7_eq]": "",
            "q[prefecture_id_eq]": str(pref_id),
            "commit": "検索",
            "page": str(page),
        }
        query = urlencode(params)
        return urlunparse(parts._replace(query=query))

    def _parse_row(self, row, source_url: str) -> dict | None:
        tds = row.find_all("td")
        if len(tds) < 5:
            return None

        name = tds[0].get_text(strip=True)
        if not name:
            return None
        pref = tds[1].get_text(strip=True)
        addr = tds[2].get_text(strip=True)

        # 業種: 単字コードが空白/改行区切りで並ぶ → フル名称へ展開
        biz = [_BIZ.get(c, c) for c in tds[3].get_text(" ", strip=True).split()]

        tel = tds[4].get_text(strip=True)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: "/".join(biz),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa33()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute(
        "https://kameiin.ajssa.or.jp/?utf8=%E2%9C%93&q%5Bprefecture_id_eq%5D=%E9%81%B8"
        "%E3%82%93%E3%81%A7%E3%81%8F%E3%81%A0%E3%81%95%E3%81%84&q%5Bsuppliername_cont"
        "%5D=&q%5Bbussinessold1_or_bussinessold2_or_bussinessold3_or_bussinessold4_or_"
        "bussinessold5_or_bussinessold6_or_bussinessold7_eq%5D=&commit=%E6%A4%9C%E7%B4%A2"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
