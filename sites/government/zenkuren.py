"""
全国クリーニング生活衛生同業組合連合会 (全ク連) — 全国の組合加盟クリーニング店

運営: 全国クリーニング生活衛生同業組合連合会 (www.zenkuren.or.jp)
一覧URL: https://www.zenkuren.or.jp/shopsearch/result_list?pref=hokkaidou&cityCode=

取得対象:
    - 全国 47 都道府県の組合加盟クリーニング店の公表情報
    - 店名 / 都道府県 / 住所 / 郵便番号 / TEL / 代表者(店主) / HP 等の構造化情報

取得フロー (公開 JSON API を都道府県ごとに叩く):
    1. 一覧ページ (result_list) は空の枠だけを返し、店舗データは JS が
       `apiUrl + "find/"` (= /django/django.cgi/api/2.0/find/?prefecture=<pref_id>)
       を GET して描画する。requests でこの API を直接叩く。
    2. find API のレスポンス JSON:
         { "count": N,
           "prefecture": "北海道",
           "cities": [...],
           "shops": [ { "city": {...}, "shops": [ <店舗>, ... ] }, ... ] }   # 市区町村ごとにグループ化
       店舗オブジェクト: name / address / zip_code / tel / manager /
                         link_url / date_of_acquisition / location{lat,lng} / city / id
    3. 都道府県 → 市区町村グループ → 店舗 の順に走査し、1 店舗ごとに即 yield する
       (詳細ページは存在せず、一覧 API に全情報が揃う)。

設計メモ:
    - API のルートは引数 url と同一オリジンから urljoin で導出する (別ドメインをハードコードしない)。
      表示用の取得URL (Schema.URL) も引数 url と同じ result_list パスから都道府県別に組み立てる。
    - 備考の要件どおり都道府県 (Schema.PREF) を city.prefecture から付与する。
    - 「同時実行数の制限」要件に対応し、都道府県は ThreadPool を使わず 1 リクエストずつ
      逐次取得する (DELAY を挟む)。サーバ負荷とワーカー破綻を避ける。
    - 取得フィールドはすべて構造化された事実情報 (店名・住所・電話・郵便番号・代表者名・年度・座標)。
      長文の自由記述 (プロース) カラムは無いため著作権リスクは無い。
    - robots.txt は /wp-admin/ のみ Disallow。利用規約はコンテンツの商用複製を禁じるが、
      スクレイピング/自動アクセス自体の明示的禁止は無い。

実行方法:
    # ローカルテスト
    python scripts/sites/government/zenkuren.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id zenkuren
"""

import logging
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# find API へ渡す都道府県 ID (shopsearch.js の prefectures 定義順)。
# 引数 url の pref= と同じ値空間。全国を巡回するために全 47 件を保持する。
PREFECTURE_IDS = [
    "hokkaidou", "aomoriken", "iwateken", "miyagiken", "akitaken", "yamagataken",
    "hukusimaken", "ibarakiken", "totigiken", "gunmaken", "saitamaken", "tibaken",
    "toukyouto", "kanagawaken", "niigataken", "toyamaken", "isikawaken", "hukuiken",
    "yamanasiken", "naganoken", "gihuken", "sizuokaken", "aitiken", "mieken",
    "sigaken", "kyoutohu", "oosakahu", "hyougoken", "naraken", "wakayamaken",
    "tottoriken", "simaneken", "okayamaken", "hirosimaken", "yamagutiken",
    "tokusimaken", "kagawaken", "ehimeken", "koutiken", "hukuokaken", "sagaken",
    "nagasakiken", "kumamotoken", "ooitaken", "miyazakiken", "kagosimaken",
    "okinawaken",
]

# find API の (オリジンからの) パス。ルートは引数 url のオリジンに urljoin して導出する。
_API_PATH = "/django/django.cgi/api/2.0/find/"


def _clean(value) -> str:
    """None / 改行 / 全角空白を正規化して 1 行の文字列にする。"""
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("　", " ")).strip()


class ZenkurenScraper(StaticCrawler):
    """全国クリーニング生活衛生同業組合連合会 スクレイパー"""

    DELAY = 1.5

    # サイト固有の構造化カラム (いずれも短い事実情報。自由記述プロースは含めない)
    EXTRA_COLUMNS = [
        "市区町村",           # 例: 札幌市中央区 (city.name)
        "LDマーク等取得年度",  # 例: 平成22年度 (date_of_acquisition)
        "緯度",               # location.lat
        "経度",               # location.lng
        "店舗ID",             # API 上の店舗 id
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルートとして扱う。
        #   - API エンドポイント: url と同一オリジンへ urljoin
        #   - 表示用 URL (Schema.URL): url と同じ result_list パスを都道府県別に組み立て
        api_url = urljoin(url, _API_PATH)
        parts = urlsplit(url)
        list_base = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

        self.total_items = len(PREFECTURE_IDS)  # 進捗は都道府県数ベース (総店舗数は事前に不明)
        seen = 0

        for pref_id in PREFECTURE_IDS:
            try:
                data = self._fetch_prefecture(api_url, pref_id)
            except Exception as e:  # 1 都道府県の失敗で全体を止めない
                logger.warning("都道府県取得に失敗 (スキップ): %s — %s", pref_id, e)
                continue

            pref_name = _clean(data.get("prefecture"))
            source_url = f"{list_base}?pref={pref_id}&cityCode="

            # shops は「市区町村グループ」の配列。各グループの shops が実店舗。
            for group in data.get("shops", []):
                for shop in group.get("shops", []):
                    item = self._build_item(shop, pref_name, source_url)
                    if item:
                        yield item

            seen += 1
            if seen < len(PREFECTURE_IDS):
                time.sleep(self.DELAY)

    def _fetch_prefecture(self, api_url: str, pref_id: str) -> dict:
        """find API を 1 都道府県分だけ取得して JSON を返す。

        session.get はテストランナー/スモークテストがラップする中断ポイントなので、
        ここを起点に使う (逐次 1 リクエスト = 同時実行を抑制)。
        """
        resp = self.session.get(
            api_url,
            params={"prefecture": pref_id},
            timeout=self.TIMEOUT,
            headers={"Referer": urljoin(api_url, "/shopsearch/result_list")},
        )
        resp.raise_for_status()
        return resp.json()

    def _build_item(self, shop: dict, pref_name: str, source_url: str) -> dict | None:
        name = _clean(shop.get("name"))
        if not name:
            return None

        city = shop.get("city") or {}
        location = shop.get("location") or {}
        # city.prefecture を優先し、無ければ find レスポンス直下の prefecture を使う
        pref = _clean(city.get("prefecture")) or pref_name

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: _clean(shop.get("address")),
            Schema.POST_CODE: _clean(shop.get("zip_code")),
            Schema.TEL: _clean(shop.get("tel")),
            Schema.REP_NM: _clean(shop.get("manager")),
            Schema.HP: _clean(shop.get("link_url")),
            Schema.URL: source_url,
            "市区町村": _clean(city.get("name")),
            "LDマーク等取得年度": _clean(shop.get("date_of_acquisition")),
            "緯度": location.get("lat", "") if location.get("lat") is not None else "",
            "経度": location.get("lng", "") if location.get("lng") is not None else "",
            "店舗ID": shop.get("id", ""),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = ZenkurenScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.zenkuren.or.jp/shopsearch/result_list?pref=hokkaidou&cityCode=")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
