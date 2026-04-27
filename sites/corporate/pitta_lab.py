# scripts/sites/corporate/pitta_lab.py
"""
ピタットラボ (pitta-lab.com) — BtoB企業比較サイト スクレイパー

取得対象:
    - 全カテゴリの企業/サービス情報
    - 企業共通: 企業名、住所、都道府県、代表者、設立年月、事業内容
    - カテゴリ別比較項目: 料金、対応エリア、得意業界、サポート体制 等
    - レビュー評価

取得フロー:
    1. トップページから Next.js buildId を取得
    2. サイトマップから全カテゴリのURLパスを取得
    3. 各カテゴリの一覧HTMLをページネーション巡回 → __NEXT_DATA__ からサービスID収集
    4. 各サービスの詳細HTML → __NEXT_DATA__ から企業情報・比較項目・レビューを抽出

実行方法:
    # ローカルテスト（最初の3カテゴリのみ）
    python scripts/sites/corporate/pitta_lab.py

    # Prefect Flow 経由（全カテゴリ）
    python bin/run_flow.py --site-id pitta_lab
"""

import json
import re
import sys
import time
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://pitta-lab.com"
LISTING_DELAY = 1.5
MAX_BACKOFF = 60
CONSECUTIVE_403_LIMIT = 5

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県"
    r"|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県"
    r"|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県"
    r"|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県"
    r"|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県"
    r"|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県"
    r"|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# queryAnswers キー → EXTRA_COLUMNS マッピング
_QA_MAP = {
    "料金": ["料金", "料金体系"],
    "対応エリア": ["対応エリア"],
    "得意業界": ["得意業界", "得意業種", "得意職種"],
    "サポート体制": ["制作後のサポート", "サポート", "ワンストップ対応"],
    "対応日時": ["土日の対応", "対応日"],
}


class PittaLabScraper(StaticCrawler):
    """ピタットラボ BtoB企業比較サイト スクレイパー"""

    DELAY = 2.0
    EXTRA_COLUMNS = [
        "サービスID",
        "企業ID",
        "料金",
        "対応エリア",
        "得意業界",
        "サポート体制",
        "無料見積もり",
        "対応日時",
        "レビュー評価",
        "レビュー件数",
        "比較項目",
    ]
    _max_categories = None  # テスト用カテゴリ上限

    def prepare(self):
        """Referer ヘッダーを設定"""
        self.session.headers.update({"Referer": f"{BASE_URL}/"})

    def _get_next_data(self, url: str) -> dict | None:
        """HTMLページを取得し、__NEXT_DATA__ の pageProps を返す。403時はバックオフ。"""
        backoff = 5
        for attempt in range(3):
            try:
                soup = self.get_soup(url)
                script = soup.find("script", id="__NEXT_DATA__")
                if not script:
                    return None
                data = json.loads(script.string)
                return data.get("props", {}).get("pageProps", {})
            except Exception as e:
                if "403" in str(e):
                    self.logger.warning(
                        "403 → %d秒待機後にリトライ (%d/3): %s", backoff, attempt + 1, url
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                raise
        return None

    def _fetch_category_paths(self):
        """サイトマップから全カテゴリの URLパスを取得"""
        resp = self.session.get(f"{BASE_URL}/sitemap.xml", timeout=20)
        locs = re.findall(r"<loc>(.*?)</loc>", resp.text)
        paths = []
        for loc in locs:
            if "/services/1" not in loc:
                continue
            path = loc.replace(f"{BASE_URL}/", "").replace("/services/1", "")
            if path:
                paths.append(path)
        return paths

    def parse(self, url: str):
        categories = self._fetch_category_paths()
        if self._max_categories:
            categories = categories[: self._max_categories]
        self.logger.info("対象カテゴリ数: %d", len(categories))

        # Phase 1: 全カテゴリの一覧ページを巡回し、ユニークなサービスIDを収集
        service_ids = []
        seen = set()
        consecutive_403 = 0

        for cat_idx, cat_path in enumerate(categories, 1):
            cat_name = urllib.parse.unquote(cat_path)
            page = 1

            if consecutive_403 >= CONSECUTIVE_403_LIMIT:
                self.logger.warning(
                    "403が%d回連続 → %d秒クールダウン", consecutive_403, MAX_BACKOFF
                )
                time.sleep(MAX_BACKOFF)
                consecutive_403 = 0

            while True:
                time.sleep(LISTING_DELAY)
                list_url = f"{BASE_URL}/{cat_path}/services/{page}"
                data = self._get_next_data(list_url)

                if data is None:
                    self.logger.warning("一覧取得失敗: %s page=%d", cat_name, page)
                    consecutive_403 += 1
                    break

                consecutive_403 = 0
                services = data.get("services", [])
                if not services:
                    break

                if page == 1:
                    total = data.get("totalServices", "?")
                    self.logger.info(
                        "[%d/%d] %s: %s件",
                        cat_idx, len(categories), cat_name, total,
                    )

                for svc in services:
                    sid = svc.get("id")
                    if sid and sid not in seen:
                        seen.add(sid)
                        service_ids.append(sid)

                num_pages = data.get("numberOfPages", 1)
                if page >= num_pages:
                    break
                page += 1

        self.total_items = len(service_ids)
        self.logger.info(
            "ユニークサービス数: %d（%d カテゴリ巡回完了）",
            self.total_items, len(categories),
        )

        # Phase 2: 各サービスの詳細ページを取得
        consecutive_403 = 0
        for sid in service_ids:
            if consecutive_403 >= CONSECUTIVE_403_LIMIT:
                self.logger.warning(
                    "403が%d回連続 → %d秒クールダウン", consecutive_403, MAX_BACKOFF
                )
                time.sleep(MAX_BACKOFF)
                consecutive_403 = 0

            time.sleep(self.DELAY)
            try:
                item = self._scrape_service(sid)
                if item:
                    consecutive_403 = 0
                    yield item
                else:
                    consecutive_403 += 1
            except Exception as e:
                self.logger.warning("サービス %s 取得失敗: %s", sid, e)
                consecutive_403 += 1

    def _scrape_service(self, service_id: str) -> dict | None:
        """サービス詳細HTMLから __NEXT_DATA__ を解析して企業情報を抽出"""
        detail_url = f"{BASE_URL}/services/{service_id}"
        data = self._get_next_data(detail_url)
        if not data:
            return None

        service = data.get("service", {})
        if not service:
            return None

        enterprise = service.get("enterprise") or {}
        name = enterprise.get("name", "")
        if not name:
            return None

        parent_cat = service.get("parentCategory", "")
        child_cat = service.get("childCategory", "")
        if isinstance(child_cat, dict):
            child_cat = child_cat.get("name", "")

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.REP_NM: enterprise.get("representative", ""),
            Schema.OPEN_DATE: enterprise.get("established", ""),
            Schema.LOB: enterprise.get("business", ""),
            Schema.HP: enterprise.get("url", ""),
            Schema.CAT_SITE: f"{parent_cat} / {child_cat}" if parent_cat else child_cat,
            "サービスID": service_id,
            "企業ID": enterprise.get("id", ""),
            "無料見積もり": "あり",
        }

        # 住所 → 都道府県 + 住所
        self._parse_address(enterprise.get("address", ""), item)

        # カテゴリ別比較項目
        self._parse_query_answers(service, item)

        # レビュー
        review = service.get("reviewSummary") or {}
        avg = review.get("averageRate")
        cnt = review.get("totalCount")
        if avg:
            item["レビュー評価"] = str(avg)
        if cnt:
            item["レビュー件数"] = str(cnt)

        return item

    def _parse_address(self, address: str, item: dict):
        """住所文字列から 〒除去 → 都道府県分離 → Schema.PREF / Schema.ADDR にセット"""
        if not address:
            return
        addr = re.sub(r"^〒?\s*\d{3}-?\d{4}\s*", "", address).strip()
        m = _PREF_RE.search(addr)
        if m:
            item[Schema.PREF] = m.group(1)
            item[Schema.ADDR] = addr[m.end():].strip()
        else:
            item[Schema.ADDR] = addr

    def _parse_query_answers(self, service: dict, item: dict):
        """categorySpecificSettings[0].queryAnswers から比較項目を抽出"""
        settings = service.get("categorySpecificSettings") or []
        if not settings:
            return

        qa_list = settings[0].get("queryAnswers") or []
        all_qa = {}
        for qa in qa_list:
            query = qa.get("query") or {}
            name = query.get("name", "")
            answer = qa.get("answer", "")
            if not name or not answer or answer == "-":
                continue
            all_qa[name] = answer

            # 既知カラムへのマッピング
            for col, keywords in _QA_MAP.items():
                if name in keywords and col not in item:
                    item[col] = answer

        if all_qa:
            item["比較項目"] = json.dumps(all_qa, ensure_ascii=False)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PittaLabScraper()
    scraper.execute(f"{BASE_URL}/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
