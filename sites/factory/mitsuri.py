"""
Mitsuri (ミツリ) — 登録工場一覧スクレイパー (app.mitsu-ri.net)

取得対象:
    - Mitsuri に登録された全工場 (約 758 件)
    - 名称・住所・郵便番号・都道府県・HP・資本金・従業員数・設立年・法人番号(登録番号)・
      業種・主要銀行/取引先/仕入先・CO2排出量・対応可能な加工方法/材質 など

サイト構造 (Next.js / SSR):
    - 一覧 : /pblc/suppliers?page=N
             各ページの <script id="__NEXT_DATA__"> に
             props.pageProps.initialData.items[].slug と meta(totalItems 等) が入る
             (50 件/ページ, 16 ページ)
    - 詳細 : /pblc/profiles/{slug}
             <script id="__NEXT_DATA__"> の props.pageProps.company に
             全構造化フィールドが SSR 埋め込みされている

取得フロー (一覧→詳細, 1 件取得ごとに即 yield):
    1. ?page=N を 1 から進め、各ページの items から slug を取得
    2. slug ごとに /pblc/profiles/{slug} を取得し company JSON を yield
    3. items が空になったら終了

実行方法:
    python scripts/sites/factory/mitsuri.py
    docker compose exec worker python /app/bin/run_flow.py --site-id mitsuri
"""

import json
import sys
from pathlib import Path
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


def _next_data(soup) -> dict:
    """<script id="__NEXT_DATA__"> の JSON を辞書で返す。失敗時は空 dict。"""
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return {}
    try:
        return json.loads(tag.string)
    except (ValueError, TypeError):
        return {}


def _join(values, sep="/") -> str:
    """リスト中の非空要素を sep で連結。文字列ならそのまま返す。"""
    if isinstance(values, list):
        return sep.join(str(v).strip() for v in values if v not in (None, ""))
    return str(values).strip() if values not in (None, "") else ""


def _s(value) -> str:
    """None / 数値などを安全に文字列化 (None は空文字)。"""
    if value in (None, ""):
        return ""
    return str(value).strip()


class MitsuriScraper(StaticCrawler):
    """Mitsuri (ミツリ) 登録工場一覧スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "業種",            # industry (短い分類ラベル: 製造業 等)
        "主要銀行",        # mainBank
        "主要取引先",      # mainClient
        "主要仕入先",      # mainVendor
        "CO2排出量",       # co2Emission
        "対応可能な加工方法",  # processingMethods (短いラベルの列挙)
        "対応可能な材質",      # companyMaterials (短いラベルの列挙)
        "国内/海外",       # isDomestic 区分
    ]

    def parse(self, url: str):
        # 引数 url を唯一のルートとして使用 (SSOT = sites.yml)。
        # url から origin / list-path を取り出し、ページ送り・詳細 URL を派生させる。
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        page = 1
        while True:
            list_url = self._page_url(parsed, page)
            soup = self.get_soup(list_url)
            if soup is None:
                break

            data = _next_data(soup)
            initial = (
                data.get("props", {})
                .get("pageProps", {})
                .get("initialData", {})
            )
            items = initial.get("items") or []
            if not items:
                break

            # 初回ページで総件数を確定し進捗表示を有効化
            if page == 1:
                meta = initial.get("meta") or {}
                if meta.get("totalItems"):
                    self.total_items = int(meta["totalItems"])

            for it in items:
                slug = it.get("slug")
                if not slug:
                    continue
                detail_url = f"{origin}/pblc/profiles/{slug}"
                try:
                    # isDomestic は一覧 item にのみ存在 (詳細 company には無い)
                    item = self._scrape_detail(detail_url, it.get("isDomestic"))
                    if item:
                        yield item
                except Exception:
                    self.logger.exception("詳細取得失敗: %s", detail_url)
                    continue

            page += 1

    @staticmethod
    def _page_url(parsed, page: int) -> str:
        """ルート url のクエリを保ったまま page パラメータを付与/上書きする。"""
        query = [(k, v) for k, v in parse_qsl(parsed.query) if k != "page"]
        query.append(("page", str(page)))
        return urlunparse(parsed._replace(query=urlencode(query)))

    def _scrape_detail(self, url: str, is_domestic) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        company = (
            _next_data(soup)
            .get("props", {})
            .get("pageProps", {})
            .get("company")
        )
        if not company:
            return None

        # 住所: address + address2 を連結 (都道府県は PREF に分離)
        addr = f"{_s(company.get('address'))}{_s(company.get('address2'))}".strip()

        return {
            Schema.URL:       url,
            Schema.NAME:      _s(company.get("name")),
            Schema.PREF:      _s(company.get("prefecture")),
            Schema.POST_CODE: _s(company.get("zipcode")),
            Schema.ADDR:      addr,
            Schema.HP:        _s(company.get("url")),
            Schema.CAP:       _s(company.get("capital")),
            Schema.EMP_NUM:   _s(company.get("employee")),
            Schema.OPEN_DATE: _s(company.get("establishYear")),
            Schema.CO_NUM:    _s(company.get("registrationNumber")),
            "業種":              _s(company.get("industry")),
            "主要銀行":          _s(company.get("mainBank")),
            "主要取引先":        _s(company.get("mainClient")),
            "主要仕入先":        _s(company.get("mainVendor")),
            "CO2排出量":         _s(company.get("co2Emission")),
            "対応可能な加工方法": _join(company.get("processingMethods")),
            "対応可能な材質":     _join(company.get("companyMaterials")),
            "国内/海外":         "国内" if is_domestic else "海外",
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MitsuriScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute(
        "https://app.mitsu-ri.net/pblc/suppliers?_gl=1*wczuq3*_ga*MTg0MTM0NDU5MS4xNzgyNzg2NDg5*_ga_SQCQZHNTCS*czE3ODI3ODY0ODgkbzEkZzEkdDE3ODI3ODgyMTckajMyJGwwJGgw"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
