"""
＠Press (atpress.ne.jp) — プレスリリース配信企業の会社概要を抽出するクローラー

取得対象:
    - トップページ (https://www.atpress.ne.jp/) に掲載された新着プレスリリースから
      到達できる各配信企業の会社概要ページ (/news/company/{id})
    - 会社名・所在地・代表者名・設立年月日・従業員数・HP・業種など

取得フロー (一覧→中間→詳細 / Pattern B: 1 社ずつ即 yield):
    引数 url (トップページ = sites.yml の url) を唯一のルートとして使う。
      1. トップページの新着プレスリリースカード (/news/{id}) を列挙
      2. 各プレスリリース詳細ページから配信企業リンク (/news/company/{id}) を抽出
      3. 企業 ID を重複排除し、会社概要ページを取得して 1 社ずつ即 yield
    ※ 会社概要ページは埋め込み JSON-LD (Organization) と概要グリッドの両方に
      データがあり、requests で静的取得できるため StaticCrawler で実装する。
    ※ プレスリリース一覧サイトマップ (sitemap-news.xml) の先頭は会社リンクを持たない
      旧形式ページが多いため、会社リンクを確実に持つトップページの新着カードを起点にする。

除外フィールド (著作権リスク回避):
    JSON-LD の description (事業内容の自由記述プロース) は取得しない。
    構造化された短い業種ラベル (卸売 / おもちゃ など) のみ CAT_SITE として取得する。

備考フィルタ:
    呼び出し時の「備考」は会社概要の取得項目 (所在地・代表者名・URL・設立年月日・
    従業員数) を示すもので、絞り込み条件ではないため parse() にフィルタは実装しない。

実行方法:
    python scripts/sites/corporate/press.py
    python bin/run_flow.py --site-id press
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# トップページ上のプレスリリースカード (/news/123456)
_RELEASE_RE = re.compile(r'/news/(\d+)(?:["/?#]|$)')
# プレスリリース詳細ページ内の配信企業リンク (/news/company/19337)
_COMPANY_RE = re.compile(r'/news/company/(\d+)')
# 会社概要グリッドで拾うラベル
_GRID_LABELS = {"所在地", "代表者名", "URL", "設立年月日", "従業員数"}
# 都道府県 (住所先頭の分割用フォールバック)
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class Press(StaticCrawler):
    """＠Press (atpress.ne.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []  # すべて Schema にマッピングできるため EXTRA は無し

    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            return

        # トップページの新着プレスリリース ID を出現順で重複排除
        release_ids = list(dict.fromkeys(_RELEASE_RE.findall(str(soup))))
        self.total_items = len(release_ids)

        seen_companies: set[str] = set()
        for rid in release_ids:
            try:
                release_url = urljoin(url, f"/news/{rid}")
                rsoup = self.get_soup(release_url)
                if rsoup is None:
                    continue
                m = _COMPANY_RE.search(str(rsoup))
                if not m:
                    continue  # 旧形式など企業リンクを持たないページはスキップ
                cid = m.group(1)
                if cid in seen_companies:
                    continue
                seen_companies.add(cid)

                company_url = urljoin(url, f"/news/company/{cid}")
                item = self._scrape_company(company_url)
                if item:
                    yield item
            except Exception as exc:  # noqa: BLE001 個別失敗はログして続行
                self.logger.warning("skip release %s: %s", rid, exc)
                continue

    def _scrape_company(self, company_url: str) -> dict | None:
        soup = self.get_soup(company_url)
        if soup is None:
            return None

        org = self._extract_org_ld(soup)
        grid = self._extract_grid(soup)

        # 会社名: h1 → JSON-LD name
        h1 = soup.select_one("h1")
        name = h1.get_text(strip=True) if h1 else (org.get("name") if org else "")
        if not name:
            return None

        # 住所: JSON-LD の addressRegion(都道府県) + streetAddress を優先、
        #       無ければグリッドの「所在地」を都道府県で分割
        pref, addr = "", ""
        addr_obj = org.get("address") if org else None
        if isinstance(addr_obj, dict):
            pref = (addr_obj.get("addressRegion") or "").strip()
            addr = (addr_obj.get("streetAddress") or "").strip()
        if not (pref or addr) and grid.get("所在地"):
            full = grid["所在地"]
            pm = _PREF_RE.match(full)
            if pm:
                pref = pm.group(1)
                addr = full[pm.end():].strip()
            else:
                addr = full

        # 代表者名
        rep = grid.get("代表者名") or (org.get("founder") if org else "") or ""
        # HP
        hp = grid.get("URL") or (org.get("url") if org else "") or ""
        # 設立年月日 (表示形式 "2015年12月" を優先、無ければ JSON-LD の ISO)
        open_date = grid.get("設立年月日") or (org.get("foundingDate") if org else "") or ""
        # 従業員数 (表示形式 "5人" を優先、無ければ JSON-LD の数値)
        emp = grid.get("従業員数")
        if not emp and org and org.get("numberOfEmployees") not in (None, ""):
            emp = str(org["numberOfEmployees"])
        emp = emp or ""

        # 業種 (構造化された短いラベル: 卸売 / おもちゃ / 精密機器)
        cat_site = ""
        if h1:
            ul = h1.find_next("ul")
            if ul:
                cats = [li.get_text(strip=True) for li in ul.find_all("li")]
                cats = [c for c in cats if c]
                cat_site = " / ".join(cats)

        return {
            Schema.URL: company_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.REP_NM: rep,
            Schema.EMP_NUM: emp,
            Schema.OPEN_DATE: open_date,
            Schema.HP: hp,
            Schema.CAT_SITE: cat_site,
        }

    @staticmethod
    def _extract_org_ld(soup) -> dict:
        """JSON-LD から配信企業 (publisher=Organization) を抽出する。"""
        for s in soup.find_all("script", type="application/ld+json"):
            raw = s.string or s.get_text() or ""
            if not raw.strip():
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            for it in data if isinstance(data, list) else [data]:
                if not isinstance(it, dict):
                    continue
                pub = it.get("publisher")
                if isinstance(pub, dict) and (
                    "foundingDate" in pub or "numberOfEmployees" in pub or "founder" in pub
                ):
                    return pub
        return {}

    @staticmethod
    def _extract_grid(soup) -> dict:
        """会社概要グリッド (ラベル div.font-w6 + 値 div) をラベル→値で抽出する。"""
        grid: dict[str, str] = {}
        for d in soup.find_all("div"):
            cls = d.get("class") or []
            if "font-w6" not in cls:
                continue
            label = d.get_text(strip=True)
            if label in _GRID_LABELS and label not in grid:
                sib = d.find_next_sibling("div")
                if sib is not None:
                    grid[label] = sib.get_text(strip=True)
        return grid


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Press()
    # 🔒 この URL は sites.yml の url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.atpress.ne.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
