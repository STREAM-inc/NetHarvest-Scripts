"""
一般社団法人 埼玉県警備業協会（埼警協 / AJSSA 会員名簿・埼玉県）— 会員・賛助会員名簿

取得対象:
    - 埼玉県警備業協会の会員企業（6支部 + 賛助会員・約277社）
    - 会社名 / 支部 / 郵便番号 / 住所 / 都道府県 / TEL / HP / 警備種別（業務内容） /
      災害警備登録企業フラグ

取得フロー:
    member-list.html は索引ページで、6支部 + 賛助会員の各名簿サブページ
    (/member-list/{slug}-list.html) へのリンクを列挙するだけ。実データは各サブページの
    <table> にある。各会員は 2 行構成の <tbody>:
        行1: リンク(HP) / 会員名 / 業務内容 / 電話番号  (賛助会員は 会員名(HP内包) / 電話番号)
        行2: 〒郵便番号 住所 (colspan)
    災害警備登録企業は行1の <tr class="saigai ..."> で示される。
    索引 → 各サブページの順に巡回し、会員 1 件ごとに即 yield する (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_10.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_10
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 住所先頭の都道府県 (さいたま市等・県名省略の場合は 埼玉県 を既定とする)
_PREF_RE = re.compile(r"^(北海道|東京都|(?:京都|大阪)府|..県)")
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")


class Ajssa10(StaticCrawler):
    """一般社団法人 埼玉県警備業協会 会員・賛助会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務内容(施設/交通 等の短い警備種別ラベル) → Schema.CAT_SITE。
    # 支部・災害警備登録フラグはサイト固有の構造化ラベルとして EXTRA。
    EXTRA_COLUMNS = ["支部", "災害警備登録"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url / 索引ページ) を唯一のルートとして派生させる
        index_soup = self.get_soup(url)
        if index_soup is None:
            logger.warning("索引ページの取得に失敗: %s", url)
            return

        main = index_soup.find("main") or index_soup
        # 6支部 + 賛助会員のサブページリンク (テキスト=支部名, href=名簿ページ)
        subpages = []
        seen = set()
        for a in main.select("ul li a[href]"):
            href = a.get("href", "")
            if "/member-list/" in href and href.endswith("-list.html"):
                full = urljoin(url, href)
                if full in seen:
                    continue
                seen.add(full)
                subpages.append((a.get_text(strip=True), full))

        if not subpages:
            logger.warning("サブページリンクが見つかりません: %s", url)
            return

        for branch, page_url in subpages:
            soup = self.get_soup(page_url)
            if soup is None:
                logger.warning("名簿ページの取得に失敗しskip: %s", page_url)
                continue
            page_main = soup.find("main") or soup
            bodies = [
                b
                for b in page_main.find_all("tbody")
                if "tableHeader" not in (b.get("class") or [])
            ]
            for body in bodies:
                try:
                    item = self._parse_member(body, branch, page_url)
                    if item:
                        yield item
                except Exception as e:  # 個別会員のエラーはスキップして継続
                    logger.warning("会員の解析に失敗しskip: %s", e)
                    continue

    def _parse_member(self, body, branch: str, source_url: str) -> dict | None:
        trs = body.find_all("tr", recursive=False)
        if len(trs) < 2:
            return None
        data_tr, addr_tr = trs[0], trs[1]

        # 災害警備登録企業は行1の tr に class="saigai ..." が付く
        saigai = "○" if "saigai" in (data_tr.get("class") or []) else ""

        tds = data_tr.find_all("td", recursive=False)
        if not tds:
            return None
        # 会員ページは先頭に rowspan のリンクセル。賛助会員ページには無い。
        rest = tds[1:] if tds[0].get("rowspan") else tds
        if not rest:
            return None

        name = rest[0].get_text(" ", strip=True)
        if not name:
            return None

        # 業務内容(警備種別): 会員ページのみ (rest = 会員名/業務内容/電話番号 の 3 列)
        gyoumu = rest[1].get_text(" ", strip=True) if len(rest) >= 3 else ""
        gyoumu = re.sub(r"\s+", " ", gyoumu).strip()

        # 電話番号
        tel_el = body.select_one("td.tel")
        tel = tel_el.get_text(strip=True) if tel_el else ""

        # HP: 協会外部ドメインへの絶対 URL (会員ページはリンクセル / 賛助会員は会員名セル内)
        a = body.find(
            "a",
            href=lambda h: h and h.startswith("http") and "saikeikyo" not in h,
        )
        hp = a["href"].strip() if a else ""

        # 住所行 (colspan): 〒郵便番号 住所
        addr_td = addr_tr.find("td")
        addr_full = addr_td.get_text(" ", strip=True) if addr_td else ""
        pm = _POST_RE.search(addr_full)
        post = pm.group(1) if pm else ""
        addr = _POST_RE.sub("", addr_full).strip()
        prefm = _PREF_RE.match(addr)
        # さいたま市等 (県名省略) は 埼玉県。県名明記の県外本社はその都道府県。
        pref = prefm.group(1) if prefm else "埼玉県"

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: gyoumu,
            "支部": branch,
            "災害警備登録": saigai,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa10()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://saikeikyo.or.jp/member-list.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
