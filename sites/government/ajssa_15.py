"""
一般社団法人 長野県警備業協会（AJSSA 会員名簿・長野県）— 会員名簿

取得対象:
    - 長野県警備業協会の全会員企業（地区別 4 ページ・約108社）
      北信(hokushin)=39 / 中信(chuushin)=26 / 東信(toushin)=13 / 南信(nanshin)=30

取得フロー:
    /area/{地区slug} は WordPress の静的アーカイブページ。会員は 1 会員 = 1 個の
    <div class="... member ...">。ページネーションは無い。各会員の構造:
      - h1.entry-title: 会社名（業務区分）  例「グリーン警備保障㈱長野支社（２号業務）」
      - span.media-body_excerpt 内の <p> 群（順不同・任意）:
          「〒郵便番号 住所」 / 「TEL 電話番号」 / 「URLホームページ」
    詳細ページ (/member/{id}) は 302 リダイレクトで実体が無く、全情報が一覧に載る。

    ※ 備考: 全会員をまとめた一覧ページは存在しないため、引数 url を基点に
      4 地区ページ (hokushin/chuushin/toushin/nanshin) を巡回する。各地区 URL は
      引数 url からの相対解決 (urljoin) で導出し、別ドメイン/別パスはハードコードしない。
      会員を 1 件取得するごとに即 yield する (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_15.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_15
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

# 地区スラッグ → 地区名（備考で指定された 4 地区。全会員一覧が無いため巡回する）
_AREAS = {
    "hokushin": "北信",
    "chuushin": "中信",
    "toushin": "東信",
    "nanshin": "南信",
}


class Ajssa15(StaticCrawler):
    """一般社団法人 長野県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務区分(○号業務) / 地区 はサイト固有の短い構造化ラベル → EXTRA。
    EXTRA_COLUMNS = ["業務区分", "地区"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url, 例 .../area/hokushin) を唯一の基点として使い、
        # 4 地区ページを urljoin で導出して巡回する。
        total = 0
        for slug, area_name in _AREAS.items():
            area_url = urljoin(url, slug)
            soup = self.get_soup(area_url)
            if not soup:
                logger.warning("地区ページ取得に失敗しskip: %s", area_url)
                continue

            members = soup.find_all(class_="member")
            total += len(members)
            self.total_items = total  # 進捗表示用（累積）

            for m in members:
                try:
                    item = self._parse_member(m, area_url, area_name)
                    if item:
                        yield item
                except Exception as e:  # 個別会員のエラーはスキップして継続
                    logger.warning("会員の解析に失敗しskip: %s", e)
                    continue

    def _parse_member(self, node, source_url: str, area_name: str) -> dict | None:
        h = node.select_one("h1.entry-title, .entry-title, h1")
        if not h:
            return None
        raw_name = h.get_text(strip=True).replace("　", " ").strip()
        if not raw_name:
            return None

        # 末尾の全角括弧が業務区分 (○号業務) の場合は名称から分離する
        gyoumu = ""
        name = raw_name
        m = re.search(r"（([^（）]*)）\s*$", raw_name)
        if m and ("号" in m.group(1) or "業務" in m.group(1)):
            gyoumu = m.group(1).strip()
            name = raw_name[: m.start()].strip()

        post_code = ""
        addr = ""
        tel = ""
        hp = ""
        for p in node.select(".media-body_excerpt p"):
            text = p.get_text(strip=True).replace("　", " ").strip()
            if not text:
                continue
            if text.startswith("〒"):
                body = text[1:].strip()
                parts = re.split(r"\s+", body, maxsplit=1)
                post_code = parts[0].strip()
                addr = parts[1].strip() if len(parts) > 1 else ""
            elif text.upper().startswith("TEL"):
                tel = text[3:].strip(" :：").strip()
            elif text.upper().startswith("URL"):
                # HP リンクは <a> があればそれを優先、無ければテキストから抽出
                a = p.find("a", href=True)
                hp = (a["href"] if a else text[3:]).replace(" ", "").strip()

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "長野県",  # 長野県警備業協会の会員 = 全て長野県
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            "業務区分": gyoumu,
            "地区": area_name,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa15()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://nagano-keibi.or.jp/area/hokushin")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
