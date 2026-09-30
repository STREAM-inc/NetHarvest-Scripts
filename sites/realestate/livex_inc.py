# -*- coding: utf-8 -*-
"""
ライヴェックス（貸事務所・賃貸オフィス検索） — オフィスビル情報

取得対象:
    - 掲載中の賃貸オフィスビル（ビル単位のマスタ情報）

取得フロー:
    トップ (https://www.livex-inc.com/)
      → /search/list.php?page=N（賃貸オフィス検索結果・約437ページ）
        → /search/detail/facility_index.php?id={ビルコード}（ビル詳細）を取得して即 yield

取得カラム（備考指定）:
    ビルコード / ビル名 / ビル住所 / 規模（地上・地下階数） / 竣工 /
    基準階面積（坪） / 大型ビルタグ（基準階面積300坪以上の大型オフィスビル） / ビル詳細URL

    ※「備考」欄は自由記述のプロースのため著作権リスクを避けて取得対象外とした。

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/livex_inc.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id livex_inc
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 一覧ページ（全エリア・条件なし）
_LIST_PATH = "/search/list.php"
# ビル詳細ページ
_DETAIL_PATH = "/search/detail/facility_index.php"

# 一覧のページ送り安全上限（2026-09 時点で実測 437 ページ）
_MAX_PAGES = 600

# 詳細リンクからビルコードを取り出す
_FACILITY_ID = re.compile(r"facility_index\.php\?id=(\d+)")
# 「名称」欄に併記されるビルコード: "○○ビル (ビルコード：12345)"
_NAME_CODE = re.compile(r"[（(]\s*ビルコード\s*[:：]\s*(\d+)\s*[）)]")

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 規模「地上19階　地下3階」
_FLOOR_ABOVE = re.compile(r"地上\s*(\d+)\s*階")
_FLOOR_BELOW = re.compile(r"地下\s*(\d+)\s*階")
# 基準階面積「676.50坪」
_TSUBO = re.compile(r"([\d,]+(?:\.\d+)?)\s*坪")

# 大型ビルタグ（サイト側の「特徴」タグ表記）
_LARGE_TAG = "基準階面積300坪以上の大型オフィスビル"
_LARGE_TSUBO = 300.0


def _norm(text: str) -> str:
    """全角スペースを半角に寄せて余分な空白を畳む。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("　", " ")).strip()


class LivexIncScraper(StaticCrawler):
    """ライヴェックス オフィスビル スクレイパー（一覧→詳細 / 取得即 yield）"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "ビルコード",
        "構造",
        "規模",
        "地上階数",
        "地下階数",
        "基準階面積(坪)",
        "大型ビルタグ",
        "竣工",
        "エレベーター",
        "交通",
        "設備",
        "特徴",
        "情報更新日",
    ]

    def parse(self, url: str):
        list_url = urljoin(url, _LIST_PATH)
        seen: set[str] = set()

        for page in range(1, _MAX_PAGES + 1):
            soup = self.get_soup(f"{list_url}?page={page}")
            if soup is None:
                return

            # このページに載っているビル（詳細リンク）を列挙
            codes: list[str] = []
            for a in soup.select(f'a[href*="{_DETAIL_PATH}"]'):
                m = _FACILITY_ID.search(a["href"])
                if m and m.group(1) not in codes:
                    codes.append(m.group(1))

            # 詳細リンクが 1 件も無ければ最終ページを越えている
            if not codes:
                return

            new_found = False
            for code in codes:
                if code in seen:
                    continue
                seen.add(code)
                new_found = True

                detail_url = urljoin(url, f"{_DETAIL_PATH}?id={code}")
                try:
                    item = self._scrape_detail(detail_url, code)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            # 新規ビルが 1 件も無ければ実質末尾（同一ページの返却）とみなす
            if not new_found:
                return

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str, code: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 物件概要テーブル（td[0] = ラベル, td[1] = 値）
        fields: dict[str, str] = {}
        overview = soup.select_one("div.property-overview")
        if overview:
            for tr in overview.select("tr"):
                tds = tr.find_all("td", recursive=False) or tr.find_all("td")
                if len(tds) < 2:
                    continue
                label = _norm(tds[0].get_text(" "))
                if label and label not in fields:
                    fields[label] = _norm(tds[1].get_text(" "))

        # ビル名（「名称」からビルコードの括弧を除去）。無い場合は h2「○○の空き室情報」で補完
        name = _NAME_CODE.sub("", fields.get("名称", "")).strip()
        if not name:
            for h2 in soup.find_all("h2"):
                t = _norm(h2.get_text(" "))
                if t.endswith("の空き室情報"):
                    name = t[: -len("の空き室情報")]
                    break
        if not name:
            return None

        # ビルコード: 名称欄の表記を優先し、無ければ URL 由来の id
        m = _NAME_CODE.search(fields.get("名称", ""))
        building_code = m.group(1) if m else code

        # 住所を都道府県 / 市区町村以降に分割
        addr = fields.get("所在地", "")
        pref = ""
        pm = _PREF_PATTERN.match(addr)
        if pm:
            pref = pm.group(1)
            addr = addr[pm.end():].strip()

        # 規模（地上/地下階数）
        scale = fields.get("規模", "")
        am = _FLOOR_ABOVE.search(scale)
        bm = _FLOOR_BELOW.search(scale)

        # 基準階面積（坪）— 数値のみ
        tsubo_raw = fields.get("基準階面積", "")
        tm = _TSUBO.search(tsubo_raw)
        tsubo = tm.group(1).replace(",", "") if tm else ""

        # 設備 / 特徴（構造化された短いタグ）
        equipments = self._tag_list(soup, "tooltips")
        features = self._tag_list(soup, "feature")

        # 大型ビルタグ: サイト側の「特徴」タグを優先し、
        # 未付与でも基準階面積 300 坪以上なら同タグを付ける
        large_tag = _LARGE_TAG if _LARGE_TAG in features else ""
        if not large_tag and tsubo:
            try:
                if float(tsubo) >= _LARGE_TSUBO:
                    large_tag = _LARGE_TAG
            except ValueError:
                pass

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            "ビルコード": building_code,
            "構造": fields.get("構造", ""),
            "規模": _norm(scale),
            "地上階数": am.group(1) if am else "",
            "地下階数": bm.group(1) if bm else "",
            "基準階面積(坪)": tsubo,
            "大型ビルタグ": large_tag,
            "竣工": fields.get("竣工", ""),
            "エレベーター": fields.get("エレベーター", ""),
            "交通": fields.get("交通", ""),
            "設備": " / ".join(equipments),
            "特徴": " / ".join(features),
            "情報更新日": fields.get("情報更新日", ""),
        }

    @staticmethod
    def _tag_list(soup, class_name: str) -> list[str]:
        """設備・特徴のタグ名を重複なしで取り出す（PC/SP で重複表示されるため）。"""
        tags: list[str] = []
        for box in soup.find_all("div", class_=class_name):
            for li in box.find_all("li"):
                t = _norm(li.get_text(" "))
                if t and t not in tags:
                    tags.append(t)
        return tags


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = LivexIncScraper()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.livex-inc.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
