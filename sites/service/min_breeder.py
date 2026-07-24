"""
みんなのブリーダー（犬） — 犬ブリーダー検索ポータル

取得対象:
    - 全国の犬ブリーダー一覧（事業所名・所在地・代表者・動物取扱業登録情報・評価等）

取得フロー:
    一覧ページ (breederList.php, pageNum=N でページ送り, 1ページ10件) を巡回し、
    各ブリーダーの詳細ページ (breeder_xxxx.html) を取得して即 yield する (Pattern B)。
    - 一覧: 名称・総合評価・口コミ件数・子犬掲載数
    - 詳細: 事業所名・所在地(住所)・種別・氏名(代表者)・動物取扱責任者・登録番号 等
            名称カナは <title> の括弧内から抽出

実行方法:
    # ローカルテスト
    python scripts/sites/service/min_breeder.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id min_breeder
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県の先頭マッチ用パターン
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class MinBreeder(StaticCrawler):
    """みんなのブリーダー（犬） スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "動物取扱業種別",        # 種別 (例: 販売)
        "動物取扱責任者",        # 動物取扱責任者の氏名
        "動物取扱業登録番号",    # 登録番号
        "動物取扱業登録年月日",  # 登録年月日
        "動物取扱業有効期限",    # 有効期限
        "子犬掲載数",            # 子犬一覧の掲載数
    ]

    def _page_url(self, url: str, page: int) -> str:
        """引数 url を起点に pageNum=page を付与したページURLを生成する。"""
        pr = urlparse(url)
        q = parse_qs(pr.query, keep_blank_values=True)
        q["pageNum"] = [str(page)]
        return urlunparse(pr._replace(query=urlencode(q, doseq=True)))

    def parse(self, url: str):
        page = 1
        while True:
            soup = self.get_soup(self._page_url(url, page))
            boxes = soup.select("div.b_SearchBox")
            if not boxes:
                break

            for box in boxes:
                try:
                    name_el = box.select_one("a.grid_brTable_breederName")
                    if not name_el:
                        continue
                    detail_url = urljoin(url, name_el.get("href", ""))

                    # 一覧ページ側のフィールド
                    name = name_el.get_text(" ", strip=True)
                    scores = ""
                    rate_el = box.select_one(".rateNum")
                    if rate_el:
                        scores = rate_el.get_text(strip=True)
                    rev = ""
                    voice_el = box.select_one(".voiceNum")
                    if voice_el:
                        m = re.search(r"([\d,]+)", voice_el.get_text(strip=True))
                        if m:
                            rev = m.group(1).replace(",", "")
                    puppy = ""
                    grid = box.select_one("div.grid_brTable")
                    if grid:
                        m = re.search(r"子犬一覧\s*([\d,]+)件", grid.get_text(" ", strip=True))
                        if m:
                            puppy = m.group(1).replace(",", "")

                    item = self._scrape_detail(detail_url)
                    if item is None:
                        continue

                    item[Schema.NAME] = name
                    item[Schema.SCORES] = scores
                    item[Schema.REV_SCR] = rev
                    item[Schema.CAT_SITE] = "犬"
                    item["子犬掲載数"] = puppy
                    yield item
                except Exception as e:  # 個別アイテムのエラーはスキップ
                    self.logger.warning(f"アイテム取得失敗: {e}")
                    continue

            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        item = {
            Schema.URL: url,
            Schema.NAME_KANA: "",
            Schema.FAC_NAME: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.REP_NM: "",
            "動物取扱業種別": "",
            "動物取扱責任者": "",
            "動物取扱業登録番号": "",
            "動物取扱業登録年月日": "",
            "動物取扱業有効期限": "",
        }

        # 名称カナ: <title>【埼玉/犬】平　ほの花(たいら　ほのか)ブリーダー｜... の括弧内
        if soup.title:
            m = re.search(r"[（(]([^（()）]+)[）)]", soup.title.get_text(strip=True))
            if m:
                item[Schema.NAME_KANA] = m.group(1).strip()

        # 動物取扱業に基づく表記 (dt「ラベル：」 / dd「値」)
        profile: dict[str, str] = {}
        for cont in soup.select("div.breeder_basicInfoContent"):
            for dl in cont.find_all("dl"):
                dt = dl.find("dt")
                dd = dl.find("dd")
                if dt and dd and dt.get_text(strip=True).endswith("："):
                    label = dt.get_text(strip=True).rstrip("：")
                    profile.setdefault(label, dd.get_text(" ", strip=True))

        item[Schema.FAC_NAME] = profile.get("事業所名", "")
        item[Schema.REP_NM] = profile.get("氏名", "")
        item["動物取扱業種別"] = profile.get("種別", "")
        item["動物取扱責任者"] = profile.get("動物取扱責任者", "")
        item["動物取扱業登録番号"] = profile.get("登録番号", "")
        item["動物取扱業登録年月日"] = profile.get("登録年月日", "")
        item["動物取扱業有効期限"] = profile.get("有効期限", "")

        addr = profile.get("所在地", "")
        if addr:
            m = _PREF_PATTERN.match(addr)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr[m.end():].strip()
            else:
                item[Schema.ADDR] = addr

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MinBreeder()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.min-breeder.com/breederList.php?search_act=search&search_flg=1&search_push=1&pref_mode=&key_dog_color_cd=&key_dog_sort_cd=&key_b_name=")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
