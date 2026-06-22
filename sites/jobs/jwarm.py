"""
ジェイウォーム (jwarm.net) — 求人・掲載企業情報スクレイパー (DynamicCrawler 版)

取得対象:
    掲載企業の基本情報
    (企業名称・郵便番号・住所・電話番号・代表者・資本金・創立/創業・売上高・従業員数・事業内容・HP)

取得フロー (逐次 yield — タイムアウト耐性のため一覧巡回と詳細取得をインターリーブ):
    1. 引数 url (sites.yml の正規URL) を起点に pg=1 から一覧ページを 1 ページずつ巡回
    2. div#itemList 内の a[href*=uni_item_detail.php] から詳細URL (id=...) を収集・重複排除
    3. ★ ページごとに、その場で各詳細ページを取得して 1 件ずつ即 yield する。
       全URLを先に集めてから取得する方式だと、巡回途中で時間切れ kill された際に
       CSV が close() 時にしか書かれず 0 件になる。逐次 yield なら取得済み分は必ず残る。
    4. itemList が無い / 詳細リンクが空 / 新規リンクが無い (範囲超過・先頭ページへの巻き戻り)
       のいずれかで巡回終了。
    5. 各詳細ページの div#kigyou_data テーブル (<th>ラベル</th><td>値</td>) を抽出。

備考:
    - 当サイトは完全サーバーサイドレンダリング。素の requests が bot 系 UA を弾くため、
      実ブラウザ UA/挙動で安定取得できる Playwright (DynamicCrawler) を使う。
    - SSR なので待機は networkidle ではなく domcontentloaded で十分・高速・安定。
      (広告/トラッカーで networkidle が発火せずタイムアウトするのを避ける)
    - 1件の取得失敗で全体を止めない (ログを残して継続)。
    - 詳細ページの企業情報テーブルでは設立日のラベルが「創立/創業」(「設立」ではない)。

実行方法:
    python scripts/sites/jobs/jwarm.py
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

# itemList が永遠に空にならない異常時の無限ループ保険 (実データは数十ページ程度)
_MAX_PAGES = 500


class JwarmScraper(DynamicCrawler):
    """ジェイウォーム 掲載企業スクレイパー (jwarm.net)"""

    DELAY = 1.0
    # 代表者(REP_NM)/資本金(CAP)/売上高(SALES)/従業員数(EMP_NUM) は Schema 標準項目。
    # EXTRA には Schema に無い 設立日・事業内容 のみを追加する。
    EXTRA_COLUMNS = ["設立日", "事業内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """引数 url を唯一のルートとして一覧を巡回し、詳細を 1 件ずつ即 yield する。"""
        parsed = urlparse(url)
        # 元の query (ig=i 等) を保持したまま pg だけ差し替える
        params = {k: v[0] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}

        seen: set[str] = set()
        total = 0

        for page in range(1, _MAX_PAGES + 1):
            params["pg"] = str(page)
            page_url = urlunparse(parsed._replace(query=urlencode(params)))

            soup = self.get_soup(page_url, wait_until="domcontentloaded")
            if soup is None:
                self.logger.warning("一覧取得失敗 page=%d: soup is None", page)
                break

            item_list = soup.find("div", id="itemList")
            if not item_list:
                self.logger.info("itemList 無し page=%d → 巡回終了", page)
                break

            # 同一詳細URLが画像/見出し/ボタンと複数回出るため、順序保持で重複排除
            page_links: list[str] = []
            page_seen: set[str] = set()
            for a in item_list.select("a[href*='uni_item_detail.php']"):
                href = a.get("href", "")
                if href and href not in page_seen:
                    page_seen.add(href)
                    page_links.append(href)

            if not page_links:
                self.logger.info("詳細リンク無し page=%d → 巡回終了", page)
                break

            # ページ範囲を超えるとサイトが先頭ページへ巻き戻る場合がある。
            # 新規リンクが 1 件も無ければ終了 (無限ループ防止)。
            new_links = [urljoin(url, h) for h in page_links if urljoin(url, h) not in seen]
            if not new_links:
                self.logger.info("新規リンク無し page=%d (巻き戻り) → 巡回終了", page)
                break

            for detail_url in new_links:
                seen.add(detail_url)
                item = self._scrape_detail(detail_url)
                if item:
                    total += 1
                    yield item
                time.sleep(self.DELAY)

            self.logger.info("page %d 完了: 累計 %d 件", page, total)
            time.sleep(self.DELAY)
        else:
            self.logger.warning("ページ上限 %d に到達。巡回を打ち切りました。", _MAX_PAGES)

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url, wait_until="domcontentloaded")
        if soup is None:
            self.logger.warning("詳細取得失敗: %s", url)
            return None

        data: dict = {Schema.URL: url}

        tel_span = soup.find("span", class_="Tel")
        if tel_span:
            data[Schema.TEL] = tel_span.get_text(strip=True)

        target_div = soup.find("div", id="kigyou_data")
        if target_div:
            for row in target_div.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = re.sub(r"[　\xa0]", " ", td.get_text(" ", strip=True)).strip()

                if label == "企業名称":
                    data[Schema.NAME] = value
                elif label == "掲載住所":
                    if value.startswith("〒"):
                        parts = re.split(r"\s+", value, maxsplit=1)
                        if len(parts) == 2:
                            data[Schema.POST_CODE] = parts[0]
                            data[Schema.ADDR] = parts[1].strip()
                        else:
                            data[Schema.ADDR] = value
                    else:
                        data[Schema.ADDR] = value
                # 設立日のラベルは実ページ上「創立/創業」。表記揺れに備え両方を許容。
                elif "創立" in label or "創業" in label or label == "設立":
                    data["設立日"] = value
                elif label == "URL":
                    data[Schema.HP] = value
                elif label == "代表者":
                    data[Schema.REP_NM] = value
                elif label == "資本金":
                    data[Schema.CAP] = value
                elif label == "事業内容":
                    data["事業内容"] = value
                elif label == "売上高":
                    data[Schema.SALES] = value
                elif label == "従業員数":
                    data[Schema.EMP_NUM] = value

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JwarmScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jwarm.net/uni_items.php?pg=1&ig=i")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
