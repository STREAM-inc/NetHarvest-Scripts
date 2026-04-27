# ===== 【pip install -e . を実行していない場合のみ必要】===========
import sys
from pathlib import Path
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
# ================================================================

import csv
import math
import re
import time
from datetime import datetime
from urllib.parse import urljoin

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


class AnnyGiftScraper(DynamicCrawler):
    """
    Anny gift スクレイパー（JS描画対応・DynamicCrawler）

    Step1: 一覧ページを巡回して詳細URLを収集
    Step2: 詳細ページから各フィールドを取得
    ※ リアルタイムCSVを並行出力（クラッシュ時もデータを保持）
    """

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "アクセス",
        "最寄り駅",
        "キャンセルポリシー",
        "席数",
        "お子様",
        "子供椅子",
        "授乳室",
        "ドレスコード",
        "駐車場",
        "喫煙可否",
        "車いす",
        "留意点",
    ]

    # リアルタイムCSVに書き出すカラム順
    _RT_FIELDNAMES = [
        Schema.GET_TIME, Schema.URL, Schema.NAME,
        Schema.TEL, Schema.ADDR,
        Schema.SCORES, Schema.REV_SCR,
        Schema.TIME, Schema.HOLIDAY, Schema.PAYMENTS,
        "アクセス", "最寄り駅", "キャンセルポリシー",
        "席数", "お子様", "子供椅子", "授乳室",
        "ドレスコード", "駐車場", "喫煙可否", "車いす", "留意点",
    ]

    def prepare(self):
        """リアルタイムCSVファイルを準備する"""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        rt_path = self.local_output_dir / f"realtime_Anny_{ts}.csv"
        self._rt_file = open(rt_path, mode="w", encoding="utf-8-sig", newline="")
        self._rt_writer = csv.DictWriter(
            self._rt_file,
            fieldnames=self._RT_FIELDNAMES,
            extrasaction="ignore",
        )
        self._rt_writer.writeheader()
        self._rt_file.flush()
        self.logger.info("リアルタイムCSV出力先: %s", rt_path)

    def finalize(self):
        """リアルタイムCSVを閉じる"""
        if hasattr(self, "_rt_file") and not self._rt_file.closed:
            self._rt_file.close()

    def _goto(self, url: str):
        """ページ遷移（networkidle → load へ自動フォールバック・最大2回リトライ）"""
        for attempt, wait in enumerate(["networkidle", "load", "load"]):
            try:
                self.page.goto(url, wait_until=wait, timeout=60_000)
                if wait != "networkidle":
                    time.sleep(2)   # load の場合は JS 描画を少し待つ
                return
            except Exception as e:
                if attempt < 2:
                    self.logger.warning("遷移リトライ [%d/2] %s / %s", attempt + 1, url, e)
                    time.sleep(3)
                else:
                    raise

    def parse(self, url: str):

        # ===== Step1: 一覧ページを巡回して詳細URLを収集 =====
        self._goto(url)

        # 総件数を取得（span.ListPlansNumber）
        plans_text = self.page.evaluate(
            "() => document.querySelector('.ListPlansNumber')?.innerText || ''"
        )
        total_count = int(re.sub(r"[^\d]", "", plans_text)) if plans_text else 0
        if total_count == 0:
            self.logger.warning("件数取得に失敗しました。タイトルから取得を試みます")
            title = self.page.title()
            m = re.search(r"TOP(\d+)", title)
            total_count = int(m.group(1)) if m else 100

        total_pages = math.ceil(total_count / 10)
        self.total_items = total_count
        self.logger.info("総件数: %s / 総ページ数: %s", total_count, total_pages)

        collected_urls: set[str] = set()

        for page_num in range(1, total_pages + 1):
            page_url = f"{url}&pg={page_num}" if "?" in url else f"{url}?pg={page_num}"
            self.logger.info("一覧ページ取得中 [%d/%d]: %s", page_num, total_pages, page_url)

            try:
                self._goto(page_url)

                # 各カードの最初のリンクを1件だけ取得（画像・空席リンクを除外）
                hrefs = self.page.evaluate("""
                    () => [...document.querySelectorAll('.ListingCardV2_ListingCard__kDrbz')]
                        .map(card => card.querySelector('a[href*="celebration-plans"]')?.getAttribute('href'))
                        .filter(Boolean)
                """)

                for href in hrefs:
                    # ?from=search 等のクエリを除去して正規化
                    clean_path = href.split("?")[0]
                    full_url = urljoin(url, clean_path)
                    collected_urls.add(full_url)

            except Exception as e:
                self.logger.warning("一覧ページ取得に失敗しました: %s / %s", page_url, e)
                continue

            time.sleep(self.DELAY)

        self.logger.info("詳細URL収集件数: %s", len(collected_urls))

        # ===== Step2: 詳細ページを巡回して情報を取得 =====
        for detail_url in collected_urls:
            try:
                self.logger.info("詳細ページ取得中: %s", detail_url)
                self._goto(detail_url)

                data = self.page.evaluate("""
                    () => {
                        // dt → dd を辞書化（全フィールド一括取得）
                        const dtMap = {};
                        [...document.querySelectorAll('dt')].forEach(dt => {
                            const dd = dt.nextElementSibling;
                            if (dd) dtMap[dt.innerText.trim()] = dd.innerText.trim();
                        });

                        // 名称
                        const name = document.querySelector('.LuxuryTag')?.innerText?.trim() || '';

                        // プランの星の数・レビュー件数（同クラスの最初の要素）
                        const ratingEl   = document.querySelector('.StarsLinePlanItem_stars-line_rate__ZsaRN');
                        const reviewEl   = document.querySelector('.StarsLinePlanItem_stars-line_count__DVcOI');
                        const rating     = ratingEl?.innerText?.trim() || '';
                        const reviewRaw  = reviewEl?.innerText?.trim() || '';
                        const reviewCount = reviewRaw.replace(/[(）()件]/g, '').trim();

                        return { name, rating, reviewCount, dtMap };
                    }
                """)

                item = {
                    Schema.URL:      detail_url,
                    Schema.NAME:     data["name"],
                    Schema.ADDR:     data["dtMap"].get("住所", ""),
                    Schema.TEL:      data["dtMap"].get("電話番号", ""),
                    Schema.TIME:     data["dtMap"].get("営業時間", ""),
                    Schema.HOLIDAY:  data["dtMap"].get("定休日", ""),
                    Schema.PAYMENTS: data["dtMap"].get("支払い方法", ""),
                    Schema.SCORES:   data["rating"],
                    Schema.REV_SCR:  data["reviewCount"],
                    "アクセス":          data["dtMap"].get("アクセス", ""),
                    "最寄り駅":          data["dtMap"].get("最寄駅", ""),
                    "キャンセルポリシー":   data["dtMap"].get("キャンセルポリシー", ""),
                    "席数":             data["dtMap"].get("席数", ""),
                    "お子様":           data["dtMap"].get("お子様", ""),
                    "子供椅子":          data["dtMap"].get("子供椅子", ""),
                    "授乳室":           data["dtMap"].get("授乳室", ""),
                    "ドレスコード":       data["dtMap"].get("ドレスコード", ""),
                    "駐車場":           data["dtMap"].get("駐車場", ""),
                    "喫煙可否":          data["dtMap"].get("喫煙可否", ""),
                    "車いす":           data["dtMap"].get("車いす", ""),
                    "留意点":           data["dtMap"].get("留意点", ""),
                }

                # リアルタイムCSVに即時書き出し
                if hasattr(self, "_rt_writer"):
                    rt_row = {Schema.GET_TIME: datetime.now().strftime("%Y-%m-%d %H:%M:%S"), **item}
                    self._rt_writer.writerow(rt_row)
                    self._rt_file.flush()

                yield item

            except Exception as e:
                self.logger.warning("詳細取得に失敗しました: %s / %s", detail_url, e)
                continue

            time.sleep(self.DELAY)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    AnnyGiftScraper().execute(
        "https://oiwai.anny.gift/search?numPpl=2"
    )
