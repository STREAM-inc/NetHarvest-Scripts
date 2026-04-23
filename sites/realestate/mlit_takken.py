# -*- coding: utf-8 -*-
"""
MLIT 建設業者・宅建業者等企業情報検索システム（宅地建物取引業者）

対象サイト: https://etsuran2.mlit.go.jp/TAKKEN/takkenKensaku.do
取得フロー:
    都道府県コードを順番に選択 → 一覧ページネーション →
    各行の詳細ページを開いて企業情報を取得 → 前の画面に戻る
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

from playwright.sync_api import TimeoutError as PWTimeoutError

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

START_URL = "https://etsuran2.mlit.go.jp/TAKKEN/takkenKensaku.do"
SLEEP_MS = 200

PREFS: List[Tuple[str, str]] = [
    ("01", "北海道"), ("02", "青森県"), ("03", "岩手県"), ("04", "宮城県"),
    ("05", "秋田県"), ("06", "山形県"), ("07", "福島県"), ("08", "茨城県"),
    ("09", "栃木県"), ("10", "群馬県"), ("11", "埼玉県"), ("12", "千葉県"),
    ("13", "東京都"), ("14", "神奈川県"), ("15", "新潟県"), ("16", "富山県"),
    ("17", "石川県"), ("18", "福井県"), ("19", "山梨県"), ("20", "長野県"),
    ("21", "岐阜県"), ("22", "静岡県"), ("23", "愛知県"), ("24", "三重県"),
    ("25", "滋賀県"), ("26", "京都府"), ("27", "大阪府"), ("28", "兵庫県"),
    ("29", "奈良県"), ("30", "和歌山県"), ("31", "鳥取県"), ("32", "島根県"),
    ("33", "岡山県"), ("34", "広島県"), ("35", "山口県"), ("36", "徳島県"),
    ("37", "香川県"), ("38", "愛媛県"), ("39", "高知県"), ("40", "福岡県"),
    ("41", "佐賀県"), ("42", "長崎県"), ("43", "熊本県"), ("44", "大分県"),
    ("45", "宮崎県"), ("46", "鹿児島県"), ("47", "沖縄県"),
]

X_SEL_PREF     = "//select[@id='kenCode']"
X_BTN_SEARCH   = "//img[contains(@src,'btn_search')]"
X_TBL_RESULTS  = "//table[contains(@class,'re_disp')]"
X_BACK         = "//p[contains(@class,'foot_pancuz')]//a[contains(normalize-space(.),'前画面に戻る')]"
X_LINKS        = "//table[contains(@class,'re_disp')]//tr[position()>1]/td[4]//a"

DETAIL_XPATHS = {
    "免許証番号":                         "//th[normalize-space()='免許証番号']/following-sibling::td[1]",
    "免許の有効期間":                      "//th[normalize-space()='免許の有効期間']/following-sibling::td[1]",
    "法人・個人の別":                      "//th[normalize-space()='法人・個人の別']/following-sibling::td[1]",
    "最初の免許年月日":                    "//th[normalize-space()='最初の免許年月日']/following-sibling::td[1]",
    "商号又は名称":                        "//th[normalize-space()='商号又は名称']/following-sibling::td[1]",
    "代表者の氏名":                        "//th[contains(normalize-space(.),'代表者')]/following-sibling::td[1]",
    "主たる事務所の所在地":                "//th[contains(normalize-space(.),'主たる事務所') and contains(normalize-space(.),'所在地')]/following-sibling::td[1]",
    "総従事者数":                          "//th[normalize-space()='総従事者数']/following-sibling::td[1]",
    "うち、専任の宅地建物取引士の数":     "//th[contains(normalize-space(.),'専任') and contains(normalize-space(.),'宅地建物取引士')]/following-sibling::td[1]",
    "電話番号":                            "//th[normalize-space()='電話番号']/following-sibling::td[1]",
    "加入している宅地建物取引業保証協会": "//th[contains(normalize-space(.),'保証協会')]/following-sibling::td[1]",
    "免許申請時の資本金":                  "//th[contains(normalize-space(.),'資本金')]/following-sibling::td[1]",
}


# ====================== ユーティリティ ======================

def norm_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("　", " ")
    return re.sub(r"\s+", " ", s).strip()


def is_kana_only(s: str) -> bool:
    s = norm_text(s)
    if not s:
        return False
    return re.fullmatch(r"[ぁ-ゖァ-ヴー・\-\sｦ-ﾝｰﾞﾟ･]+", s) is not None


def split_kana_kanji(value: str) -> Tuple[str, str]:
    v = norm_text(value)
    if not v:
        return "", ""
    parts = v.split(" ")
    if parts and is_kana_only(parts[0]):
        i, kana_tokens = 0, []
        while i < len(parts) and is_kana_only(parts[i]):
            kana_tokens.append(parts[i])
            i += 1
        return " ".join(parts[i:]).strip(), " ".join(kana_tokens).strip()
    if len(parts) >= 2:
        a, b = parts[0], " ".join(parts[1:])
        if is_kana_only(a) and not is_kana_only(b):
            return b, a
        if is_kana_only(b) and not is_kana_only(a):
            return a, b
    if is_kana_only(v):
        return "", v
    return v, ""


def split_shogo(value: str) -> Tuple[str, str]:
    """(名称_カナ, 名称_漢字) を返す。"""
    v = norm_text(value)
    if not v:
        return "", ""
    parts = v.split(" ")
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return "", v


def safe_inner_text(page, xpath: str) -> str:
    loc = page.locator(f"xpath={xpath}")
    if loc.count() == 0:
        return ""
    return norm_text(loc.first.inner_text())


def parse_total_pages(page) -> int:
    try:
        txt = norm_text(page.locator("#pageListNo1 option:checked").inner_text())
        m = re.search(r"/\s*(\d+)\s*$", txt)
        return int(m.group(1)) if m else 1
    except Exception:
        return 1


# ====================== NetHarvestクローラー ======================

class MlitTakkenScraper(DynamicCrawler):
    """国土交通省 宅地建物取引業者検索 — 47都道府県を順番に取得"""

    EXTRA_COLUMNS = [
        "都道府県コード",
        "免許証番号",
        "免許の有効期間",
        "法人・個人の別",
        "最初の免許年月日",
        "代表者名_カナ",
        "うち、専任の宅地建物取引士の数",
        "加入している宅地建物取引業保証協会",
    ]

    def _setup(self):
        """locale と timeout を追加して Playwright を起動する。"""
        from playwright.sync_api import sync_playwright
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context(
            user_agent=self.USER_AGENT,
            locale="ja-JP",
        )
        self.context.set_default_timeout(60_000)
        self.page = self.context.new_page()

    def parse(self, url: str) -> Generator[dict, None, None]:
        self.page.goto(url, wait_until="domcontentloaded")

        for pref_code, pref_name in PREFS:
            self.logger.info("=== 都道府県: %s (%s) ===", pref_name, pref_code)

            # 都道府県を選択して検索
            self.page.wait_for_selector(f"xpath={X_SEL_PREF}")
            self.page.select_option("#kenCode", pref_code)
            if self.page.locator("#dispCount").count() > 0:
                self.page.select_option("#dispCount", "10")
            self.page.locator(f"xpath={X_BTN_SEARCH}").click()
            self.page.wait_for_load_state("domcontentloaded")

            # 結果テーブル確認
            if self.page.locator(f"xpath={X_TBL_RESULTS}").count() == 0:
                body = norm_text(self.page.inner_text("body"))
                if "検索結果：0件" in body:
                    self.logger.info("%s: 0件 → スキップ", pref_name)
                    continue
                self.page.wait_for_timeout(500)
                if self.page.locator(f"xpath={X_TBL_RESULTS}").count() == 0:
                    self.logger.warning("%s: 結果テーブルなし → スキップ", pref_name)
                    continue

            total_pages = parse_total_pages(self.page)
            self.logger.info("%s: %d ページ", pref_name, total_pages)

            for page_no in range(1, total_pages + 1):
                if page_no > 1 and self.page.locator("#pageListNo1").count() > 0:
                    first_license = ""
                    try:
                        first_license = norm_text(
                            self.page.locator(
                                "xpath=(//table[contains(@class,'re_disp')]//tr[position()>1]/td[3])[1]"
                            ).inner_text()
                        )
                    except Exception:
                        pass
                    self.page.select_option("#pageListNo1", str(page_no))
                    try:
                        if first_license:
                            self.page.wait_for_function(
                                """(prev) => {
                                    const el = document.evaluate(
                                      "(//table[contains(@class,'re_disp')]//tr[position()>1]/td[3])[1]",
                                      document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null
                                    ).singleNodeValue;
                                    return el && el.textContent && el.textContent.trim() !== prev;
                                }""",
                                arg=first_license,
                                timeout=20_000,
                            )
                    except PWTimeoutError:
                        self.page.wait_for_timeout(800)

                links = self.page.locator(f"xpath={X_LINKS}")
                link_count = links.count()

                for idx in range(link_count):
                    self.page.locator(f"xpath={X_LINKS}").nth(idx).click()
                    self.page.wait_for_selector(f"xpath={X_BACK}")

                    detail: Dict[str, str] = {}
                    for k, xp in DETAIL_XPATHS.items():
                        detail[k] = safe_inner_text(self.page, xp)

                    name_kana, name_kanji = split_shogo(detail.get("商号又は名称", ""))
                    rep_kanji, rep_kana = split_kana_kanji(detail.get("代表者の氏名", ""))

                    item = {
                        Schema.URL:      START_URL,
                        Schema.PREF:     pref_name,
                        Schema.NAME:     name_kanji or detail.get("商号又は名称", ""),
                        Schema.NAME_KANA: name_kana,
                        Schema.REP_NM:   rep_kanji,
                        Schema.ADDR:     detail.get("主たる事務所の所在地", ""),
                        Schema.TEL:      detail.get("電話番号", ""),
                        Schema.EMP_NUM:  detail.get("総従事者数", ""),
                        Schema.CAP:      detail.get("免許申請時の資本金", ""),
                        "都道府県コード":                        pref_code,
                        "免許証番号":                           detail.get("免許証番号", ""),
                        "免許の有効期間":                        detail.get("免許の有効期間", ""),
                        "法人・個人の別":                        detail.get("法人・個人の別", ""),
                        "最初の免許年月日":                      detail.get("最初の免許年月日", ""),
                        "代表者名_カナ":                         rep_kana,
                        "うち、専任の宅地建物取引士の数":        detail.get("うち、専任の宅地建物取引士の数", ""),
                        "加入している宅地建物取引業保証協会":    detail.get("加入している宅地建物取引業保証協会", ""),
                    }
                    yield item

                    back = self.page.locator(f"xpath={X_BACK}")
                    if back.count() > 0:
                        back.first.click()
                    else:
                        self.page.get_by_text("前画面に戻る").first.click()
                    self.page.wait_for_selector(f"xpath={X_TBL_RESULTS}")
                    self.page.wait_for_timeout(SLEEP_MS)


# ====================== ローカル実行用エントリーポイント ======================
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = MlitTakkenScraper()
    scraper.execute(START_URL)
    print(f"\n取得件数: {scraper.item_count}")
    print(f"出力先:   {scraper.output_filepath}")
