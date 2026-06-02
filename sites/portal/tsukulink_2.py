# scripts/sites/portal/tsukulink_2.py
"""
ツクリンクリスト (tsukulink.net) — 建設業者 一覧→詳細 スクレイパー

取得対象:
    全国の建設業者（約 123,164 件）。一覧ページで基本情報を取得し、
    各社の詳細ページに遷移して会社概要（資本金・従業員数・設立年月日・
    対応可能エリア・保有建設機材・技術者資格 等）の構造化情報を取得する。

    ※ 既存の `tsukulink`（一覧のみ）に対し、本クローラーは詳細ページまで
      巡回して会社概要の構造化フィールドを追加取得する拡充版。

取得フロー:
    /companies?page=N （一覧, 1ページ20件）
      └─ 各社 /{pref}/city_{code}/{company_id} （詳細）へ遷移
         → 1件取得するごとに即 yield（Pattern B）

著作権配慮:
    会社紹介文・事業内容・募集案件本文など「自由記述の長文プロース」は
    取得しない（構造化された短いラベル・数値・リストのみを取得）。

実行方法:
    # ローカルテスト
    python scripts/sites/portal/tsukulink_2.py
    python scripts/sites/portal/tsukulink_2.py --start-page 100

    # Prefect Flow 経由
    python bin/run_flow.py --site-id tsukulink_2
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県抽出パターン（東京都・北海道・〇〇府・〇〇県）
_PREF_RE = re.compile(r"^(東京都|北海道|(?:.+?[都道府県]))")

# 建設業許可番号（例: 茨城県知事許可-第30410号 / 国土交通大臣許可-第12345号）
_PERMIT_RE = re.compile(r"((?:\S+?知事|国土交通大臣)許可[-－]?第?\s*[0-9０-９]+\s*号)")


class TsukulinkListScraper(StaticCrawler):
    """ツクリンクリスト 建設業者（一覧→詳細）スクレイパー"""

    DELAY = 1.5
    START_PAGE = 1  # 再開時はここを変更
    EXTRA_COLUMNS = [
        "評価点",          # 一覧の星評価スコア（例: 3.57）
        "企業ラベル",       # 受発注両方 / プレミアム など
        "認証・許可ラベル",  # 認証済｜法人 / インボイス登録あり / 建設業許可 / 社会保険 など
        "主力工事",        # 一覧
        "工事区分",        # 一覧（新築改修両方 など）
        "対応可能工事種別",  # 詳細
        "主な施工工事区分",  # 詳細
        "主な建物種別",     # 詳細
        "保有建設機材",     # 詳細
        "技術者資格保有状況",  # 詳細
        "対応可能エリア",    # 詳細
        "主要取引先",       # 詳細（取引先名・カテゴリの列挙）
        "建設業許可番号",    # 詳細
    ]

    # 詳細ページ会社概要 h4 ラベル → 取得先
    _SCHEMA_LABEL_MAP = {
        "資本金": Schema.CAP,
        "売上": Schema.SALES,
        "従業員数": Schema.EMP_NUM,
        "設立年月日": Schema.OPEN_DATE,
        "ウェブサイト": Schema.HP,
    }
    _EXTRA_LABEL_KEYS = {
        "対応可能工事種別", "主な施工工事区分", "主な建物種別",
        "保有建設機材", "技術者資格保有状況", "対応可能エリア",
        "主要取引先",
    }

    # ------------------------------------------------------------------
    # 一覧巡回
    # ------------------------------------------------------------------
    def parse(self, url: str) -> Generator[dict, None, None]:
        base_url = url.rstrip("/")
        page = self.START_PAGE
        while True:
            list_url = f"{base_url}/companies?page={page}"
            self.logger.info("一覧ページ取得: page=%d", page)

            try:
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning("一覧ページ取得失敗: %s (%s)", list_url, e)
                break

            if soup is None:
                self.logger.warning("soup取得失敗（スキップ）: page=%d", page)
                page += 1
                time.sleep(self.DELAY)
                continue

            # 初回ページで総件数を拾って進捗表示を有効化
            if self.total_items is None:
                self.total_items = self._extract_total(soup)

            items = soup.select("li.p-companies-list-item")
            if not items:
                break

            for li in items:
                try:
                    item = self._parse_item(li, base_url)
                except Exception as e:
                    self.logger.warning("一覧アイテム解析失敗: %s", e)
                    continue
                if item:
                    yield item

            # 「次へ」リンクがあれば継続
            has_next = any("次へ" in a.get_text() for a in soup.select("a"))
            if not has_next:
                break
            page += 1
            time.sleep(self.DELAY)

    @staticmethod
    def _extract_total(soup) -> int | None:
        el = soup.select_one(".c-pagination-entries__total")
        if el:
            m = re.search(r"([0-9,]+)", el.get_text())
            if m:
                return int(m.group(1).replace(",", ""))
        return None

    # ------------------------------------------------------------------
    # 一覧アイテム解析（＋詳細ページ取得）
    # ------------------------------------------------------------------
    def _parse_item(self, li, base_url: str) -> dict | None:
        name_a = li.select_one("a.p-companies-list-item__name")
        if not name_a:
            return None

        href = name_a.get("href", "")
        detail_url = base_url + href if href.startswith("/") else href
        item = {
            Schema.NAME: name_a.get_text(strip=True),
            Schema.URL: detail_url,
        }

        # 住所 → 都道府県 / 市区町村以降
        addr_div = li.select_one("div.p-companies-list-item__address")
        if addr_div:
            addr_raw = addr_div.get_text(strip=True)
            m = _PREF_RE.match(addr_raw)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr_raw[m.end():]
            else:
                item[Schema.ADDR] = addr_raw

        # 代表者名（"代表　梁川 貴正" → "梁川 貴正"）
        rep_div = li.select_one("div.p-companies-list-item__ceo-container .c-t-dark")
        if rep_div:
            rep_text = re.sub(r"^代表[\s　]*", "", rep_div.get_text(strip=True)).strip()
            if rep_text:
                item[Schema.REP_NM] = rep_text

        # 評価点（星評価スコア）
        score_div = li.select_one(".c-rating__score")
        if score_div:
            score = score_div.get_text(strip=True)
            if score:
                item["評価点"] = score

        # 企業ラベル（受発注両方 / プレミアム 等）
        header_labels = [
            s.get_text(strip=True)
            for s in li.select(".c-companies-header-labels__label-text")
            if s.get_text(strip=True)
        ]
        if header_labels:
            item["企業ラベル"] = " / ".join(header_labels)

        # 認証・許可ラベル
        cert_labels = [
            s.get_text(strip=True)
            for s in li.select(".c-companies-certified-labels__container span")
            if s.get_text(strip=True)
        ]
        if cert_labels:
            item["認証・許可ラベル"] = " / ".join(cert_labels)

        # 一覧の dl（業種 / 主力工事 / 工事区分）
        for dl in li.select("dl.p-companies-list-item__job-list-item"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if not (dt and dd):
                continue
            label = dt.get_text(strip=True)
            value = re.sub(r"[\s　]+", " ", dd.get_text(" ", strip=True)).strip("、 ")
            if not value:
                continue
            if label == "業種":
                item[Schema.CAT_SITE] = value
            elif label == "主力工事":
                item["主力工事"] = value
            elif label == "工事区分":
                item["工事区分"] = value

        # 詳細ページから会社概要を補完
        if detail_url:
            try:
                self._enrich_from_detail(detail_url, item)
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
            time.sleep(self.DELAY)

        return item

    # ------------------------------------------------------------------
    # 詳細ページ解析
    # ------------------------------------------------------------------
    def _enrich_from_detail(self, url: str, item: dict) -> None:
        soup = self.get_soup(url)
        if soup is None:
            return

        # 会社概要の h4 見出し → 直後の兄弟要素群（次の見出しまで）をまとめてテキスト化
        for h4 in soup.select("h4.p-companies-show-detail__heading--small"):
            label = h4.get_text(strip=True)
            if label not in self._SCHEMA_LABEL_MAP and label not in self._EXTRA_LABEL_KEYS:
                continue
            value = self._collect_until_heading(h4)
            if not value:
                continue
            if label in self._SCHEMA_LABEL_MAP:
                key = self._SCHEMA_LABEL_MAP[label]
                # HP はリンク href を優先
                if key == Schema.HP:
                    a = h4.find_next("a")
                    if a and a.get("href", "").startswith("http"):
                        value = a.get("href").strip()
                item.setdefault(key, value)
            else:
                item.setdefault(label, value)

        # 建設業許可番号（許認可セクションのテキストから抽出）
        m = _PERMIT_RE.search(soup.get_text(" ", strip=True))
        if m:
            item.setdefault("建設業許可番号", re.sub(r"\s+", "", m.group(1)))

        # 代表者名が一覧で取れていなければ詳細から補完
        if not item.get(Schema.REP_NM):
            for h4 in soup.select("h4.p-companies-show-detail__heading--small"):
                if h4.get_text(strip=True) == "代表者":
                    rep = self._collect_until_heading(h4)
                    if rep:
                        item[Schema.REP_NM] = rep
                    break

    @staticmethod
    def _collect_until_heading(h4) -> str:
        """h4 見出しの直後から、次の見出し(h2-h4)が現れるまでの兄弟テキストを連結する。"""
        parts: list[str] = []
        for sib in h4.find_next_siblings():
            name = getattr(sib, "name", None)
            if name in ("h2", "h3", "h4"):
                break
            if name is None:
                continue
            text = sib.get_text(" ", strip=True)
            if text:
                parts.append(text)
        value = " ".join(parts)
        value = value.replace("\xa0", " ")
        value = re.sub(r"[\s　]+", " ", value).strip()
        return value


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, default=1)
    args = parser.parse_args()

    scraper = TsukulinkListScraper()
    scraper.START_PAGE = args.start_page
    scraper.execute("https://tsukulink.net")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
