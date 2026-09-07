"""
ツクリンクリスト (tsukulink.net) — 建設業者 一覧→詳細 スクレイパー（北陸3県版）

取得対象:
    富山県・石川県・福井県を拠点とする建設業者（富山 7,681 件 / 石川 8,568 件 /
    福井 6,229 件 = 計 22,478 件）。一覧ページで基本情報を取得し、各社の詳細
    ページに遷移して会社概要（資本金・従業員数・設立年月日・対応可能エリア・
    保有建設機材・技術者資格 等）の構造化情報を取得する。

    ※ 既存の `tsukulink`（一覧のみ）に対し、本クローラーは詳細ページまで
      巡回して会社概要の構造化フィールドを追加取得する拡充版。

取得フロー:
    /{pref_slug} （県別一覧, 1ページ20件, ?page=N で「次へ」がある限りページ送り）
      ├─ toyama → ishikawa → fukui の順で順次処理
      └─ 各社 /{pref_slug}/city_{code}/{company_id} （詳細）へ遷移
         → 1件取得するごとに即 yield（Pattern B）

注意:
    /{pref_slug} 一覧ページは Accept ヘッダが無いと HTTP 400 を返すため、
    _setup() でブラウザ相当の Accept / Accept-Language を付与している。

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
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県抽出パターン（東京都・北海道・〇〇府・〇〇県）
_PREF_RE = re.compile(r"^(東京都|北海道|(?:.+?[都道府県]))")

# 建設業許可番号（例: 茨城県知事許可-第30410号 / 国土交通大臣許可-第12345号）
_PERMIT_RE = re.compile(r"((?:\S+?知事|国土交通大臣)許可[-－]?第?\s*[0-9０-９]+\s*号)")

# 郵便番号（例: 〒916-0001）
_POSTCODE_RE = re.compile(r"〒\s*([0-9０-９]{3}[-－][0-9０-９]{4})")


class TsukulinkListScraper(StaticCrawler):
    """ツクリンクリスト 建設業者（北陸3県 一覧→詳細）スクレイパー"""

    DELAY = 1.5
    START_PAGE = 1  # 再開時はここを変更

    # 巡回対象の県（この順で順次処理）
    PREF_SLUGS = ["toyama", "ishikawa", "fukui"]

    EXTRA_COLUMNS = [
        "評価点",          # 一覧の星評価スコア（例: 3.57）
        "企業ラベル",       # 受発注両方 / プレミアム など
        "認証・許可ラベル",  # 認証済｜法人 / インボイス登録あり / 建設業許可 / 社会保険 など
        "インボイス登録有無",  # 認証・許可ラベルに「インボイス登録あり」を含むか（あり/なし）
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

    def _setup(self):
        """セッション初期化後、ブラウザ相当のヘッダを付与する。

        /{pref_slug} 一覧ページは Accept ヘッダが無いと HTTP 400 で拒否される。
        """
        super()._setup()
        self.session.headers.update({
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Upgrade-Insecure-Requests": "1",
        })

    # ------------------------------------------------------------------
    # 一覧巡回（富山 → 石川 → 福井）
    # ------------------------------------------------------------------
    def parse(self, url: str) -> Generator[dict, None, None]:
        base_url = url if url.endswith("/") else url + "/"

        for pref_slug in self.PREF_SLUGS:
            pref_url = urljoin(base_url, pref_slug)
            self.logger.info("県別一覧の巡回開始: %s", pref_url)
            yield from self._parse_pref(pref_url, base_url)

    def _parse_pref(self, pref_url: str, base_url: str) -> Generator[dict, None, None]:
        """1県分の一覧をページ送りしながら巡回する。"""
        page = self.START_PAGE
        while True:
            list_url = f"{pref_url}?page={page}"
            self.logger.info("一覧ページ取得: %s page=%d", pref_url, page)

            try:
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning("一覧ページ取得失敗: %s (%s)", list_url, e)
                break

            if soup is None:
                self.logger.warning("soup取得失敗（スキップ）: %s page=%d", pref_url, page)
                page += 1
                time.sleep(self.DELAY)
                continue

            # 各県の1ページ目で総件数を拾って進捗表示に加算
            if page == self.START_PAGE:
                total = self._extract_total(soup)
                if total:
                    self.total_items = (self.total_items or 0) + total

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
        detail_url = urljoin(base_url, href) if href else ""
        item = {
            Schema.NAME: name_a.get_text(strip=True),
            Schema.URL: detail_url,
        }

        # 住所 → 郵便番号 / 都道府県 / 市区町村以降
        addr_div = li.select_one("div.p-companies-list-item__address")
        if addr_div:
            self._set_address(item, addr_div.get_text(" ", strip=True))

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

        # 認証・許可ラベル ＋ インボイス登録有無
        cert_labels = [
            s.get_text(strip=True)
            for s in li.select(".c-companies-certified-labels__container span")
            if s.get_text(strip=True)
        ]
        if cert_labels:
            item["認証・許可ラベル"] = " / ".join(cert_labels)
        item["インボイス登録有無"] = (
            "あり" if any("インボイス登録あり" in lb for lb in cert_labels) else "なし"
        )

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

    @staticmethod
    def _set_address(item: dict, addr_raw: str) -> None:
        """住所文字列から 郵便番号 / 都道府県 / 市区町村以降 を分解して格納する。"""
        addr_raw = re.sub(r"[\s　]+", " ", addr_raw.replace("\xa0", " ")).strip()
        if not addr_raw:
            return

        m_zip = _POSTCODE_RE.search(addr_raw)
        if m_zip:
            item.setdefault(Schema.POST_CODE, m_zip.group(1).replace("－", "-"))
            addr_raw = _POSTCODE_RE.sub("", addr_raw, count=1).strip()

        m = _PREF_RE.match(addr_raw)
        if m:
            item.setdefault(Schema.PREF, m.group(1))
            rest = addr_raw[m.end():].strip()
            if rest:
                item.setdefault(Schema.ADDR, rest)
        elif addr_raw:
            item.setdefault(Schema.ADDR, addr_raw)

    # ------------------------------------------------------------------
    # 詳細ページ解析
    # ------------------------------------------------------------------
    def _enrich_from_detail(self, url: str, item: dict) -> None:
        soup = self.get_soup(url)
        if soup is None:
            return

        # 郵便番号付き住所（一覧には郵便番号が無いため詳細から補完）
        addr_div = soup.select_one(".p-companies-show-profile__info-address")
        if addr_div:
            self._set_address(item, addr_div.get_text(" ", strip=True))

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
        page_text = soup.get_text(" ", strip=True)
        m = _PERMIT_RE.search(page_text)
        if m:
            item.setdefault("建設業許可番号", re.sub(r"\s+", "", m.group(1)))

        # 郵便番号がまだ取れていなければページ全体から抽出
        if not item.get(Schema.POST_CODE):
            m_zip = _POSTCODE_RE.search(page_text)
            if m_zip:
                item[Schema.POST_CODE] = m_zip.group(1).replace("－", "-")

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
    scraper.execute("https://tsukulink.net/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
