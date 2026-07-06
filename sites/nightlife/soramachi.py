"""
そら街ナイトワーク (soramachi.net) — キャバクラ/ラウンジ/ガールズバー等 ナイトワーク求人スクレイパー

取得対象:
    - 全国の掲載店舗求人 (推定 約1,500件 / 102ページ)
    - 店舗名 / 名称カナ / 都道府県 / 郵便番号 / 住所 / TEL / ジャンル(サイト定義業種) /
      営業時間 / 定休日 / HP / LINE / Instagram / X / Facebook / TikTok
    - サイト固有(構造化情報のみ): 職種 / 雇用形態 / 給与 / 最寄駅 / 携帯連絡先

取得フロー:
    1. 一覧(引数 url を起点、?pageno=N でページ送り)から求人詳細URL(/recruit/{id})を収集
    2. 各詳細ページの募集情報テーブル(.company-area table)と店舗詳細テーブル(.detail-info table)を
       1つのラベル辞書にマージして抽出
    3. 電話番号は AJAX (POST mode=ajax&sub_mode=tel_view) で JSON {tel, cell} を取得
    4. 詳細1件を取得するごとに即 yield (重複URL除外) — 全件収集はしない

備考(取得方針):
    - 求人本文(catch/コメント)・給料詳細・資格 等の自由記述プロースは著作権リスクのため取得しない

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/soramachi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id soramachi
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup, Tag

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_MAP_NOTE_RE = re.compile(r"\s*【GoogleMAPで場所を確認する】\s*")
_LINE_ID_RE = re.compile(r"LINE\s*ID\s*[：:]\s*([^\s　]+)")
_DETAIL_PATH_RE = re.compile(r"^/recruit/(\d+)")

# 詳細ページのテーブルから拾う EXTRA カラム(構造化情報のみ。プロースは除外)
_EXTRA_LABELS = {
    "職種": "職種",
    "雇用形態": "雇用形態",
    "給料": "給与",
    "最寄駅": "最寄駅",
}
_CELL_COLUMN = "携帯連絡先"


class SoramachiScraper(StaticCrawler):
    """そら街ナイトワーク スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = list(_EXTRA_LABELS.values()) + [_CELL_COLUMN]

    # ------------------------------------------------------------------ #
    # メインフロー (引数 url を唯一のルートとして使用)
    # ------------------------------------------------------------------ #

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1
        while True:
            page_url = self._page_url(url, page)
            soup = self.get_soup(page_url)
            if soup is None:
                break

            detail_urls = self._detail_urls(soup, url)
            if not detail_urls:
                break

            for detail_url in detail_urls:
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                try:
                    record = self._scrape_detail(detail_url)
                except Exception as e:  # 個別詳細の失敗は握りつぶして続行
                    self.logger.warning("詳細取得失敗: %s (%s)", detail_url, e)
                    continue
                if record:
                    self.logger.info(
                        "取得: %s (%s)",
                        record.get(Schema.NAME) or "?",
                        record.get(Schema.PREF) or "",
                    )
                    yield record

            page += 1

    # ------------------------------------------------------------------ #
    # 一覧
    # ------------------------------------------------------------------ #

    @staticmethod
    def _page_url(root_url: str, page: int) -> str:
        """引数 url を起点に pageno クエリだけを付与/更新する。"""
        parts = urlparse(root_url)
        query = [(k, v) for k, v in parse_qsl(parts.query) if k != "pageno"]
        if page > 1:
            query.append(("pageno", str(page)))
        return urlunparse(parts._replace(query=urlencode(query)))

    def _detail_urls(self, soup: BeautifulSoup, root_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("div.card a.box-link[href]"):
            href = a.get("href", "")
            if not _DETAIL_PATH_RE.match(href):
                continue
            full = urljoin(root_url, href)
            if full not in seen:
                seen.add(full)
                urls.append(full)
        return urls

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        detail_tbl = soup.select_one("div.detail-info table")
        info = self._label_map(soup)
        if detail_tbl is None and not info:
            return None

        name, kana = self._name_kana(detail_tbl, info)
        if not name:
            return None

        post_code, addr = self._split_address(info.get("所在地", ""))
        pref = self._pref(addr)
        sns = self._detect_sns(soup)
        tel, cell = self._fetch_tel(soup, url)

        record = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.CAT_SITE: self._clean(info.get("ジャンル") or info.get("業種", "")),
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.TIME: self._clean(info.get("営業時間", "")),
            Schema.HOLIDAY: self._clean(info.get("定休日", "")),
            Schema.HP: self._homepage(info),
            Schema.LINE: sns["line"],
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.TIKTOK: sns["tiktok"],
        }
        for label, col in _EXTRA_LABELS.items():
            record[col] = self._clean(info.get(label, ""))
        record[_CELL_COLUMN] = cell
        return record

    def _label_map(self, soup: BeautifulSoup) -> dict[str, str]:
        """募集情報(.company-area) と店舗詳細(.detail-info) の th/td を1つの辞書にマージ。"""
        info: dict[str, str] = {}
        for tbl in soup.select("div.company-area table, div.detail-info table"):
            for tr in tbl.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = self._clean(th.get_text())
                value = self._clean(_MAP_NOTE_RE.sub(" ", td.get_text(" ")))
                if label and label not in info:
                    info[label] = value
        return info

    def _name_kana(self, detail_tbl: Tag | None, info: dict[str, str]) -> tuple[str, str]:
        """店舗詳細テーブルの店舗名セル(<p>×2: 名称 / カナ)から名称とカナを取得。"""
        if detail_tbl is not None:
            for tr in detail_tbl.select("tr"):
                th = tr.find("th")
                if th and self._clean(th.get_text()) == "店舗名":
                    ps = [self._clean(p.get_text(" ")) for p in tr.find("td").select("p")]
                    ps = [p for p in ps if p]
                    if ps:
                        return ps[0], (ps[1] if len(ps) > 1 else "")
        # フォールバック: マージ済みラベル辞書
        return self._clean(info.get("店舗名", "")), ""

    def _homepage(self, info: dict[str, str]) -> str:
        val = self._clean(info.get("オフィシャルサイト", ""))
        return val if val.startswith("http") else ""

    def _detect_sns(self, soup: BeautifulSoup) -> dict[str, str]:
        """店舗詳細ブロック内のアンカー/テキストのみから SNS を判定 (フッター混入回避)。"""
        sns = {"insta": "", "x": "", "fb": "", "line": "", "tiktok": ""}
        block = soup.select_one("div.detail-info") or soup
        for a in block.select("a[href]"):
            href = a.get("href", "").strip()
            low = href.lower()
            if ("instagram.com" in low) and not sns["insta"]:
                sns["insta"] = href
            elif (
                ("//x.com" in low or "twitter.com" in low)
                and "intent" not in low
                and "share" not in low
                and not sns["x"]
            ):
                sns["x"] = href
            elif "facebook.com" in low and not sns["fb"]:
                sns["fb"] = href
            elif ("line.me" in low or "lin.ee" in low or href.startswith("line://")) and not sns["line"]:
                sns["line"] = href
            elif "tiktok.com" in low and not sns["tiktok"]:
                sns["tiktok"] = href
        # 「LINE ID：xxx」の掲載を優先採用
        m = _LINE_ID_RE.search(block.get_text(" "))
        if m:
            sns["line"] = m.group(1)
        return sns

    # ------------------------------------------------------------------ #
    # 電話番号 (AJAX POST で JSON {tel, cell} を取得)
    # ------------------------------------------------------------------ #

    def _fetch_tel(self, soup: BeautifulSoup, url: str) -> tuple[str, str]:
        link = soup.select_one("#telModalLink[data-id]")
        if link is None:
            return "", ""
        recruit_id = link.get("data-id", "").strip()
        if not recruit_id:
            return "", ""

        token_el = soup.select_one("meta[name='csrf-token']")
        headers = {"X-Requested-With": "XMLHttpRequest"}
        if token_el and token_el.get("content"):
            headers["X-CSRF-TOKEN"] = token_el["content"]

        payload = {
            "mode": "ajax",
            "sub_mode": "tel_view",
            # 電話番号表示に必要な必須項目 (ダミーの応募者情報)
            "name01": "問合", "name02": "希望",
            "kana01": "トイ", "kana02": "キボウ",
            "tel": "0000000000",
            "recruit_id": recruit_id,
            "shop_id": recruit_id,
        }
        try:
            resp = self.session.post(url, data=payload, headers=headers, timeout=self.TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self.logger.warning("電話番号取得失敗: %s (%s)", url, e)
            return "", ""
        return str(data.get("tel") or "").strip(), str(data.get("cell") or "").strip()

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #

    @classmethod
    def _split_address(cls, text: str) -> tuple[str, str]:
        text = cls._clean(_MAP_NOTE_RE.sub(" ", text or ""))
        if not text:
            return "", ""
        m = _POST_CODE_RE.search(text)
        post_code = ""
        if m:
            post_code = m.group(1)
            text = (text[: m.start()] + text[m.end():]).strip()
        # 郵便番号未登録などで残った単独の 〒 記号を除去
        text = cls._clean(text.lstrip("〒 ").strip())
        return post_code, text

    @staticmethod
    def _pref(address: str) -> str:
        m = _PREF_PATTERN.search(address)
        return m.group(1) if m else ""

    @staticmethod
    def _clean(value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SoramachiScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://soramachi.net/recruit?mode=search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
