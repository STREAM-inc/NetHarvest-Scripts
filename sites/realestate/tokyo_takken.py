# -*- coding: utf-8 -*-
"""
公益社団法人東京都宅地建物取引業協会 (東京都宅建協会) — 会員検索

対象サイト:
    https://www.tokyo-takken.or.jp/search-member

サイト構造 (2026-10 調査):
    起点ページ (/search-member) に
        - ブロック別会員一覧: /search-member/list/block1 〜 block12 (12 ブロック)
        - 区市町村別一覧    : /search-member/list/{ローマ字スラッグ}
    の 2 系統のリンクがある。両者は同一母集団を異なる切り口で分割したもので、
    12 ブロックの件数合計 = 16,700 件 = サイト公称の会員数と完全に一致するため、
    **ブロック別 12 本の巡回で東京都内の全会員を網羅できる**。
    (区市町村別ナビには 404 になる壊れたリンク `kokubunzi` が混ざっており、
     また島嶼部 `tokyototoshobu` がナビに出ないため、区市町村別だけでは取りこぼす)

    一覧は 1 ブロック = 1 ページ (ページネーション無し)。
        ul.result-list > li.result-list_item > div.result_detail
            div[data-title="所属ブロック"]   : 第一ブロック
            div[data-title="免許証番号"]     : 知事（1）113444 / 大臣（5）6367
            a  [data-title="商号または名称"] : 会員名 (href=/search-member/detail/{id})
            div[data-title="事務所所在地"]   : 市区町村以降の住所

    詳細 (/search-member/detail/{id}) は ul.detail_list の
    div.detail_content-title (ラベル) / .detail_content-text (値) の固定 12 項目:
        免許証番号 / 会員種別 / 所属ブロック / 商号または名称 / フリガナ /
        郵便番号 / 所在地 / 電話番号 / FAX番号 / ホームページ /
        代表者区分 / 代表者氏名
    (ホームページ・FAX は空のことが多いが、項目自体は常に存在する)

取得フロー (Pattern B / 早期 yield):
    parse(url) が引数 url を起点にブロックリンクを抽出 → 1 ブロックの一覧を取得 →
    1 会員ごとに詳細ページを取得して即 yield する。詳細 ID で重複排除。

    2026-10-09 修正: 初回実行で「第一ブロックの 2,371 件のみ取得・残り 11 ブロックが
    未処理」という不具合が発生した。原因はブロックごとのループ本体に例外処理が無く、
    いずれかのブロックの取得・パース中の例外でジェネレータ全体が異常終了し、以降の
    ブロックへ進めなかったため。対策として、ブロック単位・会員単位それぞれを
    try/except で囲み、1 ブロック/1 会員の失敗が全体を止めないようにした。

備考への対応:
    - 本店・支店区分: 専用フィールドはサイトに存在しない。支店・営業所は
      独立した 1 行 (独立した detail ID) として掲載されているため、商号末尾の
      「〜支店 / 〜営業所 / 〜出張所」と代表者区分 (政令使用人) から判定して
      EXTRA カラム「本支店区分」に格納する。本支店ともに 1 件ずつ取得する。
    - 事業内容 (賃貸管理/賃貸仲介/売買/開発): 一覧・詳細ともに掲載が無く取得不可。
    - 静的 HTML (StaticCrawler)。JS レンダリング不要。

実行方法:
    python scripts/sites/realestate/tokyo_takken.py
    docker compose exec worker python /app/bin/run_flow.py --site-id tokyo_takken
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from typing import Dict, Generator, List, Optional
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 備考: 東京都宅建協会 = 東京都内の会員のみ。都道府県は固定値。
PREFECTURE = "東京都"

# ブロックリンクが取得できなかった場合のフォールバック (第一〜第十二ブロック)
FALLBACK_BLOCKS = [f"block{i}" for i in range(1, 13)]

# 支店・営業所を示す商号サフィックス
_BRANCH_PATTERN = re.compile(r"(支店|支社|営業所|出張所|営業部|事業所)\s*$")

# 住所先頭の市区町村 (例: 千代田区 / 八王子市 / 奥多摩町 / 檜原村 / 大島町)
_CITY_PATTERN = re.compile(r"^([^\s0-9０-９]{1,8}?[区市町村])")


def _clean(text: Optional[str]) -> str:
    """全角スペース・連続空白を正規化してトリムする。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("　", " ")).strip()


class TokyoTakkenScraper(StaticCrawler):
    """東京都宅建協会 会員検索 — 12 ブロックを巡回して全会員を取得"""

    DELAY = 0.5

    EXTRA_COLUMNS = [
        "免許証番号",
        "会員種別",
        "所属ブロック",
        "市区町村",
        "本支店区分",
        "FAX",
    ]

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        root = url.rstrip("/")

        top = self.get_soup(root)
        block_urls = self._block_urls(top, root)
        logger.info("巡回対象ブロック数: %d", len(block_urls))

        seen_ids: set[str] = set()
        total = 0

        for block_url in block_urls:
            block_item_count = 0
            try:
                list_soup = self.get_soup(block_url)
                if list_soup is None:
                    logger.warning("一覧を取得できませんでした: %s", block_url)
                    continue

                # 進捗 ETA 用に総件数を積み上げる
                count = self._result_count(list_soup)
                if count:
                    total += count
                    self.total_items = total

                rows = list_soup.select("li.result-list_item div.result_detail")
                logger.info("一覧 %s: %d 件", block_url, len(rows))

                for row in rows:
                    try:
                        base = self._parse_row(row, root)
                        if base is None:
                            continue
                        member_id = base.pop("_id")
                        if member_id in seen_ids:
                            continue
                        seen_ids.add(member_id)

                        detail_url = urljoin(root + "/", f"detail/{member_id}")
                        detail = self._parse_detail(detail_url)

                        yield self._build_item(detail_url, base, detail)
                        block_item_count += 1
                    except Exception:
                        logger.exception(
                            "会員 1 件の取得に失敗しました (継続): block=%s", block_url
                        )
                        continue
            except Exception:
                logger.exception(
                    "ブロックの取得に失敗しました (次のブロックへ継続): %s", block_url
                )
                continue
            finally:
                logger.info(
                    "ブロック完了 %s: 取得 %d 件 (累計 seen_ids=%d)",
                    block_url,
                    block_item_count,
                    len(seen_ids),
                )

    # ------------------------------------------------------------------ #
    # 一覧
    # ------------------------------------------------------------------ #
    def _block_urls(self, soup, root: str) -> List[str]:
        """起点ページからブロック別一覧 (/search-member/list/blockN) の URL を抽出する。"""
        urls: List[str] = []
        if soup is not None:
            found: Dict[int, str] = {}
            for a in soup.select('a[href*="/search-member/list/block"]'):
                href = a.get("href") or ""
                m = re.search(r"/list/block(\d+)", href)
                if m:
                    found[int(m.group(1))] = urljoin(root + "/", href)
            urls = [found[k] for k in sorted(found)]

        if not urls or len(urls) < len(FALLBACK_BLOCKS):
            logger.warning(
                "ブロックリンクを %d 件しか抽出できなかったため、フォールバックの"
                " block1〜block12 で補完します (抽出できなかった分のみ追加)",
                len(urls),
            )
            existing_nums = set()
            for u in urls:
                m = re.search(r"/list/block(\d+)", u)
                if m:
                    existing_nums.add(int(m.group(1)))
            for slug in FALLBACK_BLOCKS:
                m = re.search(r"block(\d+)", slug)
                num = int(m.group(1)) if m else None
                if num is not None and num in existing_nums:
                    continue
                urls.append(urljoin(root + "/", f"list/{slug}"))
        return urls

    def _result_count(self, soup) -> Optional[int]:
        node = soup.select_one(".search_result-text_item")
        if node is None:
            return None
        digits = re.sub(r"[^0-9]", "", node.get_text())
        return int(digits) if digits else None

    def _parse_row(self, row, root: str) -> Optional[Dict[str, str]]:
        """一覧 1 行から ブロック / 免許証番号 / 商号 / 所在地 / 詳細 ID を取る。"""
        link = row.select_one('a[href*="/search-member/detail/"]')
        if link is None:
            return None
        m = re.search(r"/detail/(\d+)", link.get("href") or "")
        if m is None:
            return None

        def cell(title: str) -> str:
            node = row.select_one(f'[data-title="{title}"]')
            return _clean(node.get_text()) if node else ""

        name = _clean(link.get_text())
        if not name:
            return None

        return {
            "_id": m.group(1),
            "name": name,
            "block": cell("所属ブロック"),
            "license_no": cell("免許証番号"),
            "addr": cell("事務所所在地"),
        }

    # ------------------------------------------------------------------ #
    # 詳細
    # ------------------------------------------------------------------ #
    def _parse_detail(self, detail_url: str) -> Dict[str, str]:
        """詳細ページのラベル/値ペアを dict で返す。取得失敗時は空 dict。"""
        try:
            soup = self.get_soup(detail_url)
        except Exception:
            logger.exception("詳細ページの取得に失敗しました: %s", detail_url)
            return {}
        if soup is None:
            return {}

        data: Dict[str, str] = {}
        for content in soup.select("ul.detail_list div.detail_content"):
            label_node = content.select_one(".detail_content-title")
            if label_node is None:
                continue
            label = _clean(label_node.get_text())
            value_node = content.select_one(".detail_content-text")
            value = _clean(value_node.get_text()) if value_node else ""

            # ホームページは本文が空でも href に URL が入る場合があるため補完する
            if not value and value_node is not None and value_node.name == "a":
                href = (value_node.get("href") or "").strip()
                if href.startswith("http"):
                    value = href
            data[label] = value
        return data

    # ------------------------------------------------------------------ #
    # 組み立て
    # ------------------------------------------------------------------ #
    def _build_item(self, detail_url: str, base: Dict[str, str], detail: Dict[str, str]) -> Dict[str, str]:
        name = detail.get("商号または名称") or base["name"]
        addr = detail.get("所在地") or base["addr"]
        hp = detail.get("ホームページ", "")
        if hp and not hp.startswith("http"):
            hp = ""

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: detail.get("フリガナ", ""),
            Schema.PREF: PREFECTURE,
            Schema.POST_CODE: detail.get("郵便番号", ""),
            Schema.ADDR: addr,
            Schema.TEL: detail.get("電話番号", ""),
            Schema.REP_NM: detail.get("代表者氏名", ""),
            Schema.POS_NM: detail.get("代表者区分", ""),
            Schema.HP: hp,
            "免許証番号": detail.get("免許証番号") or base["license_no"],
            "会員種別": detail.get("会員種別", ""),
            "所属ブロック": detail.get("所属ブロック") or base["block"],
            "市区町村": self._city(addr),
            "本支店区分": self._branch_type(name, detail.get("代表者区分", "")),
            "FAX": detail.get("FAX番号", ""),
        }

    def _city(self, addr: str) -> str:
        m = _CITY_PATTERN.match(addr or "")
        return m.group(1) if m else ""

    def _branch_type(self, name: str, rep_type: str) -> str:
        """商号サフィックスと代表者区分から本店/支店を判定する。

        支店・営業所は商号に明記され、代表者区分が「政令使用人」になる。
        """
        if _BRANCH_PATTERN.search(name or ""):
            return "支店"
        if rep_type and rep_type != "代表者":
            return "支店"
        return "本店"


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TokyoTakkenScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.tokyo-takken.or.jp/search-member")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
