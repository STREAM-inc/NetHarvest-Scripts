"""
Re就活30 — 30代向け転職・求人情報 (re-katsu30.jp)

取得対象（すべて一覧ページから取得）:
    検索結果一覧の各カードに 会社名・業種カテゴリ・雇用形態・年収範囲・勤務地 が
    揃っているため、詳細ページは開かない（高速・タイムアウト回避）。
    勤務地テキストからは都道府県・住所を best-effort で抽出する。
    ※ 代表者・設立・従業員数・資本金・売上高・郵便番号・HP は一覧には無く
      詳細ページ専用のため、本実装では取得しない（取得には別途詳細巡回が必要）。

★ WAF について（重要 / 切り分け済み）
    re-katsu30.jp は AWS WAF が「送信元 IP」でブロックする（IP 起因）。
    - UA / ヘッダは無関係。クリーン IP からは素の requests でも 200 で全件取得可。
    - データセンタ帯（AWS 等）の IP は 403。→ Playwright に替えても同 IP なら 0 件。
    成功条件は「クリーン IP からの egress」だけ。よって本実装はプロキシ対応を持つ。
        RE30_PROXY=http://user:pass@host:port  （JP 系 / レジデンシャル推奨）
    プロキシ無しでブロックされた場合は黙って 0 件にせず RuntimeError で止める。

実行方法:
    # ローカルテスト（要 RE30_PROXY、または非ブロック IP）
    RE30_PROXY=http://... python scripts/sites/jobs/re30.py

    # Prefect Flow 経由
    docker compose exec -e RE30_PROXY=http://... worker python /app/bin/run_flow.py --site-id re30
"""

import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 正常時は ~74 ページ (全1107件 / 15件)。next リンク消失で自然終端するが、保険の上限。
_MAX_PAGES = 200


def _parse_location(text: str) -> tuple[str, str]:
    """一覧カードの「勤務地」テキストから (都道府県, 住所) を best-effort で抽出する。

    例:
        「本社（東京都練馬区）／西武池袋線…」 → ("東京都", "練馬区")
        「東京都渋谷区恵比寿1-23-23 …」      → ("東京都", "渋谷区恵比寿1-23-23 …")
        「東京都」                            → ("東京都", "")
    都道府県が含まれない（「全国のプロジェクト先」等）場合は ("", "") を返す。
    """
    text = re.sub(r"[\s　]+", " ", text).strip()
    pref_m = _PREF_PATTERN.search(text)
    if not pref_m:
        return "", ""
    pref = pref_m.group(1)
    addr = text[pref_m.end():]
    # 「本社（東京都練馬区）」のような囲み・路線アクセス情報を住所から切り落とす
    addr = re.split(r"[）)／「(（]", addr)[0].strip()
    return pref, addr


def _looks_waf_blocked(soup) -> bool:
    """WAF / ELB の 403 ブロックページか判定する。"""
    if soup is None:
        return False
    # 正常ページには必ず検索結果コンテナがある
    if soup.select_one(".searchResult__list, article.searchResult__item"):
        return False
    text = soup.get_text(" ", strip=True)[:600].lower()
    markers = ("403", "forbidden", "request blocked", "access denied", "akamai", "cloudfront")
    return any(m in text for m in markers) or len(text) < 200


class Re30Crawler(DynamicCrawler):
    """Re就活30 クローラー"""

    DELAY = 2.0
    # 業種カテゴリは Schema.CAT_SITE (標準カラム) に格納するため EXTRA には含めない。
    # Schema に無い 雇用形態・年収範囲、および勤務地の原文を追加する。
    EXTRA_COLUMNS = ["雇用形態", "年収範囲", "勤務地"]

    # ★ WAF は IP 起因。クリーン IP の egress をプロキシで与えると成功する。
    #   DynamicCrawler 側が PROXY 属性を読んで Playwright 起動時に渡す想定。
    #   もし基底が別名（PROXIES 等）なら 1 行合わせるだけ。
    PROXY = os.environ.get("RE30_PROXY")

    def parse(self, url: str):
        page_url = url
        page_no = 1
        yielded = 0
        seen_pages: set[str] = set()

        while True:
            # ★ 無限ループ保険: 同一URL再訪 / ページ上限で打ち切る。
            if page_url in seen_pages:
                self.logger.warning("同一ページURLを再訪。巡回を打ち切ります: %s", page_url)
                break
            seen_pages.add(page_url)
            if page_no > _MAX_PAGES:
                self.logger.warning("ページ上限 %d に到達。巡回を打ち切ります。", _MAX_PAGES)
                break

            soup = self.get_soup(page_url)
            if soup is None:
                break

            items = soup.select("article.searchResult__item")
            if not items:
                # ★ 黙って 0 件で終わらせない。WAF ブロックは明示エラーにする。
                if page_no == 1 and _looks_waf_blocked(soup):
                    raise RuntimeError(
                        "WAF にブロックされました（IP 起因）。RE30_PROXY にクリーン IP の"
                        "プロキシを設定するか、非ブロック IP のホストで実行してください。"
                        f" url={page_url}"
                    )
                break

            if page_no == 1:
                total_el = soup.select_one(".pageNumber__all")
                if total_el:
                    m = re.search(r"(\d[\d,]+)", total_el.get_text())
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))

            for item in items:
                try:
                    name_el = item.select_one("p.featuredJob__item__company")
                    name = name_el.get_text(strip=True) if name_el else ""

                    cats = item.select("ul.featuredJob__item__categories li")
                    cat_site = "/".join(c.get_text(strip=True) for c in cats)

                    conditions = item.select("ul.scoutDetail__info__tagList li.-condition")
                    employ_type = conditions[0].get_text(strip=True) if len(conditions) > 0 else ""
                    salary_range = conditions[1].get_text(strip=True) if len(conditions) > 1 else ""

                    detail_a = item.select_one("h2.featuredJob__item__title a")
                    href = detail_a.get("href", "") if detail_a else ""
                    # ★ URL は現在ページ url から派生させる（ルートをハードコードしない）。
                    detail_url = urljoin(page_url, href) if href else ""

                    # 勤務地は dl.details__list の <dt>勤務地</dt><dd>…</dd> に入る。
                    work_location = ""
                    for dt in item.select("div.featuredJob__item__details dt.details__heading"):
                        if dt.get_text(strip=True) == "勤務地":
                            dd = dt.find_next_sibling("dd")
                            if dd:
                                work_location = dd.get_text(" ", strip=True)
                            break

                    pref, addr = _parse_location(work_location)

                    row = {
                        Schema.NAME: name,
                        Schema.CAT_SITE: cat_site,
                        "雇用形態": employ_type,
                        "年収範囲": salary_range,
                        "勤務地": work_location,
                        Schema.PREF: pref,
                        Schema.ADDR: addr,
                        Schema.URL: detail_url,
                    }

                    yield row
                    yielded += 1
                except Exception as e:
                    self.logger.warning(f"page {page_no}: item skip — {e}")
                    continue

            self.logger.info("page %d done (累計 %d 件)", page_no, yielded)

            next_a = soup.select_one(".pager li.next a")
            if not next_a:
                break
            next_href = next_a.get("href", "")
            if not next_href:
                break
            # ★ next は「?...&offset=N」形式の相対 query。ルート url から派生させる。
            page_url = urljoin(page_url, next_href)
            page_no += 1
            time.sleep(self.DELAY)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if not os.environ.get("RE30_PROXY"):
        logging.warning(
            "RE30_PROXY 未設定。データセンタ IP から実行すると WAF で 0 件になります。"
        )

    scraper = Re30Crawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://re-katsu30.jp/search/result?income%5B0%5D=&income%5B1%5D=&btn_search=1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
