"""
求人ボックス（ナイトワーク系求人）スクレイパー

取得対象:
    キャバクラ・ガールズバー・コンカフェ・ホスト・スナック・クラブ・ラウンジ・
    ナイトワーク・夜職・フロアレディ・カウンターレディ・黒服・ボーイ・
    バーテンダー・メンズキャバクラ・送迎ドライバー 等 全国求人

取得フロー:
    7つのキーワードグループで /adv/ 検索 → 各グループをページネーション巡回
    → カード解析（一覧データ）+ 内部詳細ページ補完（TEL・勤務時間）
    → URL重複除外・ナイトワーク判定フィルター適用

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/xn_pckua2a7gp15o89zb_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id xn_pckua2a7gp15o89zb_2
"""

import html
import json
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_DETAIL_PATH_RE = re.compile(r"^/(jb|jbi|jbn)/[0-9a-z]+$", re.I)

_NIGHTWORK_RE = re.compile(
    r"キャバクラ|ガールズバー|コンカフェ|ホスト|スナック|クラブ|ラウンジ|"
    r"ナイトワーク|夜職|フロアレディ|カウンターレディ|黒服|メンズキャバクラ|"
    r"ホステス|水商売|ナイトクラブ|パブ|セクキャバ|送迎ドライバー|"
    r"バーテンダー|ボーイ|キャスト|夜のお仕事"
)

KEYWORD_GROUPS = [
    "キャバクラ or:ガールズバー or:コンカフェ or:フロアレディ or:カウンターレディ",
    "ホスト or:スナック or:クラブ or:ラウンジ or:メンズキャバクラ",
    "ナイトワーク or:夜職 or:黒服 or:ボーイ",
    "バーテンダー 夜",
    "バー ナイトワーク",
    "ホールスタッフ ナイトワーク",
    "送迎ドライバー ナイトワーク",
]


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _extract_pref(text: str) -> tuple[str, str]:
    text = _clean(text)
    if not text:
        return "", ""
    m = _PREF_PATTERN.search(text)
    if m:
        return m.group(1), text[m.end():].strip()
    return "", text


def _safe_json_loads(text: str) -> dict:
    try:
        return json.loads(text)
    except Exception:
        return {}


def _is_internal_detail_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    return bool(_DETAIL_PATH_RE.match(parsed.path))


def _is_nightwork(title: str, company: str, work_area: str, employ_type: str) -> bool:
    text = f"{title} {company} {work_area} {employ_type}"
    return bool(_NIGHTWORK_RE.search(text))


class XnPckua2a7gp15o89zb2Scraper(StaticCrawler):
    """求人ボックス ナイトワーク系求人スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "給与",
        "勤務地",
        "勤務時間",
        "検索キーワード",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        search_base = urljoin(url, "adv/")
        seen_urls: set[str] = set()
        total_set = False

        for kw_group in KEYWORD_GROUPS:
            kw_enc = urllib.parse.quote(kw_group)
            search_url = f"{search_base}?keyword={kw_enc}&area="
            page = 1

            while True:
                page_url = f"{search_url}&pg={page}" if page > 1 else search_url
                soup = self.get_soup(page_url)
                if soup is None:
                    self.logger.warning("一覧取得失敗: %s", page_url)
                    break

                if not total_set and page == 1:
                    num_el = soup.select_one("div.p-resultArea_num")
                    if num_el:
                        m = re.search(r"([\d,]+)\s*件", num_el.get_text(" ", strip=True))
                        if m:
                            self.total_items = int(m.group(1).replace(",", ""))
                            total_set = True

                cards = soup.select("section.p-result_card")
                if not cards:
                    self.logger.info("pg%d [%s]: カードなし、終了", page, kw_group[:20])
                    break

                page_count = 0
                for card in cards:
                    try:
                        item = self._parse_card(card, page_url, kw_group)
                        if not item:
                            continue

                        dedup_key = _clean(item.get(Schema.URL, ""))
                        if not dedup_key or dedup_key in seen_urls:
                            continue

                        if not _is_nightwork(
                            item.get("求人タイトル", ""),
                            item.get(Schema.NAME, ""),
                            item.get("勤務地", ""),
                            item.get("雇用形態", ""),
                        ):
                            continue

                        seen_urls.add(dedup_key)
                        page_count += 1
                        yield item

                    except Exception as e:
                        self.logger.warning("カード解析失敗: %s", e)
                        continue

                self.logger.info("pg%d [%s]: %d件出力", page, kw_group[:20], page_count)

                if not soup.select_one("a.c-pager_btn--next"):
                    self.logger.info("pg%d [%s]: 次ページなし、終了", page, kw_group[:20])
                    break

                page += 1

    def _parse_card(self, card, list_url: str, kw_group: str) -> dict | None:
        a_tag = card.select_one("h2.p-result_title--ver2 a.p-result_title_link")
        if not a_tag:
            return None

        href = a_tag.get("href", "")
        raw_url = urljoin(list_url, href) if href else ""
        preview = self._extract_preview_json(a_tag)

        title = _clean(
            preview.get("title")
            or preview.get("originalTitle")
            or (
                a_tag.select_one("span.p-result_name").get_text(" ", strip=True)
                if a_tag.select_one("span.p-result_name")
                else a_tag.get_text(" ", strip=True)
            )
        )
        company = _clean(
            preview.get("company")
            or self._sel_text(card, "p.p-result_company")
        )
        work_area = _clean(
            preview.get("workArea")
            or self._sel_text(card, "li.p-result_area")
        )
        payment = _clean(
            preview.get("payment")
            or self._sel_text(card, "li.p-result_pay")
        )
        employ_type = _clean(
            preview.get("employType")
            or self._sel_text(card, "li.p-result_employType")
        )

        pref, addr = _extract_pref(work_area)

        item: dict = {
            Schema.URL: raw_url or list_url,
            Schema.NAME: company,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: "",
            "求人タイトル": title,
            "雇用形態": employ_type,
            "給与": payment,
            "勤務地": work_area,
            "勤務時間": "",
            "検索キーワード": kw_group,
        }

        # 内部詳細ページURL判定
        detail_url = ""
        rd_url = _clean(preview.get("rdUrl", ""))
        if rd_url:
            rd_abs = urljoin(list_url, rd_url)
            if _is_internal_detail_url(rd_abs):
                detail_url = rd_abs

        if not detail_url and _is_internal_detail_url(raw_url):
            detail_url = raw_url

        if detail_url:
            detail = self._scrape_detail(detail_url)
            if detail.get(Schema.TEL):
                item[Schema.TEL] = detail[Schema.TEL]
            if detail.get("勤務時間"):
                item["勤務時間"] = detail["勤務時間"]
            if detail.get("給与") and not item.get("給与"):
                item["給与"] = detail["給与"]
            if detail.get("勤務地"):
                item["勤務地"] = detail["勤務地"]
                if not item.get(Schema.PREF):
                    pref2, addr2 = _extract_pref(detail["勤務地"])
                    if pref2:
                        item[Schema.PREF] = pref2
                        item[Schema.ADDR] = addr2

        # 外部求人URLがあれば Schema.URL を上書き
        outer_url = _clean(preview.get("url", ""))
        if outer_url:
            item[Schema.URL] = outer_url

        if not title:
            return None

        return item

    def _extract_preview_json(self, a_tag) -> dict:
        raw = a_tag.get("data-func-show-arg", "")
        if not raw:
            return {}
        outer = _safe_json_loads(raw)
        inner = outer.get("json")
        if not inner:
            return {}
        if isinstance(inner, dict):
            return inner
        try:
            inner = html.unescape(inner)
            return _safe_json_loads(inner)
        except Exception:
            return {}

    def _scrape_detail(self, url: str) -> dict:
        soup = self.get_soup(url)
        if soup is None:
            return {}

        data: dict = {}

        tel_el = soup.select_one("span.p-detail_tel_num")
        if tel_el:
            tel = _clean(tel_el.get_text(" "))
            if tel:
                data[Schema.TEL] = tel

        for dl in soup.select("dl.p-detail_table"):
            dts = dl.select("dt.p-detail_table_title")
            dds = dl.select("dd.p-detail_table_data")
            for dt, dd in zip(dts, dds):
                key = _clean(dt.get_text(" "))
                val = _clean(dd.get_text(" "))
                if not key or not val:
                    continue

                if "勤務地" in key:
                    data["勤務地"] = val
                elif any(w in key for w in ["給与", "月給", "時給", "年収", "日給"]):
                    if not data.get("給与"):
                        data["給与"] = val
                elif "雇用形態" in key:
                    if not data.get("雇用形態"):
                        data["雇用形態"] = val
                elif "勤務時間" in key:
                    data["勤務時間"] = val

        return data

    def _sel_text(self, root, selector: str) -> str:
        el = root.select_one(selector)
        return _clean(el.get_text(" ", strip=True)) if el else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = XnPckua2a7gp15o89zb2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://xn--pckua2a7gp15o89zb.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
