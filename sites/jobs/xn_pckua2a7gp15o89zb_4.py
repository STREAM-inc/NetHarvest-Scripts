"""
求人ボックス（建設・土木の仕事）スクレイパー

取得対象:
- 求人ボックスのキーワード検索「建設の仕事」一覧 (25件/ページ)
- 一覧カードの埋め込み JSON + 詳細ページ (/jbn/, /jbi/) の補完

取得カラム:
- 取得日時 / 取得URL (掲載URL)
- 名称 (会社名) / 都道府県 / 住所 / TEL
- サイト定義業種・ジャンル (職種カテゴリ)
- 求人タイトル / 雇用形態 / 給与 / 勤務地 / 勤務時間 / 特徴 / 更新日

備考:
- 建設・土木に無関係な求人 (検索キーワードのゆらぎで混入するもの) は
  _is_construction() のキーワード判定で除外する。
- 仕事内容・応募資格・給与補足などの長文プロースは著作権リスクのため取得しない。
- robots.txt で /jb/ と /rd/ は Disallow のため、詳細は Allow されている
  /jbi/ /jbn/ のみを取得する。
"""

import html
import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# robots.txt で Allow されている詳細パス (/jb/ と /rd/ は Disallow)
_DETAIL_PATH_RE = re.compile(r"^/(jbi|jbn)/[0-9a-z]+$", re.I)
# robots.txt が Disallow している詳細パス (掲載URLとしては記録するが取得はしない)
_DISALLOWED_DETAIL_PATH_RE = re.compile(r"^/jb/[0-9a-z]+$", re.I)

# 建設・土木カテゴリ判定用キーワード
_CONSTRUCTION_JOBTYPES = ("建築・土木・建設工事",)
_CONSTRUCTION_KEYWORDS = (
    "建設", "土木", "建築", "施工", "工事", "現場", "解体", "とび", "鳶",
    "型枠", "鉄筋", "内装", "外装", "塗装", "足場", "基礎",
    "舗装", "掘削", "配管", "電気工事", "設備工事", "大工", "職人", "重機",
    "クレーン", "ゼネコン", "住宅", "リフォーム", "測量", "土工", "躯体",
)

_TEL_RE = re.compile(r"0\d{1,4}[-−－]?\d{1,4}[-−－]?\d{3,4}")

# 「8:00〜17:00」形式の時間帯だけを拾う (前後の自由記述は取らない)
_WORKTIME_RE = re.compile(r"\d{1,2}\s*[:：]\s*\d{2}\s*[〜～~ー\-–]\s*\d{1,2}\s*[:：]\s*\d{2}")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _extract_pref(text: str) -> str:
    text = _clean(text)
    if not text:
        return ""
    m = _PREF_PATTERN.search(text)
    return m.group(1) if m else ""


def _strip_pref(addr: str) -> str:
    """住所から先頭の都道府県を取り除いた市区町村以降を返す。"""
    addr = _clean(addr)
    if not addr:
        return ""
    m = _PREF_PATTERN.match(addr)
    if m:
        rest = addr[m.end():].strip()
        # 「京都府京都府京都市…」のような重複表記を吸収
        m2 = _PREF_PATTERN.match(rest)
        if m2:
            rest = rest[m2.end():].strip()
        return rest or addr
    return addr


def _safe_json_loads(text: str) -> dict:
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _norm_value(v) -> str:
    """埋め込み JSON の 'None' / 'null' 文字列を空に正規化する。"""
    s = _clean(v)
    if s in ("None", "null", "undefined"):
        return ""
    return s


def _parse_tags(raw: str) -> str:
    """"['未経験OK', '急募']" 形式のタグ文字列を ' / ' 連結に変換する。"""
    raw = _clean(raw)
    if not raw:
        return ""
    tags = re.findall(r"'([^']+)'", raw) or re.findall(r'"([^"]+)"', raw)
    if tags:
        return " / ".join(t.strip() for t in tags if t.strip())
    return raw.strip("[]")


def _extract_worktime(text: str) -> str:
    """自由記述の勤務時間ブロックから時間帯 (例: 8:00〜17:00) だけを抽出する。

    休日・待遇などの長文プロースは著作権リスクのため取り込まない。
    """
    text = _clean(text)
    if not text:
        return ""
    ranges: list[str] = []
    for m in _WORKTIME_RE.finditer(text):
        v = re.sub(r"\s+", "", m.group(0))
        if v not in ranges:
            ranges.append(v)
        if len(ranges) >= 3:
            break
    return " / ".join(ranges)


def _parse_date(raw: str) -> str:
    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", _clean(raw))
    if not m:
        return ""
    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


class KyujinBoxKensetsuScraper(StaticCrawler):
    """求人ボックス（建設・土木の仕事）スクレイパー"""

    DELAY = 1.0
    # 長文プロース (仕事内容・応募資格・給与補足など) は著作権リスクのため取得しない
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "給与",
        "勤務地",
        "勤務時間",
        "特徴",
        "更新日",
    ]

    # 勤務時間などの構造化想定カラムに長文が入った場合の足切り文字数
    MAX_SHORT_LEN = 60

    # 一覧カードの約85%は詳細が /jb/ 配下 = robots.txt で Disallow のため取得しない。
    # その結果 TEL・住所 は /jbi/ /jbn/ の求人 (約15%) でのみ埋まる。
    # 既存の求人ボックス系スクレイパーと同様に /jb/ も取得する場合は True にする。
    CRAWL_DISALLOWED_DETAIL = False
    MAX_PAGES = 2000

    def parse(self, url: str) -> Generator[dict, None, None]:
        root = url.rstrip("/") if url.endswith("/") else url
        seen: set[str] = set()

        for page in range(1, self.MAX_PAGES + 1):
            sep = "&" if "?" in root else "?"
            list_url = root if page == 1 else f"{root}{sep}pg={page}"

            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧取得失敗: %s", list_url)
                break

            if page == 1:
                num_el = soup.select_one("div.p-resultArea_num")
                if num_el:
                    m = re.search(r"([\d,]+)\s*件", num_el.get_text(" ", strip=True))
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))

            cards = soup.select("section.p-result_card")
            if not cards:
                self.logger.info("pg%d: カードなし、終了", page)
                break

            page_count = 0
            for card in cards:
                try:
                    item = self._parse_card(card, root)
                except Exception as e:  # 1件の失敗で全体を止めない
                    self.logger.warning("カード解析失敗: %s", e)
                    continue

                if not item:
                    continue

                key = _clean(item.get(Schema.URL)) or "|".join(
                    [
                        _clean(item.get(Schema.NAME)),
                        _clean(item.get("求人タイトル")),
                        _clean(item.get("勤務地")),
                    ]
                )
                if not key or key in seen:
                    continue
                seen.add(key)
                page_count += 1
                yield item

            self.logger.info("pg%d: %d件出力", page, page_count)

            if not soup.select_one("a.c-pager_btn--next"):
                self.logger.info("pg%d: 次ページなし、終了", page)
                break

    # ------------------------------------------------------------------ 一覧
    def _parse_card(self, card, root: str) -> dict | None:
        a_tag = card.select_one("a.p-result_title_link")
        if not a_tag:
            return None

        preview = self._extract_preview_json(a_tag)

        title = _norm_value(
            preview.get("originalTitle")
            or preview.get("title")
            or self._sel_text(card, "span.p-result_name")
            or a_tag.get_text(" ", strip=True)
        )
        company = _norm_value(
            preview.get("company")
            or preview.get("siteName")
            or self._sel_text(card, "p.p-result_company")
        )
        work_area = _norm_value(
            preview.get("workArea") or self._sel_text(card, "li.p-result_area")
        )
        job_type = _norm_value(preview.get("jobType"))

        if not title and not company:
            return None

        if not self._is_construction(title, job_type, preview):
            return None

        detail_url = self._detail_url(preview, a_tag, root)
        # 取得しない /jb/ 配下の求人も掲載URLだけは求人単位で残す
        page_url = self._page_url(preview, root)

        item = {
            Schema.URL: page_url,
            Schema.NAME: company,
            Schema.PREF: _extract_pref(work_area),
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.CAT_SITE: job_type,
            "求人タイトル": title,
            "雇用形態": _norm_value(preview.get("employType"))
            or self._sel_text(card, "li.p-result_employType"),
            "給与": _norm_value(preview.get("payment"))
            or self._sel_text(card, "li.p-result_pay"),
            "勤務地": work_area,
            "勤務時間": "",
            "特徴": _parse_tags(preview.get("allFeatureTags") or preview.get("featureTagSp") or ""),
            "更新日": _parse_date(preview.get("updatedAt") or ""),
        }

        if detail_url:
            detail = self._scrape_detail(detail_url)
            for k, v in detail.items():
                if _clean(v) and not _clean(item.get(k)):
                    item[k] = v

        if not item[Schema.PREF]:
            item[Schema.PREF] = _extract_pref(item.get(Schema.ADDR, "")) or _extract_pref(
                item.get("勤務地", "")
            )
        if not _clean(item.get(Schema.NAME)):
            return None

        return item

    def _is_construction(self, title: str, job_type: str, preview: dict) -> bool:
        """建設・土木に関連する求人かをキーワードで判定する。

        jobType は 8 割方が空のため、jobType 一致 or タイトル等のキーワード一致で判定する。
        """
        if job_type and job_type in _CONSTRUCTION_JOBTYPES:
            return True

        haystack = " ".join(
            [
                title,
                job_type,
                _norm_value(preview.get("formatTitle")),
                _norm_value(preview.get("firstYamlHead")),
            ]
        )
        return any(kw in haystack for kw in _CONSTRUCTION_KEYWORDS)

    def _extract_preview_json(self, a_tag) -> dict:
        raw = a_tag.get("data-func-show-arg", "")
        if not raw:
            return {}
        outer = _safe_json_loads(raw)
        inner = outer.get("json")
        if isinstance(inner, dict):
            return inner
        if isinstance(inner, str):
            return _safe_json_loads(html.unescape(inner))
        return {}

    def _detail_url(self, preview: dict, a_tag, root: str) -> str:
        """取得してよい詳細ページ URL を返す (無ければ空文字)。"""
        for cand in (_norm_value(preview.get("rdUrl")), _clean(a_tag.get("href", ""))):
            if not cand:
                continue
            abs_url = urljoin(root, cand)
            parsed = urlparse(abs_url)
            if parsed.netloc != urlparse(root).netloc:
                continue
            allowed = bool(_DETAIL_PATH_RE.match(parsed.path)) or (
                self.CRAWL_DISALLOWED_DETAIL
                and bool(_DISALLOWED_DETAIL_PATH_RE.match(parsed.path))
            )
            if allowed:
                return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return ""

    def _page_url(self, preview: dict, root: str) -> str:
        """取得可否によらず、求人ごとの掲載 URL (パーマリンク) を組み立てる。"""
        rd_url = _norm_value(preview.get("rdUrl"))
        if rd_url:
            parsed = urlparse(urljoin(root, rd_url))
            if parsed.netloc == urlparse(root).netloc and (
                _DETAIL_PATH_RE.match(parsed.path)
                or _DISALLOWED_DETAIL_PATH_RE.match(parsed.path)
            ):
                return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        unique_id = _norm_value(preview.get("uniqueId"))
        return urljoin(root, f"/jb/{unique_id}") if unique_id else root

    # ------------------------------------------------------------------ 詳細
    def _scrape_detail(self, url: str) -> dict:
        soup = self.get_soup(url)
        if soup is None:
            return {}

        data: dict = {}

        company_el = soup.select_one("p.p-detail_head_company")
        if company_el:
            data[Schema.NAME] = _clean(company_el.get_text(" "))

        title_el = soup.select_one("p.p-detail_head_title, h1.p-detail_head_title")
        if title_el:
            data["求人タイトル"] = _clean(title_el.get_text(" "))

        tel_el = soup.select_one("span.p-detail_tel_num")
        tel = _clean(tel_el.get_text(" ")) if tel_el else ""
        if not tel:
            m = _TEL_RE.search(soup.get_text(" "))
            tel = m.group(0) if m else ""
        if tel:
            data[Schema.TEL] = tel

        for dt in soup.select("dt.p-detail_table_title"):
            label = _clean(dt.get_text(" "))
            dd = dt.find_next_sibling("dd")
            if not label or dd is None:
                continue

            head = self._dd_head_text(dd)
            if not head:
                continue

            if "勤務地" in label:
                addr = head
                data[Schema.ADDR] = _strip_pref(addr)
                pref = _extract_pref(addr)
                if pref:
                    data[Schema.PREF] = pref
            elif "給与" in label or "報酬" in label:
                data["給与"] = head
            elif "雇用形態" in label:
                data["雇用形態"] = head
            elif "勤務時間" in label:
                # 休日・待遇の自由記述が続くため、時間帯パターンのみを採用する
                worktime = _extract_worktime(_clean(dd.get_text(" ")))
                if not worktime and len(head) <= self.MAX_SHORT_LEN:
                    worktime = head
                if worktime:
                    data["勤務時間"] = worktime
            elif "特徴" in label:
                data["特徴"] = " / ".join(
                    _clean(li.get_text(" ")) for li in dd.select("li") if _clean(li.get_text(" "))
                ) or head

        return data

    def _dd_head_text(self, dd) -> str:
        """dd 内の見出し (h3.p-detail_subTitle) より前のテキストだけを取り出す。

        後続は「給与補足」「交通手段・勤務地補足」等の長文プロースなので除外する。
        """
        parts: list[str] = []
        for el in dd.descendants:
            name = getattr(el, "name", None)
            if name == "h3" and "p-detail_subTitle" in (el.get("class") or []):
                break
            if name is None:
                txt = _clean(el)
                if txt:
                    parts.append(txt)
        text = _clean(" ".join(parts))
        if not text:
            text = _clean(dd.get_text(" "))
        return text

    def _sel_text(self, root, selector: str) -> str:
        el = root.select_one(selector)
        return _clean(el.get_text(" ", strip=True)) if el else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KyujinBoxKensetsuScraper()
    scraper.execute("https://xn--pckua2a7gp15o89zb.com/建設の仕事")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
