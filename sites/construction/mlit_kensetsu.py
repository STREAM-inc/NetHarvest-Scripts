# -*- coding: utf-8 -*-
"""
MLIT 建設業者・宅建業者等企業情報検索システム（建設業）

静的版：Selenium を使わず requests のみで完結する。
    1. ルート URL(検索フォーム)を GET し、hidden 群を採取
    2. CMD=search で POST → 一覧ページ(50件/ページ)
    3. 一覧の各行の js_ShowDetail('<許可番号>') から許可番号を抽出
    4. ksGaiyo.do へ sv_licenseNo を付けて POST → 会社概要ページを取得しパース
    5. 次ページは一覧フォームを CMD=next で再 POST
"""

from __future__ import annotations
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple
from urllib.parse import urljoin, urlencode

import bs4
from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# ====================== 固定設定 ======================
# 一覧の1ページあたり表示件数（50 が最大）
DISP_COUNT = "50"


# ====================== ユーティリティ ======================
def norm(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def looks_like_detail(text: str) -> bool:
    if not text:
        return False
    return ("建設業者の詳細情報" in text) or ('class="re_summ"' in text) or ("許可番号" in text and "商号又は名称" in text)


def is_system_error_page(html: str) -> bool:
    return ("システムエラーが発生しました" in html) or ('id="information_body"' in html)


def tel_to_digits_text(raw: str) -> str:
    digits = re.sub(r"[^0-9]", "", str(raw or ""))
    return f"'{digits}" if digits else ""


def form_fields(soup: BeautifulSoup, form_id: str = "ksModel") -> Dict[str, str]:
    """指定フォーム内の input/select を name->value の辞書として採取する。"""
    form = soup.find("form", id=form_id) or soup.find("form")
    data: Dict[str, str] = {}
    if form is None:
        return data
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        typ = (inp.get("type") or "").lower()
        if typ in ("radio", "checkbox") and not inp.has_attr("checked"):
            # 未選択のラジオ/チェックはスキップ（後で明示的に上書きする）
            continue
        data[name] = inp.get("value") or ""
    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opt = sel.find("option", selected=True)
        if opt is None:
            opt = sel.find("option")
        data[name] = opt.get("value") if opt is not None else ""
    return data


def extract_detail_ids(html: str) -> List[str]:
    """一覧HTMLから許可番号(js_ShowDetail の引数)を重複除去して返す。"""
    ids = re.findall(r"js_ShowDetail\(['\"]?(\d+)['\"]?\)", html)
    return list(dict.fromkeys(ids))


# ====================== HTMLパース ======================
ABBR_COLUMNS = [
    "土", "建", "大", "左", "と", "石", "屋", "電", "管", "夕",
    "鋼", "筋", "舗", "し", "ゅ", "板", "ガ", "塗", "防", "内",
    "機", "絶", "通", "園", "井", "具", "水", "消", "清", "解",
]

PREF_PAT = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def find_value_cell_by_label(soup: BeautifulSoup, label_regex: str):
    pat = re.compile(label_regex)
    for tbl in soup.find_all("table"):
        for tr in tbl.find_all("tr"):
            tds = tr.find_all(["th", "td"])
            if len(tds) < 2:
                continue
            if pat.search(norm(tds[0].get_text(" "))):
                return tds[1]
    return None


def split_phonetic_cell(td) -> Tuple[str, str]:
    if td is None:
        return "", ""
    kana_list = [norm(p.get_text(" ")) for p in td.find_all("p", class_=lambda c: c and "phonetic" in c)]
    kana = " ".join([k for k in kana_list if k])
    td2 = BeautifulSoup(str(td), "html.parser")
    for p in td2.find_all("p", class_=lambda c: c and "phonetic" in c):
        p.decompose()
    text = norm(td2.get_text(" "))
    return kana, text


def parse_insurance(soup: BeautifulSoup) -> Dict[str, str]:
    res = {"保険加入状況(健康)": "", "保険加入状況(年金)": "", "保険加入状況(雇用)": ""}
    for tbl in soup.find_all("table"):
        for tr in tbl.find_all("tr"):
            th = tr.find("th")
            if not th:
                continue
            if "保険加入状況" in norm(th.get_text(" ")):
                parent = tr.parent
                trs = parent.find_all("tr")
                try:
                    idx = trs.index(tr)
                except ValueError:
                    continue
                if idx + 2 < len(trs):
                    header_tds = trs[idx + 1].find_all("td")
                    value_tds = trs[idx + 2].find_all("td")
                    if len(header_tds) >= 3 and len(value_tds) >= 3:
                        labels = [norm(td.get_text(" ")) for td in header_tds[:3]]
                        values = [norm(td.get_text(" ")) for td in value_tds[:3]]
                        mapping = dict(zip(labels, values))
                        res["保険加入状況(健康)"] = mapping.get("健康", "")
                        res["保険加入状況(年金)"] = mapping.get("年金", "")
                        res["保険加入状況(雇用)"] = mapping.get("雇用", "")
                        return res
    return res


def parse_industry_table_numbers(soup: BeautifulSoup) -> Dict[str, str]:
    res = {abbr: "" for abbr in ABBR_COLUMNS}

    def num_to_label(s: str) -> str:
        s = re.sub(r"\s+", "", s)
        if s == "1":
            return "一般"
        if s == "2":
            return "特定"
        return ""

    cand_tables = []
    for tbl in soup.find_all("table"):
        txt = norm(tbl.get_text(" "))
        if "許可を受けた" in txt and "建設業" in txt and "種類" in txt:
            cand_tables.append(tbl)
    for tbl in cand_tables:
        header_tr, value_tr = None, None
        for tr in tbl.find_all("tr"):
            cls = " ".join((tr.get("class") or []))
            if "re_summ_ev" in cls:
                header_tr = tr
            elif "re_summ_odd" in cls:
                value_tr = tr
        if not header_tr or not value_tr:
            continue
        header_cells = header_tr.find_all("td")
        value_cells = value_tr.find_all("td")
        if not header_cells or not value_cells:
            continue
        if len(value_cells) < len(header_cells):
            continue

        idx_to_labels: List[List[str]] = []
        for td in header_cells:
            raw = td.get_text("")
            lab = norm(raw).replace("\n", "").replace(" ", "")
            if lab in ("しゅ", "し\nゅ", "しゅんせつ", "しゅんせつ工事"):
                idx_to_labels.append(["し", "ゅ"])
            else:
                idx_to_labels.append([lab])
        for i, labels in enumerate(idx_to_labels):
            raw_val = norm(value_cells[i].get_text(""))
            label_val = num_to_label(raw_val)
            for lab in labels:
                if lab in res:
                    res[lab] = label_val
        if any(res.values()):
            return res
    return res


def parse_overview(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    row: Dict[str, str] = {}

    td = find_value_cell_by_label(soup, r"^許可番号$")
    row["許可番号"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"(商号又は名称|名称)")
    kana, name = split_phonetic_cell(td)
    row[Schema.NAME] = name
    row[Schema.NAME_KANA] = kana

    td = find_value_cell_by_label(soup, r"代表者")
    _k, rep = split_phonetic_cell(td)
    row[Schema.REP_NM] = rep

    td = find_value_cell_by_label(soup, r"(所在地|住所)")
    raw_addr = norm(td.get_text(" ")) if td else ""
    mzip = re.search(r"(?:〒\s*)?(\d{3}-\d{4})", raw_addr)
    row[Schema.POST_CODE] = mzip.group(1) if mzip else ""
    addr_wo_zip = re.sub(r"(?:〒\s*)?\d{3}-\d{4}", "", raw_addr).strip()
    row[Schema.ADDR] = re.sub(r"\s+", " ", addr_wo_zip)
    m_pref = PREF_PAT.search(row[Schema.ADDR])
    row[Schema.PREF] = m_pref.group(1) if m_pref else ""

    td = find_value_cell_by_label(soup, r"(電話番号|TEL)")
    row["_tel_raw"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"(法人・個人区分|法人・個人の別|法人・個人)")
    row["法人・個人区分"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"(資本金|資本金額)")
    row[Schema.CAP] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"建設業以外の兼業の有無")
    row["建設業以外の兼業の有無"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"許可の有効期間")
    row["許可の有効期間"] = norm(td.get_text(" ")) if td else ""

    inds = parse_industry_table_numbers(soup)
    row.update(inds)
    row["許可を受けた建設業の種類(土)"] = row.get("土", "")

    ins = parse_insurance(soup)
    row.update(ins)

    return row


# ====================== NetHarvestクローラー ======================
class MlitKensetsuScraper(StaticCrawler):
    """国土交通省 建設業者・宅建業者等企業情報検索システム（建設業）"""

    # Shift_JIS 固定サイトなので文字コードを固定
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "許可番号",
        "法人・個人区分",
        "建設業以外の兼業の有無",
        "保険加入状況(健康)",
        "保険加入状況(年金)",
        "保険加入状況(雇用)",
        "許可を受けた建設業の種類(土)",
        "許可の有効期間",
    ] + ABBR_COLUMNS

    def _post(self, action_url: str, data: Dict[str, str], referer: str) -> Optional[str]:
        """Shift_JIS デコード付きの POST。失敗時は None。"""
        try:
            resp = self.session.post(
                action_url, data=data, timeout=self.TIMEOUT,
                headers={"Referer": referer},
            )
            resp.raise_for_status()
            resp.encoding = "shift_jis"
            return resp.text
        except Exception as e:
            self.logger.warning("POST 失敗 %s: %s", action_url, e)
            return None

    def _fetch_detail(self, gaiyo_url: str, list_fields: Dict[str, str], license_no: str,
                      referer: str) -> Optional[str]:
        """一覧フォームの状態を引き継いで ksGaiyo.do から会社概要ページを取得。"""
        data = dict(list_fields)
        data["sv_licenseNo"] = license_no
        html = self._post(gaiyo_url, data, referer)
        if html and looks_like_detail(html) and not is_system_error_page(html):
            return html
        return None

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルートとして扱い、派生 URL は urljoin で導出する
        search_url = urljoin(url, "kensetuKensaku.do")
        gaiyo_url = urljoin(url, "ksGaiyo.do")

        # --- 1) 検索フォームを取得 ---
        soup = self.get_soup(url)
        if soup is None:
            self.logger.error("ルートページ取得失敗: %s", url)
            return
        data = form_fields(soup, "ksModel")

        # 検索条件: 全件（絞り込みなし）、カナ順・AND・50件表示
        data.update({
            "CMD": "search",
            "choice": "2",
            "dispCount": DISP_COUNT,
            "rdoSelect": "1",
            "rdoSelectJoken": "1",
            "rdoSelectSort": "1",
        })

        # --- 2) 検索実行 ---
        html = self._post(search_url, data, referer=url)
        if not html:
            self.logger.error("検索 POST に失敗しました")
            return
        if is_system_error_page(html):
            self.logger.error("検索結果がシステムエラーページでした")
            return

        try:
            page_count = int(re.search(r'name="pageCount"[^>]*value="(\d+)"', html).group(1))
        except Exception:
            page_count = 1
        try:
            result_count = int(re.search(r'name="resultCount"[^>]*value="(\d+)"', html).group(1))
        except Exception:
            result_count = 0
        self.logger.info("[SETUP] pages=%d results≈%d", page_count, result_count)

        page = 1
        seen_first_id = ""
        while True:
            if is_system_error_page(html):
                self.logger.warning("[page %d] system error, 中断", page)
                break

            soup = BeautifulSoup(html, "html.parser")
            list_fields = form_fields(soup, "ksModel")
            detail_ids = extract_detail_ids(html)
            self.logger.info("[page %d/%d] ids=%d", page, page_count, len(detail_ids))

            if not detail_ids:
                break
            # ページ送りが進んでいない(同じ先頭ID)場合は終了
            if detail_ids[0] == seen_first_id:
                break
            seen_first_id = detail_ids[0]

            for lic in detail_ids:
                detail_html = self._fetch_detail(gaiyo_url, list_fields, lic, referer=search_url)
                if not detail_html:
                    self.logger.warning("[detail] 取得失敗 license=%s", lic)
                    continue
                row = parse_overview(detail_html)
                if not any(row.get(k) for k in [Schema.NAME, "許可番号", Schema.ADDR]):
                    continue
                row[Schema.TEL] = tel_to_digits_text(row.pop("_tel_raw", ""))
                row[Schema.URL] = f"{gaiyo_url}?{urlencode({'sv_licenseNo': lic})}"
                yield row

            # --- 次ページ ---
            if page >= page_count:
                break
            next_data = dict(list_fields)
            next_data["CMD"] = "next"
            html = self._post(search_url, next_data, referer=search_url)
            if not html:
                break
            page += 1


# ====================== ローカル実行用エントリーポイント ======================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    scraper = MlitKensetsuScraper()
    scraper.execute("https://etsuran2.mlit.go.jp/TAKKEN/kensetuKensaku.do?outPutKbn=1")
    print(f"\n取得件数: {scraper.item_count}")
    print(f"出力先:   {scraper.output_filepath}")
