"""
キャリタス就活_27卒本選考受付中 (job_9) — キャリタス就活 掲載企業 (本社所在地 11 県) スクレイパー

取得対象:
    - 企業検索結果一覧 (condition-search/result) のうち、本社所在地が
      青森・岩手・宮城・秋田・山形・福島・栃木・新潟・石川・山梨・静岡 の企業
    - 企業ページ (/corp/{id}/) の会社概要・受付状況
    - 採用情報ページ (/corp/{id}/employment/ → 302 で /employment/detail/{N}/ 等) の
      対象卒年・募集職種・勤務地・採用予定人数・初任給・採用連絡先

取得フロー:
    1. ルート URL (= sites.yml の url) に本社所在地の絞り込みパラメータ
       (honPrefectureCd=NN-県名 を複数) を付けて GET し、?p=N でページ送り (40社/頁)
    2. 一覧パネルの都道府県 (= 本社所在地) が対象 11 県を含まない企業は除外
       (サイト側の絞り込みに対象外の企業が少数混入するため)
    3. 企業ごとに /corp/{id}/default/ (302 で企業/グループ/未掲載企業ページへ) と
       /corp/{id}/employment/ を取得して即 yield (Pattern B)

備考:
    - 受付状況での除外はしない (後工程で「本選考エントリー受付中」かつ 27卒 を絞る)。
    - robots.txt で禁止の /corp/*/entry/ (エントリー画面) には絶対にアクセスしない。
      受付状況欄のエントリーリンクはテキストを読むだけで辿らない。
    - 事業内容・沿革・求める人材像・選考フロー等の長文の自由記述は著作権リスク回避のため除外。
      勤務地・初任給・採用連絡先は依頼で明示されたカラムのため取得する。

実行方法:
    python scripts/sites/jobs/job_9.py
    docker compose exec worker python /app/bin/run_flow.py --site-id job_9
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urlencode, urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 本社所在地の絞り込み (サイトの honPrefectureCd 値)
TARGET_PREFS = [
    ("02", "青森県"), ("03", "岩手県"), ("04", "宮城県"), ("05", "秋田県"),
    ("06", "山形県"), ("07", "福島県"), ("09", "栃木県"), ("15", "新潟県"),
    ("17", "石川県"), ("19", "山梨県"), ("22", "静岡県"),
]
TARGET_PREF_NAMES = {name for _, name in TARGET_PREFS}

PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|"
    r"静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    r"熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
POST_PATTERN = re.compile(r"〒?\s*(\d{3})\s*[-‐－ー―]\s*(\d{4})")
TOTAL_PATTERN = re.compile(r"\d+\s*/\s*([\d,]+)\s*社")
TEL_PATTERN = re.compile(r"(0\d{1,4}[-‐－ー―(（)）\s]?\d{1,4}[-‐－ー―)）\s]?\d{3,4})")
EMAIL_PATTERN = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
GRAD_PATTERN = re.compile(r"(\d{2})卒")
CORP_ID_PATTERN = re.compile(r"/corp/(\d+)/")
DATE_PATTERN = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
POS_PREFIX = re.compile(
    r"^(代表|取締役|社長|会長|理事|院長|園長|店長|頭取|学長|校長|CEO|社主|理事長|執行役員)"
)

# 連続して結果 0 件のページがこの回数続いたら終了 (途中に 0 件ページが混じるため)
MAX_EMPTY_PAGES = 3


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _multiline(el) -> str:
    """<br> 区切りの要素を「 / 」連結の 1 行テキストにする。"""
    if el is None:
        return ""
    lines = [_clean(x) for x in el.get_text("\n").split("\n")]
    return " / ".join(x for x in lines if x)


def _norm_digits(s: str) -> str:
    return s.translate(str.maketrans("０１２３４５６７８９－", "0123456789-"))


def _table_rows(soup) -> dict[str, str]:
    """th→td のラベル/値。同一ラベルは最初の非空値を採用 (グループは先頭企業)。

    会社概要の th は閉じタグが壊れている (<th>資本金</td>) ため html.parser では
    td が th の子になる。ラベルは th 直下の文字列から td を除いて取る。
    """
    rows: dict[str, str] = {}
    for tr in soup.select("tr"):
        th = tr.find("th")
        if th is None:
            continue
        td = tr.find("td")
        key = _clean("".join(t for t in th.find_all(string=True, recursive=False)))
        if not key:
            key = _clean(th.get_text())
            if td is not None and td in th.descendants:
                key = _clean(key.replace(_clean(td.get_text()), ""))
        val = _clean(td.get_text(" ")) if td is not None else ""
        if key and val and key not in rows:
            rows[key] = val
    return rows


class CaritasJob27Scraper(StaticCrawler):
    """キャリタス就活 27卒 掲載企業 (本社所在地 11 県) スクレイパー"""

    DELAY = 0.0
    ITEM_DELAY = 0.0
    REQUEST_INTERVAL = 2.0  # 依頼: アクセス間隔 2 秒程度 (全リクエストに適用)
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    EXTRA_COLUMNS = [
        "受付状況",
        "対象卒年",
        "募集職種",
        "勤務地",
        "採用予定人数",
        "初任給",
        "採用連絡先",
        "最終更新日",
    ]

    # ------------------------------------------------------------------ list
    def _list_url(self, url: str, page: int) -> str:
        params = [("honPrefectureCd", f"{code}-{name}") for code, name in TARGET_PREFS]
        params.append(("p", str(page)))
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}{urlencode(params)}"

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1
        empty_streak = 0
        max_page = None
        while True:
            soup = self.get_soup(self._list_url(url, page))
            if soup is None:
                break
            if page == 1:
                m = TOTAL_PATTERN.search(soup.get_text(" "))
                if m:
                    total = int(m.group(1).replace(",", ""))
                    self.total_items = total
                    max_page = (total + 39) // 40
                    self.logger.info("本社所在地 11 県の検索結果: %d 社 (%d 頁)", total, max_page)

            anchors = [
                a for a in soup.select('a.c_panelLink[href^="/corp/"]')
                if a.select_one(".c_panelCompanyInfoMain")
            ]
            if not anchors:
                empty_streak += 1
                if empty_streak >= MAX_EMPTY_PAGES or (max_page and page >= max_page):
                    break
                page += 1
                continue
            empty_streak = 0

            for a in anchors:
                m = CORP_ID_PATTERN.search(a.get("href", ""))
                if not m or m.group(1) in seen:
                    continue
                corp_id = m.group(1)
                seen.add(corp_id)

                list_data = self._parse_list_panel(a)
                prefs = list_data.pop("_prefs", [])
                if not TARGET_PREF_NAMES.intersection(prefs):
                    self.logger.debug("本社所在地が対象外のため除外: %s %s", corp_id, prefs)
                    continue

                item = self._scrape_company(url, corp_id, list_data)
                if item:
                    yield item

            if max_page and page >= max_page:
                break
            page += 1

    def _parse_list_panel(self, anchor) -> dict:
        data: dict = {}
        ttl = anchor.select_one(".c_panelCompanyInfoMain__ttl")
        if ttl:
            data[Schema.NAME] = _clean(ttl.get_text())
        # "<span>青森県｜東京都</span>商社（自動車）｜…" : span = 本社所在地 (複数あり)
        txt_el = anchor.select_one(".c_panelCompanyInfoMain__txt")
        prefs: list[str] = []
        if txt_el:
            span = txt_el.find("span")
            pref_txt = _clean(span.get_text()) if span else ""
            prefs = PREF_PATTERN.findall(pref_txt)
            full = _clean(txt_el.get_text())
            data[Schema.CAT_SITE] = full[len(pref_txt):].strip() if pref_txt and full.startswith(pref_txt) else full
        data["_prefs"] = prefs
        return data

    # --------------------------------------------------------------- company
    def _scrape_company(self, root_url: str, corp_id: str, list_data: dict) -> dict | None:
        corp_url = urljoin(root_url, f"/corp/{corp_id}/")
        data = dict(list_data)
        data[Schema.URL] = corp_url

        # /default/ は掲載形態に応じて /corp/{id}/・/group/・/detail-uc/(未掲載) 等へ 302
        soup = self.get_soup(urljoin(root_url, f"/corp/{corp_id}/default/"))
        rows = _table_rows(soup) if soup is not None else {}
        if soup is None or "本社所在地1" not in rows:
            # /contents/{N}/ 着地など会社概要が無い場合は企業トップを直接取得
            soup2 = self.get_soup(corp_url)
            if soup2 is not None:
                soup, rows = soup2, _table_rows(soup2)
        if soup is None:
            return data if data.get(Schema.NAME) else None

        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1.c_panelCompanyInfoMain__ttl") or soup.select_one("h1")
            if h1:
                data[Schema.NAME] = _clean(h1.get_text())
        if not data.get(Schema.NAME):
            return None

        self._apply_profile(data, rows)

        # 受付状況 (例: 本選考エントリー受付中 / インターン・キャリア受付中)
        status = [
            _clean(p.get_text())
            for p in soup.select(".c_contentToggle__content .m_sec__head p.c_typoLabel")
        ]
        data["受付状況"] = " / ".join(s for s in status if s)

        # 受付状況欄のエントリー名の【27卒】表記 (リンク先 entry は robots 禁止のため辿らない)
        status_grads = []
        for a in soup.select(".c_contentToggle__content .c_dataList a"):
            for g in GRAD_PATTERN.findall(a.get_text()):
                if f"{g}卒" not in status_grads:
                    status_grads.append(f"{g}卒")

        corp_dates = [d for d in (_clean(t.get_text()) for t in soup.select("time")) if DATE_PATTERN.search(d)]

        # 採用情報 (掲載企業のみ。非掲載は /page-error/ へ 302 → 抽出 0 で空欄)
        has_employment = soup.select_one(f'a[href="/corp/{corp_id}/employment/"]') is not None
        emp = self._scrape_employment(root_url, corp_id) if has_employment else {}
        data.update(emp)
        if not data.get("対象卒年"):
            data["対象卒年"] = " / ".join(status_grads)
        if not data.get("最終更新日") and corp_dates:
            data["最終更新日"] = max(
                corp_dates,
                key=lambda d: tuple(int(x) for x in DATE_PATTERN.search(d).groups()),
            )
        for col in self.EXTRA_COLUMNS:
            data.setdefault(col, "")
        return data

    def _apply_profile(self, data: dict, rows: dict[str, str]) -> None:
        addr = re.sub(r"\s*MAP\s*$", "", rows.get("本社所在地1", "")).strip()
        addr_n = _norm_digits(addr)
        pc = POST_PATTERN.search(addr_n)
        if pc:
            data[Schema.POST_CODE] = f"{pc.group(1)}-{pc.group(2)}"
            addr = POST_PATTERN.sub("", addr_n, count=1).strip()
        pm = PREF_PATTERN.search(addr)
        data[Schema.PREF] = pm.group(1) if pm else ""
        data[Schema.ADDR] = addr

        rep = rows.get("代表者", "").replace("　", " ").strip()
        pos, name = "", rep
        if " " in rep and POS_PREFIX.match(rep):
            pos, name = rep.split(" ", 1)
        data[Schema.POS_NM] = pos.strip()
        data[Schema.REP_NM] = name.strip()

        data[Schema.OPEN_DATE] = rows.get("創業/設立", "")
        data[Schema.CAP] = rows.get("資本金", "")
        data[Schema.EMP_NUM] = rows.get("従業員数", "")
        data[Schema.SALES] = rows.get("売上高", "")

    # ------------------------------------------------------------ employment
    def _scrape_employment(self, root_url: str, corp_id: str) -> dict:
        emp_url = urljoin(root_url, f"/corp/{corp_id}/employment/")
        soup = self.get_soup(emp_url)
        if soup is None:
            return {}
        if soup.select_one("section#contact") is None:
            # 複数掲載の一覧ページだった場合: 詳細リンクのうち 27卒 を優先して 1 件取得
            links: list[tuple[str, bool]] = []
            for a in soup.select(f'a[href*="/corp/{corp_id}/employment/detail/"]'):
                href = a.get("href", "")
                if "/entry/" in href or any(h == href for h, _ in links):
                    continue
                links.append((href, "27卒" in a.get_text()))
            if not links:
                return {}
            links.sort(key=lambda x: not x[1])
            soup = self.get_soup(urljoin(root_url, links[0][0]))
            if soup is None or soup.select_one("section#contact") is None:
                return {}

        out: dict = {}
        badges = [_clean(b.get_text()) for b in soup.select(".c_badgeList .c_badge")]
        out["対象卒年"] = " / ".join(b for b in badges if GRAD_PATTERN.fullmatch(b))

        # 募集職種: h2「募集職種」セクションの h3 (無ければ職種バッジ)
        jobs: list[str] = []
        for sec in soup.select("section.m_secContents"):
            h2 = sec.find("h2")
            if h2 and _clean(h2.get_text()) == "募集職種":
                jobs = [_clean(h.get_text()) for h in sec.select("h3")]
                break
        if not jobs:
            jobs = [_clean(b.get_text()) for b in soup.select(".c_badgeList .c_badge--medium")]
        out["募集職種"] = " / ".join(j for j in jobs if j)

        rows = _table_rows(soup)
        out["勤務地"] = rows.get("勤務地", "")
        out["初任給"] = rows.get("初任給", "")
        out["採用予定人数"] = next(
            (v for k, v in rows.items() if k.startswith("今年度採用予定数")), ""
        )

        contact = soup.select_one("section#contact .c_typoTxt") or soup.select_one("section#contact")
        contact_txt = _multiline(contact)
        if contact_txt.startswith("採用連絡先 / "):
            contact_txt = contact_txt[len("採用連絡先 / "):]
        out["採用連絡先"] = contact_txt
        # 郵便番号 (7 桁) を電話番号と誤認しないよう、10 桁以上のものだけ採用
        out[Schema.TEL] = next(
            (t.strip() for t in TEL_PATTERN.findall(_norm_digits(contact_txt))
             if len(re.sub(r"\D", "", t)) >= 10),
            "",
        )
        em = EMAIL_PATTERN.search(contact_txt)
        out[Schema.EMAIL] = em.group(0) if em else ""

        dd = None
        for dt in soup.select("dl dt"):
            if "最終更新日" in dt.get_text():
                dd = dt.find_next_sibling("dd")
                break
        out["最終更新日"] = _clean(dd.get_text()) if dd else ""
        return out


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CaritasJob27Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://job.career-tasu.jp/condition-search/result/?keyword=")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
