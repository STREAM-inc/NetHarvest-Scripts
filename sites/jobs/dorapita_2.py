"""
ドラピタ — 運送・ドライバー系求人 (全国47都道府県 / 求人単位1行)

対象サイト: https://dorapita.com/search (→ /driver/search へ 301)

取得対象:
    - 詳細ページ /recruits/detail/{ID}
        募集要項 table.tbl_job_description (th/td):
            職種, 給与 (strong.salary_num), 勤務地, 雇用形態, 応募条件, 待遇・福利厚生,
            勤務時間, 休日・休暇, 電話番号, 応募ナンバー
        企業情報 div.job-description-area-item (label/value):
            法人名, 代表者, 設立, 住所, 資本金, 従業員数, 業務内容, HP
        求人タグ div.job_detail_tag a.job_tag
            → 一覧の絞り込みモーダル (input#condition-hinmoku-* / condition-keijou-*)
              を語彙として「輸送品目」「車両形状」に分類
        JSON-LD JobPosting: datePosted (掲載日), jobLocation.postalCode (勤務地郵便番号)
        ※ JSON-LD description は勤務時間/休日の見出しが入れ替わっているため HTML を正とする

取得フロー:
    sitemap.xml には詳細 URL が無い (一覧ページのみ) ため、
    起点 url (検索トップ) の都道府県リンク /driver/search/prefecture/{JIS} (全47県)
    → 県別一覧 (1ページ25件、/page/N のパス形式。robots は /*?*page= を禁止しているため
       ?page= は使わない) 総ページ数は見出し「〇〇県のドライバー求人（1／N）」から取得
    → 一覧カード article.job_item の /recruits/detail/{ID} を取得即 yield (求人IDで dedupe)
    503 等は StaticCrawler のセッションリトライ (500/502/503/504, 指数バックオフ) で再試行

電話番号について:
    詳細の「電話番号」欄は全求人共通の応募受付ダイヤル (050-1807-7777) +
    求人ごとの「応募ナンバー」方式で、企業固有番号は掲載されていない。
    共通番号は Schema.TEL に入れず EXTRA「応募専用電話番号」へ出力する
    (共通番号以外の番号が載っている求人のみ Schema.TEL に入れる)。

サイトに存在しない項目 (備考で要望あり・空欄で出力):
    業種 / 募集人数 / 勤務地へのアクセス / 定休日

文章項目について:
    応募条件 / 待遇・福利厚生 / 勤務時間 / 休日・休暇 / 事業内容(業務内容) は備考で
    明示指定されたため出力する。仕事内容 / 応募方法 / 選考の流れ / 企業PR は指定外の
    長文自由記述のため著作権リスクで除外。

利用規約 (https://dorapita.com/static/terms):
    スクレイピング・クローリングの明示禁止条項は無い。ただし禁止事項に
    「ドラピタの利用目的(求職活動における情報支援)の範囲を超えて、営利を目的として、
    求人情報を収集する行為」があるため、取得データの利用目的に留意すること。

実行方法:
    python scripts/sites/jobs/dorapita_2.py
    python bin/run_flow.py --site-id dorapita_2
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_PREF_LINK_RE = re.compile(r"/driver/search/prefecture/(\d+)/?$")
_DETAIL_RE = re.compile(r"/recruits/detail/(\d+)")
_TOTAL_PAGES_RE = re.compile(r"ドライバー求人（\s*\d+\s*／\s*(\d+)\s*）")
_TEL_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")
_POSTAL_RE = re.compile(r'"postalCode"\s*:\s*"([^"]*)"')
_DATE_POSTED_RE = re.compile(r'"datePosted"\s*:\s*"([^"]*)"')
_ORG_NAME_RE = re.compile(r'"hiringOrganization"\s*:\s*\{.*?"name"\s*:\s*"([^"]*)"', re.S)
_DATE_RE = re.compile(r"(\d{4})年\s*(\d{1,2})月(?:\s*(\d{1,2})日)?")

# 全求人共通の応募受付ダイヤル (応募ナンバー入力方式)
_COMMON_TEL = "050-1807-7777"

# 代表者欄の「役職 氏名」分割用
_POSITION_RE = re.compile(
    r"^((?:代表取締役|取締役|代表執行役|代表社員|代表理事|理事長|代表者|代表|"
    r"社長|会長|所長|支店長|営業所長|センター長|執行役員|CEO)"
    r"(?:社長|会長|副社長|専務|常務|兼[^\s　]*?社長|CEO)?)[\s　:：]*(.+)$"
)
_WAREKI = {"明治": 1867, "大正": 1911, "昭和": 1925, "平成": 1988, "令和": 2018}
_WAREKI_RE = re.compile(r"(明治|大正|昭和|平成|令和)\s*(元|\d{1,2})年")


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _split_pref(addr: str) -> tuple[str, str]:
    addr = _clean(addr)
    m = _PREF_RE.match(addr)
    if not m:
        return "", addr
    return m.group(1), addr[m.end():].strip()


def _to_date(text: str) -> str:
    text = _WAREKI_RE.sub(
        lambda m: f"{_WAREKI[m.group(1)] + (1 if m.group(2) == '元' else int(m.group(2)))}年",
        text or "",
    )
    m = _DATE_RE.search(text)
    if not m:
        return _clean(text)
    y, mo, d = m.group(1), int(m.group(2)), m.group(3)
    return f"{y}-{mo:02d}-{int(d):02d}" if d else f"{y}-{mo:02d}"


class Dorapita2Scraper(StaticCrawler):
    """ドラピタ スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = [
        "求人番号",
        "職種",
        "輸送品目",
        "車両形状",
        "雇用形態",
        "給与情報",
        "勤務地住所",
        "勤務地郵便番号",
        "応募専用電話番号",
        "応募ナンバー",
        "掲載日",
        "業種",
        "募集人数",
        "勤務地へのアクセス",
        "応募条件",
        "勤務時間",
        "休日・休暇",
        "福利厚生",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        # 起点ページの都道府県リンクから全 47 県の一覧 URL を得る
        pref_urls: dict[str, str] = {}
        for a in soup.select("a[href]"):
            m = _PREF_LINK_RE.search(a["href"])
            if m and m.group(1) not in pref_urls:
                pref_urls[m.group(1)] = urljoin(url, a["href"])
        for code in range(1, 48):
            pref_urls.setdefault(str(code), urljoin(url, f"/driver/search/prefecture/{code}"))

        self._hinmoku: set[str] = set()
        self._keijou: set[str] = set()
        seen: set[str] = set()

        for code in sorted(pref_urls, key=int):
            pref_url = pref_urls[code].rstrip("/")
            total_pages = 1
            page = 1
            while page <= total_pages:
                list_url = pref_url if page == 1 else f"{pref_url}/page/{page}"
                list_soup = self.get_soup(list_url)
                if list_soup is None:
                    break
                if page == 1:
                    m = _TOTAL_PAGES_RE.search(list_soup.get_text(" ", strip=True))
                    total_pages = int(m.group(1)) if m else 1
                    self._load_vocab(list_soup)

                for card in list_soup.select("article.job_item"):
                    a = card.select_one('a[href*="/recruits/detail/"]')
                    if not a:
                        continue
                    m = _DETAIL_RE.search(a["href"])
                    if not m or m.group(1) in seen:
                        continue
                    seen.add(m.group(1))
                    item = self._parse_detail(urljoin(url, f"/recruits/detail/{m.group(1)}"), m.group(1))
                    if item:
                        yield item
                page += 1

    def _load_vocab(self, soup) -> None:
        """一覧の絞り込みモーダルから 輸送品目 / 車両形状 の語彙を取る。"""
        for inp in soup.select('input[name="kodawari[]"][id]'):
            value = _clean(inp.get("value", ""))
            if not value:
                continue
            if inp["id"].startswith("condition-hinmoku-"):
                self._hinmoku.add(value)
            elif inp["id"].startswith("condition-keijou-"):
                self._keijou.add(value)

    def _parse_detail(self, detail_url: str, job_id: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        job: dict[str, object] = {}
        for tr in soup.select("table.tbl_job_description tr"):
            th, td = tr.find("th"), tr.find("td")
            if th and td:
                job.setdefault(_clean(th.get_text()), td)

        company: dict[str, object] = {}
        for row in soup.select("div.job-description-area-item"):
            label = row.select_one(".job-description-area-item-label")
            value = row.select_one(".job-description-area-item-value")
            if label and value:
                company.setdefault(_clean(label.get_text()), value)

        ld = "\n".join(s.string or "" for s in soup.select('script[type="application/ld+json"]'))

        # 勤務地
        loc_td = job.get("勤務地")
        location = ""
        if loc_td is not None:
            for btn in loc_td.select(".map_btn"):
                btn.decompose()
            location = _clean(loc_td.get_text(" "))

        name = _clean(company["法人名"].get_text()) if "法人名" in company else ""
        if not name:
            el = soup.select_one("h2.company-name-title")
            name = _clean(el.get_text()) if el else ""
        if not name:
            m = _ORG_NAME_RE.search(ld)
            name = _clean(m.group(1)) if m else ""
        if not name:
            return None

        def cval(label: str) -> str:
            el = company.get(label)
            return _clean(el.get_text(" ")) if el is not None else ""

        def jval(label: str) -> str:
            el = job.get(label)
            return _clean(el.get_text(" ")) if el is not None else ""

        def jtext(label: str) -> str:
            """複数行の文章欄: 行単位で空白整理し " / " で連結する。"""
            el = job.get(label)
            if el is None:
                return ""
            lines = (_clean(s) for s in el.get_text("\n").splitlines())
            return " / ".join(s for s in lines if s)

        pref, addr = _split_pref(cval("住所"))

        rep = cval("代表者")
        pos_nm, rep_nm = "", rep
        m = _POSITION_RE.match(rep)
        if m:
            pos_nm, rep_nm = m.group(1), m.group(2)

        hp = ""
        hp_el = company.get("HP")
        if hp_el is not None:
            a = hp_el.find("a", href=True)
            hp = a["href"].strip() if a else cval("HP")

        # 電話番号: 共通応募ダイヤル以外があれば企業番号として採用
        tels = _TEL_RE.findall(jval("電話番号"))
        tel = next((t for t in tels if t != _COMMON_TEL), "")
        apply_tel = _COMMON_TEL if _COMMON_TEL in tels else ""

        salary_td = job.get("給与")
        salary = ""
        if salary_td is not None:
            strong = salary_td.select_one("strong.salary_num")
            salary = _clean(strong.get_text()) if strong else _clean(salary_td.get_text(" "))[:100]

        tags = [_clean(a.get_text()) for a in soup.select("div.job_detail_tag a.job_tag")]
        hinmoku = [t for t in dict.fromkeys(tags) if t in self._hinmoku]
        keijou = [t for t in dict.fromkeys(tags) if t in self._keijou]

        m = _POSTAL_RE.search(ld)
        postal = m.group(1) if m else ""
        m = _DATE_POSTED_RE.search(ld)
        posted = m.group(1).replace("/", "-") if m else ""

        occupation = jval("職種")

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep_nm,
            Schema.POS_NM: pos_nm,
            Schema.OPEN_DATE: _to_date(cval("設立")),
            Schema.CAP: cval("資本金"),
            Schema.EMP_NUM: cval("従業員数"),
            Schema.HP: hp,
            Schema.LOB: cval("業務内容"),
            Schema.HOLIDAY: "",
            Schema.CAT_SITE: occupation,
            "求人番号": job_id,
            "職種": occupation,
            "輸送品目": "/".join(hinmoku),
            "車両形状": "/".join(keijou),
            "雇用形態": jval("雇用形態"),
            "給与情報": salary,
            "勤務地住所": location,
            "勤務地郵便番号": postal,
            "応募専用電話番号": apply_tel,
            "応募ナンバー": jval("応募ナンバー"),
            "掲載日": posted,
            "業種": "",
            "募集人数": "",
            "勤務地へのアクセス": "",
            "応募条件": jtext("応募条件"),
            "勤務時間": jtext("勤務時間"),
            "休日・休暇": jtext("休日・休暇"),
            "福利厚生": jtext("待遇・福利厚生"),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Dorapita2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://dorapita.com/search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
