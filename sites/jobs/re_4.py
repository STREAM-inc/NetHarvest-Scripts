"""
Ｒｅ就活キャンパス【採用】 — 新卒採用情報スクレイパー

取得対象:
    大学4年生・既卒向けの「採用情報」を掲載している企業（インターン専用掲載は除外）
    - 企業概要（設立/代表者/資本金/売上高/従業員数/本社所在地/事業内容/事業所/HP/企業SNS）
    - 採用情報（採用職種/勤務地/勤務時間/休日休暇/諸手当/採用予定人数/若者雇用促進法の指標等）
    - 合同説明会・セミナー出展の有無（判定語）と根拠URL

取得フロー:
    一覧（/campus/search/sch_result?p0=1&p1=1&pagCnt=N）を全ページ巡回
      → 企業ごとに 企業情報(/campus/company/baseinfo/{code}/)
         + 採用情報(/campus/company/employ/{code}/) を取得して即 yield

注意:
    - 文章本文（仕事内容/給与本文/人事担当者コメント/先輩社員 等）は保存しない。
      「研修の有無」等は先頭の判定語（有/無）だけを残す。
    - ラベルは企業により揺れる（代表者/グループ代表者、資本金/グループ総資本金、
      売上高/グループ売上高、従業員数/従業員総数）ため部分一致で照合する。

実行方法:
    python scripts/sites/jobs/re_4.py
    docker compose exec worker python /app/bin/run_flow.py --site-id re_4
"""

import math
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PER_PAGE = 20

_POSTAL_RE = re.compile(r"〒?\s*(\d{3}[-－‐]\d{4})")
_TEL_RE = re.compile(r"(0\d{1,4}[\-－‐(（]\d{1,4}[)）\-－‐]\d{3,4})")
_FAX_RE = re.compile(r"(?:FAX|Fax|ＦＡＸ|fax)[^0-9]{0,8}(0\d{1,4}[\-－‐(（]\d{1,4}[)）\-－‐]\d{3,4})")
_MAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    r"東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|"
    r"香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 政令市・特別区の住所から都道府県を補完するための最小マップ
_CITY_TO_PREF = {
    "札幌市": "北海道", "仙台市": "宮城県", "さいたま市": "埼玉県", "千葉市": "千葉県",
    "横浜市": "神奈川県", "川崎市": "神奈川県", "相模原市": "神奈川県", "新潟市": "新潟県",
    "静岡市": "静岡県", "浜松市": "静岡県", "名古屋市": "愛知県", "京都市": "京都府",
    "大阪市": "大阪府", "堺市": "大阪府", "神戸市": "兵庫県",
    "岡山市": "岡山県", "広島市": "広島県", "北九州市": "福岡県", "福岡市": "福岡県",
    "熊本市": "熊本県",
}
# 代表者欄の役職を切り出すためのキーワード（長い順に照合）
_POSITIONS = [
    "代表取締役社長執行役員", "代表取締役会長兼社長", "代表取締役社長", "代表取締役会長",
    "代表取締役副社長", "代表取締役専務", "代表取締役CEO", "代表取締役", "取締役社長",
    "代表理事長", "理事長", "代表理事", "会長兼社長", "社長執行役員", "会長", "社長",
    "代表社員", "代表者", "院長", "園長", "代表",
]
_SNS_MAP = [
    ("instagram.com", Schema.INSTA),
    ("facebook.com", Schema.FB),
    ("fb.com", Schema.FB),
    ("twitter.com", Schema.X),
    ("x.com", Schema.X),
    ("tiktok.com", Schema.TIKTOK),
    ("line.me", Schema.LINE),
    ("lin.ee", Schema.LINE),
]
# 有無だけを残す（本文は保存しない）項目: サイトのラベル -> 出力カラム名
_YESNO_FIELDS = {
    "研修の有無及び内容": "研修制度",
    "自己啓発支援の有無及び内容": "自己啓発支援",
    "メンター制度の有無": "メンター制度",
    "キャリアコンサルティング制度の有無及び内容": "キャリアコンサルティング制度",
    "社内検定等の制度の有無及び内容": "社内検定制度",
}
# 1〜3年生向けインターン限定掲載を弾くための語
_INTERN_ONLY_RE = re.compile(r"インターン|1・2・3年生|大学1〜3年|1〜3年生")
_GRAD_YEAR_RE = re.compile(r"(20\d{2})年\s*(\d{1,2})?\s*月?\s*卒")


def _norm(text: str) -> str:
    return re.sub(r"[ \t　]+", " ", (text or "").replace("\r", "")).strip()


def _is_prose(text: str) -> bool:
    """長い散文かどうか（著作権リスク回避のため本文は保存しない）。"""
    if not text:
        return False
    one = re.sub(r"\s+", "", text)
    return len(one) > 200 and one.count("。") >= 3


def _yesno(text: str) -> str:
    """「有：〜」「無」等の先頭の判定語のみを返す。"""
    t = _norm(text)
    if not t:
        return ""
    if t.startswith("有"):
        return "有"
    if t.startswith("無") or t.startswith("なし"):
        return "無"
    return "有" if len(t) > 1 else ""


class Re4Scraper(StaticCrawler):
    """Ｒｅ就活キャンパス【採用】 スクレイパー"""

    DELAY = 1.0
    TIMEOUT = 30
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    EXTRA_COLUMNS = [
        "FAX", "職種", "採用職種", "採用区分", "雇用形態", "働き方の特徴",
        "勤務地", "勤務時間", "休日休暇", "諸手当", "社会保険", "各種制度",
        "昇給・賞与", "初任給", "対象卒業年", "採用予定学科", "応募・選考時の提出書類",
        "採用予定人数／実績", "採用実績校", "受動喫煙対策",
        "研修制度", "自己啓発支援", "メンター制度", "キャリアコンサルティング制度",
        "社内検定制度",
        "平均継続勤務年数", "従業員の平均年齢",
        "過去3年間の新卒採用者数", "過去3年間の新卒離職者数",
        "前年度の月平均所定外労働時間の実績", "前年度の有給休暇の平均取得日数",
        "育児休業取得者数／対象者数", "女性役員・管理職比率",
        "事業所", "関連会社", "主要取引先",
        "採用担当者名", "採用担当者連絡先",
        "合同説明会・セミナー出展記録", "セミナー・説明会名", "セミナー・説明会URL",
        "企業SNS_その他", "企業コード", "最終更新日", "掲載終了日",
        "求人URL", "取得元URL",
    ]

    # ------------------------------------------------------------------ 一覧
    def parse(self, url: str):
        root = url.rstrip("/")
        list_base = f"{root}/sch_result"
        page = 1
        last_page = None
        seen: set[str] = set()

        while True:
            list_url = f"{list_base}?p0=1&p1=1&pagCnt={page}"
            list_soup = self.get_soup(list_url)
            if list_soup is None:
                break

            codes = [
                (inp.get("value") or "").strip()
                for inp in list_soup.select("input.hdnComCode")
            ]
            codes = [c for c in codes if c]
            if not codes:
                break

            if last_page is None:
                total_el = list_soup.select_one("#lblCmpCount")
                if total_el:
                    m = re.search(r"(\d[\d,]*)", total_el.get_text())
                    if m:
                        total = int(m.group(1).replace(",", ""))
                        self.total_items = total
                        last_page = max(1, math.ceil(total / _PER_PAGE))
                if last_page is None:
                    last_page = 0  # 総件数不明: 空ページまで進む

            for code in codes:
                if code in seen:
                    continue
                seen.add(code)
                try:
                    record = self._scrape_company(url, code, list_url)
                except Exception as e:  # noqa: BLE001 — 1社の失敗で全体を止めない
                    self.logger.warning("detail error [code=%s]: %s", code, e)
                    continue
                if record:
                    yield record

            if last_page and page >= last_page:
                break
            page += 1

    # ------------------------------------------------------------------ 詳細
    def _scrape_company(self, root_url: str, code: str, list_url: str) -> dict | None:
        base_url = urljoin(root_url, f"/campus/company/baseinfo/{code}/")
        employ_url = urljoin(root_url, f"/campus/company/employ/{code}/")

        base_soup = self.get_soup(base_url)
        if base_soup is None:
            return None
        employ_soup = self.get_soup(employ_url)

        base_dl = self._dl_pairs(base_soup)
        emp_dl = self._dl_pairs(employ_soup) if employ_soup is not None else {}

        # --- 4年生向け採用情報のみ採用（インターン専用掲載を除外） ---
        if not self._is_shinsotsu(emp_dl):
            self.logger.info("skip (採用情報なし/インターン限定): %s", employ_url)
            return None

        name = self._name(employ_soup) or self._name(base_soup)
        if not name:
            return None

        # --- 住所 / 郵便番号 / 都道府県 ---
        addr_raw = _norm(self._get(base_dl, "本社所在地", "所在地", "本社住所"))
        postal = ""
        pm = _POSTAL_RE.search(addr_raw)
        if pm:
            postal = pm.group(1).replace("－", "-").replace("‐", "-")
            addr_raw = _norm(addr_raw[:pm.start()] + " " + addr_raw[pm.end():])
        contact = self._get_raw(base_dl, "連絡先", "問い合わせ先", "応募先")
        if not postal:
            pm = _POSTAL_RE.search(contact)
            if pm:
                postal = pm.group(1).replace("－", "-").replace("‐", "-")

        pref = ""
        honsya = self._get(base_dl, "本社")
        pm = _PREF_RE.search(addr_raw) or _PREF_RE.search(honsya)
        if pm:
            pref = pm.group(1)
        else:
            for city, p in _CITY_TO_PREF.items():
                if p and city and city in addr_raw:
                    pref = p
                    break
        addr = addr_raw
        # 「■本社物流センター 京都府…」のような事業所見出しが先頭に付く例があるため、
        # 都道府県より前の見出し部分を落とす
        if pref:
            pos = addr.find(pref)
            if pos > 0 and re.match(r"^[■▶▼●◆□◇★☆・【]", addr):
                addr = addr[pos:]
            if addr.startswith(pref):
                addr = addr[len(pref):].strip()

        # --- TEL / FAX / メール / 採用担当 ---
        tel_src = contact or addr_raw
        tel = ""
        tm = _TEL_RE.search(re.sub(_FAX_RE, " ", tel_src))
        if tm:
            tel = self._clean_tel(tm.group(1))
        fax = ""
        fm = _FAX_RE.search(tel_src)
        if fm:
            fax = self._clean_tel(fm.group(1))
        mails = _MAIL_RE.findall(contact)
        email = mails[0] if mails else ""
        staff_name = self._staff_name(contact)
        staff_contact = "／".join(x for x in [tel, email] if x)

        # --- 代表者 / 役職 ---
        rep_raw = _norm(self._get(base_dl, "代表者", "グループ代表者", "代表取締役"))
        position, rep = self._split_rep(rep_raw)

        # --- SNS ---
        sns = {Schema.INSTA: "", Schema.FB: "", Schema.X: "", Schema.TIKTOK: "", Schema.LINE: ""}
        other_sns = []
        for a in self._sns_links(base_soup) + self._sns_links(employ_soup):
            href = a.strip()
            hit = False
            for domain, key in _SNS_MAP:
                if domain in href.lower():
                    if not sns[key]:
                        sns[key] = href
                    hit = True
                    break
            if not hit and href not in other_sns:
                other_sns.append(href)

        # --- ホームページ ---
        hp = self._get(base_dl, "ホームページ", "会社ＨＰ", "URL")
        hm = re.search(r"https?://\S+", hp)
        hp = hm.group(0) if hm else ""

        # --- セミナー・説明会出展（判定語＋根拠URL） ---
        seminar_titles = self._seminar_titles(employ_soup)
        seminar_urls = self._seminar_urls(root_url, employ_soup)
        seminar_flag = "有" if (seminar_titles or seminar_urls) else "無"

        dates = self._dates(employ_soup) or self._dates(base_soup)

        lob = self._get(base_dl, "事業内容", "事業概要")
        if _is_prose(lob):
            lob = ""

        record = {
            Schema.NAME: name,
            Schema.URL: base_url,
            Schema.PREF: pref,
            Schema.POST_CODE: postal,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CO_NUM: "",
            Schema.REP_NM: rep,
            Schema.POS_NM: position,
            Schema.EMP_NUM: self._get(base_dl, "従業員数", "従業員総数", "社員数"),
            Schema.LOB: lob,
            Schema.CAP: self._get(base_dl, "資本金", "グループ総資本金", "総資本金"),
            Schema.CAT_SITE: self._get(base_dl, "業種"),
            Schema.SALES: self._get(base_dl, "売上高", "グループ売上高", "売上"),
            Schema.OPEN_DATE: self._get(base_dl, "設立", "創業", "設立年月"),
            Schema.HP: hp,
            Schema.EMAIL: email,
            Schema.INSTA: sns[Schema.INSTA],
            Schema.FB: sns[Schema.FB],
            Schema.X: sns[Schema.X],
            Schema.TIKTOK: sns[Schema.TIKTOK],
            Schema.LINE: sns[Schema.LINE],
            # --- EXTRA ---
            "FAX": fax,
            "職種": self._get(base_dl, "職種") or self._get(emp_dl, "職種"),
            "採用職種": self._get(emp_dl, "採用職種", "募集職種"),
            "採用区分": self._saiyo_kubun(emp_dl, employ_soup),
            "雇用形態": self._employment(employ_soup) or self._employment(base_soup),
            "働き方の特徴": self._get(base_dl, "働き方の特徴") or self._get(emp_dl, "働き方の特徴"),
            "勤務地": self._get(emp_dl, "勤務地"),
            "勤務時間": self._get(emp_dl, "勤務時間"),
            "休日休暇": self._get(emp_dl, "休日休暇"),
            "諸手当": self._get(emp_dl, "諸手当"),
            "社会保険": self._get(emp_dl, "社会保険"),
            "各種制度": self._get(emp_dl, "各種制度"),
            "昇給・賞与": self._get(emp_dl, "昇給・賞与"),
            "初任給": self._salary(self._get(emp_dl, "給与", "初任給")),
            "対象卒業年": self._grad_years(self._get(emp_dl, "応募資格")),
            "採用予定学科": self._get(emp_dl, "採用予定学科"),
            "応募・選考時の提出書類": self._get(emp_dl, "応募・選考時の提出書類"),
            "採用予定人数／実績": self._get(emp_dl, "採用予定人数／実績", "採用予定人数"),
            "採用実績校": self._get(emp_dl, "採用実績校"),
            "受動喫煙対策": self._get(emp_dl, "受動喫煙対策"),
            "平均継続勤務年数": self._get(emp_dl, "平均継続勤務年数"),
            "従業員の平均年齢": self._get(emp_dl, "従業員の平均年齢"),
            "過去3年間の新卒採用者数": self._get(
                emp_dl, "過去３年間の新卒採用者数", "過去3年間の新卒採用者数"),
            "過去3年間の新卒離職者数": self._get(
                emp_dl, "過去３年間の新卒離職者数", "過去3年間の新卒離職者数"),
            "前年度の月平均所定外労働時間の実績": self._get(
                emp_dl, "前年度の月平均所定外労働時間の実績"),
            "前年度の有給休暇の平均取得日数": self._get(emp_dl, "前年度の有給休暇の平均取得日数"),
            "育児休業取得者数／対象者数": self._get(
                emp_dl, "取得者数／前年度の育児休業取得対象者数（男女別）", "育児休業"),
            "女性役員・管理職比率": self._get(
                emp_dl, "役員に占める女性の割合及び管理的地位にある者に占める女性の割合"),
            "事業所": self._get(base_dl, "事業所", "支店・営業所"),
            "関連会社": self._get(base_dl, "関連会社", "関連企業", "グループ企業"),
            "主要取引先": self._get(base_dl, "主要取引先", "主な取引先", "主要顧客"),
            "採用担当者名": staff_name,
            "採用担当者連絡先": staff_contact,
            "合同説明会・セミナー出展記録": seminar_flag,
            "セミナー・説明会名": seminar_titles,
            "セミナー・説明会URL": "／".join(seminar_urls),
            "企業SNS_その他": "／".join(other_sns),
            "企業コード": code,
            "最終更新日": dates.get("最終更新日", ""),
            "掲載終了日": dates.get("掲載終了日", ""),
            "求人URL": employ_url,
            "取得元URL": list_url,
        }
        for label, col in _YESNO_FIELDS.items():
            record[col] = _yesno(self._get(emp_dl, label))
        return record

    # ------------------------------------------------------------- ヘルパー
    def _dl_pairs(self, soup) -> dict[str, list[str]]:
        """ページ内の dt/dd を label -> [values] にまとめる。
        ページ下部の「業種から探す」以降（サイト内リンク集）は除外する。"""
        pairs: dict[str, list[str]] = {}
        if soup is None:
            return pairs
        for dt in soup.select("dt"):
            label = _norm(dt.get_text(" ", strip=True))
            if not label:
                continue
            if label.endswith("から探す"):
                break
            dd = dt.find_next_sibling()
            while dd is not None and dd.name not in ("dd", "dt"):
                dd = dd.find_next_sibling()
            if dd is None or dd.name != "dd":
                continue
            value = _norm(dd.get_text("\n", strip=True))
            value = re.sub(r"\n{2,}", "\n", value)
            pairs.setdefault(label, []).append(value)
        return pairs

    def _get(self, pairs: dict[str, list[str]], *labels: str) -> str:
        """ラベル完全一致 → 部分一致の順で最初の非空値を返す。"""
        return re.sub(r"\s*\n\s*", " ", self._get_raw(pairs, *labels)).strip()

    def _get_raw(self, pairs: dict[str, list[str]], *labels: str) -> str:
        """_get と同じ照合で、改行を保持したまま値を返す。"""
        for label in labels:
            for v in pairs.get(label, []):
                if v:
                    return v
        for label in labels:
            for key, values in pairs.items():
                if label in key:
                    for v in values:
                        if v:
                            return v
        return ""

    def _name(self, soup) -> str:
        if soup is None:
            return ""
        el = soup.select_one("h1.sep__name__ttl")
        if not el:
            return ""
        text = _norm(el.get_text(" ", strip=True))
        text = re.sub(r"\s*(NEW!!|UPDATE|NEW)\s*$", "", text).strip()
        # 末尾の【営業職募集】【東証スタンダード上場】等の注記を除去
        text = re.sub(r"(\s*【[^】]*】)+\s*$", "", text).strip()
        return text

    def _employment(self, soup) -> str:
        if soup is None:
            return ""
        vals = [
            _norm(el.get_text(strip=True))
            for el in soup.select(".sep__employment .sep-icon.is-emp")
        ]
        return "／".join(v for v in dict.fromkeys(vals) if v)

    def _saiyo_kubun(self, emp_dl: dict, soup) -> str:
        emp = self._employment(soup)
        years = self._grad_years(self._get(emp_dl, "応募資格"))
        parts = ["新卒採用（大学4年生・既卒）"]
        if years:
            parts.append(f"{years}卒対象")
        if emp:
            parts.append(emp)
        return "／".join(parts)

    def _grad_years(self, text: str) -> str:
        if not text:
            return ""
        years = []
        for m in _GRAD_YEAR_RE.finditer(text):
            y = m.group(1) + "年" + (f"{m.group(2)}月" if m.group(2) else "")
            if y not in years:
                years.append(y)
        with_month = {y[:5] for y in years if len(y) > 5}
        years = [y for y in years if len(y) > 5 or y[:5] not in with_month]
        return "／".join(years)

    def _salary(self, text: str) -> str:
        """給与本文は保存せず、月給・初任給の金額のみを抽出する。"""
        if not text:
            return ""
        amounts = []
        for m in re.finditer(r"(?:（?月給）?|初任給)\s*[:：]?\s*([0-9０-９,，]{5,12})\s*円", text):
            v = m.group(1).translate(str.maketrans("０１２３４５６７８９，", "0123456789,")) + "円"
            if v not in amounts:
                amounts.append(v)
        return "／".join(amounts[:5])

    def _staff_name(self, contact: str) -> str:
        """連絡先ブロックから採用担当（部署・担当者名）の行だけを取り出す。"""
        if not contact:
            return ""
        names = []
        for line in re.split(r"[\n/／]|\s{2,}", contact):
            line = _norm(line)
            if not line or _POSTAL_RE.search(line) or _MAIL_RE.search(line):
                continue
            if re.search(r"担当|人事|採用|窓口|課|部$", line) and not _TEL_RE.search(line):
                line = re.sub(r"^[●■◆・]+", "", line).strip()
                if line and line not in names:
                    names.append(line)
        return "／".join(names[:3])

    def _sns_links(self, soup) -> list[str]:
        if soup is None:
            return []
        links = []
        for dt in soup.select("dt"):
            if "ＳＮＳ" in dt.get_text() or "SNS" in dt.get_text():
                dd = dt.find_next_sibling()
                while dd is not None and dd.name not in ("dd", "dt"):
                    dd = dd.find_next_sibling()
                if dd is not None and dd.name == "dd":
                    links += [a.get("href", "") for a in dd.select("a[href]")]
        return [l for l in links if l]

    def _seminar_titles(self, soup) -> str:
        """セミナー・説明会の名称（短いラベル）のみ。本文・詳細は保存しない。"""
        if soup is None:
            return ""
        titles = []
        for li in soup.select(".sep__detail__body__seminar li, .sep__detail__events-sub li"):
            head = li.select_one("a") or li.select_one("h4, .sep-heading, .sep__seminar__ttl")
            text = _norm((head.get_text(" ", strip=True) if head else li.get_text(" ", strip=True)))
            # 「【開催地／開催日時】…」以降は本文扱いなので落とす（名称が【】で始まる例があるため
            # 単純な split("【") は使わない）
            text = re.split(r"【開催[地日]", text)[0].strip()
            if text and len(text) <= 60 and text not in titles:
                titles.append(text)
        return "／".join(titles[:10])

    def _seminar_urls(self, root_url: str, soup) -> list[str]:
        """セミナー・説明会情報の根拠URL（本文は保存しない）。"""
        if soup is None:
            return []
        urls = []
        sels = [
            ".sep__detail__body__seminar a[href]",
            ".sep__detail__events-sub a[href]",
            '#smnPnlMt a[href*="cmp_seminar"]',
        ]
        for sel in sels:
            for a in soup.select(sel):
                href = (a.get("href") or "").strip()
                if not href or href.startswith("javascript"):
                    continue
                # タブナビの「インターンシップ＆キャリア情報」リンクは 4 年生向けではないため除外
                if "sitemode=intern" in href:
                    continue
                full = urljoin(root_url, href)
                if full not in urls:
                    urls.append(full)
        return urls[:5]

    def _dates(self, soup) -> dict[str, str]:
        out: dict[str, str] = {}
        if soup is None:
            return out
        el = soup.select_one(".sep__date")
        if not el:
            return out
        for span in el.select("span"):
            t = _norm(span.get_text(strip=True))
            m = re.match(r"(最終更新日|掲載終了日)[：:]\s*([\d/]+)", t)
            if m:
                out[m.group(1)] = m.group(2)
        return out

    def _is_shinsotsu(self, emp_dl: dict) -> bool:
        """採用情報タブに新卒採用の内容があるか（インターン限定掲載を除外）。"""
        if not emp_dl:
            return False
        if not (self._get(emp_dl, "採用職種", "募集職種") or self._get(emp_dl, "応募資格")):
            return False
        shikaku = self._get(emp_dl, "応募資格")
        if shikaku and _INTERN_ONLY_RE.search(shikaku) and not _GRAD_YEAR_RE.search(shikaku):
            return False
        return True

    @staticmethod
    def _split_rep(rep_raw: str) -> tuple[str, str]:
        """代表者欄を「役職」と「氏名」に分割する（例: 代表取締役社長 徳田 祥恭）。"""
        t = _norm(rep_raw)
        if not t:
            return "", ""
        t = re.sub(r"代表$", "", t).strip()
        for pos in _POSITIONS:
            # 「代表取締役 社長」のように役職内に空白が入る表記があるため空白許容で照合する
            mt = re.search(r"\s*".join(re.escape(c) for c in pos), t)
            if not mt:
                continue
            name = (t[:mt.start()] + " " + t[mt.end():]).strip()
            name = re.sub(r"^[　\s・/／]+|[　\s・/／]+$", "", name)
            return pos, _norm(name)
        return "", t

    @staticmethod
    def _clean_tel(tel: str) -> str:
        t = tel.translate(str.maketrans("０１２３４５６７８９（）－‐", "0123456789()--"))
        t = t.replace("(", "-").replace(")", "-")
        t = re.sub(r"-{2,}", "-", t).strip("-")
        return t


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Re4Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.gakujo.ne.jp/campus/search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
