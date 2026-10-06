# scripts/sites/jobs/work_2.py
"""
ホットペッパービューティーワーク (work.beauty.hotpepper.jp) — 全国・全職種 求人情報スクレイパー

取得単位:
    1 行 = 1 求人 (求人ID = JP 番号)。
    同一 JP ページ内に雇用形態違いの募集要項パネル (正社員 / パート等) が複数ある場合は、
    雇用形態・給与・勤務時間などを " / " で連結して 1 行にまとめる。

取得フロー:
    一覧 /{OC}/prefecture{NN}/?page=N   (職種16 × 都道府県47 = 752 検索。1ページ20件)
      → 求人詳細 /WC********/JP**********/   (募集要項 + 企業情報 + 勤務店舗)
      → 企業ページ /WC********/              (事業内容・店舗数)  ※WC 単位でキャッシュ
      → 店舗ページ /WC********/WS**********/ (営業時間・定休日・席数・スタッフ数) ※WS 単位でキャッシュ

    一覧は 1 検索あたり最大 500 ページで打ち切られるため、職種 × 都道府県の組み合わせ単位で
    巡回する。時間切れで打ち切られても職種・地域が偏らないよう、全組み合わせを
    1 ページずつ交互に進めるラウンドロビン方式とする (起点 URL の組み合わせを先頭に置く)。

robots.txt:
    Disallow は /search/ /*/application /my/ /bookmarks/ /recpon-callback/ /api/ のみ。
    本スクレイパーが辿る /{OC}/prefecture{NN}/ ・ /WC*/ ・ /WC*/JP*/ ・ /WC*/WS*/ は対象外。

利用規約 (https://cdn.p.recruit.co.jp/terms/hpj-t-1001/index.html):
    スクレイピング・クローリングを明示的に禁止する条項は無い。
    第6条1項が掲載情報の引用範囲を超えた使用を禁じているため、
    仕事内容・企業紹介・口コミ等の長文 (自由記述) は取得対象から除外している。

実行方法:
    python scripts/sites/jobs/work_2.py
    python bin/run_flow.py --site-id work_2
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 職種コード (トップページのナビゲーションより / 全16職種)
_OCCUPATIONS = {
    "OC01": "美容師",
    "OC02": "理容師",
    "OC03": "ネイリスト",
    "OC04": "アイデザイナー",
    "OC05": "セラピスト",
    "OC06": "リフレクソロジスト",
    "OC07": "整体師",
    "OC08": "カイロプラクター",
    "OC09": "柔道整復師",
    "OC10": "あん摩マッサージ指圧師",
    "OC11": "鍼灸師",
    "OC12": "エステティシャン",
    "OC13": "脱毛スタッフ",
    "OC14": "インストラクター・トレーナー",
    "OC15": "美容部員",
    "OC16": "受付・事務",
}

_PREF_NAMES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
_PREF_RE = re.compile("(" + "|".join(_PREF_NAMES) + ")")

# 求人詳細 URL  例: https://work.beauty.hotpepper.jp/WC00035457/JP0000099005/?prefecture=13
_JOB_URL_RE = re.compile(r"/(WC\d+)/(JP\d+)/")
# 勤務店舗リンク 例: /WC00035457/WS0000088360/
_STORE_URL_RE = re.compile(r"/(WC\d+)/(WS\d+)/")
# 起点 URL から職種・都道府県を読み取る
_LIST_URL_RE = re.compile(r"/(OC\d{2})/prefecture(\d{2})/")


def _clean(text: str) -> str:
    """連続する空白・改行を 1 つの半角スペースに潰す。"""
    return re.sub(r"\s+", " ", text or "").strip()


def _join(values) -> str:
    """空でない値を重複を除いて ' / ' で連結する。"""
    out = []
    for v in values:
        v = _clean(v)
        if v and v not in out:
            out.append(v)
    return " / ".join(out)


class HotpepperBeautyWorkScraper(StaticCrawler):
    """ホットペッパービューティーワーク 全国・全職種 求人スクレイパー"""

    # 低負荷運用: 一覧/詳細の取得ごとに待機し、アイテム書き出し時にも軽く待つ
    DELAY = 1.0
    ITEM_DELAY = 0.5

    # 1 検索あたりのページ数上限 (サイト側が約500ページで打ち切る)
    MAX_PAGE = 500

    EXTRA_COLUMNS = [
        "企業ID",
        "本社所在地",
        "設立年",
        "店舗数",
        "店舗ID",
        "店舗住所",
        "店舗アクセス",
        "席数",
        "スタッフ数",
        "運営会社",
        "求人ID",
        "雇用形態",
        "給与",
        "勤務地住所",
        "勤務地アクセス",
        "勤務時間",
        "休日・休暇",
        "福利厚生",
        "応募資格",
        "募集人数",
    ]

    def prepare(self):
        """同一 ID の企業ページ・店舗ページを重複取得しないためのキャッシュ。"""
        self._company_cache: dict[str, dict] = {}
        self._store_cache: dict[str, dict] = {}
        self._seen_jobs: set[str] = set()

    # ------------------------------------------------------------------
    # メイン処理
    # ------------------------------------------------------------------
    def parse(self, url: str):
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"

        combos = self._build_combos(url)
        self.logger.info("巡回対象: %d 組み合わせ (職種 × 都道府県)", len(combos))

        # 各組み合わせの一覧ルート URL
        roots = {c: f"{base}/{c[0]}/prefecture{c[1]}/" for c in combos}
        active = list(combos)

        page = 1
        while active and page <= self.MAX_PAGE:
            next_active = []
            for combo in active:
                root = roots[combo]
                list_url = root if page == 1 else f"{root}?page={page}"

                soup = self.get_soup(list_url)
                if soup is None:
                    continue

                cards = soup.select("div.job-posting[data-job-posting-url]")
                if not cards:
                    continue

                for card in cards:
                    m = _JOB_URL_RE.search(card.get("data-job-posting-url", ""))
                    if not m:
                        continue
                    company_id, job_id = m.group(1), m.group(2)
                    if job_id in self._seen_jobs:
                        continue
                    self._seen_jobs.add(job_id)

                    job_url = f"{base}/{company_id}/{job_id}/"
                    item = self._build_item(job_url, company_id, job_id, combo[0])
                    if item:
                        yield item

                # 「次へ」リンクがあるページだけ次ラウンドに残す
                if soup.select_one("li.c-pagination__action--next a[href]"):
                    next_active.append(combo)

            active = next_active
            page += 1

    # ------------------------------------------------------------------
    # 巡回対象の組み立て
    # ------------------------------------------------------------------
    def _build_combos(self, url: str) -> list[tuple[str, str]]:
        """職種 × 都道府県の全組み合わせ。起点 URL の組み合わせを先頭に置く。"""
        all_combos = [(oc, f"{n:02d}") for oc in _OCCUPATIONS for n in range(1, 48)]

        m = _LIST_URL_RE.search(url)
        if m:
            head = (m.group(1), m.group(2))
            if head in all_combos:
                all_combos.remove(head)
                all_combos.insert(0, head)
        return all_combos

    # ------------------------------------------------------------------
    # 1 求人分のデータ組み立て
    # ------------------------------------------------------------------
    def _build_item(self, job_url: str, company_id: str, job_id: str, occupation_code: str) -> dict | None:
        soup = self.get_soup(job_url)
        if soup is None:
            return None

        # --- 募集要項 (雇用形態ごとのパネルを横断して集約) ---
        panels = soup.select("div.l-job-description.js-job-description") or [soup]
        occupations, positions = [], []
        employment, salary, work_hours, holidays = [], [], [], []
        benefits, qualifications, headcounts = [], [], []

        for panel in panels:
            labels = self._label_map(panel)
            occ_pos = labels.get("職種／役職", "")
            if occ_pos:
                parts = [p.strip() for p in re.split(r"[／/]", occ_pos, maxsplit=1)]
                occupations.append(parts[0])
                if len(parts) > 1:
                    positions.append(parts[1])
            employment.append(labels.get("雇用形態", ""))
            salary.append(labels.get("給与", ""))
            work_hours.append(labels.get("勤務時間", ""))
            holidays.append(
                _join([labels.get("休日", ""), labels.get("年間休日数", ""), labels.get("休暇", "")])
            )
            benefits.append(
                _join([labels.get("社会保険", ""), labels.get("その他制度", "")])
            )
            qualifications.append(
                _join([labels.get("必要資格", ""), labels.get("必要経験", "")])
            )
            for key, value in labels.items():
                if "募集人数" in key or "採用予定人数" in key:
                    headcounts.append(value)

        # --- 勤務店舗 (求人詳細側) ---
        store_id = ""
        work_address = ""
        work_access = ""
        store_block = soup.select_one("li.recruiting-stores__list a[href]")
        if store_block:
            ms = _STORE_URL_RE.search(store_block.get("href", ""))
            if ms:
                store_id = ms.group(2)
            el = store_block.select_one("p.recruiting-stores__store-address")
            work_address = _clean(el.get_text(" ", strip=True)) if el else ""
            el = store_block.select_one("p.recruiting-stores__store-access")
            work_access = _clean(el.get_text(" ", strip=True)) if el else ""

        # --- 企業情報 (求人詳細ページ内の企業情報ブロック) ---
        company = self._company_information(soup)

        # --- 企業ページ (事業内容・店舗数) — WC 単位でキャッシュ ---
        company_detail = self._fetch_company(urljoin(job_url, f"/{company_id}/"), company_id)

        # --- 店舗ページ (営業時間・定休日・席数・スタッフ数) — WS 単位でキャッシュ ---
        store = {}
        if store_id:
            store = self._fetch_store(urljoin(job_url, f"/{company_id}/{store_id}/"), store_id)

        company_name = company.get("企業名", "") or company_detail.get("企業名", "")
        head_office = company.get("所在地", "") or company_detail.get("所在地", "")
        pref, rest = self._split_address(head_office)

        occupation = _join(occupations) or _OCCUPATIONS.get(occupation_code, "")

        return {
            Schema.URL: job_url,
            Schema.NAME: company_name,
            Schema.PREF: pref,
            Schema.ADDR: rest,
            Schema.LOB: company_detail.get("事業内容", ""),
            Schema.CAT_SITE: occupation,
            Schema.FAC_NAME: store.get("店舗名", ""),
            Schema.TIME: store.get("営業時間", ""),
            Schema.HOLIDAY: store.get("定休日", ""),
            "企業ID": company_id,
            "本社所在地": head_office,
            "設立年": company.get("設立年", "") or company_detail.get("設立年", ""),
            "店舗数": company.get("店舗数", "") or company_detail.get("店舗数", ""),
            "店舗ID": store_id,
            "店舗住所": store.get("所在地", ""),
            "店舗アクセス": store.get("アクセス", ""),
            "席数": store.get("席数", ""),
            "スタッフ数": store.get("スタッフ数", ""),
            "運営会社": store.get("運営会社", "") or company_name,
            "求人ID": job_id,
            "雇用形態": _join(employment),
            "給与": _join(salary),
            "勤務地住所": work_address,
            "勤務地アクセス": work_access,
            "勤務時間": _join(work_hours),
            "休日・休暇": _join(holidays),
            "福利厚生": _join(benefits),
            "応募資格": _join(qualifications),
            "募集人数": _join(headcounts),
        }

    # ------------------------------------------------------------------
    # 企業ページ / 店舗ページ (キャッシュ付き)
    # ------------------------------------------------------------------
    def _fetch_company(self, company_url: str, company_id: str) -> dict:
        if company_id in self._company_cache:
            return self._company_cache[company_id]

        data: dict = {}
        soup = self.get_soup(company_url)
        if soup is not None:
            data = self._company_information(soup)
        self._company_cache[company_id] = data
        return data

    def _fetch_store(self, store_url: str, store_id: str) -> dict:
        if store_id in self._store_cache:
            return self._store_cache[store_id]

        data: dict = {}
        soup = self.get_soup(store_url)
        if soup is not None:
            el = soup.select_one("h1.page-summary__title")
            if el:
                data["店舗名"] = re.sub(r"の求人・転職・採用情報$", "", _clean(el.get_text(strip=True)))

            # 営業時間 / 定休日 (dl.c-definition-list)
            for dl in soup.select("dl.c-definition-list"):
                for dt in dl.select("dt"):
                    dd = dt.find_next_sibling("dd")
                    label = _clean(dt.get_text(strip=True))
                    if label in ("営業時間", "定休日") and dd and label not in data:
                        data[label] = _clean(dd.get_text(" ", strip=True))

            # 見出し駆動のサブセクション (所在地・アクセス / 席数・設備数 / スタッフ数)
            for sec in soup.select("div.l-sub-section--lv3"):
                heading = sec.select_one("h3")
                if not heading:
                    continue
                title = _clean(heading.get_text(strip=True))
                if title == "所在地・アクセス":
                    el = sec.select_one("p.c-paragraph")
                    if el and "所在地" not in data:
                        data["所在地"] = _clean(el.get_text(" ", strip=True))
                    access = [_clean(li.get_text(" ", strip=True)) for li in sec.select("ul.c-list li")]
                    if access and "アクセス" not in data:
                        data["アクセス"] = _join(access)
                elif title == "席数・設備数" and "席数" not in data:
                    data["席数"] = _join(
                        p.get_text(" ", strip=True) for p in sec.select("p.c-paragraph")
                    )
                elif title == "スタッフ数" and "スタッフ数" not in data:
                    data["スタッフ数"] = _join(
                        p.get_text(" ", strip=True) for p in sec.select("p.c-paragraph")
                    )

            # 店舗を運営する企業 (企業情報ブロックの企業名)
            data["運営会社"] = self._company_information(soup).get("企業名", "")

        self._store_cache[store_id] = data
        return data

    # ------------------------------------------------------------------
    # 共通パーサ
    # ------------------------------------------------------------------
    @staticmethod
    def _label_map(node) -> dict:
        """dt/dd 定義リストを {ラベル: 値} に変換する (同一ラベルは最初の値を採用)。"""
        result: dict[str, str] = {}
        for dt in node.select("dl.job-description__table dt"):
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            label = _clean(dt.get_text(strip=True))
            if label and label not in result:
                result[label] = _clean(dd.get_text(" ", strip=True))
        return result

    @staticmethod
    def _company_information(node) -> dict:
        """dl.company-information (企業名 / 所在地 / 設立年 / 店舗数 / 事業内容) を辞書化。"""
        result: dict[str, str] = {}
        for dl in node.select("dl.company-information"):
            for dt in dl.select("dt"):
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue
                label = _clean(dt.get_text(strip=True))
                if label and label not in result:
                    result[label] = _clean(dd.get_text(" ", strip=True))
        return result

    @staticmethod
    def _split_address(address: str) -> tuple[str, str]:
        """住所を (都道府県, 市区町村以降) に分割する。"""
        address = _clean(address)
        if not address:
            return "", ""
        m = _PREF_RE.search(address)
        if not m:
            return "", address
        return m.group(1), address[m.end():].strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = HotpepperBeautyWorkScraper()
    scraper.execute("https://work.beauty.hotpepper.jp/OC01/prefecture13/")
