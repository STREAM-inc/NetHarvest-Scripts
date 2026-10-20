"""
対象サイト: https://tenshoku.create-jobs.com/JobChangeConstruction/ (クリエイト転職 建設・建築)

サイト構造メモ (2026-10 調査):
- 建設・建築カテゴリの全国一覧。`?page=N` でページ送り (20 件/ページ、約 368 件 / 19 ページ)。
  `?p=N` 等は無視され 1 ページ目が返る。最終ページ + 1 は 404。
- 一覧カード `li.item` の詳細リンクは `href="javascript:void(0)"` で、
  `data-company-id` / `data-recruitment-id` から `/jobs/{企業ID}/{求人ID}/` を組み立てる。
- 企業ページ `/company/detail/{ID}/` は 404 (存在しない)。会社名・所在地・代表者・URL 等は
  詳細ページの「企業情報」セクションにあるためそこから取る。
- 電話番号は `div.tel` に静的に埋め込まれている (`span.pc-only` と `a.sp-only` に同じ番号)。
- 更新日・掲載期間は詳細ページ上部に「更新日 : YYYY/MM/DD 掲載期間 : YYYY/MM/DD ～ YYYY/MM/DD」。
- 備考により勤務地が関東・甲信越・東海の対象 14 都県の求人のみを出力する
  (勤務地が無い場合は会社所在地の都道府県で判定)。
"""

import re
from typing import Generator
from urllib.parse import urljoin

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 都道府県 (住所文字列からの切り出し用)
PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# 元号 → 西暦の開始年 (元年 = 開始年 + 1)
ERA_BASE = {"明治": 1867, "大正": 1911, "昭和": 1925, "平成": 1988, "令和": 2018}

# 代表者欄によく現れる役職 (全角スペース等が無いケースの分割用)
POSITION_PATTERN = re.compile(
    r"^(代表取締役社長|代表取締役会長|代表取締役CEO|代表取締役|取締役社長|"
    r"代表社員|代表理事|理事長|代表者|会長|社長|院長|園長|店主|代表)"
)


# 出力対象とする勤務地の都道府県 (備考指定)
TARGET_PREFECTURES = [
    "東京都", "神奈川県", "埼玉県", "千葉県", "茨城県", "栃木県", "群馬県",
    "新潟県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県", "三重県",
]

_UPDATE_RE = re.compile(r"更新日\s*[：:]\s*(\d{4}/\d{1,2}/\d{1,2})")
_PERIOD_RE = re.compile(
    r"掲載期間\s*[：:]\s*(\d{4}/\d{1,2}/\d{1,2})\s*～\s*(\d{4}/\d{1,2}/\d{1,2})?"
)


class TenshokuCreateJobsConstructionScraper(StaticCrawler):
    """クリエイト転職 建設・建築求人 スクレイパー (静的)"""

    DELAY = 3.0

    EXTRA_COLUMNS = [
        "求人タイトル",
        "職種カテゴリ",
        "雇用形態",
        "給与",
        "勤務地",
        "勤務地_都道府県",
        "最寄り駅",
        "更新日",
        "掲載開始日",
        "掲載終了日",
        "応募先名称",
        "担当者名",
        "企業ID",
        "求人ID",
    ]

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        """一覧 (?page=N) → 詳細の順に巡回し、詳細を 1 件取得するごとに yield する。"""
        list_url = url.split("?")[0]
        seen: set[str] = set()

        page = 1
        while True:
            page_url = list_url if page == 1 else f"{list_url}?page={page}"
            soup = self.get_soup(page_url)
            # 最終ページ + 1 は 404 (CONTINUE_ON_ERROR により None)
            if soup is None:
                break

            cards = soup.select("li.item")
            detail_urls = [u for u in self._iter_detail_urls(url, cards) if u not in seen]
            if not detail_urls:
                break

            for detail_url in detail_urls:
                seen.add(detail_url)
                item = self._parse_detail(detail_url)
                if item and self._in_target_area(item):
                    yield item

            page += 1

    @staticmethod
    def _in_target_area(item: dict) -> bool:
        """勤務地 (複数可) に対象都県が含まれるか。勤務地が無ければ会社所在地で判定。"""
        work = item.get("勤務地", "")
        if work:
            return any(pref in work for pref in TARGET_PREFECTURES)
        return item.get(Schema.PREF, "") in TARGET_PREFECTURES

    # ------------------------------------------------------------------ #
    # 一覧
    # ------------------------------------------------------------------ #
    def _iter_detail_urls(self, root_url: str, cards: list[bs4.Tag]) -> list[str]:
        """一覧カードの data 属性から詳細 URL を組み立てる (href は javascript:void(0))。"""
        urls: list[str] = []
        for card in cards:
            for a in card.select("a.recruitment-detail-link"):
                company_id = a.get("data-company-id")
                recruit_id = a.get("data-recruitment-id")
                if not company_id or not recruit_id:
                    continue
                detail_url = urljoin(root_url, f"/jobs/{company_id}/{recruit_id}/")
                if detail_url not in urls:
                    urls.append(detail_url)
        return urls

    # ------------------------------------------------------------------ #
    # 詳細
    # ------------------------------------------------------------------ #
    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        company = self._labeled_values(self._section(soup, "企業情報"))
        contact = self._labeled_values(self._section(soup, "応募方法"))
        taxonomy = self._taxonomy(soup)

        # --- 名称: 企業情報の会社名を優先し、無ければ応募先名称 ---
        name = company.get("会社名", "") or contact.get("お問合せ先", "")
        if not name:
            return None

        # --- 住所: 企業情報の所在地を優先し、無ければ応募先の所在地 ---
        raw_addr = company.get("所在地", "") or contact.get("所在地", "")
        post_code, pref, addr = self._split_address(raw_addr)
        if not pref:
            # 会社所在地から取れない場合は勤務地の都道府県で補完
            pref = self._first_token(taxonomy.get("勤務地", ""))
            pref = pref if pref in PREFECTURES else ""

        position, rep_name = self._split_representative(company.get("代表者", ""))

        title_el = soup.select_one("h1")
        title = title_el.get_text(" ", strip=True) if title_el else ""

        work_pref = self._first_token(taxonomy.get("勤務地", ""))
        text = soup.get_text(" ", strip=True)

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: self._tel(soup),
            Schema.REP_NM: rep_name,
            Schema.POS_NM: position,
            Schema.EMP_NUM: company.get("従業員数", ""),
            Schema.CAP: company.get("資本金", ""),
            Schema.OPEN_DATE: self._to_iso_date(company.get("設立", "")),
            Schema.HP: company.get("URL", ""),
            Schema.CAT_SITE: taxonomy.get("業種", ""),
            "求人タイトル": title,
            "職種カテゴリ": taxonomy.get("職種", ""),
            "雇用形態": taxonomy.get("雇用形態", ""),
            "給与": self._salary(soup),
            "勤務地": taxonomy.get("勤務地", ""),
            "勤務地_都道府県": work_pref if work_pref in PREFECTURES else "",
            "最寄り駅": taxonomy.get("最寄り駅", ""),
            "更新日": self._date_text(_UPDATE_RE, text, 1),
            "掲載開始日": self._date_text(_PERIOD_RE, text, 1),
            "掲載終了日": self._date_text(_PERIOD_RE, text, 2),
            "応募先名称": contact.get("お問合せ先", ""),
            "担当者名": contact.get("担当者名", ""),
            "企業ID": self._id_from_url(detail_url, 0),
            "求人ID": self._id_from_url(detail_url, 1),
        }

    # ------------------------------------------------------------------ #
    # 詳細ページ内のパーツ抽出
    # ------------------------------------------------------------------ #
    @staticmethod
    def _section(soup: bs4.BeautifulSoup, heading: str) -> bs4.Tag | None:
        """`details.list-parent` を summary の見出し文字列で特定する。"""
        for details in soup.select("details.list-parent"):
            h3 = details.select_one("summary h3")
            if h3 and h3.get_text(strip=True) == heading:
                return details
        return None

    @staticmethod
    def _labeled_values(scope: bs4.Tag | None) -> dict[str, str]:
        """`div.fw6` / `p.fw6` のラベルと、その次兄弟に入る値を対応付ける。

        同じラベル (「所在地」など) が複数現れる場合は最初のものを採用する。
        """
        values: dict[str, str] = {}
        if scope is None:
            return values

        for label_el in scope.find_all(["p", "div"], class_="fw6"):
            label = label_el.get_text(strip=True)
            sibling = label_el.find_next_sibling()
            if not label or sibling is None:
                continue
            value = sibling.get_text("\n", strip=True)
            if value and label not in values:
                values[label] = value
        return values

    @staticmethod
    def _taxonomy(soup: bs4.BeautifulSoup) -> dict[str, str]:
        """ページ下部の「職種・勤務地・こだわり条件で転職・求人を探す」ブロックを読む。

        職種 / 勤務地 / 条件 / 雇用形態 / 業種 / 最寄り駅 が構造化された短いラベルで並ぶ。
        """
        block = None
        for h4 in soup.select("h4.ttl-left-bdr"):
            if "こだわり条件" in h4.get_text():
                block = h4.parent
                break
        if block is None:
            return {}

        values: dict[str, str] = {}
        for label_el in block.find_all("p", class_="fw6", recursive=False):
            label = label_el.get_text(strip=True)
            parts: list[str] = []
            for sibling in label_el.find_next_siblings():
                # 次のラベル / 見出しに到達したら終了
                if sibling.name == "h4":
                    break
                if sibling.name == "p" and "fw6" in (sibling.get("class") or []):
                    break
                text = sibling.get_text(" ", strip=True)
                if text:
                    parts.append(text)
            if parts and label not in values:
                values[label] = " ".join(parts)
        return values

    @staticmethod
    def _tel(soup: bs4.BeautifulSoup) -> str:
        """`div.tel` は同じ番号を 2 箇所に持つため、先頭の子要素だけを見る。"""
        tel_box = soup.select_one("div.tel")
        if tel_box is None:
            return ""
        for child in tel_box.find_all(["span", "a"], recursive=False):
            text = child.get_text(strip=True)
            if re.fullmatch(r"[\d\-()＋+]{8,}", text):
                return text
        return ""

    @staticmethod
    def _salary(soup: bs4.BeautifulSoup) -> str:
        """募集情報の「給与」見出し直下、`div.fw6` に入る短い給与表記を取る。

        続く `span.pre-wrap` は補足の長文なので採用しない。
        """
        for h4 in soup.select("h4.fw6"):
            if h4.get_text(strip=True) != "給与":
                continue
            block = h4.find_next_sibling("div")
            if block is None:
                continue
            head = block.find(class_="fw6")
            if head is not None:
                return head.get_text(" ", strip=True)
        return ""

    @staticmethod
    def _date_text(pattern: re.Pattern, text: str, group: int) -> str:
        """YYYY/M/D を YYYY-MM-DD に正規化して返す。"""
        m = pattern.search(text)
        if not m or not m.group(group):
            return ""
        y, mo, d = (int(x) for x in m.group(group).split("/"))
        return f"{y:04d}-{mo:02d}-{d:02d}"

    @staticmethod
    def _id_from_url(detail_url: str, index: int) -> str:
        m = re.search(r"/jobs/([^/]+)/([^/]+)/?$", detail_url)
        return m.group(index + 1) if m else ""

    # ------------------------------------------------------------------ #
    # 値の正規化
    # ------------------------------------------------------------------ #
    @staticmethod
    def _first_token(text: str) -> str:
        return text.split()[0] if text.split() else ""

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        """所在地文字列を (郵便番号, 都道府県, 市区町村以降) に分解する。

        企業情報側は `<span>160-0023</span><span>東京都…</span>` の 2 span 構成、
        応募先側は `〒160-0023 東京都…` のような 1 行構成になっている。
        """
        if not raw:
            return "", "", ""

        text = raw.replace("　", " ")
        post_code = ""
        m = re.search(r"〒?\s*(\d{3}-?\d{4})", text)
        if m:
            post_code = m.group(1)
            if "-" not in post_code:
                post_code = f"{post_code[:3]}-{post_code[3:]}"
            text = text.replace(m.group(0), " ", 1)

        text = re.sub(r"\s+", " ", text).strip()

        for pref in PREFECTURES:
            idx = text.find(pref)
            if idx != -1:
                return post_code, pref, text[idx + len(pref):].strip()

        return post_code, "", text

    @staticmethod
    def _split_representative(raw: str) -> tuple[str, str]:
        """代表者欄を (役職, 氏名) に分解する。例: 「代表取締役　岡本 和也」"""
        if not raw:
            return "", ""

        text = raw.replace("　", " ").strip()
        m = POSITION_PATTERN.match(text)
        if m:
            return m.group(1), text[m.end():].strip(" ／/:：")

        parts = text.split(" ", 1)
        if len(parts) == 2:
            return parts[0], parts[1].strip()
        return "", text

    @staticmethod
    def _to_iso_date(raw: str) -> str:
        """設立年月日を YYYY-MM-DD に正規化する。

        例: 「昭和48年7月11日」「1962年（昭和37年）7月7日」「2010年4月」
        年しか取れない場合は年のみを返す。
        """
        if not raw:
            return ""

        text = raw.replace("　", " ")

        year = None
        m = re.search(r"(19|20)\d{2}\s*年", text)
        if m:
            year = int(m.group(0).replace("年", "").strip())
        else:
            m = re.search(r"(明治|大正|昭和|平成|令和)\s*(\d{1,2}|元)\s*年", text)
            if m:
                era_year = 1 if m.group(2) == "元" else int(m.group(2))
                year = ERA_BASE[m.group(1)] + era_year
        if year is None:
            return ""

        # 元号併記 (1962年（昭和37年）7月7日) に備え、年表記より後ろから月日を探す
        tail = text[m.end():]
        month_m = re.search(r"(\d{1,2})\s*月", tail)
        if not month_m:
            return f"{year:04d}"
        month = int(month_m.group(1))

        day_m = re.search(r"(\d{1,2})\s*日", tail[month_m.end():])
        if not day_m:
            return f"{year:04d}-{month:02d}"
        return f"{year:04d}-{month:02d}-{int(day_m.group(1)):02d}"


if __name__ == "__main__":
    scraper = TenshokuCreateJobsConstructionScraper()
    scraper.execute("https://tenshoku.create-jobs.com/JobChangeConstruction/")
