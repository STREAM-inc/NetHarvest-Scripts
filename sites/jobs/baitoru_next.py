# scripts/sites/jobs/baitoru_next.py
"""
バイトルNEXT — 全国 正社員・契約社員求人 企業情報スクレイパー

取得対象:
    - 詳細ページの企業情報セクション (div.detail-companyInfo)
        社名, 住所, TEL, 代表者名, 事業内容, HP URL
    - 詳細ページの基本情報セクション (div.detail-basicInfo)
        職種, 雇用形態, 給与, 勤務時間, 最寄り駅, 休日・休暇, 福利厚生
    - 詳細ページの募集情報セクション (div.detail-recruitInfo)
        仕事内容, 応募条件
    - 詳細ページの求人特徴セクション (div.detail-companyChar)
        求人特徴タグ (未経験OK / 学歴不問 / 車通勤OK 等)

取得フロー:
    起点 (/shain/) から地域ブロックリンク /{block}/shain/ を収集
      (tohoku(北海道含む) / kanto / koshinetsu / tokai / kansai / chushikoku / kyushu)
    → 各ブロックの求人一覧 /{block}/jlist/shain/ を link[rel="next"] でページ送り
      (/{block}/jlist/pageN/shain/) しながらラウンドロビンで巡回
    → 各詳細ページから企業・求人情報を取得

    ※ 全国で約81万件と大規模なため、ブロックを 1 ページずつ交互に進める
      ラウンドロビン方式とし、時間切れで打ち切られても地域が偏らないようにする。

    起点 URL に一覧ページ (/{block}/jlist/shain/ 等) が直接渡された場合は、
    そのページ自身をルート一覧として扱う。

実行方法:
    python scripts/sites/jobs/baitoru_next.py
    python bin/run_flow.py --site-id baitoru_next
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

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

# 起点ページ (/shain/) にある地域ブロックリンク  例: /kanto/shain/
_BLOCK_LINK_RE = re.compile(r"^/([a-z]+)/shain/?$")

# 詳細ページ  例: /tohoku/jlist/hokkaido/sapporoshi/job162906706/shain/
_JOB_DETAIL_RE = re.compile(r"/job\d+/shain/?$")

_TYPE_MAP = {
    "正": "正社員",
    "契": "契約社員",
    "派": "派遣社員",
    "業": "業務委託",
    "紹": "紹介予定派遣",
    "無": "無期雇用派遣",
}


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


class BaitoruNextScraper(StaticCrawler):
    """バイトルNEXT 全国 求人企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "職種",
        "雇用形態",
        "給与",
        "勤務時間",
        "最寄り駅",
        "休日・休暇",
        "仕事内容",
        "応募条件",
        "福利厚生",
        "求人特徴タグ",
    ]

    def parse(self, url: str):
        list_roots = self._collect_list_roots(url)
        if not list_roots:
            self.logger.warning("一覧ページを検出できませんでした: %s", url)
            return

        self.logger.info("巡回対象一覧: %s", ", ".join(list_roots))

        # 各一覧のカーソル (次に取得するページ URL)。None になったブロックは完了。
        cursors = {root: root for root in list_roots}
        counted = set()
        total = 0
        seen_details = set()

        # ブロックを 1 ページずつ交互に進める (地域の偏りを防ぐ)
        while any(cursors.values()):
            for root in list_roots:
                list_url = cursors.get(root)
                if not list_url:
                    continue

                soup = self.get_soup(list_url)
                if soup is None:
                    cursors[root] = None
                    continue

                if root not in counted:
                    counted.add(root)
                    count_el = soup.select_one("#js-job-count")
                    if count_el:
                        try:
                            total += int(count_el.get_text(strip=True).replace(",", ""))
                            self.total_items = total
                        except ValueError:
                            pass

                articles = soup.select("article.list-jobListDetail")
                if not articles:
                    cursors[root] = None
                    continue

                for article in articles:
                    a_tag = article.select_one("h3 a[href]")
                    if not a_tag:
                        continue
                    detail_url = urljoin(list_url, a_tag["href"])
                    if not _JOB_DETAIL_RE.search(urlparse(detail_url).path):
                        continue
                    if detail_url in seen_details:
                        continue
                    seen_details.add(detail_url)
                    try:
                        item = self._scrape_detail(detail_url)
                        if item:
                            yield item
                    except Exception as e:
                        self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
                        continue

                # ページ送り (最後まで辿る)
                next_link = soup.find("link", rel="next")
                if next_link and next_link.get("href"):
                    next_url = urljoin(list_url, next_link["href"])
                    cursors[root] = next_url if next_url != list_url else None
                else:
                    cursors[root] = None

    def _collect_list_roots(self, url: str) -> list[str]:
        """起点 URL から巡回すべき求人一覧 URL 群を導出する。

        - 起点が一覧ページそのもの (article を含む) の場合はその URL のみ
        - 起点がポータル (/shain/) の場合は地域ブロックリンクから
          /{block}/jlist/shain/ を組み立てて返す
        """
        soup = self.get_soup(url)
        if soup is None:
            return [url]

        if soup.select_one("article.list-jobListDetail"):
            return [url]

        roots = []
        for a in soup.select("a[href]"):
            path = urlparse(urljoin(url, a["href"])).path
            m = _BLOCK_LINK_RE.match(path)
            if not m:
                continue
            list_url = urljoin(url, f"/{m.group(1)}/jlist/shain/")
            if list_url not in roots:
                roots.append(list_url)
        return roots

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None
        data = {Schema.URL: url}

        # ── 企業情報 (div.detail-companyInfo) ──────────────────────────
        company_section = soup.select_one("div.detail-companyInfo")
        if company_section:
            # 会社名
            pt02_p = company_section.select_one(".pt02 dd p")
            if pt02_p:
                data[Schema.NAME] = pt02_p.get_text(strip=True)

            # pt03: ラベル–値ペアの dl 群
            for dl in company_section.select(".pt03 > dl"):
                dt_span = dl.select_one("dt span")
                dd = dl.select_one("dd")
                if not dt_span or not dd:
                    continue
                label = dt_span.get_text(strip=True)

                if label == "所在地":
                    p = dd.select_one("p")
                    if p:
                        text = p.get_text(separator="\n").strip()
                        lines = [l.strip() for l in text.splitlines() if l.strip()]
                        addr_parts = []
                        for line in lines:
                            if "TEL:" in line:
                                tel = re.sub(r".*TEL:", "", line.split("FAX:")[0]).strip()
                                if tel:
                                    data[Schema.TEL] = tel
                            elif "FAX:" not in line:
                                addr_parts.append(line)
                        if addr_parts:
                            addr = " ".join(addr_parts)
                            m = _PREF_RE.match(addr)
                            if m:
                                data[Schema.PREF] = m.group(1)
                                data[Schema.ADDR] = addr[m.end():].strip()
                            else:
                                data[Schema.ADDR] = addr

                elif label == "代表者名":
                    data[Schema.REP_NM] = dd.get_text(strip=True)

                elif label == "事業内容":
                    data[Schema.LOB] = _clean(dd.get_text())

                elif label == "URL":
                    a = dd.select_one("a[href]")
                    if a:
                        data[Schema.HP] = a["href"]

        # ── 基本情報 (div.detail-basicInfo) ────────────────────────────
        basic_section = soup.select_one("div.detail-basicInfo")
        if basic_section:
            # 職種・雇用形態
            dl01 = basic_section.select_one("dl.dl01")
            if dl01:
                em_els = dl01.select("dd > ul li p em")
                job_types = [e.get_text(strip=True) for e in em_els if e.get_text(strip=True)]
                if job_types:
                    data["職種"] = "、".join(job_types)
                # 雇用形態は [正] のような略号 (span01 クラスが付かないページが多い)
                emp_types = []
                for span in dl01.select("dd > ul li p span"):
                    code = span.get_text(strip=True).strip("[]［］")
                    label = _TYPE_MAP.get(code, code)
                    if label and label not in emp_types:
                        emp_types.append(label)
                if emp_types:
                    data["雇用形態"] = "、".join(emp_types)

            # 給与
            dl02 = basic_section.select_one("dl.dl02")
            if dl02:
                em_els = dl02.select("dd > ul li p em")
                salaries = [e.get_text(strip=True) for e in em_els if e.get_text(strip=True)]
                if salaries:
                    data["給与"] = "、".join(salaries)

            # 勤務時間 (求人の勤務時間帯)
            dl03 = basic_section.select_one("dl.dl03")
            if dl03:
                em_els = dl03.select("dd ul.ul02 li p em")
                times = [e.get_text(strip=True) for e in em_els if e.get_text(strip=True)]
                if times:
                    data["勤務時間"] = "、".join(times)

            # 勤務地・面接地 → 最寄り駅・住所 (PREF/ADDR を勤務地で上書き)
            dl04 = basic_section.select_one("dl.dl04")
            if dl04:
                dd04 = dl04.select_one("dd")
                if dd04:
                    for inner_dl in dd04.select("dl"):
                        inner_dt = inner_dl.select_one("dt")
                        inner_dd = inner_dl.select_one("dd")
                        if not inner_dt or not inner_dd:
                            continue
                        inner_label = inner_dt.get_text(strip=True)
                        if "最寄" in inner_label:
                            data["最寄り駅"] = _clean(inner_dd.get_text())
                        elif "住所" in inner_label:
                            li = inner_dd.select_one("ul li")
                            addr = li.get_text(strip=True) if li else _clean(inner_dd.get_text())
                            if addr:
                                m = _PREF_RE.match(addr)
                                if m:
                                    data[Schema.PREF] = m.group(1)
                                    data[Schema.ADDR] = addr[m.end():].strip()
                                else:
                                    data[Schema.ADDR] = addr

            # 休日・休暇
            dl08 = basic_section.select_one("dl.dl08")
            if dl08:
                dd08 = dl08.select_one("dd")
                if dd08:
                    data["休日・休暇"] = _clean(dd08.get_text())

            # 待遇・福利厚生
            dl09 = basic_section.select_one("dl.dl09")
            if dl09:
                dd09 = dl09.select_one("dd")
                if dd09:
                    data["福利厚生"] = _clean(dd09.get_text())[:500]

        # ── 募集情報 (div.detail-recruitInfo) ──────────────────────────
        recruit_section = soup.select_one("div.detail-recruitInfo")
        if recruit_section:
            dl01_r = recruit_section.select_one("dl.dl01")
            if dl01_r:
                dd = dl01_r.select_one("dd")
                if dd:
                    data["仕事内容"] = _clean(dd.get_text())[:500]

            dl05_r = recruit_section.select_one("dl.dl05")
            if dl05_r:
                dd = dl05_r.select_one("dd")
                if dd:
                    data["応募条件"] = _clean(dd.get_text())[:500]

        # ── 求人特徴タグ (div.detail-companyChar) ──────────────────────
        tags = self._extract_feature_tags(soup)
        if tags:
            data["求人特徴タグ"] = "、".join(tags)

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _extract_feature_tags(soup) -> list[str]:
        """求人特徴タグ (未経験OK / 駅チカ / 車通勤OK 等) を重複なしで抽出する。"""
        tags = []

        def _add(text: str):
            text = _clean(text)
            if text and text not in tags:
                tags.append(text)

        char_section = soup.select_one("div.detail-companyChar")
        if char_section:
            # 「人気の特徴」「稼ぎ方」「～な方を歓迎」「職場環境」「魅力的な待遇」の各グループ
            for span in char_section.select("dl dd ul li span"):
                _add(span.get_text())

        if not tags:
            # フォールバック: ページ上部の特徴バッジ
            header = soup.select_one("div.detail-detailHeader")
            if header:
                for span in header.select(".pt01 ul li p span"):
                    _add(span.get_text())

        return tags


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BaitoruNextScraper()
    scraper.execute("https://www.baitoru.com/tohoku/jlist/shain/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
