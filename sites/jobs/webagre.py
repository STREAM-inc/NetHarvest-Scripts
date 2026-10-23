"""
Agreキャリア (アグレキャリア) — 沖縄の転職・正社員求人 (webagre.com/career)

取得対象:
    - 沖縄県の転職・正社員求人（全件）。求人カードの求人タイトル・雇用形態・勤務地と、
      詳細ページの企業情報（会社名・住所・代表者・設立・事業内容・勤務時間・休日・電話番号）を取得する。

取得フロー:
    一覧ページ (career/list?page=N) → 求人カード → 詳細ページ (career/view/web/{id})
    会社名・住所・業種・事業内容・代表者・設立は詳細ページ「会社概要」ブロックの
    h3 見出し＋値 div の組を見出し名で対応付けて取得（テーブル/dl ではない）。
    住所は「〒NNN-NNNN」を郵便番号に分離、代表者は先頭の役職語で役職/氏名に分割する。
    電話番号は詳細ページの JobPosting JSON-LD の description 内
    「【電話番号】」ブロックから抽出する（可視 DOM には電話番号ラベルが無いため）。
    求人タイトル・雇用形態・勤務地は一覧カードのバッジから取得。
    detail を 1 件取得するごとに即 yield する (Pattern B / 早期 yield)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/webagre.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id webagre
"""

import json
import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_RE = re.compile(r"〒\s*([\d０-９][\d０-９\-－]+)")
# JobPosting JSON-LD の description 内「【電話番号】 …」ブロックから電話番号を抽出する。
# 区切りの罫線（－－－…）または次の【…】ラベルまでを電話番号ブロックとみなす。
_TEL_BLOCK_RE = re.compile(r"電話番号】(.*?)(?:[-－─]{4,}|【|$)", re.S)
_TEL_NUM_RE = re.compile(r"0[\d０-９][\d０-９\-－]{7,12}")
# 代表者欄の先頭役職（例: 「代表取締役　加藤 健史」「代表取締役社長：山田太郎」）。
# 役職語の後ろは空白/コロン区切りで氏名が続く。役職が無ければ全体を氏名とする。
_REP_TITLE_RE = re.compile(
    r"^((?:代表取締役|取締役|代表社員|代表理事|理事長|代表者|代表|社長|会長|院長|園長|"
    r"施設長|所長|店長|オーナー|CEO|COO)[^\s\u3000:：]*)[\s\u3000:：]+(.+)$"
)


class WebagreCrawler(StaticCrawler):
    """Agreキャリア（沖縄の転職・正社員求人）スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["求人タイトル", "雇用形態", "勤務地", "業種"]

    def parse(self, url: str):
        # 引数 url ("https://webagre.com/career") を唯一のルートとし、一覧 URL を派生させる。
        list_url = urllib.parse.urljoin(url.rstrip("/") + "/", "list")
        page = 1
        while True:
            soup = self.get_soup(f"{list_url}?page={page}")
            if soup is None:
                break
            cards = soup.select('div.card[id^="jobCard-"]')
            if not cards:
                break
            if page == 1:
                count_el = soup.select_one(".search-count")
                if count_el:
                    try:
                        self.total_items = int(
                            count_el.get_text(strip=True).replace(",", "")
                        )
                    except ValueError:
                        pass
            for card in cards:
                try:
                    link = card.select_one('h2 a[href*="/career/view/web/"]')
                    if not link or not link.get("href"):
                        continue
                    detail_url = urllib.parse.urljoin(url, link["href"])
                    job_title = link.get_text(strip=True)
                    emp_type = ""
                    area = ""
                    for badge in card.select("div.border.rounded"):
                        icon = badge.select_one("i")
                        label = badge.select_one("div.col.text-truncate-1")
                        if icon and label:
                            icon_classes = " ".join(icon.get("class", []))
                            if "fa-location-dot" in icon_classes:
                                area = label.get_text(strip=True)
                            elif "fa-address-card" in icon_classes:
                                emp_type = label.get_text(strip=True)
                    item = self._scrape_detail(detail_url)
                    if item:
                        item["求人タイトル"] = job_title
                        item["雇用形態"] = emp_type
                        item["勤務地"] = area
                        yield item  # detail 取得ごとに即 yield
                except Exception as e:
                    self.logger.warning(f"page {page}: card skip — {e}")
                    continue
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        def get_section(section_id: str) -> str:
            el = soup.select_one(f"#{section_id}")
            return el.get_text(" ", strip=True) if el else ""

        # --- 会社概要: h2「会社概要」配下の section ごとに h3 見出し → 値 div を対応付ける ---
        overview = self._parse_company_overview(soup)

        # --- 住所: 先頭の「〒NNN-NNNN」を郵便番号に分離し、残りを住所本体とする ---
        addr_raw = overview.get("住所", "")
        post_code = ""
        pc_m = _POST_CODE_RE.search(addr_raw)
        if pc_m:
            post_code = pc_m.group(1)
        addr = re.sub(r"\s+", "", _POST_CODE_RE.sub("", addr_raw, count=1))
        pref_m = _PREF_PATTERN.match(addr)
        pref = pref_m.group(1) if pref_m else ""

        # --- 代表者: 先頭が役職語なら役職と氏名に分割、無ければ全体を氏名とする ---
        pos_nm, rep_nm = self._split_representative(overview.get("代表者", ""))

        return {
            Schema.URL: url,
            Schema.NAME: overview.get("企業名") or get_section("company_name"),
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: self._extract_tel(soup),
            Schema.REP_NM: rep_nm,
            Schema.POS_NM: pos_nm,
            Schema.OPEN_DATE: overview.get("設立", ""),
            Schema.LOB: overview.get("事業内容", ""),
            Schema.CAP: overview.get("資本金", ""),
            Schema.EMP_NUM: overview.get("従業員数", ""),
            Schema.TIME: get_section("working_hours"),
            Schema.HOLIDAY: get_section("day_off"),
            "業種": overview.get("業種", ""),
        }

    @staticmethod
    def _parse_company_overview(soup) -> dict:
        """「会社概要」h2 を持つブロック内の h3 見出し＋値 div を {見出し: 値} にする。"""
        result: dict[str, str] = {}
        heading = soup.find(
            lambda t: t.name in ("h2", "h3") and t.get_text(strip=True) == "会社概要"
        )
        if heading is None:
            return result
        block = heading.find_parent("section") or heading.parent
        for h3 in block.find_all("h3"):
            label = h3.get_text(strip=True)
            if not label or label in result:
                continue
            values = []
            for sib in h3.find_next_siblings():
                if sib.name == "h3":
                    break
                # 「続きを見る」トグルの input/label は除外して値のみ取る
                for junk in sib.select("input, label.read-more-toggle"):
                    junk.decompose()
                text = sib.get_text("\n", strip=True)
                if text:
                    values.append(text)
            value = "\n".join(values)
            # 1 行に収まる項目は空白を正規化（事業内容など複数行はそのまま）
            if "\n" not in value:
                value = re.sub(r"[ \u3000]+", " ", value).strip()
            result[label] = value
        return result

    @staticmethod
    def _split_representative(rep_raw: str) -> tuple[str, str]:
        rep_raw = rep_raw.strip()
        if not rep_raw:
            return "", ""
        m = _REP_TITLE_RE.match(rep_raw)
        if m and m.group(2).strip():
            return m.group(1).strip(), m.group(2).strip()
        return "", rep_raw

    @staticmethod
    def _extract_tel(soup) -> str:
        for sc in soup.find_all("script", type="application/ld+json"):
            if not sc.string:
                continue
            try:
                data = json.loads(sc.string)
            except (ValueError, TypeError):
                continue
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                desc = data.get("description", "") or ""
                mb = _TEL_BLOCK_RE.search(desc)
                if mb:
                    mn = _TEL_NUM_RE.search(mb.group(1))
                    if mn:
                        return mn.group(0).strip()
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = WebagreCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://webagre.com/career")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
