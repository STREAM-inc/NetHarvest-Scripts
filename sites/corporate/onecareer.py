"""
ワンキャリア (onecareer) — 掲載企業の会社概要を抽出するクローラー

取得対象:
    - https://www.onecareer.jp/companies の掲載企業 (約51,000社)
    - 各企業の会社概要 (会社名・所在地・TEL・設立・資本金・従業員数・代表者・HP・業種など)

取得フロー:
    引数 url (公開一覧ページ) から内部 JSON API を導出して取得する。
      1. 一覧 API (/api/v1/companies?per=&page=N&...) で企業 ID を列挙
      2. 各 ID の詳細 API (/api/v1/companies/{id}) を取得し、1 件ずつ即 yield
    ※ 一覧/詳細とも Nuxt.js の JS レンダリングだが、内部 API は `x-csrf: onecareer`
      ヘッダを付ければ requests で JSON 取得できるため Static で実装する。

除外フィールド (著作権リスク回避):
    official_description / catchphrase / 事業内容(LOB) / クチコミ本文 / company_strengths
    はいずれも長文の自由記述プロースのため取得しない。

実行方法:
    python scripts/sites/corporate/onecareer.py
    python bin/run_flow.py --site-id onecareer
"""

import re
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 内部 API を呼ぶ際に必須のヘッダ (これが無いと WAF が 403 を返す)
_API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "x-csrf": "onecareer",
    "x-requested-with": "XMLHttpRequest",
    "Referer": "https://www.onecareer.jp/",
}

_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 代表者欄の役職接頭辞 (例: "理事長 髙原 一郎")
_TITLE_RE = re.compile(
    r"^(代表取締役社長兼CEO|代表取締役社長|代表取締役会長|代表取締役CEO|代表取締役|"
    r"取締役社長|取締役会長|取締役|代表理事|理事長|会長兼社長|社長執行役員|社長|会長|"
    r"頭取|総裁|学長|院長|校長|理事|代表者|代表|CEO|President)\s+"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _normalize_post_code(raw: str) -> str:
    m = _POST_CODE_RE.search(raw or "")
    if not m:
        return ""
    p = m.group(1)
    return p if "-" in p else f"{p[:3]}-{p[3:]}"


def _split_pref(addr: str) -> tuple[str, str]:
    m = _PREF_PATTERN.match(addr or "")
    if not m:
        return "", _clean(addr)
    return m.group(1), _clean(addr[m.end():])


def _normalize_date(s: str) -> str:
    """'2004年2月29日' / '2004.02.29' → '2004-02-29' (取れなければ原文整形)"""
    m = re.search(r"(\d{4})[.\-/年]\s*(\d{1,2})[.\-/月]\s*(\d{1,2})", s or "")
    if not m:
        return _clean(s)
    y, mo, d = m.groups()
    return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"


def _split_rep(raw: str) -> tuple[str, str]:
    """代表者欄を (役職, 氏名) に分割する。役職が無ければ ('', 氏名)。"""
    raw = _clean(raw)
    m = _TITLE_RE.match(raw)
    if m:
        return m.group(1), _clean(raw[m.end():])
    return "", raw


class OnecareerScraper(StaticCrawler):
    """ワンキャリア 掲載企業スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "企業ID",
        "系列",       # 日系 / 外資
        "業種詳細",   # business_subcategory_name
        "売上高",
        "総合評価",   # クチコミ評価の平均 (数値)
        "クチコミ数",
        "公式企業",   # はい / いいえ
    ]

    # ------------------------------------------------------------------ #
    # URL 導出 (引数 url を唯一のルートとする / SSOT = sites.yml の url)
    # ------------------------------------------------------------------ #
    def _api_list_url(self, url: str, page: int) -> str:
        """公開一覧 url から内部一覧 API の URL を導出する (page を上書き)。"""
        parts = urlsplit(url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query["page"] = str(page)
        query.setdefault("per", "20")
        api_netloc = f"oc2-api.{parts.netloc}"       # oc2-api.www.onecareer.jp
        api_path = f"/api/v1{parts.path}"            # /api/v1/companies
        return f"{parts.scheme}://{api_netloc}{api_path}?{urlencode(query)}"

    def _api_detail_url(self, url: str, company_id) -> str:
        parts = urlsplit(url)
        return f"{parts.scheme}://oc2-api.{parts.netloc}/api/v1{parts.path}/{company_id}"

    def _public_detail_url(self, url: str, company_id) -> str:
        parts = urlsplit(url)
        return f"{parts.scheme}://{parts.netloc}{parts.path}/{company_id}"

    def _get_json(self, api_url: str):
        """session.get (自動リトライ+スモークガード対象) で JSON を取得する。"""
        resp = self.session.get(api_url, headers=_API_HEADERS, timeout=self.TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        per = int(dict(parse_qsl(urlsplit(url).query)).get("per", "20") or "20")
        page = 1
        while True:
            try:
                data = self._get_json(self._api_list_url(url, page))
            except Exception as e:
                self.logger.warning("一覧API取得失敗 (page=%s): %s", page, e)
                break

            companies = data.get("companies") or []
            if not companies:
                break

            # 進捗表示用に総件数を初回のみ設定
            if self.total_items is None:
                total = data.get("total_counts")
                if isinstance(total, int) and total > 0:
                    self.total_items = total

            for c in companies:
                cid = c.get("id")
                if cid is None:
                    continue
                try:
                    item = self._scrape_detail(url, cid, c)
                except Exception as e:
                    self.logger.warning("詳細取得失敗 (id=%s): %s", cid, e)
                    continue
                if item:
                    yield item

            # per 未満しか返らなければ最終ページ
            if len(companies) < per:
                break
            page += 1

    def _scrape_detail(self, url: str, company_id, list_row: dict) -> dict | None:
        data = self._get_json(self._api_detail_url(url, company_id))
        c = data.get("company") or {}

        # official_data (公式企業) を優先し company_basic_info で補完してラベル→値辞書に集約
        info: dict[str, str] = {}
        for row in c.get("company_basic_info") or []:
            label = _clean(row.get("label"))
            if label and label not in info:
                info[label] = _clean(row.get("text"))
        for row in c.get("official_data") or []:  # 公式データを優先上書き
            label = _clean(row.get("label"))
            if label:
                info[label] = _clean(row.get("text"))

        def pick(*labels) -> str:
            for lb in labels:
                if info.get(lb):
                    return info[lb]
            return ""

        name = pick("会社名") or _clean(c.get("name")) or _clean(list_row.get("name"))
        if not name:
            return None

        addr_raw = pick("本社所在地")
        pref, addr = _split_pref(addr_raw)
        if not pref:
            pref = _clean(c.get("headquarters_prefecture"))
        rep_pos, rep_name = _split_rep(pick("代表者"))

        # 業種 (サイト定義): 大分類 / 小分類
        cat = _clean(c.get("business_category_name") or list_row.get("business_category_name"))
        subcat = _clean(c.get("business_subcategory_name"))
        cat_site = " / ".join(x for x in (cat, subcat) if x)

        rating = c.get("review_total_rating_average")
        if rating is None:
            rating = list_row.get("review_total_rating_average")

        return {
            Schema.URL: self._public_detail_url(url, company_id),
            Schema.NAME: name,
            Schema.HP: pick("ホームページURL", "ホームページ"),
            Schema.POST_CODE: _normalize_post_code(pick("本社郵便番号")),
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: pick("本社電話番号"),
            Schema.REP_NM: rep_name,
            Schema.POS_NM: rep_pos,
            Schema.OPEN_DATE: _normalize_date(pick("設立")),
            Schema.CAP: pick("資本金"),
            Schema.EMP_NUM: pick("従業員数"),
            Schema.CAT_SITE: cat_site,
            # --- EXTRA ---
            "企業ID": str(company_id),
            "系列": _clean(c.get("affiliated") or list_row.get("affiliated")),
            "業種詳細": subcat,
            "売上高": pick("売上高"),
            "総合評価": "" if rating in (None, "") else str(rating),
            "クチコミ数": "" if c.get("votes_total") in (None, "") else str(c.get("votes_total")),
            "公式企業": "はい" if c.get("is_official") else "いいえ",
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = OnecareerScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute(
        "https://www.onecareer.jp/companies?per=20&page=1&sort=standard"
        "&lowest_rating=0&highest_rating=5&search_query="
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
