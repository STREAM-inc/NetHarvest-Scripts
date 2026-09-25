# scripts/sites/government/koujuren_2.py
"""
高齢者住まい事業者団体連合会（高住連） — 届出紹介事業者検索

取得対象:
    - 届出紹介事業者（法人単位）約863件 / 全国
    - 一覧: search.php （検索条件なし = 都道府県フィルタなしで全件）20件/ページ × 約44ページ
    - 詳細: search_details.php?id=N
      （代表者・TEL・FAX・HP・契約法人数などは詳細ページにのみ掲載）

取得フロー:
    1. ルート URL に ?pg_now=N&pager=1 を付与して一覧ページを 1 ページずつ取得
    2. table.search_results_table の各行から法人名リンク (search_details.php?id=N) を収集
    3. 1 件ずつ詳細ページを取得し、table.sheet_basic の th→td をマッピングして即 yield
    4. 1 ページ目の「検索結果：N件中」から総ページ数を確定。
       総件数が読めない場合は詳細リンク 0 件のページで終了（安全弁 _MAX_PAGES）

備考:
    - 「備考」欄は法人が書いた長文の自由記述のため取得しない（著作権リスク）
    - 「相談所一覧」テーブルは法人単位レコードに展開せず、件数のみ EXTRA に持つ
    - 電話番号・FAX はサイト側がハイフン無しで掲載しているためそのまま格納する

実行方法:
    python scripts/sites/government/koujuren_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id koujuren_2
"""

import re
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 住所先頭からの都道府県抽出（長い名称から順に照合）
_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
_PREF_RE = re.compile("^(" + "|".join(sorted(_PREFS, key=len, reverse=True)) + ")")

# 「〒107-0052」「〒1070052」
_POST_RE = re.compile(r"〒\s*(\d{3})-?(\d{4})")

# 詳細ページリンク
_DETAIL_RE = re.compile(r"search_details\.php\?id=(\d+)")

# 「検索結果：863件中1～20件」
_TOTAL_RE = re.compile(r"検索結果：\s*([\d,]+)\s*件中")

# 「代表取締役　笹川　泰宏」→ 役職 / 氏名 に分離
_POSITIONS = [
    "代表取締役社長", "代表取締役会長", "代表取締役", "取締役社長", "代表理事長",
    "代表理事", "代表社員", "理事長", "会長", "社長", "院長", "所長", "園長",
    "取締役", "代表者", "施設長", "管理者", "代表",
]
_POS_RE = re.compile("^(" + "|".join(sorted(_POSITIONS, key=len, reverse=True)) + r")\s*")

# 「85（基礎講座受講完了者数：112人/132%）」
_ADVISOR_RE = re.compile(r"^\s*([\d,]+)")
_COMPLETED_RE = re.compile(r"受講完了者数：\s*([\d,]+)\s*人")

_PER_PAGE = 20          # 1ページあたりの表示件数
_MAX_PAGES = 200        # 安全弁（総件数が取れなかった場合の上限ページ数）


def _clean(text: str) -> str:
    """全角スペース・改行を含む空白を半角スペース1つに畳む。"""
    return re.sub(r"[\s　]+", " ", text or "").strip()


def _num(text: str) -> str:
    """「1,234」「ー」などから数値のみ取り出す（数値が無ければ空文字）。"""
    m = re.search(r"([\d,]+)", text or "")
    return m.group(1).replace(",", "") if m else ""


def _page_url(base_url: str, page: int) -> str:
    """ルート URL（引数の url）に pg_now / pager を付与したページ URL を組み立てる。"""
    parts = urlsplit(base_url)
    query = dict(parse_qsl(parts.query))
    query["pg_now"] = str(page)
    query["pager"] = "1"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


class Koujuren2Crawler(StaticCrawler):
    """高住連 届出紹介事業者検索 スクレイパー（法人単位）"""

    DELAY = 0.7
    EXTRA_COLUMNS = [
        "届出公表番号",
        "運営上の組織名称",
        "FAX",
        "対応エリア",
        "相談員数",
        "基礎講座受講完了者数",
        "契約法人数",
        "契約ホーム数",
        "相談所数",
        "紹介事業開始日",
    ]

    def parse(self, url: str):
        total_pages = None
        seen: set[str] = set()

        for page in range(1, _MAX_PAGES + 1):
            list_url = _page_url(url, page)
            list_soup = self.get_soup(list_url)
            if list_soup is None:
                self.logger.warning("一覧ページ取得失敗: %s", list_url)
                break

            # 1ページ目で総件数 → 総ページ数を確定
            if total_pages is None:
                m = _TOTAL_RE.search(list_soup.get_text(" ", strip=True))
                if m:
                    total = int(m.group(1).replace(",", ""))
                    total_pages = max(1, -(-total // _PER_PAGE))
                    self.logger.info("総件数: %s件 / %sページ", total, total_pages)

            rows = self._extract_rows(list_soup, url)
            if not rows:
                self.logger.info("詳細リンクが0件のため終了: %s", list_url)
                break

            for row in rows:
                if row["detail_url"] in seen:
                    continue
                seen.add(row["detail_url"])

                item = self._parse_detail(row)
                if item:
                    yield item  # 1件取得ごとに即 yield（Pattern B）

            if total_pages is not None and page >= total_pages:
                break

    # ------------------------------------------------------------------
    # 一覧ページ
    # ------------------------------------------------------------------
    def _extract_rows(self, soup, base_url: str) -> list[dict]:
        """一覧テーブルから 法人名 / 郵便番号 / 法人住所 / 詳細URL を抜き出す。"""
        rows: list[dict] = []
        table = soup.find("table", class_="search_results_table")
        if not table:
            return rows

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue  # ヘッダー行
            a = tds[0].find("a", href=_DETAIL_RE)
            if not a or not _clean(a.get_text()):
                continue
            rows.append({
                "name": _clean(a.get_text()),
                "detail_url": urljoin(base_url, a["href"]),
                "post_code": _clean(tds[1].get_text()).replace("〒", "") if len(tds) > 1 else "",
                "addr": _clean(tds[2].get_text(" ")) if len(tds) > 2 else "",
            })
        return rows

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _parse_detail(self, row: dict) -> dict | None:
        detail_url = row["detail_url"]
        soup = self.get_soup(detail_url)
        if soup is None:
            self.logger.warning("詳細ページ取得失敗: %s", detail_url)
            return None

        # 法人名称 / 運営上の組織名称（h3.title03 の 1行目 / 2行目）
        name = row["name"]
        org_name = ""
        h3 = soup.find("h3", class_="title03")
        if h3:
            lines = [_clean(t) for t in h3.get_text("\n").split("\n") if _clean(t)]
            if lines:
                name = lines[0] or name
            if len(lines) > 1:
                org_name = lines[1].strip("（）()")

        fields = self._basic_fields(soup)

        # 住所: 「〒107-0052　東京都港区赤坂3-4-3 赤坂ゲイトウェイビル8階」
        addr_raw = fields.get("住所", "")
        post_code = row["post_code"]
        m = _POST_RE.search(addr_raw)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
            addr_raw = _POST_RE.sub("", addr_raw, count=1)
        addr_raw = _clean(addr_raw) or row["addr"]

        pref = ""
        addr = addr_raw
        pm = _PREF_RE.match(addr_raw)
        if pm:
            pref = pm.group(1)
            addr = addr_raw[len(pref):].strip()

        # 代表者: 「代表取締役　笹川　泰宏」→ 役職 + 氏名
        rep_raw = fields.get("代表者", "")
        position = ""
        rep_name = rep_raw
        pos_m = _POS_RE.match(rep_raw)
        if pos_m:
            position = pos_m.group(1)
            rep_name = rep_raw[pos_m.end():].strip()

        # 相談員数: 「85（基礎講座受講完了者数：112人/132%）」（未登録は「ー」）
        advisor_raw = fields.get("相談員数", "")
        advisors = am.group(1).replace(",", "") if (am := _ADVISOR_RE.match(advisor_raw)) else ""
        completed = cm.group(1).replace(",", "") if (cm := _COMPLETED_RE.search(advisor_raw)) else ""

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: fields.get("電話番号", ""),
            Schema.REP_NM: rep_name,
            Schema.POS_NM: position,
            Schema.HP: fields.get("ホームページ", ""),
            "届出公表番号": fields.get("届出公表番号", ""),
            "運営上の組織名称": org_name,
            "FAX": fields.get("FAX番号", ""),
            "対応エリア": fields.get("紹介可能都道府県", ""),
            "相談員数": advisors,
            "基礎講座受講完了者数": completed,
            "契約法人数": _num(fields.get("契約法人数", "")),
            "契約ホーム数": _num(fields.get("契約ホーム数", "")),
            "相談所数": self._count_offices(soup),
            "紹介事業開始日": fields.get("紹介事業開始日", ""),
        }

    def _basic_fields(self, soup) -> dict:
        """table.sheet_basic の th→td を辞書化する（備考は長文のため取得しない）。"""
        fields: dict[str, str] = {}
        table = soup.find("table", class_="sheet_basic")
        if not table:
            return fields

        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = _clean(th.get_text())
            if not label or label == "備考":
                continue
            if label == "ホームページ":
                a = td.find("a", href=True)
                fields[label] = a["href"].strip() if a else _clean(td.get_text())
            else:
                fields[label] = _clean(td.get_text(" "))
        return fields

    def _count_offices(self, soup) -> str:
        """「この法人が運営する相談所一覧」テーブルの行数（ヘッダー除く）。"""
        table = soup.find("table", class_="table_basic")
        if not table:
            return ""
        count = sum(1 for tr in table.find_all("tr") if tr.find("td"))
        return str(count) if count else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Koujuren2Crawler()
    scraper.execute("https://koujuren.jp/search.php")
