# scripts/sites/government/koujuren_3.py
"""
高齢者住まい事業者団体連合会（高住連） — 届出紹介事業者検索 / 相談所（相談拠点）単位

取得対象:
    - 届出紹介事業者が運営する「相談所」単位のレコード（法人 約863件 × 相談所 1〜数十件）
    - 一覧: search.php （検索条件なし = 全件）20件/ページ × 約44ページ
      ページ送りは `?pg_now=N&pager=1`（GET で取得可・POST 不要）
    - 詳細: search_details.php?id=N
      ├ table.sheet_basic  … 法人の基本情報（届出公表番号・代表者・TEL・HP 等）
      └ table.table_basic  … 「この法人が運営する相談所一覧」（名称 / 住所・TEL / 相談方法）

    ※ 既存の koujuren_2（sites.yml 登録済）は「法人単位」で、相談所一覧を件数しか
      持たない。本クローラーはその相談所一覧を 1 行 1 レコードに展開した拠点単位版。

取得フロー:
    1. ルート URL に ?pg_now=N&pager=1 を付与して一覧ページを 1 ページずつ取得
    2. table.search_results_table の各行から詳細リンク (search_details.php?id=N) を収集
    3. 詳細ページを 1 件取得するたびに、相談所テーブルの各行を即 yield（Pattern B）
    4. 1 ページ目の「検索結果：N件中」から総ページ数を確定。
       読めない場合は詳細リンク 0 件のページで終了（安全弁 _MAX_PAGES）

備考:
    - 「備考」欄は法人が書いた長文の自由記述のため取得しない（著作権リスク）
    - 相談所テーブルが無い / 空の法人は、法人所在地を拠点とみなして 1 件だけ出力する
      （法人を取りこぼさないため。名称は法人名・相談方法は空）
    - 電話番号・FAX はサイト側がハイフン無しで掲載しているためそのまま格納する
    - 利用規約・robots.txt はサイト上に存在せず（robots.txt は 404、フッターにも
      規約/ポリシーのリンク無し）、スクレイピングを禁止する記載は確認できなかった

実行方法:
    python scripts/sites/government/koujuren_3.py
    docker compose exec worker python /app/bin/run_flow.py --site-id koujuren_3
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

# 相談所セル内の「TEL：0120654246」行
_TEL_LABEL_RE = re.compile(r"^TEL\s*[:：]?\s*")

_PER_PAGE = 20          # 1ページあたりの表示件数
_MAX_PAGES = 200        # 安全弁（総件数が取れなかった場合の上限ページ数）


def _clean(text: str) -> str:
    """全角スペース・改行を含む空白を半角スペース1つに畳む。"""
    return re.sub(r"[\s　]+", " ", text or "").strip()


def _num(text: str) -> str:
    """「1,234」「ー」などから数値のみ取り出す（数値が無ければ空文字）。"""
    m = re.search(r"([\d,]+)", text or "")
    return m.group(1).replace(",", "") if m else ""


def _split_address(raw: str) -> tuple[str, str, str]:
    """「〒107-0052 東京都港区赤坂3-4-3 ビル8階」→ (郵便番号, 都道府県, 住所)。"""
    text = raw or ""
    post_code = ""
    m = _POST_RE.search(text)
    if m:
        post_code = f"{m.group(1)}-{m.group(2)}"
        text = _POST_RE.sub("", text, count=1)
    text = _clean(text)

    pref = ""
    pm = _PREF_RE.match(text)
    if pm:
        pref = pm.group(1)
        text = text[len(pref):].strip()
    return post_code, pref, text


def _page_url(base_url: str, page: int) -> str:
    """ルート URL（引数の url）に pg_now / pager を付与したページ URL を組み立てる。"""
    parts = urlsplit(base_url)
    query = dict(parse_qsl(parts.query))
    query["pg_now"] = str(page)
    query["pager"] = "1"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


class Koujuren3Crawler(StaticCrawler):
    """高住連 届出紹介事業者検索 スクレイパー（相談所＝相談拠点単位）"""

    DELAY = 0.7
    ITEM_DELAY = 0      # 1詳細から複数の相談所を出力するためアイテム側は待たない
    EXTRA_COLUMNS = [
        "運営法人名称",
        "運営上の組織名称",
        "届出公表番号",
        "相談方法",
        "法人住所",
        "法人TEL",
        "法人FAX",
        "対応エリア",
        "相談員数",
        "基礎講座受講完了者数",
        "契約法人数",
        "契約ホーム数",
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

            # 1ページ目で総件数 → 総ページ数を確定（法人数ベース）
            if total_pages is None:
                m = _TOTAL_RE.search(list_soup.get_text(" ", strip=True))
                if m:
                    total = int(m.group(1).replace(",", ""))
                    total_pages = max(1, -(-total // _PER_PAGE))
                    self.logger.info("法人総件数: %s件 / %sページ", total, total_pages)

            detail_urls = self._extract_detail_urls(list_soup, url)
            if not detail_urls:
                self.logger.info("詳細リンクが0件のため終了: %s", list_url)
                break

            for detail_url in detail_urls:
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                # 1法人の詳細を取得 → 相談所1行ごとに即 yield（Pattern B）
                for item in self._parse_detail(detail_url):
                    yield item

            if total_pages is not None and page >= total_pages:
                break

    # ------------------------------------------------------------------
    # 一覧ページ
    # ------------------------------------------------------------------
    def _extract_detail_urls(self, soup, base_url: str) -> list[str]:
        """一覧テーブルから詳細ページ URL を出現順に抜き出す。"""
        urls: list[str] = []
        table = soup.find("table", class_="search_results_table")
        if not table:
            return urls

        for a in table.find_all("a", href=_DETAIL_RE):
            full = urljoin(base_url, a["href"])
            if full not in urls:
                urls.append(full)
        return urls

    # ------------------------------------------------------------------
    # 詳細ページ（1法人 → 相談所 N 件）
    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str):
        soup = self.get_soup(detail_url)
        if soup is None:
            self.logger.warning("詳細ページ取得失敗: %s", detail_url)
            return

        # 法人名称 / 運営上の組織名称（h3.title03 の 1行目 / 2行目）
        corp_name = ""
        org_name = ""
        h3 = soup.find("h3", class_="title03")
        if h3:
            lines = [_clean(t) for t in h3.get_text("\n").split("\n") if _clean(t)]
            if lines:
                corp_name = lines[0]
            if len(lines) > 1:
                org_name = lines[1].strip("（）()")

        fields = self._basic_fields(soup)

        corp_post, corp_pref, corp_addr = _split_address(fields.get("住所", ""))
        corp_addr_full = _clean(f"{corp_pref}{corp_addr}")

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

        corp_common = {
            Schema.REP_NM: rep_name,
            Schema.POS_NM: position,
            Schema.HP: fields.get("ホームページ", ""),
            "運営法人名称": corp_name,
            "運営上の組織名称": org_name,
            "届出公表番号": fields.get("届出公表番号", ""),
            "法人住所": corp_addr_full,
            "法人TEL": fields.get("電話番号", ""),
            "法人FAX": fields.get("FAX番号", ""),
            "対応エリア": fields.get("紹介可能都道府県", ""),
            "相談員数": advisors,
            "基礎講座受講完了者数": completed,
            "契約法人数": _num(fields.get("契約法人数", "")),
            "契約ホーム数": _num(fields.get("契約ホーム数", "")),
            "紹介事業開始日": fields.get("紹介事業開始日", ""),
        }

        offices = self._parse_offices(soup)
        if not offices:
            # 相談所一覧が無い法人は法人所在地を 1 拠点として出力（取りこぼし防止）
            offices = [{
                "name": corp_name,
                "post_code": corp_post,
                "pref": corp_pref,
                "addr": corp_addr,
                "tel": fields.get("電話番号", ""),
                "methods": "",
            }]

        for office in offices:
            item = {
                Schema.URL: detail_url,
                Schema.NAME: office["name"] or corp_name,
                Schema.FAC_NAME: office["name"],
                Schema.PREF: office["pref"],
                Schema.POST_CODE: office["post_code"],
                Schema.ADDR: office["addr"],
                Schema.TEL: office["tel"],
            }
            item.update(corp_common)
            item["相談方法"] = office["methods"]
            yield item

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

    def _parse_offices(self, soup) -> list[dict]:
        """「この法人が運営する相談所一覧」(table.table_basic) を 1 行 1 件で取り出す。"""
        offices: list[dict] = []
        table = soup.find("table", class_="table_basic")
        if not table:
            return offices

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue  # ヘッダー行 / 「登録がありません」行

            name = _clean(tds[0].get_text(" "))

            # 住所セル: 〒 / 住所（複数行） / TEL：<span class="tel-link">
            addr_td = tds[1]
            tel = ""
            tel_span = addr_td.find("span", class_="tel-link")
            if tel_span:
                tel = _clean(tel_span.get_text())
                tel_span.extract()  # 住所テキストから TEL を取り除く

            addr_lines = [
                _TEL_LABEL_RE.sub("", _clean(line))
                for line in addr_td.get_text("\n").split("\n")
                if _clean(line)
            ]
            addr_raw = " ".join(line for line in addr_lines if line)
            post_code, pref, addr = _split_address(addr_raw)

            # 相談方法（対面 / WEB / 電話）— 短い定型ラベルのみ
            methods = ""
            if len(tds) > 2:
                methods = " / ".join(
                    _clean(li.get_text()) for li in tds[2].find_all("li") if _clean(li.get_text())
                )

            if not (name or addr):
                continue
            offices.append({
                "name": name,
                "post_code": post_code,
                "pref": pref,
                "addr": addr,
                "tel": tel,
                "methods": methods,
            })
        return offices


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Koujuren3Crawler()
    scraper.execute("https://koujuren.jp/search.php#search_content")
