"""
Ministerstvo pro místní rozvoj ČR (チェコ地域開発省) 「Seznam cestovních kanceláří」
旅行会社登録簿 — クローラー  (STREAMREQ-18753)

取得対象:
    チェコ地域開発省 (MMR) が消費者保護目的で公表している旅行会社 (cestovní kancelář)
    の公的登録簿。倒産補償保険 (pojistka / bankovní záruka) の加入確認用に、
    正規登録された旅行会社のみが掲載される。2026-09-30 時点で 658 社。

取得フロー:
    1. 起点 URL (sites.yml の url = 掲載ページ) を GET する。
       このページ自体には一覧表が無く、
       「Seznam cestovních kanceláří k DD.MM.YYYY」という 1 本のリンクだけがある。
       ⚠ リンク先は定期更新のたびに文言 (基準日) が変わるため URL は決め打ちせず、
         必ず起点ページのリンクから urljoin で導出する。
       (将来 起点ページに表が直接載る形へ変わった場合に備え、
        起点ページに table#unitable があればそれをそのまま使う。)
    2. 一覧ページ (静的 HTML) の table#unitable を 1 行ずつ読み、即 yield する。
       DataTables によるクライアントサイド描画・ページングだが、
       全 658 行が HTML にベタ書きされているためページネーション通信は不要。
       行クリックで開く詳細モーダル (table.detail-table) は同じ行の値を
       JS で流し込むだけの器であり、詳細ページは存在しない。

元データの列 (2026-09 時点):
    Název / IČO / Adresa sídla /
    Pojistka (bankovní záruka) cestovní kanceláře /
    Příspěvek do garančního fondu 2026 / Rozsah předmětu podnikání
    (以降の 8 列は DataTables のレイアウト用で常に空)

カラム設計 (呼び出しの【参考スクレイピングカラム】への対応):
    名称        ← Název (会社名。チェコ語のまま UTF-8 で保持)
    法人番号    ← IČO (チェコの事業者識別番号 8 桁)。EXTRA「IČO」にも同値を出す
                  (依頼で「IČO(作業用)」として明示されているため)
    住所        ← Adresa sídla の原文全体
    事業内容    ← Rozsah předmětu podnikání
                  (「Provozování cestovní kanceláře - pořádání zájezdů」等、
                   3 種類しかない定型の許可区分ラベル。自由記述ではない)
    サイト定義業種 ← 同上
    取得URL     ← 実際に掲載されている一覧ページの URL
    EXTRA       ← IČO / 都市 / 郵便番号(PSČ) / 国 / 倒産保険状況 /
                  倒産保険証書URL / 保証基金拠出金 / 名簿基準日

    ※ 都道府県 (Schema.PREF) は日本の行政区分カラムであり、
      元データにチェコの kraj (州) 列が存在しないため空欄とする (推測で埋めない)。
    ※ 郵便番号は Schema.POST_CODE を使わない。チェコの PSČ は 5 桁で、
      基盤の正規化 (7 桁必須) が空文字に落としてしまうため EXTRA に出す。
    ※ TEL・HP・メール・代表者名・従業員数・資本金は登録簿に列自体が存在しない
      (依頼どおり後工程の⑬HP巡回で補完する想定)。空欄のままとする。
    ※ 国カラムは依頼指示どおり「チェコ」で固定する。

住所の分解:
    2026-09 時点の全 658 件は次の 2 形式のみ (実測):
      - "Mělnická 228/31, 250 01, Brandýs nad Labem-Stará Boleslav - Stará Boleslav"
        → 番地, PSČ, 市区町村(- 地区名)          … 624 件
      - "783 42, Slatinice 24"
        → PSČ, 市町村+家屋番号 (街路名の無い小規模自治体)  … 34 件
    PSČ は "NNN NN" 形式で必ず 1 個含まれるため、これを基準に
    直後の要素を都市として取り出す。都市名末尾の " - 地区名" は落とす
    ("Praha 6 - Veleslavín" → "Praha 6")。街路名が無い形式のときのみ
    末尾の家屋番号を除去する ("Slatinice 24" → "Slatinice")。

利用規約:
    https://mmr.gov.cz/robots.txt は /admin/ /Migrace/ /archiv/ /CMSPages/
    /CMSModules/ (CMS 管理系パス) のみ Disallow で、対象ページは対象外。
    サイトに利用規約ページは無く (フッタは アクセシビリティ宣言 / Cookie ポリシー /
    GDPR のみ)、スクレイピング・自動取得を禁止する記載は確認できなかった。
    官公庁が消費者保護のために公開している登録簿であり取得を継続する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/streamreq_18753mmr.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_18753mmr
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 一覧表の ID (Kentico の DataTables ウィジェット)
_TABLE_SELECTOR = "table#unitable"

# 一覧ページへのリンク判定 ("Seznam cestovních kanceláří k 24.09.2026")
_LIST_LINK_HREF_KEY = "seznam-cestovnich-kancelari/"
_LIST_LINK_TEXT_KEY = "seznam cestovních kanceláří"

# 名簿基準日 "k 24.09.2026"
_BASIS_DATE_RE = re.compile(r"\b(\d{2})\.(\d{2})\.(\d{4})\b")

# チェコの郵便番号 PSČ ("250 01")
_PSC_RE = re.compile(r"^\d{3}\s\d{2}$")

# 街路名が無い住所の末尾に付く家屋番号 ("Slatinice 24" / "Zásada 49/2")
_HOUSE_NO_RE = re.compile(r"\s+\d+(?:/\d+\w?)?$")

# 依頼指示により固定する国名
_COUNTRY = "チェコ"


class StreamReq18753Mmr(StaticCrawler):
    """チェコ地域開発省 旅行会社登録簿 (Seznam cestovních kanceláří) スクレイパー"""

    # 通信は起点ページ 1 回 + 一覧ページ 1 回のみ。
    # 基盤は yield ごとに DELAY 秒待つため、待機はページ取得側に寄せて 0 にする。
    DELAY = 0.0
    ITEM_DELAY = 0.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 60

    EXTRA_COLUMNS = [
        "IČO",
        "都市",
        "郵便番号(PSČ)",
        "国",
        "倒産保険状況",
        "倒産保険証書URL",
        "保証基金拠出金",
        "名簿基準日",
    ]

    # ------------------------------------------------------------------ utils

    @staticmethod
    def _txt(node_or_str) -> str:
        """テキストを取り出し、改行・連続空白を 1 個の半角スペースに整理する。"""
        if node_or_str is None:
            return ""
        if hasattr(node_or_str, "get_text"):
            s = node_or_str.get_text(" ", strip=True)
        else:
            s = str(node_or_str)
        # ノーブレークスペース (チェコ語表記で頻出) も通常の空白に寄せる
        s = s.replace(" ", " ").replace("\r", " ").replace("\n", " ")
        return re.sub(r"\s+", " ", s).strip()

    @staticmethod
    def _basis_date(text: str) -> str:
        """"... k 24.09.2026" から名簿基準日を YYYY-MM-DD で取り出す。"""
        m = _BASIS_DATE_RE.search(text or "")
        if not m:
            return ""
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"

    @classmethod
    def _split_address(cls, address: str) -> tuple[str, str]:
        """住所原文から (PSČ, 都市) を取り出す。判定できない場合は空文字を返す。"""
        parts = [p.strip() for p in address.split(",") if p.strip()]
        psc = ""
        city = ""
        for i, part in enumerate(parts):
            if not _PSC_RE.match(part):
                continue
            psc = part
            if i + 1 < len(parts):
                city = parts[i + 1]
                # 街路名が無い形式 (PSČ が先頭) のときだけ末尾の家屋番号を落とす。
                # "Praha 6" のような数字付き都市名を壊さないための条件。
                if i == 0:
                    city = _HOUSE_NO_RE.sub("", city).strip()
            break
        if not city and parts:
            city = parts[-1]
        # "Hradec Králové - Nový Hradec Králové" → 地区名を落として市名だけにする
        city = city.split(" - ")[0].strip()
        return psc, city

    # ------------------------------------------------------- 一覧ページの解決

    def _resolve_list_page(self, url: str):
        """
        起点 URL から一覧表を持つページの (soup, ページURL, 名簿基準日) を返す。

        起点ページに表が無ければ「Seznam cestovních kanceláří k …」リンクを辿る。
        見つからない場合は黙って 0 件にせず例外を送出する。
        """
        soup = self.get_soup(url)
        if soup is None:
            raise RuntimeError(f"起点ページを取得できませんでした: {url}")

        # 将来 起点ページに表が直接載るようになった場合はそのまま使う
        if soup.select_one(_TABLE_SELECTOR):
            return soup, url, self._basis_date(self._txt(soup))

        link = None
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            label = self._txt(a)
            if _LIST_LINK_HREF_KEY in href.lower() and _LIST_LINK_TEXT_KEY in label.lower():
                link = a
                break
        if link is None:
            raise RuntimeError(
                f"一覧ページ (Seznam cestovních kanceláří k …) のリンクが見つかりませんでした: {url}"
            )

        # 引数 url を唯一のルートとして相対 URL を解決する
        list_url = urljoin(url, link.get("href", "").strip())
        basis_date = self._basis_date(self._txt(link))
        logger.info("一覧ページ: %s (基準日 %s)", list_url, basis_date or "不明")

        list_soup = self.get_soup(list_url)
        if list_soup is None:
            raise RuntimeError(f"一覧ページを取得できませんでした: {list_url}")
        if not basis_date:
            basis_date = self._basis_date(self._txt(list_soup))
        return list_soup, list_url, basis_date

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        """起点 URL から登録簿の一覧表を辿り、旅行会社を 1 件ずつ yield する。"""
        soup, list_url, basis_date = self._resolve_list_page(url)

        table = soup.select_one(_TABLE_SELECTOR)
        if table is None:
            raise RuntimeError(f"登録簿の一覧表 ({_TABLE_SELECTOR}) が見つかりません: {list_url}")

        body = table.find("tbody") or table
        rows = body.find_all("tr", recursive=False)
        self.total_items = len(rows)

        count = 0
        for tr in rows:
            tds = tr.find_all("td", recursive=False)
            # 先頭 td は DataTables の展開ボタン用の空セル
            if len(tds) < 7:
                continue

            name = self._txt(tds[1])
            if not name:
                continue

            ico = self._txt(tds[2])
            address = self._txt(tds[3])
            psc, city = self._split_address(address)

            # Pojistka 列: PDF リンク or 未提出を示す定型文
            pojistka_link = tds[4].find("a", href=True)
            pojistka_text = self._txt(tds[4])
            if pojistka_link is not None:
                certificate_url = urljoin(list_url, pojistka_link["href"].strip())
                insurance_status = "保険証書あり"
            else:
                certificate_url = ""
                insurance_status = pojistka_text

            scope = self._txt(tds[6])

            yield {
                Schema.NAME: name,
                Schema.CO_NUM: ico,
                Schema.ADDR: address,
                Schema.LOB: scope,
                Schema.CAT_SITE: scope,
                Schema.URL: list_url,
                "IČO": ico,
                "都市": city,
                "郵便番号(PSČ)": psc,
                "国": _COUNTRY,
                "倒産保険状況": insurance_status,
                "倒産保険証書URL": certificate_url,
                "保証基金拠出金": self._txt(tds[5]),
                "名簿基準日": basis_date,
            }
            count += 1

        logger.info("登録旅行会社として抽出した件数: %d", count)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    scraper = StreamReq18753Mmr()
    scraper.execute("https://mmr.gov.cz/cs/ministerstvo/cestovni-ruch/seznam-cestovnich-kancelari")
