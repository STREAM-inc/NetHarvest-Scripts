"""
ジョブハウス工場 (jobhouse.jp/factory) — 製造業求人クローラー

備考の指示:
    - 製造業 (加工・食品製造・機械等) の求人のみを厳密に対象とする
      → 業界カテゴリ (c_*) / 募集職種 (j_*) を **ホワイトリスト方式** で判定する
        (_MFG_CATEGORY_IDS / _MFG_OCCUPATION_IDS, _is_manufacturing)
      → 建築・住宅 / 物流・配送 のカテゴリ、募集職種が「事務」等のみの求人、
        カテゴリも職種も製造業と確認できない求人は除外する
    - 直近3ヶ月以内に掲載された求人のみ (RECENT_MONTHS = 3)
    - 取得カラム: 会社名 / 募集職種 / 応募先電話番号 / 住所 / 従業員数 / 求人掲載日 / 求人URL

取得フロー:
    1. robots.txt が公開するサイトマップ index ({url}/sitemap_articles) を取得する
       ※ robots.txt が `Disallow: /*?*` を宣言しているため、クエリ付き一覧 (?page=N) は
         使わずサイトマップから列挙する
    2. index が指す S3 の gzip シャード (factory_articlesN.xml.gz) を **新しい順に1本ずつ**
       取得し、各シャード内も求人 ID の降順 (= 公開日の新しい順) に処理する。
       シャードは遅延取得なので、最初の1件は数秒で yield できる。
    3. 求人詳細 (/factory/articles/{id}) を1件取得するごとに、その場で即 yield する
    4. 公開日が3ヶ月より古い求人が連続したら打ち切る (ID 降順 ≒ 公開日降順)

詳細ページの情報源:
    - JSON-LD (JobPosting): 公開日 / 掲載終了日 / 雇用形態 / 給与 / 勤務地 (県・市区町村・郵便番号)
    - table.articleTable (.articleMain 配下): 求人番号 / 企業 / 最寄り駅 / 寮・社宅 / 勤務時間 / 公開日
    - table.articleTable (企業情報): 企業名 / 住所 (本社所在地) ※掲載のない求人もある
    - .articleTelEntry-tel: 応募先電話番号 ※掲載のない求人もある
      (フッターの 0120-413-542 はサイト共通の相談ダイヤルなので TEL には採用しない)
    - .sectionHeader a.articleCard-label.is-jobType: c_* = 業界カテゴリ / j_* = 募集職種

サイトに存在しないカラム:
    - 従業員数: 求人詳細にも企業ページ (/factory/articles/b_{id}) にも掲載が無い。
      仕様上まず空になるが、備考の「掲載があれば」に従い実装だけは残す。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/jobhouse_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jobhouse_2
"""

import gzip
import json
import logging
import re
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 求人詳細 URL (例: https://jobhouse.jp/factory/articles/1233510)
_ARTICLE_PATTERN = re.compile(r"/factory/articles/(\d+)/?$")
_LOC_PATTERN = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
# 公開日 (例: 2026年08月13日) / JSON-LD の datePosted (例: 2026-08-13)
_JP_DATE_PATTERN = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")
_ISO_DATE_PATTERN = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")
_TEL_PATTERN = re.compile(r"0\d{1,4}-\d{2,4}-\d{3,4}")
_POST_CODE_PATTERN = re.compile(r"^(\d{3})-?(\d{4})")
# 勤務時間の時間帯 (例: 09:00〜17:45)
_HOURS_PATTERN = re.compile(r"\d{1,2}:\d{2}\s*[~〜～\-−]\s*\d{1,2}:\d{2}")
_EMP_NUM_PATTERN = re.compile(r"従業員数[^\d]{0,10}([\d,]+\s*[人名])")
_CATEGORY_ID_PATTERN = re.compile(r"/articles/(c_\d+)")
_OCCUPATION_ID_PATTERN = re.compile(r"/articles/(j_\d+)")
_CORP_ID_PATTERN = re.compile(r"/factory/articles/b_(\d+)")
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    r"東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|"
    r"香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 詳細ページの table から拾うラベル (自由記述プロースになるラベルは意図的に含めない)
# ※ 給与詳細 / 応募の流れ / 必須資格 / 福利厚生 / 交通費 は長文の自由記述なので取得しない
_WANTED_LABELS = (
    "求人番号", "企業", "企業名", "住所", "最寄り駅", "寮・社宅",
    "勤務時間", "公開日", "従業員数",
)

# --- 製造業フィルタ (備考: 製造業(加工・食品製造・機械等)の求人のみ対象) -------------
# ホワイトリスト方式: 「製造業に該当すると分かっているカテゴリ/職種」だけを残す。
# ブラックリスト方式 (非製造だけ除外) は未知の ID / 新設カテゴリを取りこぼすため採らない。
#
# /factory の業界カテゴリ (c_*) のうち製造業に該当するもの。
# 除外されるのは c_9 建築・住宅 / c_14 物流・配送。
_MFG_CATEGORY_IDS = {
    "c_7":  "印刷・製紙",
    "c_8":  "化学・石油化学",
    "c_10": "機械・金属・鉄鋼",
    "c_11": "食品・飲料",
    "c_12": "製薬・化粧品",
    "c_13": "半導体",
    "c_15": "自動車・部品・バイク",
    "c_16": "家電・パソコン・スマホ",
    "c_42": "電気・電子",
}
# ID が変わった / 新設された場合に備えたカテゴリ名によるホワイトリスト (完全一致)
_MFG_CATEGORY_NAMES = set(_MFG_CATEGORY_IDS.values())

# /factory の募集職種 (j_*) のうち製造 (加工・組立・検査・生産技術等) に該当するもの。
# 「事務」「店舗販売」「営業」「倉庫業務」「清掃」「IT保守運用」「設備工事」
# 「軽作業・ピッキング」「クレーン・フォークリフト・重機」「フィールドエンジニア」は
# 製造工程そのものではないため ホワイトリストに入れない。
_MFG_OCCUPATION_IDS = {
    "j_98":  "機械系メンテナンス",
    "j_106": "組立・加工・プレス",
    "j_107": "機械操作・製造補助",
    "j_109": "溶接・バリ取り",
    "j_110": "食品加工",
    "j_122": "CADオペレーター",
    "j_137": "検査・検品",
    "j_140": "機械・機械設計",
    "j_141": "電気設計",
    "j_144": "製品開発・知的財産",
    "j_145": "生産技術・設計",
    "j_146": "生産管理・工場長",
}
_MFG_OCCUPATION_NAMES = set(_MFG_OCCUPATION_IDS.values())

_EMPLOYMENT_TYPE = {
    "FULL_TIME": "正社員",
    "PART_TIME": "アルバイト・パート",
    "CONTRACTOR": "業務委託",
    "TEMPORARY": "派遣",
    "INTERN": "インターン",
    "OTHER": "その他",
}
_SALARY_UNIT = {"HOUR": "時給", "DAY": "日給", "WEEK": "週給", "MONTH": "月収例", "YEAR": "年収例"}


class JobhouseFactory2Crawler(StaticCrawler):
    """ジョブハウス工場 (製造業求人・直近3ヶ月) スクレイパー"""

    DELAY = 1.0
    TIMEOUT = 30
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )

    # 備考: 直近3ヶ月以内に掲載された求人のみ取得する
    RECENT_MONTHS = 3
    # 公開日が古い求人が この件数 連続したら打ち切る (ID 降順 ≒ 公開日降順)
    MAX_CONSECUTIVE_OLD = 300
    # サイトマップ取得のリトライ上限
    MAX_SITEMAP_ATTEMPTS = 3

    EXTRA_COLUMNS = [
        "求人掲載日",
        "掲載終了日",
        "募集職種",
        "雇用形態",
        "給与",
        "勤務地市区町村",
        "企業住所",
        "最寄り駅",
        "寮・社宅",
        "求人番号",
        "企業ID",
        "求人ID",
    ]

    def prepare(self):
        self._cutoff = self._months_ago(self.RECENT_MONTHS)
        self._seen: set[str] = set()
        logger.info(
            "公開日の下限: %s 以降 / 製造業カテゴリの求人のみ取得します", self._cutoff
        )

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        """求人詳細を新しい順に1件ずつ取得して即 yield する。

        Args:
            url: sites.yml に登録された正規 URL (https://jobhouse.jp/factory)
        """
        consecutive_old = 0

        for article_url in self._iter_article_urls(url):
            if article_url in self._seen:
                continue
            self._seen.add(article_url)

            try:
                item, posted = self._scrape_detail(article_url)
            except Exception as e:  # noqa: BLE001 — 1件の失敗で全体を止めない
                logger.warning("求人詳細の解析に失敗 (スキップ): %s — %s", article_url, e)
                continue

            # 備考の指示: 直近3ヶ月より古い掲載は取得しない
            if posted is not None and posted < self._cutoff:
                consecutive_old += 1
                if consecutive_old >= self.MAX_CONSECUTIVE_OLD:
                    logger.info(
                        "公開日が %s より古い求人が %d 件連続したため打ち切ります",
                        self._cutoff, consecutive_old,
                    )
                    return
                continue

            consecutive_old = 0
            if item:
                yield item

    # -------------------------------------------------------------- URL 列挙

    def _iter_article_urls(self, url: str):
        """求人詳細 URL を「公開日の新しい順」に遅延列挙する。

        サイトマップのシャードは1本ずつ取得し、取得できた分から順に yield するため、
        全シャードを読み終える前に最初の詳細ページへ進める。
        """
        root = url if url.endswith("/") else url + "/"
        index_url = urljoin(root, "sitemap_articles")

        shard_urls = self._sitemap_shards(index_url, root)
        if not shard_urls:
            logger.warning("サイトマップを取得できないため一覧ページに切り替えます")
            yield from self._iter_from_list(root)
            return

        total = 0
        # シャードは ID 昇順に並んでいるので、末尾のシャードから遡る
        for shard_url in reversed(shard_urls):
            xml = self._fetch_xml(shard_url)
            if not xml:
                continue
            found: dict[int, str] = {}
            for loc in _LOC_PATTERN.findall(xml):
                m = _ARTICLE_PATTERN.search(loc)
                if m:
                    found[int(m.group(1))] = urljoin(root, loc)
            total += len(found)
            self.total_items = total
            logger.info("サイトマップ %s: 求人 %d 件", shard_url.rsplit("/", 1)[-1], len(found))
            for article_id in sorted(found, reverse=True):
                yield found[article_id]

    def _sitemap_shards(self, index_url: str, root: str) -> list[str]:
        """サイトマップ index から子サイトマップ URL を返す (index でなければ自身を返す)。"""
        xml = self._fetch_xml(index_url)
        if not xml:
            return []
        if "<sitemapindex" not in xml:
            return [index_url]
        return [urljoin(root, loc) for loc in _LOC_PATTERN.findall(xml)]

    def _fetch_xml(self, url: str) -> str:
        """サイトマップ (gzip の場合あり) を取得してテキストで返す。

        一時的な失敗は指数バックオフで MAX_SITEMAP_ATTEMPTS 回まで再試行し、
        全て失敗したら空文字を返して呼び出し元のフォールバックに委ねる。
        """
        import time

        for attempt in range(self.MAX_SITEMAP_ATTEMPTS):
            try:
                res = self.session.get(url, timeout=self.TIMEOUT)
                res.raise_for_status()
            except Exception as e:  # noqa: BLE001 — 失敗時は一覧にフォールバックする
                logger.warning(
                    "サイトマップの取得に失敗 (%d/%d): %s — %s",
                    attempt + 1, self.MAX_SITEMAP_ATTEMPTS, url, e,
                )
                if attempt + 1 < self.MAX_SITEMAP_ATTEMPTS:
                    time.sleep(min(2 ** attempt, 8))
                continue

            body = res.content
            if body[:2] == b"\x1f\x8b":  # gzip マジックナンバー
                try:
                    body = gzip.decompress(body)
                except OSError as e:
                    logger.warning("サイトマップの展開に失敗: %s — %s", url, e)
                    return ""
            return body.decode("utf-8", errors="replace")
        return ""

    def _iter_from_list(self, root: str):
        """フォールバック: 新着順の一覧ページ (?sort=new&page=N) から詳細 URL を列挙する。"""
        list_root = urljoin(root, "articles")
        page = 1
        while True:
            list_url = f"{list_root}?sort=new" if page == 1 else f"{list_root}?sort=new&page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                return
            page_urls = [
                urljoin(list_url, a["href"])
                for a in soup.select(".articleItem .articleItem-title a[href]")
                if _ARTICLE_PATTERN.search(a["href"])
            ]
            if not page_urls:
                return
            yield from page_urls
            if soup.select_one("link[rel='next']") is None:
                return
            page += 1

    # -------------------------------------------------------------- 詳細ページ

    def _scrape_detail(self, url: str) -> tuple[dict | None, date | None]:
        """求人詳細を1件解析する。(item, 公開日) を返す。"""
        soup = self.get_soup(url)
        if soup is None:
            return None, None

        job_ld = self._find_job_posting(soup)
        labels, raw_labels = self._label_map(soup)

        posted = self._parse_date(labels.get("公開日")) or self._parse_date(job_ld.get("datePosted"))
        if posted is not None and posted < self._cutoff:
            # 打ち切り判定は呼び出し元に任せる (item は作らない)
            return None, posted

        # ---- 業界カテゴリ (c_*) / 募集職種 (j_*)
        category, occupation, cats, occs = self._job_types(soup)

        # 備考の指示: 製造業の求人のみ対象 (カテゴリ/職種のホワイトリスト判定)
        if not self._is_manufacturing(cats, occs):
            logger.debug("製造業以外のためスキップ: %s (%s / %s)", url, category, occupation)
            return None, posted

        m = _ARTICLE_PATTERN.search(url)
        job_id = m.group(1) if m else ""

        # ---- 会社名 (募集概要の「企業」→ 企業情報の「企業名」→ JSON-LD の順)
        org = job_ld.get("hiringOrganization") or {}
        name = (
            labels.get("企業")
            or labels.get("企業名")
            or (org.get("name", "") if isinstance(org, dict) else "")
        )

        # ---- 住所 (勤務地が主。企業情報テーブルの本社所在地は EXTRA へ)
        job_addr = ((job_ld.get("jobLocation") or {}).get("address") or {})
        if not isinstance(job_addr, dict):
            job_addr = {}
        region = job_addr.get("addressRegion", "") or ""
        locality = job_addr.get("addressLocality", "") or ""
        street = job_addr.get("streetAddress", "") or ""

        company_addr = labels.get("住所") or ""
        post_code = self._format_post_code(job_addr.get("postalCode", ""))
        pm = _POST_CODE_PATTERN.match(company_addr)
        if pm:
            company_addr = company_addr[pm.end():].strip()
            post_code = post_code or f"{pm.group(1)}-{pm.group(2)}"

        addr = f"{locality}{street}".strip()
        pref = region
        if not addr:
            # JSON-LD に勤務地が無い求人は本社所在地で代替する
            addr = company_addr
        if not pref:
            prefm = _PREF_PATTERN.search(f"{region}{addr}")
            pref = prefm.group(1) if prefm else ""
        if pref and addr.startswith(pref):
            addr = addr[len(pref):]

        # ---- 応募先電話番号 (掲載のない求人もある)
        #      フッターの 0120-413-542 はサイト共通の相談ダイヤルなので採用しない
        tel = ""
        tel_node = soup.select_one(".articleTelEntry-tel")
        if tel_node is not None:
            tm = _TEL_PATTERN.search(tel_node.get_text(" ", strip=True))
            tel = tm.group(0) if tm else ""

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.EMP_NUM: self._employee_count(labels, raw_labels),
            Schema.CAT_SITE: category,
            Schema.STS_NM: "募集終了" if soup.select_one(".articleInactivePanel") else "募集中",
            Schema.TIME: self._work_hours(raw_labels.get("勤務時間")),
            Schema.URL: url,
            "求人掲載日": posted.isoformat() if posted else "",
            "掲載終了日": self._date_str(job_ld.get("validThrough")),
            "募集職種": occupation,
            "雇用形態": self._employment_type(job_ld),
            "給与": self._format_salary(job_ld.get("baseSalary")),
            "勤務地市区町村": locality,
            "企業住所": company_addr,
            "最寄り駅": labels.get("最寄り駅") or "",
            "寮・社宅": labels.get("寮・社宅") or "",
            "求人番号": labels.get("求人番号") or "",
            "企業ID": self._corporation_id(soup),
            "求人ID": job_id,
        }, posted

    # ------------------------------------------------------------------ 補助

    @staticmethod
    def _is_manufacturing(cats: list[tuple[str, str]], occs: list[tuple[str, str]]) -> bool:
        """製造業 (加工・食品製造・機械等) の求人かどうかをホワイトリストで判定する。

        判定ルール (ホワイトリスト方式):
          1. 業界カテゴリが1つでも製造業ホワイトリスト (_MFG_CATEGORY_IDS) に一致すること。
             → 建築・住宅 / 物流・配送 のみの求人はここで落ちる。
          2. 募集職種が付いている場合は、そのうち1つ以上が製造職ホワイトリスト
             (_MFG_OCCUPATION_IDS) に一致すること。
             → 業界が製造業でも募集職種が「事務」のみの求人はここで落ちる。
          3. 業界カテゴリが取れない求人は、募集職種が製造職ホワイトリストに一致する場合のみ残す。
             どちらも取れない求人は製造業と確認できないため除外する。
        """
        cat_ok = any(
            cid in _MFG_CATEGORY_IDS or name in _MFG_CATEGORY_NAMES for cid, name in cats
        )
        occ_ok = any(
            oid in _MFG_OCCUPATION_IDS or name in _MFG_OCCUPATION_NAMES for oid, name in occs
        )
        if cats and not cat_ok:
            return False
        if occs and not occ_ok:
            return False
        # カテゴリ・職種のどちらかが製造業ホワイトリストに一致していること (両方無しは除外)
        return cat_ok or occ_ok

    @classmethod
    def _label_map(cls, soup) -> tuple[dict[str, str], dict[str, str]]:
        """table.articleTable の th(ラベル)/td(値) を辞書化する。

        おすすめ求人カード (.articleCard 配下の class 無し table) は articleTable では
        ないため自然に除外されるが、フッターの表も念のため対象外にする。
        戻り値は (整形済みの短い値, td の生テキスト) の2種類。生テキストは
        勤務時間の時間帯抽出のように <p> 内の記述から正規表現で拾う用途に使う。
        """
        out: dict[str, str] = {}
        raw: dict[str, str] = {}
        for table in soup.select("table.articleTable"):
            if table.find_parent(class_="articleFooter") is not None:
                continue
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th is None or td is None:
                    continue
                label = th.get_text(" ", strip=True)
                if label not in _WANTED_LABELS or label in out:
                    continue
                out[label] = cls._td_text(td)
                raw[label] = td.get_text("\n", strip=True)
        return out, raw

    @staticmethod
    def _td_text(td) -> str:
        """td から短い構造化された値だけを取り出す (特徴ラベル・補足文・注記を除去)。"""
        clone = BeautifulSoup(str(td), "html.parser")
        for tag in clone.select(".articleCard-label, .articleItem-label, .article-dates, p, img, i"):
            tag.decompose()
        text = clone.get_text("\n", strip=True)
        # 「1233510　※電話応募で必要になります。」のような注記を落とす
        text = re.split(r"[\n※]|（電話応募", text)[0]
        return text.strip("　 \t")

    @staticmethod
    def _employee_count(labels: dict[str, str], raw_labels: dict[str, str]) -> str:
        """従業員数を返す。ジョブハウスは掲載が無いため通常は空文字。"""
        value = labels.get("従業員数") or ""
        if value:
            return value
        for raw in raw_labels.values():
            m = _EMP_NUM_PATTERN.search(raw)
            if m:
                return m.group(1).replace(" ", "")
        return ""

    @staticmethod
    def _job_types(soup) -> tuple[str, str, list[tuple[str, str]], list[tuple[str, str]]]:
        """職種ラベルを (業界カテゴリ名, 募集職種名, c_* の(ID,名称), j_* の(ID,名称)) に分けて返す。"""
        cats: dict[str, str] = {}
        occs: dict[str, str] = {}
        for a in soup.select(".sectionHeader a.articleCard-label.is-jobType[href]"):
            text = a.get_text(" ", strip=True)
            if not text:
                continue
            om = _OCCUPATION_ID_PATTERN.search(a["href"])
            cm = _CATEGORY_ID_PATTERN.search(a["href"])
            if om:
                occs.setdefault(om.group(1), text)
            elif cm:
                cats.setdefault(cm.group(1), text)
        return (
            "/".join(dict.fromkeys(cats.values())),
            "/".join(dict.fromkeys(occs.values())),
            list(cats.items()),
            list(occs.items()),
        )

    @staticmethod
    def _corporation_id(soup) -> str:
        link = soup.select_one("table.articleTable a[href*='/articles/b_']")
        if link is None:
            return ""
        m = _CORP_ID_PATTERN.search(link["href"])
        return m.group(1) if m else ""

    @staticmethod
    def _find_job_posting(soup) -> dict:
        for script in soup.select("script[type='application/ld+json']"):
            raw = script.string or script.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                return data
        return {}

    @staticmethod
    def _months_ago(months: int) -> date:
        today = date.today()
        month = today.month - months
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        day = min(today.day, 28)
        return date(year, month, day)

    @staticmethod
    def _parse_date(value: str | None) -> date | None:
        if not value:
            return None
        m = _JP_DATE_PATTERN.search(value) or _ISO_DATE_PATTERN.search(value)
        if not m:
            return None
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None

    @classmethod
    def _date_str(cls, value: str | None) -> str:
        parsed = cls._parse_date(value)
        return parsed.isoformat() if parsed else ""

    @staticmethod
    def _format_post_code(value: str) -> str:
        digits = re.sub(r"\D", "", value or "")
        return f"{digits[:3]}-{digits[3:]}" if len(digits) == 7 else ""

    @staticmethod
    def _work_hours(raw: str | None) -> str:
        """勤務時間の td から時間帯 (09:00〜17:45) だけを抜き出す。"""
        if not raw:
            return ""
        matches = [m.group(0) for m in _HOURS_PATTERN.finditer(raw)]
        return "/".join(dict.fromkeys(matches))

    @staticmethod
    def _employment_type(job_ld: dict) -> str:
        value = job_ld.get("employmentType") or ""
        if isinstance(value, list):
            return "/".join(_EMPLOYMENT_TYPE.get(v, v) for v in value)
        return _EMPLOYMENT_TYPE.get(value, value)

    @staticmethod
    def _format_salary(base_salary) -> str:
        """JSON-LD の baseSalary を「時給 1,350 円」形式に整形する。"""
        if not isinstance(base_salary, dict):
            return ""
        value = base_salary.get("value")
        if not isinstance(value, dict):
            return ""
        amount = value.get("value") or value.get("minValue") or ""
        if amount in ("", None):
            return ""
        unit = _SALARY_UNIT.get(value.get("unitText", ""), "")
        try:
            amount_str = f"{int(float(amount)):,}"
        except (TypeError, ValueError):
            amount_str = str(amount)
        return f"{unit} {amount_str} 円".strip()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = JobhouseFactory2Crawler()
    scraper.execute("https://jobhouse.jp/factory")
