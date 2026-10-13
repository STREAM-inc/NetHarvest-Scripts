"""
メカニッ求 (整備士) — 自動車整備士・メカニック専門求人サイト (mechani-q.com)

取得対象 (依頼元の備考指定):
    職種カテゴリ「自動車整備士(メカニック)」(/job_category/mechanic, 約1,084件) と
    「板金塗装」(/job_category/auto-body-repair, 約33件) の2カテゴリの求人票。
    起点 URL (sites.yml の url) を唯一のルートとし、もう一方のカテゴリ URL は
    起点 URL の末尾スラッグを差し替えて導出する (別ルートはハードコードしない)。

取得フロー:
    一覧 (WordPress ページ送り /page/N・12件/ページ) の #jobsList .jobsite-job-card-wrap
    から詳細 URL (/job/job-{ID}) を集め、詳細ページの「募集要項」「企業情報」テーブルと
    ヘッダー部 (職種タグ・雇用形態タグ・エージェント注記) を解析する。
    詳細を 1 件取得するごとに即 yield する (Pattern B)。

依頼元が指定した取得項目との対応:
    企業名(拠点名含む) → 募集要項「会社名」(例: 株式会社レソリューション 大阪営業所)
    勤務地(都道府県・市区町村) → 募集要項「勤務地」を 都道府県 / 市区町村 / 住所 に分解
    TEL → 専用フィールドが無いため、募集要項の本文中 (応募条件 等) に記載がある場合のみ
    職種 → ヘッダーの職種タグ (Schema.CAT_SITE)
    求人タイトル → h1.job-detail-title
    雇用形態 → 募集要項「雇用形態」+ エージェント(有料職業紹介)求人の別
    給与 / 年収 → 募集要項「給与」(月給・時給の表記どおり) / 「給与例・年収例」
    掲載日・更新日 → 募集要項「掲載開始日」「掲載終了日」
        ※ サイト上に「更新日」の表示フィールドは存在しない (並び順の選択肢のみ)。
          HTML にも article:modified_time / dateModified は出力されないため、
          取得できる日付は掲載開始日・掲載終了日の 2 種類のみ。
    求人本文 → 依頼元の明示指示により全文を取得 (求人本文カラム)

利用規約:
    サイトポリシー (/site-policy) にスクレイピング・クローリングの禁止条項は無い。
    robots.txt も /wp-admin/ 以外は許可。ただし「求人に関するページは無断転載を禁止」と
    明記されているため、取得データの再配布・転載には注意すること。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/mechani_q_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id mechani_q_2
"""

import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_MEDIA_NAME = "メカニッ求"
_ITEMS_PER_PAGE = 12
# 依頼元が指定した対象カテゴリのスラッグ (起点 URL のスラッグを差し替えて導出する)
_TARGET_CATEGORY_SLUGS = ["mechanic", "auto-body-repair"]
_MAX_PAGE_LIMIT = 300  # 無限ループ防止の上限

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 政令指定都市 (○○市△△区) / 郡部 (○○郡△△町) にも対応した市区町村の切り出し
_CITY_PATTERN = re.compile(r"^(.+?郡.+?[町村]|.+?市.+?区|.+?[市区町村])")
_POST_CODE_PATTERN = re.compile(r"〒\s*(\d{3}-?\d{4})")
_TEL_PATTERN = re.compile(r"0\d{1,4}[-(－（]\d{1,4}[-)－）]?\d{3,4}")
_NUM_RE = re.compile(r"\d[\d,]*")
# 求人本文に採用する募集要項のラベル (出現順に連結する)
_BODY_LABELS = (
    "仕事内容",
    "募集背景",
    "勤務地備考",
    "給与詳細",
    "給与例・年収例",
    "応募条件",
    "勤務時間備考",
    "休日備考",
    "その他休暇",
    "待遇・福利厚生",
    "その他福利厚生",
    "選考の流れについて",
)
# TEL を探索する募集要項ラベル (求人票によって記載箇所が異なる)
_TEL_LABELS = ("応募条件", "選考の流れについて", "仕事内容", "勤務地備考", "その他福利厚生")


def _clean(text: str) -> str:
    """空白・改行を1スペースに正規化する。"""
    return re.sub(r"\s+", " ", text or "").strip()


def _multiline(el) -> str:
    """<br> 区切りを改行として保ったままテキスト化する。"""
    if el is None:
        return ""
    lines = [re.sub(r"[ \t　]+", " ", ln).strip() for ln in el.get_text("\n").split("\n")]
    return "\n".join([ln for ln in lines if ln]).strip()


class MechaniQSeibishiCrawler(StaticCrawler):
    """メカニッ求 — 整備士・板金塗装カテゴリ (mechani-q.com) スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "媒体名",
        "求人ID",
        "求人タイトル",
        "勤務地市区町村",
        "雇用形態",
        "エージェント求人",
        "給与",
        "年収",
        "年間休日",
        "掲載開始日",
        "掲載終了日",
        "掲載企業名",
        "掲載企業住所",
        "求人本文",
    ]

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        seen: set[str] = set()
        for category_url in self._category_urls(url):
            yield from self._crawl_category(category_url, seen)

    # --------------------------------------------------------------- 一覧巡回

    @staticmethod
    def _category_urls(url: str) -> list[str]:
        """起点 URL から対象 2 カテゴリの一覧 URL を導出する。

        引数 URL をそのまま先頭に置き、もう一方のカテゴリは末尾スラッグを
        差し替えて作る (別のルート URL はハードコードしない)。
        """
        base = url.rstrip("/")
        urls = [base]
        current_slug = base.rsplit("/", 1)[-1]
        for slug in _TARGET_CATEGORY_SLUGS:
            if slug == current_slug:
                continue
            sibling = urllib.parse.urljoin(base + "/", "../" + slug).rstrip("/")
            if sibling not in urls:
                urls.append(sibling)
        return urls

    def _crawl_category(self, category_url: str, seen: set[str]):
        page = 1
        max_page = None

        while page <= _MAX_PAGE_LIMIT:
            page_url = category_url if page == 1 else f"{category_url}/page/{page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            cards = soup.select("#jobsList .jobsite-job-card-wrap")
            if not cards:
                break

            if page == 1:
                max_page = self._detect_max_page(soup)

            for card in cards:
                link = card.select_one("a.job-card[href]")
                if not link:
                    continue
                detail_url = urllib.parse.urljoin(page_url, link["href"])
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url, card)
                except Exception as e:  # noqa: BLE001 — 1件の失敗で全体を止めない
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item  # 1件取得するごとに即 yield (Pattern B)

            if max_page is not None and page >= max_page:
                break
            page += 1

    @staticmethod
    def _detect_max_page(soup) -> int | None:
        """ページャーの最大ページ番号を取得する (無ければ総件数から算出)。"""
        pages = []
        for a in soup.select("ul.page-numbers a.page-numbers[href]"):
            m = re.search(r"/page/(\d+)", a["href"])
            if m:
                pages.append(int(m.group(1)))
        if pages:
            return max(pages)

        total_el = soup.select_one(".results-count")
        if total_el:
            m = _NUM_RE.search(total_el.get_text())
            if m:
                total = int(m.group(0).replace(",", ""))
                return max(1, (total + _ITEMS_PER_PAGE - 1) // _ITEMS_PER_PAGE)
        return None

    # --------------------------------------------------------------- 詳細解析

    def _scrape_detail(self, detail_url: str, card) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        job_table = self._label_map(soup, "募集要項")
        com_table = self._label_map(soup, "企業情報")
        # 関連求人カードにも .job-tag / .tag-agent が存在するため、必ずヘッダーに限定する
        head = soup.select_one("header.job-detail-head")

        title_el = head.select_one("h1.job-detail-title") if head else None
        title = _clean(title_el.get_text()) if title_el else ""

        # ヘッダーのタグ: 1つ目が職種、2つ目が雇用形態
        tags = []
        if head:
            tags = [
                _clean(sp.get_text())
                for sp in head.select(".job-detail-tags span")
                if _clean(sp.get_text())
            ]
        occupation = tags[0] if tags else ""
        if not occupation:
            occupation = self._card_occupation(card)

        employment = job_table.get("雇用形態", "") or (tags[1] if len(tags) > 1 else "")
        if not employment:
            # 旧い求人票は詳細に雇用形態が無いことがあるため、一覧カードのタグで補完する
            card_employment = card.select_one(".tag-employment")
            employment = _clean(card_employment.get_text()) if card_employment else ""

        # エージェント(有料職業紹介)求人の別。詳細ヘッダーの注記を正とし、
        # 無ければ一覧カードの .tag-agent で判定する。
        is_agent = bool(head and head.select_one("p.job-agent-notice")) or bool(
            card.select_one(".tag-agent")
        )
        employment_label = (
            f"{employment}（エージェント求人）" if is_agent and employment else employment
        )

        # 企業名 (拠点名を含む)。募集要項の「会社名」が実際の就業先で、
        # 「企業情報」セクションは掲載元 (人材会社) なので別カラムに分ける。
        company = job_table.get("会社名", "")
        if not company and head:
            company_el = head.select_one("p.job-detail-company")
            company = _clean(company_el.get_text()) if company_el else ""
        if not company:
            card_company = card.select_one("p.job-company")
            company = _clean(card_company.get_text()) if card_company else ""

        # 勤務地 (地図リンクを除去済みのテキスト)
        work_place = job_table.get("勤務地", "") or self._card_location(card)
        post_code, pref, city, addr = self._split_address(work_place)

        body = self._body_text(soup, job_table, com_table)
        tel = self._pick_tel(job_table, body)

        m_id = re.search(r"/job/([^/?#]+)", detail_url)

        return {
            Schema.URL: detail_url,
            Schema.NAME: company,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: occupation,
            Schema.TIME: job_table.get("勤務時間", ""),
            "媒体名": _MEDIA_NAME,
            "求人ID": m_id.group(1) if m_id else "",
            "求人タイトル": title,
            "勤務地市区町村": city,
            "雇用形態": employment_label,
            "エージェント求人": "あり" if is_agent else "",
            "給与": job_table.get("給与", ""),
            "年収": job_table.get("給与例・年収例", ""),
            "年間休日": job_table.get("年間休日", ""),
            "掲載開始日": job_table.get("掲載開始日", ""),
            "掲載終了日": job_table.get("掲載終了日", ""),
            "掲載企業名": com_table.get("企業名", ""),
            "掲載企業住所": com_table.get("住所", ""),
            "求人本文": body,
        }

    # ----------------------------------------------------------------- helpers

    @staticmethod
    def _card_items(card) -> list[str]:
        """一覧カードのアイコン付き項目 (職種 / 勤務地 / 給与) をテキスト化する。"""
        return [
            _clean(el.get_text()) for el in card.select(".job-details .job-detail-item span")
        ]

    @classmethod
    def _card_occupation(cls, card) -> str:
        items = cls._card_items(card)
        return items[0] if items else ""

    @classmethod
    def _card_location(cls, card) -> str:
        items = cls._card_items(card)
        return items[1] if len(items) > 1 else ""

    @staticmethod
    def _label_map(soup, section_title: str) -> dict:
        """見出しが section_title のセクション内 th/td を {見出し: 値} に畳む (先勝ち)。"""
        result: dict[str, str] = {}
        for section in soup.select("section.job-info-section"):
            heading = section.select_one("h2.job-info-title")
            if not heading or _clean(heading.get_text()) != section_title:
                continue
            for tr in section.select("table tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = _clean(th.get_text())
                if not label or label in result:
                    continue
                # 「地図で場所を確認」等の付属 UI を除いてからテキスト化する
                for extra in td.select(".map-link-wrap, a[href*='maps']"):
                    extra.decompose()
                result[label] = _clean(td.get_text(" "))
            break
        return result

    @staticmethod
    def _split_address(text: str) -> tuple[str, str, str, str]:
        """住所文字列を (郵便番号, 都道府県, 市区町村, 市区町村以降) に分割する。

        先頭に郵便番号が付く求人や、都道府県が重複入力された求人
        (「大阪府大阪府大阪市西区…」) があるため、郵便番号を切り出したうえで
        先頭の重複した都道府県を取り除く。
        """
        rest = _clean(text)

        post_code = ""
        pm = _POST_CODE_PATTERN.search(rest)
        if pm:
            post_code = pm.group(1)
            rest = _clean(rest.replace(pm.group(0), " ", 1))

        pref = ""
        pf = _PREF_PATTERN.search(rest)
        if pf:
            pref = pf.group(1)
            rest = rest[pf.end():].lstrip()
            # 「大阪府大阪府…」のように都道府県が重複している場合は続けて除去する
            while True:
                dup = _PREF_PATTERN.match(rest)
                if not dup:
                    break
                rest = rest[dup.end():].lstrip()

        city = ""
        if rest:
            cm = _CITY_PATTERN.match(rest)
            if cm:
                city = cm.group(1)
        return post_code, pref, city, rest

    @staticmethod
    def _pick_tel(job_table: dict, body: str) -> str:
        """TEL 専用フィールドが無いため、記載されやすいラベル → 本文全体の順に探す。"""
        for label in _TEL_LABELS:
            value = job_table.get(label, "")
            if not value:
                continue
            m = _TEL_PATTERN.search(value)
            if m:
                return m.group(0)
        m = _TEL_PATTERN.search(body)
        return m.group(0) if m else ""

    @staticmethod
    def _body_text(soup, job_table: dict, com_table: dict) -> str:
        """求人本文 (冒頭紹介文 + 募集要項の記述項目 + 掲載元情報) を全文で組み立てる。

        依頼元の備考「求人本文全文を抽出」の明示指示に基づき取得する。
        """
        parts: list[str] = []

        intro = soup.select_one("section.job-detail-intro .rich-text")
        intro_text = _multiline(intro)
        if intro_text:
            parts.append(intro_text)

        pr_items = [
            _clean(li.get_text(" ")) for li in soup.select("section.job-pr-box .job-pr-list li")
        ]
        pr_items = [t for t in pr_items if t]
        if pr_items:
            parts.append("【PR】" + " / ".join(pr_items))

        for label in _BODY_LABELS:
            value = job_table.get(label, "")
            if value:
                parts.append(f"【{label}】{value}")

        company_parts = [
            f"{label}: {com_table[label]}"
            for label in ("企業名", "住所")
            if com_table.get(label)
        ]
        if company_parts:
            parts.append("【掲載企業】" + " / ".join(company_parts))

        return "\n".join(parts).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MechaniQSeibishiCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://mechani-q.com/job_category/mechanic")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
