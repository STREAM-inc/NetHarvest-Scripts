"""
Re就活30 — 30代向け転職・求人情報 (re-katsu30.jp)

取得方針:
    一覧（検索結果）の各カードから「会社名・職種/業種カテゴリ・雇用形態・年収範囲・勤務地」を
    取得したうえで、各求人の詳細ページ /recruit/{id} を開いて会社情報を補完する。
        詳細ページ専用カラム:
            設立 / 代表者 / 従業員数 / 資本金 / 売上高 / 本社所在地(郵便番号) /
            事業所 / 事業内容 / ホームページ / 掲載更新日 / 掲載終了日
    これらは一覧には無いため、詳細巡回が必須（本実装で対応済み）。

    詳細ページ構造（要点 / 2026-06 時点で確認済み）:
        <section id="company">           企業情報
            <h3 class="recruitDetail__sectionSubTitle">設立</h3>
            <p  class="recruitDetail__sectionText">2003年11月</p>
            … 代表者 / 従業員数 / 資本金 / 売上高 / 本社所在地 / 事業所 / ホームページ
        <section id="business">          事業内容（p.recruitDetail__sectionText）
        <section class="recruitDetail__info">
            <p class="recruitDetail__infoHeadEndDate">最終更新日：YYYY/MM/DD(曜)</p>
            <p class="recruitDetail__infoHeadEndDate">掲載終了日：YYYY/MM/DD(曜)</p>
            <ul class="scoutDetail__info__tagList">
                <li class="-condition">正社員</li>      ← 雇用形態
                <li class="-condition">450万円〜600万円</li> ← 年収範囲
        ※ サイト上に「掲載開始日」の明示項目は無い。指示の「掲載懇親部」は
          発音由来の表記ゆれで「掲載更新日(=最終更新日)」を指すと解釈し、
          最終更新日を「掲載更新日」カラムに格納する。
        ※ 業種・職種の明示ラベルも詳細には無いため、一覧カードの
          featuredJob__item__categories（このサイトの定義業種・職種ジャンル）を流用する。

★ WAF について（重要 / 切り分け済み）
    re-katsu30.jp は AWS WAF が「送信元 IP」でブロックする（IP 起因）。
    - UA / ヘッダは無関係。クリーン IP からは素の requests でも 200 で全件取得可。
    - データセンタ帯（AWS 等）の IP は 403。→ Playwright に替えても同 IP なら 0 件。
    成功条件は「クリーン IP からの egress」だけ。よって本実装はプロキシ対応を持つ。
        RE30_PROXY=http://user:pass@host:port  （JP 系 / レジデンシャル推奨）
    プロキシ無しでブロックされた場合は黙って 0 件にせず RuntimeError で止める。

実行方法:
    # ローカルテスト（要 RE30_PROXY、または非ブロック IP）
    RE30_PROXY=http://... python scripts/sites/jobs/re30.py

    # Prefect Flow 経由
    docker compose exec -e RE30_PROXY=http://... worker python /app/bin/run_flow.py --site-id re30
"""

import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POSTCODE_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
# 正常時は ~74 ページ (全1107件 / 15件)。next リンク消失で自然終端するが、保険の上限。
_MAX_PAGES = 200


def _parse_location(text: str) -> tuple[str, str]:
    """住所/勤務地テキストから (都道府県, 住所) を best-effort で抽出する。

    例:
        「本社（東京都練馬区）／西武池袋線…」 → ("東京都", "練馬区")
        「東京都渋谷区恵比寿1-23-23 …」      → ("東京都", "渋谷区恵比寿1-23-23 …")
        「東京都」                            → ("東京都", "")
    都道府県が含まれない（「全国のプロジェクト先」等）場合は ("", "") を返す。
    """
    text = re.sub(r"[\s　]+", " ", text).strip()
    pref_m = _PREF_PATTERN.search(text)
    if not pref_m:
        return "", ""
    pref = pref_m.group(1)
    addr = text[pref_m.end():]
    # 「本社（東京都練馬区）」のような囲み・路線アクセス情報を住所から切り落とす
    addr = re.split(r"[）)／「(（]", addr)[0].strip()
    return pref, addr


def _looks_waf_blocked(soup) -> bool:
    """WAF / ELB の 403 ブロックページか判定する。"""
    if soup is None:
        return False
    # 正常ページには必ず検索結果コンテナがある
    if soup.select_one(".searchResult__list, article.searchResult__item"):
        return False
    text = soup.get_text(" ", strip=True)[:600].lower()
    markers = ("403", "forbidden", "request blocked", "access denied", "akamai", "cloudfront")
    return any(m in text for m in markers) or len(text) < 200


class Re30Crawler(DynamicCrawler):
    """Re就活30 クローラー"""

    DELAY = 1.5
    # Schema に当てはまらないサイト固有カラム。
    #   業種/職種/雇用形態/年収範囲/勤務地 … 求人属性
    #   事業所 … 本社以外の所在地（複数行）
    #   掲載更新日(=最終更新日)/掲載終了日 … 掲載期間
    EXTRA_COLUMNS = [
        "業種", "職種", "雇用形態", "年収範囲", "勤務地",
        "事業所", "掲載更新日", "掲載終了日",
    ]

    # ★ WAF は IP 起因。クリーン IP の egress をプロキシで与えると成功する。
    PROXY = os.environ.get("RE30_PROXY")

    def _setup(self):
        """Playwright 起動。RE30_PROXY があれば Chromium 起動時にプロキシを渡す。"""
        import logging as _logging
        from playwright.sync_api import sync_playwright

        log = _logging.getLogger(__name__)
        self.playwright = sync_playwright().start()
        launch_kwargs: dict = {"headless": True}
        if self.PROXY:
            launch_kwargs["proxy"] = {"server": self.PROXY}
            log.info("RE30_PROXY を Playwright に適用します。")
        self.browser = self.playwright.chromium.launch(**launch_kwargs)
        self.context = self.browser.new_context(user_agent=self.USER_AGENT)
        self.page = self.context.new_page()

    # ------------------------------------------------------------------ #
    # 詳細ページ解析
    # ------------------------------------------------------------------ #
    @staticmethod
    def _detail_value(h3) -> str:
        """h3.sectionSubTitle に続く p.sectionText を（複数あれば連結して）返す。"""
        parts = []
        for sib in h3.find_next_siblings():
            name = getattr(sib, "name", None)
            if name == "h3":
                break  # 次の項目に到達
            if name == "p" and "recruitDetail__sectionText" in (sib.get("class") or []):
                txt = sib.get_text("\n", strip=True)
                if txt:
                    parts.append(txt)
        return "\n".join(parts).strip()

    def _parse_detail(self, soup) -> dict:
        """詳細ページ /recruit/{id} から会社情報・掲載期間を抽出する。"""
        data: dict = {}

        # --- 企業情報・募集要項の「見出し → 本文」マップ ---
        labels: dict[str, str] = {}
        for h3 in soup.select("h3.recruitDetail__sectionSubTitle"):
            key = h3.get_text(strip=True)
            if key and key not in labels:
                labels[key] = self._detail_value(h3)

        def pick(*keys):
            for k in keys:
                if labels.get(k):
                    return labels[k]
            return ""

        data["設立"] = pick("設立", "設立年月", "創業")
        data["代表者"] = pick("代表者", "代表者名", "代表取締役")
        data["従業員数"] = pick("従業員数", "社員数", "従業員")
        data["資本金"] = pick("資本金")
        data["売上高"] = pick("売上高", "売上", "業績")
        data["事業内容_label"] = pick("事業内容")
        data["事業所"] = pick("事業所", "支社", "支店", "営業所")
        data["本社所在地"] = pick("本社所在地", "所在地", "本社")
        data["ホームページ"] = pick("ホームページ", "URL", "ＨＰ", "HP")

        # --- 事業内容（section#business の本文。labels に無い場合の保険） ---
        if not data["事業内容_label"]:
            biz = soup.select_one("#business .recruitDetail__sectionText")
            if biz:
                data["事業内容_label"] = biz.get_text("\n", strip=True)

        # --- ホームページ（リンク優先） ---
        hp_link = soup.select_one("#company a[href^='http'], .recruitDetail__sectionCompany a[href^='http']")
        if hp_link and hp_link.get("href"):
            data["ホームページ"] = hp_link.get("href").strip()

        # --- 会社名（詳細の正） ---
        name_el = soup.select_one(".recruitDetail__sectionCompanyName, h1.recruitDetail__headTitle")
        data["名称"] = name_el.get_text(strip=True) if name_el else ""

        # --- 雇用形態・年収範囲（条件タグ） ---
        conditions = soup.select("ul.scoutDetail__info__tagList li.-condition")
        data["雇用形態"] = conditions[0].get_text(strip=True) if len(conditions) > 0 else ""
        data["年収範囲"] = conditions[1].get_text(strip=True) if len(conditions) > 1 else ""

        # --- 掲載更新日(=最終更新日) / 掲載終了日 ---
        for p in soup.select("p.recruitDetail__infoHeadEndDate"):
            txt = p.get_text(strip=True)
            label, _, value = txt.partition("：")
            value = value.strip()
            if "更新" in label:
                data["掲載更新日"] = value
            elif "終了" in label or "掲載" in label:
                data["掲載終了日"] = value

        return data

    # ------------------------------------------------------------------ #
    # メイン巡回
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        page_url = url
        page_no = 1
        yielded = 0
        seen_pages: set[str] = set()

        while True:
            # ★ 無限ループ保険: 同一URL再訪 / ページ上限で打ち切る。
            if page_url in seen_pages:
                self.logger.warning("同一ページURLを再訪。巡回を打ち切ります: %s", page_url)
                break
            seen_pages.add(page_url)
            if page_no > _MAX_PAGES:
                self.logger.warning("ページ上限 %d に到達。巡回を打ち切ります。", _MAX_PAGES)
                break

            soup = self.get_soup(page_url)
            if soup is None:
                break

            items = soup.select("article.searchResult__item")
            if not items:
                # ★ 黙って 0 件で終わらせない。WAF ブロックは明示エラーにする。
                if page_no == 1 and _looks_waf_blocked(soup):
                    raise RuntimeError(
                        "WAF にブロックされました（IP 起因）。RE30_PROXY にクリーン IP の"
                        "プロキシを設定するか、非ブロック IP のホストで実行してください。"
                        f" url={page_url}"
                    )
                break

            if page_no == 1:
                total_el = soup.select_one(".pageNumber__all")
                if total_el:
                    m = re.search(r"(\d[\d,]+)", total_el.get_text())
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))

            for item in items:
                try:
                    name_el = item.select_one("p.featuredJob__item__company")
                    name = name_el.get_text(strip=True) if name_el else ""

                    # 一覧カードの categories はこのサイトの定義業種・職種ジャンル。
                    cats = item.select("ul.featuredJob__item__categories li")
                    cat_list = [c.get_text(strip=True) for c in cats if c.get_text(strip=True)]
                    cat_site = "/".join(cat_list)
                    # 詳細ページに業種/職種の明示ラベルが無いため、ここを正とする。
                    # 先頭=職種、残り=業種（単一なら両方に同値）として best-effort 分配。
                    shokushu = cat_list[0] if cat_list else ""
                    gyoshu = "/".join(cat_list[1:]) if len(cat_list) > 1 else (cat_list[0] if cat_list else "")

                    conditions = item.select("ul.scoutDetail__info__tagList li.-condition")
                    employ_type = conditions[0].get_text(strip=True) if len(conditions) > 0 else ""
                    salary_range = conditions[1].get_text(strip=True) if len(conditions) > 1 else ""

                    detail_a = item.select_one("h2.featuredJob__item__title a")
                    href = detail_a.get("href", "") if detail_a else ""
                    # ★ URL は現在ページ url から派生させる（ルートをハードコードしない）。
                    detail_url = urljoin(page_url, href) if href else ""

                    # 勤務地は dl.details__list の <dt>勤務地</dt><dd>…</dd> に入る。
                    work_location = ""
                    for dt in item.select("div.featuredJob__item__details dt.details__heading"):
                        if dt.get_text(strip=True) == "勤務地":
                            dd = dt.find_next_sibling("dd")
                            if dd:
                                work_location = dd.get_text(" ", strip=True)
                            break

                    pref, addr = _parse_location(work_location)
                    post_code = ""

                    # --- 詳細ページを開いて会社情報を補完する ---
                    rep = emp = cap = sales = lob = office = founded = hp = ""
                    pub_update = pub_end = ""
                    if detail_url:
                        time.sleep(self.DELAY)
                        detail_soup = self.get_soup(detail_url)
                        if detail_soup is not None:
                            d = self._parse_detail(detail_soup)
                            if d.get("名称"):
                                name = d["名称"]
                            rep = d.get("代表者", "")
                            emp = d.get("従業員数", "")
                            cap = d.get("資本金", "")
                            sales = d.get("売上高", "")
                            lob = d.get("事業内容_label", "")
                            office = d.get("事業所", "")
                            founded = d.get("設立", "")
                            hp = d.get("ホームページ", "")
                            pub_update = d.get("掲載更新日", "")
                            pub_end = d.get("掲載終了日", "")
                            if d.get("雇用形態"):
                                employ_type = d["雇用形態"]
                            if d.get("年収範囲"):
                                salary_range = d["年収範囲"]
                            # 本社所在地から 郵便番号 / 都道府県 / 住所 を上書き取得（詳細が正）。
                            honsha = d.get("本社所在地", "")
                            if honsha:
                                pc = _POSTCODE_PATTERN.search(honsha)
                                if pc:
                                    post_code = pc.group(1)
                                    honsha_addr = _POSTCODE_PATTERN.sub("", honsha)
                                else:
                                    honsha_addr = honsha
                                d_pref, d_addr = _parse_location(honsha_addr)
                                if d_pref:
                                    pref, addr = d_pref, d_addr

                    row = {
                        Schema.NAME: name,
                        Schema.CAT_SITE: cat_site,
                        "業種": gyoshu,
                        "職種": shokushu,
                        "雇用形態": employ_type,
                        "年収範囲": salary_range,
                        "勤務地": work_location,
                        Schema.REP_NM: rep,
                        Schema.EMP_NUM: emp,
                        Schema.CAP: cap,
                        Schema.SALES: sales,
                        Schema.LOB: lob,
                        "事業所": office,
                        Schema.OPEN_DATE: founded,
                        Schema.HP: hp,
                        Schema.PREF: pref,
                        Schema.POST_CODE: post_code,
                        Schema.ADDR: addr,
                        "掲載更新日": pub_update,
                        "掲載終了日": pub_end,
                        Schema.URL: detail_url,
                    }

                    yield row
                    yielded += 1
                except Exception as e:
                    self.logger.warning(f"page {page_no}: item skip — {e}")
                    continue

            self.logger.info("page %d done (累計 %d 件)", page_no, yielded)

            next_a = soup.select_one(".pager li.next a")
            if not next_a:
                break
            next_href = next_a.get("href", "")
            if not next_href:
                break
            # ★ next は「?...&offset=N」形式の相対 query。ルート url から派生させる。
            page_url = urljoin(page_url, next_href)
            page_no += 1
            time.sleep(self.DELAY)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if not os.environ.get("RE30_PROXY"):
        logging.warning(
            "RE30_PROXY 未設定。データセンタ IP から実行すると WAF で 0 件になります。"
        )

    scraper = Re30Crawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://re-katsu30.jp/search/result?income%5B0%5D=&income%5B1%5D=&btn_search=1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
