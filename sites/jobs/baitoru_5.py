"""
---------------------------------------------------------------------------
ver 1.0.0 20260701 新規作成（バイトル関西）。
                   - baitoru.com の地域別求人一覧を「関西エリアに限定」して網羅収集。
                   - parse() 引数 url（=https://www.baitoru.com/kansai/jlist/）を
                     唯一のルートとし、配信元(origin)・地域スラッグ・サイトマップ・
                     各ページURLをすべて url から派生させる（URL一貫性ルール準拠）。
                   - sitemap_ba_area.xml の「葉(leaf)」エリア一覧のうち、url の
                     地域(kansai)配下の市区町村粒度のみを巡回起点に採用。各エリアの
                     ページ送りはその総件数を完全にカバーするため、関西の全求人を
                     取りこぼさず収集できる。
                   - 重複排除キーは求人詳細URL（1求人=1行）。
ver 1.1.0 20260701 「工場だけ」に絞り込み（追加指示）。
                   - クロール対象を工場(production)カテゴリのみへ限定。
ver 1.2.0 20260703 取りこぼし修正（追加指示：234,929件あるはずが約6,046件しか
                   取れていない）。
                   - 【真因】baitoru はどの一覧でもページ送りが page400 で頭打ち
                     （約20件/頁＝1リストあたり最大約8,000件しか到達不可）。旧版は
                     単一の一覧をそのままページ送りしていたため、総数の大部分に
                     到達できず約6,000件で頭打ちになっていた（「最初のページ付近
                     でしか取れない」の正体はこのページ送り上限）。
                   - 【方針】関西全域（=引数 url の /kansai/jlist/ 全カテゴリ、
                     総数234,929件）を対象に戻し、工場(production)限定を解除。
                   - sitemap_ba_area.xml の「葉(leaf)」エリア（市区町村・区の最深
                     粒度）へ分割し、各エリアを個別にページ送りで巡回。各葉エリアは
                     概ね8,000件未満に収まるため page400 上限に阻まれず全件到達できる。
                   - サイトマップURL・エリアURL・ページURLはすべて引数 url から派生
                     （URL一貫性ルール準拠：ルートURL自体は変更しない）。
                   - 重複排除キーは求人詳細URL（1求人=1行）。
ver 1.3.0 20260703 求人明細カラムの追加取得（追加指示）。
                   - 会社情報に加え、求人詳細の基本情報セクション
                     （div.detail-basicInfo）から「職種／給与／勤務時間／勤務地／
                     仕事内容」を dt/dd 照合で抽出し EXTRA カラムへ格納する。
                   - 値は dd テキスト。UI用の「もっと見る」等の展開ボタン文言は除去。
                   - basicInfo が見つからない場合はページ全体の dl をフォールバック
                     対象にする（構造差異への耐性）。
                   - URL一貫性・エリア分割巡回・重複排除方針は 1.2.0 のまま。
ver 1.4.0 20260703 フリーワード「工場」で絞り込み（追加指示：フリーワードに
                   「工場」を入れると 14,856件ヒットする、このぶんだけ取得したい）。
                   - baitoru のフリーワード検索はクエリ(?fw= 等)ではなくパス
                     セグメント「wrd<キーワード>/」で表現される（検索フォーム→
                     /noscreen/createurl/ が生成する正規URLで確認）。例:
                     /kansai/jlist/wrd工場/ = 14,856件。
                   - このセグメントは市区町村エリアのパスにも合成でき
                     （例 /kansai/jlist/osaka/osakashi/wrd工場/ = 1,951件）、
                     page送りは /…/wrd工場/pageN/ で辿れる。
                   - 【page400 上限対策の維持】単一の /kansai/jlist/wrd工場/ を
                     20件/頁で辿ると約743頁必要だが baitoru は page400 で頭打ち
                     （≒8,000件）のため約半分を取りこぼす。そこで 1.2.0 以来の
                     葉(leaf)エリア分割巡回はそのまま維持し、各エリアURLに
                     フリーワードセグメントを付与して巡回する（各エリアは
                     page400 上限内に収まる）。総和が関西の工場求人 14,856件。
                   - URL一貫性ルール準拠：ルートURL(/kansai/jlist/)自体は変更せず、
                     フリーワードURL・エリアURL・ページURLはすべて引数 url から派生。
                   - 重複排除キーは求人詳細URL（1求人=1行）。
ver 1.5.0 20260706 応募電話ボタンの電話番号も取得（追加指示）。
                   - 求人詳細の「電話番号を表示する」ボタン(a.tel-entry)には
                     data-obo_tel 属性に募集先の電話番号（例: 0120-340-104）が
                     埋め込まれている。会社情報(pt03)側で TEL が取得できなかった
                     場合はこの data-obo_tel を Schema.TEL に採用する。
                   - 値は末尾に空白パディングを含むため _clean で整形する。
                   - URL一貫性・エリア分割巡回・重複排除方針は 1.4.0 のまま。
ver 1.6.0 20260706 絞り込みをフリーワードから職種カテゴリへ変更（追加指示：
                   /kansai/jlist/factory-productionetc で取得したい。ただし
                   バイトル側の都合で最大約1万件までしか取得できない）。
                   - baitoru の職種絞り込みはパスセグメント
                     「factory-productionetc/」（工場・製造・軽作業カテゴリ）で
                     表現される。ルート/エリアのURL末尾に付与するとこのカテゴリの
                     求人だけの一覧になる（例: /kansai/jlist/factory-productionetc/）。
                   - フリーワード(wrd工場/)による絞り込みは廃止し、カテゴリ
                     セグメントに置き換える。
                   - page400 上限対策としての葉(leaf)エリア分割巡回はそのまま維持し、
                     各エリアURLにカテゴリセグメントを付与して巡回する。
                   - URL一貫性ルール準拠：ルートURL(/kansai/jlist/)自体は変更せず、
                     カテゴリURL・エリアURL・ページURLはすべて引数 url から派生。
                   - 重複排除キーは求人詳細URL（1求人=1行）。
ver 1.7.0 20260708 約6,900件で頭打ちする不具合を修正（追加指示）。
                   - 【真因1】_scrape_list が「そのページに新規求人が1件も無い」時点で
                     即 break していた。seen_jobs を全エリアで共有しているため、派遣
                     求人（同一jobを複数エリアへ重複掲載）が多い当カテゴリでは、後続
                     エリアの先頭ページが既取得だけで埋まり、そのエリアの奥ページにある
                     固有求人へ到達する前に打ち切られていた。
                   - 【真因2】巡回起点を「葉(leaf)エリア」に限定していたため、勤務地が
                     市区町村まで特定されず府県止まりで登録された求人は、どの葉一覧にも
                     現れず原理的に取得不可だった。両者が重なり、関西合計10,218件のうち
                     約6,900件で頭打ちしていた。
                   - 【対策1】ページ送り終了判定を「新規ゼロ」ではなく「求人ゼロ、または
                     前ページと同一の求人集合（ページ送り上限クランプ）」に変更。重複は
                     yield せず巡回のみ継続し、各リストを実際の末尾まで辿る。
                   - 【対策2】巡回起点を府県粒度へ変更（例 /kansai/jlist/osaka/）。関西の
                     各府県は工場カテゴリでも8,000件未満（page400上限内）に収まるため
                     全件到達できる（実測 osaka=4,013, hyogo=2,595 等）。万一ある粒度で
                     page400 上限に達した場合は、その配下の子エリアへ自動で降りて残りを
                     回収する（適応的分割）。
                   - URL一貫性・重複排除(1求人=1行)方針は従来どおり。
---------------------------------------------------------------------------
"""

import re
import sys
import urllib.request
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

# 巡回上限ページ数。baitoru はどの一覧でもページ送りが page400 で頭打ちになる
# （それ以降は表示されない）ため、1エリアあたりの上限を実サイトの上限に合わせる。
MAX_PAGES = 400

# エリア一覧サイトマップのファイル名。origin 配下に付与して取得する。
AREA_SITEMAP = "sitemap_ba_area.xml"

# 求人詳細ページのURL（…/job123456/）にマッチ。応募フォーム(/entry/)は除外する。
_JOB_DETAIL_RE = re.compile(r"/job\d+/?$")

# 追加指示: 職種カテゴリ絞り込みセグメント。baitoru の職種絞り込みはクエリではなく
# パスセグメントで表現される。ルートやエリアのURL末尾にこのセグメントを付与すると
# 工場・製造・軽作業カテゴリの求人だけの一覧になる
# （例: /kansai/jlist/factory-productionetc/）。
# ※baitoru 側の都合でこのカテゴリの取得上限は約1万件。
CATEGORY_SEG = "factory-productionetc/"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_area(u: str) -> str:
    """エリア一覧URLを正規化（フラグメント/クエリ除去・末尾スラッシュ付与）。"""
    u = (u or "").split("#")[0].split("?")[0].strip()
    if u and not u.endswith("/"):
        u += "/"
    return u


class Baitoru5Scraper(DynamicCrawler):
    """バイトル関西 求人スクレイパー（baitoru.com /kansai/jlist/）

    引数 url（=…/kansai/jlist/）を唯一のルートとし、sitemap_ba_area.xml から
    その配下のエリア（府県／市区町村／区）を抽出、各エリアに工場・製造・軽作業
    カテゴリセグメント(factory-productionetc/)を付与して個別にページ送り巡回する。
    単一の /kansai/jlist/factory-productionetc/ をそのまま辿ると baitoru のページ
    送り上限(page400≒8,000件)に阻まれて取りこぼすため、エリア分割で各リストを
    上限内に収める。

    巡回起点は府県粒度（region_prefix の直下1階層。例 /kansai/jlist/osaka/）。
    関西の各府県は工場カテゴリでも 8,000件未満に収まり page400 上限内で全件到達
    できる。万一ある粒度で page400 上限に達した（＝取りこぼしの疑い）場合のみ、
    その配下の子エリアへ自動で降りて残りを回収する（適応的分割）。府県粒度で巡回
    するため、勤務地が市区町村まで特定されない「府県止まり」求人も取りこぼさない。

    ページ送りの終了は「求人ゼロ、または前ページと同一の求人集合（上限クランプ）」
    で判定し、既取得（重複）求人が並ぶページでも巡回自体は末尾まで継続する。
    各求人詳細から企業情報を抽出し、求人詳細URLを重複排除キー(1求人=1行)にする。
    """

    DELAY = 1.0
    # 会社情報（Schema.NAME 等）に加え、求人明細の基本情報カラムを EXTRA へ格納する。
    # 職種/給与/勤務時間/勤務地/仕事内容 は求人ごとの自由記述を含むが、追加指示により
    # 必要カラムとして取得する。派遣/紹介の許可番号は構造化ラベル。
    EXTRA_COLUMNS = [
        "求人タイトル", "職種", "給与", "勤務時間", "勤務地", "仕事内容",
        "派遣許可番号", "有料職業紹介事業許可番号",
    ]

    # 求人明細の基本情報セクションから取得するラベル（サイト定義ラベル＝出力カラム名）。
    # 「勤務時間」を「勤務地」より先に並べ、接頭辞一致の取り違えを防ぐ。
    _BASIC_LABELS = ["職種", "給与", "勤務時間", "勤務地", "仕事内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # URL一貫性ルール: 引数 url を唯一のルートとし、配信元・サイトマップ・
        # 各エリア/ページURLをすべて url から派生させる（ルートURL自体は変えない）。
        self.root = _norm_area(url)
        parsed = urlparse(self.root)
        self.origin = f"{parsed.scheme}://{parsed.netloc}"
        # ルートのパス接頭辞（例: /kansai/jlist/）。この配下のエリアのみ巡回対象。
        self.region_prefix = parsed.path if parsed.path.endswith("/") else parsed.path + "/"

        seen_jobs: set[str] = set()  # 訪問済み求人詳細URL（重複排除キー・1求人=1行）

        # ── 工場カテゴリ絞り込み + 取りこぼし対策 ──────────────────────────
        # 職種カテゴリ絞り込みはパスセグメント factory-productionetc/ で表現される。
        # ルート/エリアのURL末尾に付与すると工場・製造・軽作業カテゴリの求人だけの
        # 一覧になる。ただし baitoru はどの一覧でもページ送りが page400 で頭打ち
        # （約20件/頁≒8,000件）のため、sitemap_ba_area.xml のエリアへ分割して巡回する。
        # 巡回起点は府県粒度（region_prefix 直下1階層）。関西の各府県は工場カテゴリでも
        # 8,000件未満に収まり page400 上限内で全件到達できる。府県粒度で辿ることで、
        # 勤務地が市区町村まで特定されない「府県止まり」求人も取りこぼさない。
        self.area_paths = self._fetch_area_paths()  # region_prefix 配下の全階層エリア
        top_areas = self._child_areas(self.region_prefix)  # 府県粒度（直下1階層）
        if top_areas:
            self.logger.info("巡回対象の府県エリア数: %d件（root=%s, category=%s）",
                             len(top_areas), self.root, CATEGORY_SEG)
            for area_path in top_areas:
                yield from self._crawl_area(area_path, seen_jobs)
        else:
            # サイトマップ取得失敗時のフォールバック（少なくとも先頭～page400は巡回）。
            cat_root = self.root + CATEGORY_SEG
            self.logger.warning("サイトマップ取得に失敗。ルートのカテゴリ一覧のみ巡回します: %s", cat_root)
            yield from self._scrape_list(cat_root, "", seen_jobs)

        self.logger.info("収集求人数: %d件", len(seen_jobs))

    # ------------------------------------------------------------------ #
    # エリア巡回（適応的分割）: 上限クランプ検知時のみ子エリアへ降りる
    # ------------------------------------------------------------------ #
    def _crawl_area(self, area_path: str, seen_jobs: set) -> Generator[dict, None, None]:
        """1エリアを巡回する。page400 上限に達した（取りこぼしの疑い）場合のみ、
        その配下の子エリア（1階層深い粒度）へ降りて残りを回収する。

        通常は府県粒度で page400 上限内に収まるため子への降下は発生しない。将来の
        件数増や粒度差に備えた安全弁として適応的に分割する。エリアURL末尾には工場
        カテゴリセグメントを付与する（URLは引数 url 由来）。
        """
        cat_area = self.origin + area_path + CATEGORY_SEG
        self.logger.info("エリア巡回開始: %s", cat_area)
        capped = yield from self._scrape_list(cat_area, "", seen_jobs)
        if capped:
            children = self._child_areas(area_path)
            if children:
                self.logger.info(
                    "page400上限に到達。子エリアへ分割して残りを回収: %s（子%d件）",
                    area_path, len(children),
                )
                for child in children:
                    yield from self._crawl_area(child, seen_jobs)
            else:
                self.logger.warning(
                    "page400上限に到達したが子エリアが無く、これ以上分割できません: %s",
                    area_path,
                )

    # ------------------------------------------------------------------ #
    # サイトマップから region_prefix 配下の全階層エリアパスを取得
    # ------------------------------------------------------------------ #
    def _fetch_area_paths(self) -> set[str]:
        """sitemap_ba_area.xml を取得し、root 配下(region_prefix)の全階層エリアの
        パス集合を返す（末尾スラッシュ付き。例 "/kansai/jlist/osaka/osakashi/"）。

        サイトマップに載る各エリアについて、region_prefix 直下から当該エリアまでの
        中間の全祖先パスも合成して追加する（例: 区までしか載っていなくても、府県・市の
        粒度のノードを補完する）。これにより府県→市→区の適応的分割が可能になる。
        エリアURL・サイトマップURLはいずれも引数 url 由来の origin から派生させる。
        """
        sitemap_url = urljoin(self.origin + "/", AREA_SITEMAP)
        try:
            req = urllib.request.Request(
                sitemap_url, headers={"User-Agent": self.USER_AGENT}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                xml = resp.read().decode("utf-8", errors="ignore")
        except Exception as e:
            self.logger.warning("サイトマップ取得失敗: %s — %s", sitemap_url, e)
            return set()

        # <loc> を名前空間非依存で抽出し、root 配下（region_prefix より深い）だけに絞る。
        host = urlparse(self.origin).netloc
        paths: set[str] = set()
        for loc in re.findall(r"<loc>\s*(.*?)\s*</loc>", xml, flags=re.IGNORECASE | re.DOTALL):
            loc = _norm_area(loc)
            p = urlparse(loc)
            if p.netloc and p.netloc != host:
                continue
            path = p.path if p.path.endswith("/") else p.path + "/"
            if not (path.startswith(self.region_prefix) and path != self.region_prefix):
                continue
            # region_prefix 直下から当該エリアまでの全祖先パスを合成して追加。
            segs = path[len(self.region_prefix):].strip("/").split("/")
            for i in range(1, len(segs) + 1):
                paths.add(self.region_prefix + "/".join(segs[:i]) + "/")
        return paths

    def _child_areas(self, parent_path: str) -> list[str]:
        """area_paths のうち parent_path のちょうど1階層深い子エリアのパスを返す。"""
        parent = parent_path if parent_path.endswith("/") else parent_path + "/"
        children = []
        for path in self.area_paths:
            if not path.startswith(parent) or path == parent:
                continue
            rest = path[len(parent):].strip("/")
            if rest and "/" not in rest:  # ちょうど1階層深い
                children.append(path)
        return sorted(children)

    # ------------------------------------------------------------------ #
    # 一覧ページのページネーション巡回
    # ------------------------------------------------------------------ #
    def _scrape_list(self, base: str, pref_ja: str,
                     seen_jobs: set) -> Generator[dict, bool, bool]:
        """一覧をページ末尾まで巡回する。戻り値は「page400 上限に達して打ち切った
        （＝取りこぼしの疑いがある）」場合 True、正常に末尾まで到達した場合 False。

        終了判定は「求人ゼロ、または前ページと同一の求人集合（ページ送り上限クランプ）」。
        既取得（重複）求人だけのページでも巡回自体は継続し、そのエリア固有の求人へ確実に
        到達する（旧版の『新規ゼロで即 break』による取りこぼしを解消）。
        """
        prev_urls: set[str] | None = None
        page_no = 1
        while page_no <= MAX_PAGES:
            list_url = base if page_no == 1 else f"{base}page{page_no}/"
            try:
                self.page.goto(list_url, wait_until="domcontentloaded")
                self.page.wait_for_selector("a[href*='/job']", timeout=8000)
            except Exception:
                return False  # ページ無し or 取得失敗 → このエリアは末尾まで到達済み

            page_url = self.page.url
            soup = BeautifulSoup(self.page.content(), "html.parser")

            page_urls = self._page_job_urls(soup, page_url)
            cur_set = set(page_urls)
            if not cur_set:
                return False  # 求人ゼロ → 完結
            if prev_urls is not None and cur_set == prev_urls:
                return False  # 前ページと同一集合（上限クランプで先へ進めない）→ 完結
            prev_urls = cur_set

            for job_url in page_urls:
                if job_url in seen_jobs:
                    continue  # 重複は yield しないが巡回は継続
                seen_jobs.add(job_url)

                item = self._scrape_detail(job_url, pref_ja)
                if not item or not item.get(Schema.NAME):
                    continue
                yield item  # 1求人取得ごとに即 yield（全件バッファ禁止）

            page_no += 1

        # MAX_PAGES(=page400)まで回してもクランプ/ゼロで終わらなかった＝上限に阻まれ
        # まだ続く可能性がある。取りこぼしの疑いがあるため True を返し、呼び出し側で
        # 子エリアへ分割して残りを回収する。
        return True

    @staticmethod
    def _page_job_urls(soup, page_url: str) -> list[str]:
        """一覧ページから求人詳細URL(…/jobNNN/)を抽出（ページ内重複排除）。"""
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href*='/job']"):
            href = (a.get("href", "") or "").split("?")[0].split("#")[0]
            if "/entry/" in href:
                continue
            if not _JOB_DETAIL_RE.search(href):
                continue
            full = urljoin(page_url, href).rstrip("/") + "/"
            if full not in seen:
                seen.add(full)
                urls.append(full)
        return urls

    # ------------------------------------------------------------------ #
    # 求人明細の基本情報（職種/給与/勤務時間/勤務地/仕事内容）を抽出
    # ------------------------------------------------------------------ #
    def _extract_job_basics(self, soup, data: dict) -> None:
        """div.detail-basicInfo の dt/dd から求人明細の基本情報を抽出する。

        ラベル(dt)が _BASIC_LABELS のいずれかで始まる dl を対象に、対応する値(dd)を
        同名の EXTRA カラムへ格納する。値の「もっと見る」等の展開ボタン文言は除去する。
        basicInfo が見つからない環境ではページ全体の dl をフォールバック対象にする。
        文書順で走査するため、外側のセクション dl（例:勤務地）が先に確定し、内側の
        入れ子 dl（勤務先/最寄駅/住所 等）は既取得キーとして無視される。
        """
        scope = soup.select_one("div.detail-basicInfo") or soup
        for dl in scope.find_all("dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)
            for key in self._BASIC_LABELS:
                if key in data:
                    continue  # 既に取得済み（外側 dl 優先）
                if label.startswith(key):
                    val = _clean(dd.get_text(" ").replace("もっと見る", " "))
                    if val:
                        data[key] = val
                    break

    # ------------------------------------------------------------------ #
    # 求人詳細ページから企業情報を抽出
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        try:
            self.page.goto(url, wait_until="domcontentloaded")
            self.page.wait_for_selector("div.detail-companyInfo", timeout=10000)
        except Exception:
            return None
        soup = BeautifulSoup(self.page.content(), "html.parser")

        # 重複排除キー兼出力URLは求人詳細URL（1求人=1行）。
        data = {Schema.URL: url, Schema.PREF: pref_ja}

        # 求人タイトル（h1）。短い見出しなので EXTRA に採用。
        h1 = soup.select_one("h1")
        if h1:
            data["求人タイトル"] = _clean(h1.get_text())

        # 求人明細の基本情報（職種/給与/勤務時間/勤務地/仕事内容）を抽出。
        self._extract_job_basics(soup, data)

        company_info = soup.find("div", class_="detail-companyInfo")
        if company_info:
            pt02 = company_info.find("div", class_="pt02")
            if pt02:
                p = pt02.find("p")
                if p:
                    a = p.find("a")
                    data[Schema.NAME] = _clean(a.get_text() if a else p.get_text())

            pt03 = company_info.find("div", class_="pt03")
            if pt03:
                for dl in pt03.find_all("dl"):
                    dt = dl.find("dt")
                    dd = dl.find("dd")
                    if not dt or not dd:
                        continue
                    key = dt.get_text(strip=True)
                    val = _clean(dd.get_text(" "))
                    if "所在地" in key:
                        tel_match = re.search(
                            r"(TEL|ＴＥＬ|電話)[番号]*[：:\s　]*([\d\-（）()０-９ー‐]+)",
                            val,
                            flags=re.IGNORECASE,
                        )
                        if tel_match and not data.get(Schema.TEL):
                            data[Schema.TEL] = tel_match.group(2).strip()
                        addr = re.sub(
                            r"[\s　]*(TEL|FAX|ＴＥＬ|ＦＡＸ|電話|Fax)[番号]*[：:\s　]*[\d\-（）()０-９ー‐]+",
                            "",
                            val,
                            flags=re.IGNORECASE,
                        ).strip()
                        data[Schema.ADDR] = addr
                        pref_match = re.match(
                            r"(北海道|東京都|京都府|大阪府|.{2,3}[都道府県])", addr
                        )
                        if pref_match:
                            data[Schema.PREF] = pref_match.group(1)
                    elif "代表電話番号" in key or "電話番号" in key:
                        data[Schema.TEL] = val
                    elif "代表者" in key:
                        data[Schema.REP_NM] = val
                    elif "事業内容" in key:
                        # 短い業種の箇条書き（自由記述プロースではない）→ Schema.LOB
                        data[Schema.LOB] = val
                    elif "ホームページ" in key or "URL" in key:
                        a = dd.find("a", href=True)
                        data[Schema.HP] = a["href"] if a else val
                    elif "派遣許可番号" in key:
                        data["派遣許可番号"] = val
                    elif "有料職業紹介事業許可番号" in key:
                        data["有料職業紹介事業許可番号"] = val
                    # 「拠点」「応募プロセス」等は自由記述プロースのため取得しない。

        # 追加指示: 「電話番号を表示する」ボタン(a.tel-entry)の data-obo_tel 属性にも
        # 募集先の電話番号（例: 0120-340-104）が埋め込まれている。会社情報側で TEL が
        # 取得できなかった場合はこれを採用する（値は末尾空白パディングを含むため整形）。
        if not data.get(Schema.TEL):
            tel_entry = soup.select_one("a.tel-entry[data-obo_tel]")
            if tel_entry:
                obo_tel = _clean(tel_entry.get("data-obo_tel"))
                if obo_tel:
                    data[Schema.TEL] = obo_tel

        if not data.get(Schema.NAME) and h1:
            data[Schema.NAME] = _clean(h1.get_text())

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    # ローカル実行とコンテナ実行を一致させるため、必ず正規ルートURL（sites.yml の
    # url と同一）を渡す。
    Baitoru5Scraper().execute("https://www.baitoru.com/kansai/jlist/")
