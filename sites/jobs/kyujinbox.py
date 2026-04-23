# sites/jobs/kyujinbox.py
"""
求人ボックス — 営業代行系キーワード企業スクレイパー

対象サイト: https://xn--pckua2a7gp15o89zb.com/ (求人ボックス)
対象キーワード:
    - 携帯販売
    - 通信
    - モバイルショップ運営
    - 外勤営業
    - 訪問販売

取得フロー:
    各キーワードで /{keyword}の仕事 を GET
    → 一覧ページ (.p-result_card / .p-result_wrap) から基本情報と jbId を抽出
    → 詳細ページ /jb/{jbId} から会社情報（本社住所・代表者・資本金・従業員数など）を取得
    → ?pg=N でページネーション、同一キーワード内で会社名重複をスキップ

取得フィールド:
    Schema.NAME      = 会社名
    Schema.ADDR      = 本社所在地（詳細ページの【本社所在地】。取得不可時は一覧のエリア）
    Schema.REP_NM    = 代表者名（【代表者】）
    Schema.EMP_NUM   = 従業員数（【従業員数】）
    Schema.CAP       = 資本金（【資本金】）
    Schema.OPEN_DATE = 設立年月日（【設立】）
    Schema.URL       = 求人URL（/jb/{jbId}）
    EXTRA: エリア / 求人タイトル / 給与 / 雇用形態 / 検索キーワード / 更新日 / 条件タグ / 事業内容

実行方法:
    python sites/jobs/kyujinbox.py              # 動作確認（CSV出力なし）
    python sites/jobs/kyujinbox.py --local      # 100件 → sites/output/ に保存
    python sites/jobs/kyujinbox.py --full       # 全件 → NAS に保存
    python sites/jobs/kyujinbox.py --full --local  # 全件 → sites/output/ に保存
"""

import csv
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator
from urllib.parse import quote

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL  = "https://xn--pckua2a7gp15o89zb.com"
START_URL = BASE_URL  # execute() に渡すダミーURL（実際のURLは内部で構築）

# (検索語, CSVに記録するラベル)
KEYWORDS: list[tuple[str, str]] = [
    ("携帯販売",          "携帯販売"),
    ("通信",              "通信"),
    ("モバイルショップ運営", "モバイルショップ運営"),
    ("外勤営業",          "外勤営業"),
    ("訪問販売",          "訪問販売"),
]

PREFECTURES: list[str] = [
    # 優先エリア
    "東京都", "京都府", "大阪府", "愛知県",
    # 一都三県（残り）
    "神奈川県", "埼玉県", "千葉県",
    # 関東（残り）
    "茨城県", "栃木県", "群馬県",
    # 以降は元の順序
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "三重県",
    "滋賀県", "兵庫県", "奈良県",
    "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# ── リトライ設定 ─────────────────────────────────────────────────────────────
# 詳細ページ 1件あたり:  エラー → 10秒待機 → 1回リトライ → 失敗なら次へ
# 10件連続失敗時:       60秒待機 × 最大5回 リトライ
# 5回リトライも全滅時:   1時間待機 → 次へ進む（連続カウントリセット）
# 404 の場合:          リトライなし・即次へ（連続カウントに含めない）
_DETAIL_RETRY_WAIT   = 10   # 詳細ページ 1件リトライ待機（秒）
_CONSEC_FAIL_LIMIT   = 10   # 連続失敗しきい値（件）
_CONSEC_RETRY_WAIT   = 60   # 連続失敗後リトライ待機（秒）
_CONSEC_RETRY_MAX    = 5    # 連続失敗後リトライ回数
_BLOCK_WAIT          = 3600 # 全リトライ失敗後の待機時間（秒）= 1時間
# 一覧ページ: エラー → 10秒待機 → 最大3回リトライ
_LIST_RETRY_WAIT     = 10   # 一覧ページリトライ待機（秒）
_LIST_MAX_RETRIES    = 3    # 一覧ページ最大リトライ回数


def _clean(s) -> str:
    """空白・全角スペースを正規化して文字列を返す。None は空文字に変換。"""
    if s is None:
        return ""
    return re.sub(r"[\s\u3000]+", " ", str(s)).strip()


def _is_complete_address(addr: str) -> bool:
    """番地相当の数字（半角・全角）が含まれる場合を「完成した住所」と判定する。"""
    return bool(re.search(r"[0-9０-９]", addr))


def _extract_postal_code(addr: str) -> tuple[str, str]:
    """住所冒頭の〒XXX-XXXX を抽出し、(郵便番号, 残りの住所) を返す。
    郵便番号がない場合は ("", addr) を返す。
    """
    m = re.match(r"^(〒\d{3}-\d{4})\s*(.*)", addr)
    if m:
        return m.group(1), m.group(2).strip()
    return "", addr


_PREF_RE = re.compile(r"^(?:〒\d{3}-\d{4}\s*)?(?:東京都|北海道|京都府|大阪府|\S{2,3}[都道府県])")


def _extract_address_line(text: str) -> str:
    """複数行テキストから住所行を1行だけ抽出する。
    優先順位:
      1. 「住所：」「住所:」プレフィックス付きの行
      2. 都道府県（または〒）で始まり番地の数字を含む行
    """
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"住所[：:]\s*(.+)", line)
        if m:
            return m.group(1).strip()
        if _PREF_RE.match(line) and re.search(r"[0-9０-９]", line):
            return line
    return ""


def _parse_tags(tag_el) -> str:
    """条件タグ要素のテキストを '/' 区切りの文字列にまとめる。"""
    if tag_el is None:
        return ""
    return " / ".join(
        t for t in (s.strip() for s in tag_el.get_text("\n").splitlines()) if t
    )


def _extract_jb_id(href: str) -> str:
    """
    href から求人ボックス内部の jbId を抽出する。
    - /jb/{id} 形式: id をそのまま返す
    - /rd/?uaid={id}&... 形式: uaid の値を返す
    """
    m = re.search(r"/jb/([a-f0-9]+)", href)
    if m:
        return m.group(1)
    m = re.search(r"uaid=([a-zA-Z0-9]+)", href)
    if m:
        return m.group(1)
    return ""


class _ScrapingError(Exception):
    """ネットワーク障害など、リトライすべき一時的な取得失敗。"""


class _NotFoundError(_ScrapingError):
    """HTTP 404: ページが存在しない。リトライ不要。"""


def _parse_company_panel(text: str) -> dict:
    """
    【キー】値\n【キー2】値2... 形式のテキストを dict に変換する。
    改行で続く行は前のキーの値として連結する。
    """
    result: dict[str, str] = {}
    current_key: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        m = re.match(r"【(.+?)】(.*)", line)
        if m:
            current_key = m.group(1)
            result[current_key] = m.group(2).strip()
        elif current_key:
            result[current_key] = (result.get(current_key, "") + " " + line).strip()
    return result


def _extract_job_content(text: str) -> str:
    """【仕事内容】または【業務内容】から次の【までのテキストを抽出する。"""
    m = re.search(r"【(?:仕事内容|業務内容)】\s*(.*?)(?=\s*【|\Z)", text, re.DOTALL)
    if m:
        return re.sub(r"[\s\u3000]+", " ", m.group(1)).strip()
    return ""


class KyujinboxScraper(StaticCrawler):
    """
    求人ボックスから携帯販売・通信系キーワードで企業情報を収集するクローラー。

    全カードに対して詳細ページ(/jb/{jbId})を訪問し、
    本社所在地・代表者・資本金・従業員数・事業内容などを取得する。

    レジューム対応: 実行日と同名の CSV が存在する場合、取得済みURLをスキップして再開。
    リトライ対応（詳細ページ）:
        エラー → 10秒待機 → 1回リトライ → 失敗なら次へ（連続カウント+1）
        10件連続失敗 → 60秒待機 × 最大5回リトライ
        5回全失敗 → 1時間待機 → 次へ（連続カウントリセット）
        404 → 即次へ（連続カウントに含めない）
    """

    DELAY = 0.8  # ページ間の待機時間（秒）

    EXTRA_COLUMNS = [
        "エリア",
        "求人タイトル",
        "給与",
        "雇用形態",
        "検索キーワード",
        "都道府県",
        "更新日",
        "条件タグ",
        "仕事内容",
    ]

    _max_items: int | None = None  # None = 全件、整数 = 上限件数

    # -------------------------------------------------------------------------
    # フック: 前処理
    # -------------------------------------------------------------------------

    def prepare(self):
        """
        1. --local フラグ: CSV出力先を sites/output/ に変更する。
        2. レジューム: 実行日と同名の CSV が存在する場合、取得済みURLを読み込んで再開。
        """
        import sys as _sys

        # ── --local フラグ処理 ──────────────────────────────────
        if "--local" in _sys.argv:
            local_dir = Path(__file__).resolve().parent.parent / "output"
            local_dir.mkdir(parents=True, exist_ok=True)
            self.pipeline._output_dir = local_dir
            self.logger.info("ローカル出力モード: %s", local_dir)

        # ── レジューム処理 ──────────────────────────────────────
        self._done_urls: set[str] = set()

        today = datetime.now().strftime("%Y%m%d")
        out_dir = Path(self.pipeline._output_dir)
        existing = next((f for f in out_dir.glob(f"{today}_kyujinbox*.csv")), None)

        if not existing:
            self.logger.info("既存CSV未発見 → 新規取得開始")
            return

        self.logger.info("既存CSV発見 → レジュームモード: %s", existing.name)
        with open(existing, encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                url = row.get(Schema.URL, "")
                if url:
                    self._done_urls.add(url)
                self.pipeline.process_item(dict(row))

        self.logger.info("再開: %d 件スキップ", len(self._done_urls))

    # -------------------------------------------------------------------------
    # メインロジック
    # -------------------------------------------------------------------------

    def parse(self, url: str) -> Generator[dict, None, None]:
        """全キーワード × 全都道府県を順番に検索し、企業情報を yield する。"""
        consec_fails = 0

        for pref in PREFECTURES:
            for search_term, label in KEYWORDS:
                pref_label = f"{label} {pref}"
                self.logger.info("=== キーワード: %s ===", pref_label)
                search_base = f"{BASE_URL}/{quote(search_term + 'の仕事-' + pref)}"
                seen_companies: set[str] = set()
                page = 1

                while True:
                    page_url = search_base + (f"?pg={page}" if page > 1 else "")
                    self.logger.debug("一覧ページ取得: %s", page_url)

                    list_soup, failed = self._fetch_page(page_url)

                    if failed:
                        self.logger.error("一覧ページ取得断念 → 次のキーワードへ: %s", page_url)
                        break

                    consec_fails = 0

                    if list_soup is None:
                        self.logger.info("キーワード '%s' 終了 (page=%d)", pref_label, page)
                        break

                    cards = list_soup.select(".p-result_card, .p-result_wrap")
                    if not cards:
                        self.logger.info("キーワード '%s' 終了 (page=%d, カード0件)", pref_label, page)
                        break

                    for card in cards:
                        # ── 会社名 ──────────────────────────────────
                        company_el = card.select_one(".p-result_company")
                        if not company_el:
                            continue
                        company = _clean(company_el.get_text())
                        if not company:
                            continue

                        # 同一キーワード×都道府県内の重複スキップ
                        if company in seen_companies:
                            continue
                        seen_companies.add(company)

                        # ── jbId → 詳細URL ──────────────────────────
                        link = card.select_one("h2 a, .p-result_title_link, a[href]")
                        href = link.get("href", "") if link else ""
                        jb_id = _extract_jb_id(href)
                        jb_url = f"{BASE_URL}/jb/{jb_id}" if jb_id else ""

                        # レジューム: 取得済みURLはスキップ
                        if jb_url and jb_url in self._done_urls:
                            continue

                        # ── 一覧ページからの基本情報 ─────────────────
                        area_el  = card.select_one(".p-result_area")
                        pay_el   = card.select_one(".p-result_pay")
                        etype_el = card.select_one(".p-result_employType")
                        title_el = card.select_one(".p-result_name")
                        updat_el = card.select_one(".p-result_updatedAt_hyphen")
                        tag_el   = card.select_one(".p-result_tag")

                        list_area  = _clean(area_el.get_text())  if area_el  else ""
                        list_pay   = _clean(pay_el.get_text())   if pay_el   else ""
                        list_etype = _clean(etype_el.get_text()) if etype_el else ""
                        list_title = _clean(title_el.get_text()) if title_el else ""
                        list_updat = _clean(updat_el.get_text()) if updat_el else ""
                        list_tags  = _parse_tags(tag_el)

                        # ── 詳細ページから会社情報取得（リトライ付き） ───────
                        company_info: dict[str, str] = {}
                        if jb_url:
                            for attempt in range(2):  # 0=初回, 1=リトライ
                                try:
                                    company_info = self._scrape_company_info(jb_url)
                                    consec_fails = 0
                                    break
                                except _NotFoundError:
                                    # 404: リトライなし・連続カウントに含めない
                                    self.logger.info("404スキップ: %s", jb_url)
                                    break
                                except _ScrapingError:
                                    if attempt == 0:
                                        # 初回失敗 → 10秒後にリトライ
                                        self.logger.warning(
                                            "詳細ページ取得失敗 → %d秒後リトライ: %s",
                                            _DETAIL_RETRY_WAIT, jb_url,
                                        )
                                        time.sleep(_DETAIL_RETRY_WAIT)
                                    else:
                                        # リトライも失敗 → 連続カウント+1
                                        consec_fails += 1
                                        self.logger.warning(
                                            "詳細ページ取得失敗(リトライ含む) %d連続: %s",
                                            consec_fails, jb_url,
                                        )
                                        if consec_fails >= _CONSEC_FAIL_LIMIT:
                                            # 60秒 × 最大5回 リトライ
                                            recovered = False
                                            for block_attempt in range(_CONSEC_RETRY_MAX):
                                                self.logger.warning(
                                                    "10件連続失敗 → %d秒後リトライ (%d/%d): %s",
                                                    _CONSEC_RETRY_WAIT,
                                                    block_attempt + 1, _CONSEC_RETRY_MAX,
                                                    jb_url,
                                                )
                                                time.sleep(_CONSEC_RETRY_WAIT)
                                                try:
                                                    company_info = self._scrape_company_info(jb_url)
                                                    consec_fails = 0
                                                    recovered = True
                                                    break
                                                except _NotFoundError:
                                                    consec_fails = 0
                                                    recovered = True
                                                    break
                                                except _ScrapingError:
                                                    pass
                                            if not recovered:
                                                # 5回全滅 → 1時間待機して次へ
                                                self.logger.error(
                                                    "5回リトライも全失敗 → %d時間待機後に次へ: %s",
                                                    _BLOCK_WAIT // 3600, jb_url,
                                                )
                                                time.sleep(_BLOCK_WAIT)
                                                consec_fails = 0

                        # 本社所在地の振り分け:
                        #   郵便番号があれば切り出して「郵便番号」カラムへ
                        #   番地を含む完全な住所 → Schema.ADDR
                        #   都道府県・市区町村止まりの不完全住所 → エリアへ移動
                        raw_addr = company_info.get("本社所在地", "")
                        postal_code, detail_addr = _extract_postal_code(raw_addr)
                        if detail_addr and _is_complete_address(detail_addr):
                            addr = detail_addr
                            area = list_area
                        else:
                            addr = ""
                            area = detail_addr or list_area

                        item = {
                            Schema.NAME:      company,
                            Schema.ADDR:      addr,
                            Schema.REP_NM:    company_info.get("代表者", ""),
                            Schema.EMP_NUM:   company_info.get("従業員数", ""),
                            Schema.CAP:       company_info.get("資本金", ""),
                            Schema.OPEN_DATE: company_info.get("設立", ""),
                            Schema.URL:       jb_url or search_base,
                            "エリア":          area,
                            "求人タイトル":    list_title,
                            "給与":           list_pay,
                            "雇用形態":        list_etype,
                            "検索キーワード":  label,
                            "都道府県":        pref,
                            "更新日":          list_updat,
                            "条件タグ":        list_tags,
                            "事業内容":        company_info.get("事業内容", ""),
                            "仕事内容":        company_info.get("仕事内容", ""),
                            "郵便番号":        postal_code,
                        }
                        yield item

                        if self._max_items is not None and self.item_count >= self._max_items:
                            self.logger.info("取得上限 %d 件到達 → 終了", self._max_items)
                            return

                        time.sleep(self.DELAY)

                    # 次ページ確認（.c-pager_btn--next クラスで「次のページへ」リンクを特定）
                    nxt = list_soup.select_one(".c-pager .c-pager_btn--next")
                    if not nxt:
                        self.logger.info("キーワード '%s' 最終ページ到達 (page=%d)", pref_label, page)
                        break

                    page += 1
                    time.sleep(self.DELAY)

    # -------------------------------------------------------------------------
    # 内部メソッド
    # -------------------------------------------------------------------------

    def _scrape_company_info(self, jb_url: str) -> dict[str, str]:
        """
        詳細ページ /jb/{id} から会社情報パネルを取得・パースする。

        Returns: {"設立": ..., "代表者": ..., "資本金": ...,
                  "従業員数": ..., "本社所在地": ..., "事業内容": ...}
        404 の場合は _NotFoundError、それ以外の失敗は _ScrapingError を raise する。
        リトライは呼び出し元（parse）が制御する。
        """
        from bs4 import BeautifulSoup

        try:
            resp = self.session.get(jb_url, timeout=30)
        except Exception as exc:
            raise _ScrapingError(f"リクエスト失敗: {jb_url}") from exc

        if resp.status_code == 404:
            raise _NotFoundError(f"404 Not Found: {jb_url}")
        if resp.status_code != 200:
            raise _ScrapingError(f"HTTP {resp.status_code}: {jb_url}")

        soup = BeautifulSoup(resp.text, "html.parser")
        result: dict[str, str] = {}

        # 全パネルをスキャンして住所・仕事内容・事業内容をまとめて収集
        for panel in soup.select(".p-detail_panel"):
            subtitle_el = panel.select_one(".p-detail_subTitle")
            subtitle_text = subtitle_el.get_text() if subtitle_el else ""
            line_el = panel.select_one(".p-detail_line")
            if not line_el:
                continue

            text_nl = line_el.get_text(separator="\n")

            # 【仕事内容】/【業務内容】の抽出（サブタイトルに関わらず全パネルを対象）
            if "仕事内容" not in result:
                jc = _extract_job_content(text_nl)
                if jc:
                    result["仕事内容"] = jc

            # subtitle="事業内容": 【本社所在地】【代表者】... 形式のパネル
            if "事業内容" in subtitle_text:
                parsed = _parse_company_panel(text_nl)
                if parsed:
                    result.update(parsed)

            # subtitle="本社所在地": 直接住所が書かれているパネル
            if "本社所在地" in subtitle_text and "本社所在地" not in result:
                result["本社所在地"] = _clean(line_el.get_text())

        # 住所が取得できていない場合はテーブルの「勤務地」から補完
        if "本社所在地" not in result:
            for table in soup.select(".p-detail_table"):
                titles = table.select(".p-detail_table_title")
                datas  = table.select(".p-detail_table_data")
                for t, d in zip(titles, datas):
                    key = t.get_text(strip=True)
                    if "勤務地" in key:
                        first_p = d.select_one("p.p-detail_line")
                        if first_p:
                            # <br>タグを\nに変換してから住所行を抽出する
                            val = _extract_address_line(first_p.get_text(separator="\n"))
                            if val:
                                result["本社所在地"] = _clean(val)
                                break

        return result

    def _fetch_page(self, url: str) -> tuple[object | None, bool]:
        """
        一覧ページを取得する（リトライ付き）。

        Returns:
            (soup, failed):
                soup   — 成功時は BeautifulSoup、コンテンツなし時は None
                failed — True = リトライ上限到達後も取得失敗
        """
        for attempt in range(1, _LIST_MAX_RETRIES + 1):
            try:
                soup = self.get_soup(url)
                return soup, False
            except Exception as exc:
                self.logger.warning(
                    "一覧取得失敗 → %d秒後リトライ (%d/%d): %s — %s",
                    _LIST_RETRY_WAIT, attempt, _LIST_MAX_RETRIES, url, exc,
                )
                if attempt < _LIST_MAX_RETRIES:
                    time.sleep(_LIST_RETRY_WAIT)

        return None, True


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================

if __name__ == "__main__":
    import logging
    import sys as _sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    full_run  = "--full"  in _sys.argv
    local_run = "--local" in _sys.argv

    if not full_run and not local_run:
        # ── 動作確認: 先頭2キーワード×1ページ、詳細ページも確認（CSV出力なし） ──
        import requests as _req
        from bs4 import BeautifulSoup as _BS

        session = _req.Session()
        session.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )

        for search_term, label in KEYWORDS[:2]:
            page_url = f"{BASE_URL}/{quote(search_term)}の仕事"
            resp = session.get(page_url, timeout=30)
            soup = _BS(resp.text, "html.parser")

            count_m = re.search(r"([\d,]+)\s*件", soup.get_text())
            total   = count_m.group(1) if count_m else "?"
            cards   = soup.select(".p-result_card, .p-result_wrap")

            print(f"\n【{label}】total={total}件 / 1ページ={len(cards)}件")
            for c in cards[:3]:
                co   = c.select_one(".p-result_company")
                ar   = c.select_one(".p-result_area")
                link = c.select_one("h2 a, .p-result_title_link, a[href]")
                href = link.get("href", "") if link else ""
                jb_id = _extract_jb_id(href)
                jb_url = f"{BASE_URL}/jb/{jb_id}" if jb_id else ""

                company = _clean(co.get_text()) if co else "?"
                area    = _clean(ar.get_text()) if ar else "?"
                print(f"  [{company}] エリア={area} jbId={jb_id[:16]}...")

                # 詳細ページから会社情報をプレビュー
                if jb_url:
                    time.sleep(0.5)
                    detail = session.get(jb_url, timeout=30)
                    dsoup  = _BS(detail.text, "html.parser")
                    info: dict[str, str] = {}
                    for panel in dsoup.select(".p-detail_panel"):
                        sub = panel.select_one(".p-detail_subTitle")
                        if sub and "事業内容" in sub.get_text():
                            line = panel.select_one(".p-detail_line")
                            if line:
                                info = _parse_company_panel(line.get_text("\n"))
                            break
                    if info:
                        print(f"    本社: {info.get('本社所在地', '?')}")
                        print(f"    代表: {info.get('代表者', '?')}  従業員: {info.get('従業員数', '?')}  設立: {info.get('設立', '?')}")
                    else:
                        print(f"    会社情報パネルなし")

    elif local_run and not full_run:
        # ── --local のみ: 100件取得 → sites/output/ に保存 ──
        scraper = KyujinboxScraper()
        scraper._max_items = 100
        scraper.execute(START_URL)
        print(f"\n取得件数: {scraper.item_count}")
        print(f"出力先:   {scraper.output_filepath}")

    else:
        # ── --full (+ 任意で --local): 全件取得 ──
        scraper = KyujinboxScraper()
        scraper.execute(START_URL)
        print(f"\n取得件数: {scraper.item_count}")
        print(f"出力先:   {scraper.output_filepath}")
