"""
税理士情報検索サイト（日本税理士会連合会）— 全国版（税理士・税理士法人 登録者名簿）

既存 `government/zeirishikensaku` との違い:
    - 既存版は 14 都県 × 登録年 2025 年以降のみの絞り込み版。本モジュール (_2) は
      **47 都道府県 × 全登録年** を対象とする全国版。
    - 既存版のページ送りは `?page=N&Search=1` を送っており、サーバは `Search=1` が
      付くと 0 件を返す (= 1 都県あたり先頭 100 件しか取得できていない)。
      本モジュールは `?page=N` のみを付けた POST で 2 ページ目以降も正しく取得する。

取得フロー (全て静的 / requests で完結。Playwright 不要):
    1. ルート (https://www.zeirishikensaku.jp/) を GET
    2. POST /NzSearchDListPerson  (条件: Prefecture, RowsPerPage=100,
       Limit.Checked=true ※「検索結果100件を超えても表示する」)
       → 検索結果一覧 (webgrid / table.searchDetails)
       ※「所在地を全部表示している税理士に限る」(JimAddrChk) は送らない = 非チェック。
         所在地を一部非表示にしている税理士も取りこぼさないため。
    3. ページ送りは WebGrid の JS が POST に書き換えている:
       POST /NzSearchDListPerson?page=N に同一条件 body を再送する
    4. 一覧の各行 <input class="no"> が登録番号。詳細は
       POST /NzSearchContentPerson {torokuno, preview=1} で取得し、1 件ごとに即 yield
       ※ preview=0 (ブラウザ既定) はサーバ側で内部エラーになるため preview=1 が必須
    5. 税理士法人は /NzSearchDListCompany と
       POST /NzSearchContentCompany {houno, houeda, preview=1} で同じ流れ
       (法人は「主たる事務所」「従たる事務所」が別レコード = 枝番号で区別)

詳細ページの構造:
    <div class="row"><div class="cont-d">ラベル</div><div class="cont-t">値</div></div>
    の繰り返し。「公開情報」セクションと「任意公開情報」セクションがあり、
    任意公開情報 (性別・生年・FAX・メール・HP・取扱業務・取扱業種・対応可能言語) は
    登録している税理士のみ表示される (未登録なら空文字で出力)。

利用規約 (/Information/Precautions):
    「複製、転載、公衆送信等」「商業利用」の禁止条項はあるが、スクレイピング /
    クローリング / 自動取得を明示的に禁止する条項は無い。

実行方法:
    python scripts/sites/government/zeirishikensaku_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id zeirishikensaku_2
"""

import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 検索条件プルダウン (ConditionsPerson.Prefecture) の全 47 都道府県
PREFECTURES = [
    "北海道", "青森県", "岩手県", "秋田県", "宮城県", "山形県", "福島県",
    "東京都", "神奈川県", "千葉県", "埼玉県", "茨城県", "栃木県", "群馬県",
    "山梨県", "長野県", "新潟県", "富山県", "石川県", "福井県",
    "愛知県", "静岡県", "岐阜県", "三重県",
    "大阪府", "兵庫県", "京都府", "滋賀県", "奈良県", "和歌山県",
    "岡山県", "広島県", "鳥取県", "島根県", "山口県",
    "愛媛県", "香川県", "高知県", "徳島県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

ROWS_PER_PAGE = 100
MAX_PAGES = 500           # 無限ループ防止 (1 都県あたり最大 50,000 件)
MAX_ATTEMPTS = 3          # POST のリトライ上限 (再帰は使わない)

_POST_CODE_RE = re.compile(r"〒\s*(\d{3}-?\d{4})")
_TOTAL_RE = re.compile(r"total-row-count'>(\d+)<")
_NAME_KANA_RE = re.compile(r"^(.*?)（(.*?)）\s*$")
_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")
_WAREKI_RE = re.compile(r"(令和|平成|昭和|大正|明治)\s*(元|\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日")
_ERA_BASE = {"令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867}

# 値が空/非表示を意味する表記
_BLANK_VALUES = {"", "-", "非表示", "未登録"}

# 詳細ページの「既知ラベル」。ここに無いラベルは取扱業種 (農業〜その他の 19 分類) とみなす
_KNOWN_LABELS = {
    "登録番号", "氏名（カナ）", "登録年月日", "事務所名称", "事務所所在地",
    "事務所電話番号", "所属税理士会", "報酬のある公職による業務停止期間", "懲戒処分",
    "年度中途登録による按分月数（時間）", "免除申請による免除月数（時間）",
    "性別", "生年", "事務所FAX", "事務所メールアドレス", "事務所ホームページアドレス",
    "税務代理・税務書類の作成・税務相談", "会計業務", "経営相談等", "公益的業務等",
    "対応可能言語", "コメント",
    # 法人側
    "税理士法人番号", "法人名称", "法人名称カナ", "届出年月日",
    "主たる事務所", "従たる事務所",
}
# 「研修受講義務の達成状況（2024年度）」は年度が変わるため前方一致で判定する
_KNOWN_PREFIXES = ("研修受講義務の達成状況",)


class Zeirishikensaku2(StaticCrawler):
    """税理士情報検索サイト（日本税理士会連合会）全国版スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = [
        "区分",
        "登録番号",
        "税理士法人番号",
        "枝番号",
        "登録年月日",
        "事務所名称",
        "所属税理士会",
        "FAX",
        "性別",
        "生年",
        "研修受講義務の達成状況",
        "懲戒処分",
        "報酬のある公職による業務停止期間",
        "主たる事務所",
        "従たる事務所",
        "取扱業務_税務",
        "取扱業務_会計",
        "取扱業務_経営相談等",
        "取扱業務_公益的業務等",
        "取扱業種",
        "対応可能言語",
        "コメント",
    ]

    # ------------------------------------------------------------------ parse
    def parse(self, url: str):
        """引数 url (= sites.yml の url / サイトトップ) を唯一のルートとする。"""
        list_person_url = urljoin(url, "/NzSearchDListPerson")
        list_company_url = urljoin(url, "/NzSearchDListCompany")
        detail_person_url = urljoin(url, "/NzSearchContentPerson")
        detail_company_url = urljoin(url, "/NzSearchContentCompany")

        # ルートを 1 回だけ取得 (疎通確認 / Cookie 取得)
        self.get_soup(url)

        seen: set[str] = set()

        # 都道府県 × 区分(税理士 → 税理士法人) の順に巡回。詳細取得ごとに即 yield する。
        for pref in PREFECTURES:
            person_cond = {
                "ConditionsPerson.Prefecture": pref,
                # 「検索結果100件を超えても表示する」を有効化
                "ConditionsPerson.Limit.Checked": "true",
                "RowsPerPage": str(ROWS_PER_PAGE),
            }
            yield from self._crawl(
                pref, "税理士", list_person_url, person_cond, detail_person_url, seen
            )

            company_cond = {
                "ConditionsCompany.Prefecture": pref,
                "ConditionsCompany.Limit.Checked": "true",
                "RowsPerPage": str(ROWS_PER_PAGE),
            }
            yield from self._crawl(
                pref, "税理士法人", list_company_url, company_cond, detail_company_url, seen
            )

    # ------------------------------------------------------------- list/detail
    def _crawl(self, pref: str, kubun: str, list_url: str, cond: dict,
               detail_url: str, seen: set):
        """1 都県 × 1 区分を巡回し、詳細を 1 件取得するごとに yield する。"""
        is_company = kubun == "税理士法人"
        for page in range(1, MAX_PAGES + 1):
            # WebGrid のページ送りは同一条件を POST し直す方式。
            # ※ `&Search=1` を付けると 0 件になるので付けないこと。
            page_url = f"{list_url}?page={page}" if page > 1 else list_url
            soup = self._post_soup(page_url, cond)
            if soup is None:
                return

            rows = soup.select("table.searchDetails tbody tr")
            if not rows:
                if page == 1:
                    logger.info("該当なし: %s / %s", pref, kubun)
                return

            if page == 1:
                m = _TOTAL_RE.search(str(soup))
                total = int(m.group(1)) if m else len(rows)
                logger.info("%s / %s: 全%s件", pref, kubun, total)
                self.total_items = (self.total_items or 0) + total

            for row in rows:
                keys = self._row_keys(row, is_company)
                if not keys:
                    continue
                dedupe_key = f"{kubun}:{keys.get('torokuno') or keys.get('houno')}" \
                             f"-{keys.get('houeda', '')}"
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                item = self._fetch_detail(detail_url, keys, kubun, is_company)
                if item:
                    yield item

            # 取得件数が 1 ページ分未満なら最終ページ
            if len(rows) < ROWS_PER_PAGE:
                return

        logger.warning("ページ上限 (%s) に到達: %s / %s", MAX_PAGES, pref, kubun)

    def _row_keys(self, row, is_company: bool) -> dict | None:
        """一覧行から詳細ページの POST キー（登録番号 / 法人番号+枝番号）を取り出す。"""
        no_el = row.select_one("a.lname input.no")
        if not no_el or not (no_el.get("value") or "").strip():
            return None
        no = no_el["value"].strip()
        if not is_company:
            return {"torokuno": no, "preview": "1"}
        eda_el = row.select_one("a.lname input.eda")
        eda = (eda_el.get("value") or "0").strip() if eda_el else "0"
        return {"houno": no, "houeda": eda, "preview": "1"}

    def _fetch_detail(self, detail_url: str, keys: dict, kubun: str,
                      is_company: bool) -> dict | None:
        soup = self._post_soup(detail_url, keys)
        if soup is None:
            return None

        fields: dict[str, str] = {}
        for block in soup.select("div.row"):
            label_el = block.select_one("div.cont-d")
            value_el = block.select_one("div.cont-t")
            if not label_el or not value_el:
                continue
            label = label_el.get_text(" ", strip=True).replace("　", "").strip()
            if label and label not in fields:
                fields[label] = value_el.get_text(" ", strip=True)

        if not fields:
            logger.warning("詳細ページの解析に失敗: %s", keys)
            return None

        if is_company:
            name = self._clean(fields.get("法人名称", ""))
            kana = self._clean(fields.get("法人名称カナ", ""))
            reg_date = self._to_iso_date(fields.get("届出年月日", ""))
            office_name = name
            houno = self._clean(fields.get("税理士法人番号", "")) or keys.get("houno", "")
            houeda = keys.get("houeda", "")
            toroku_no = ""
        else:
            # 「氏名（カナ）」形式（例: 青戸　弘（アオト ヒロシ））
            name, kana = self._split_name_kana(fields.get("氏名（カナ）", ""))
            reg_date = self._to_iso_date(fields.get("登録年月日", ""))
            office_name = self._clean(fields.get("事務所名称", ""))
            houno = ""
            houeda = ""
            toroku_no = self._clean(fields.get("登録番号", "")) or keys.get("torokuno", "")

        if not name:
            logger.warning("名称を取得できずスキップ: %s", keys)
            return None

        post_code, pref, addr = self._split_address(fields.get("事務所所在地", ""))

        # 研修受講義務の達成状況はラベルに年度が入る (例: 「研修受講義務の達成状況（2024年度）」)
        kenshu = ""
        for label, value in fields.items():
            if label.startswith("研修受講義務の達成状況"):
                kenshu = self._clean(value)
                break

        # 既知ラベル以外 (農業/建設業/製造業… の 19 分類) を取扱業種としてまとめる
        gyoushu = []
        for label, value in fields.items():
            if label in _KNOWN_LABELS or label.startswith(_KNOWN_PREFIXES):
                continue
            value = self._clean(value)
            if value:
                gyoushu.append(f"{label}:{value}")

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: self._clean(fields.get("事務所電話番号", "")),
            Schema.EMAIL: self._clean(fields.get("事務所メールアドレス", "")),
            Schema.HP: self._clean(fields.get("事務所ホームページアドレス", "")),
            Schema.CAT_SITE: kubun,
            "区分": kubun,
            "登録番号": toroku_no,
            "税理士法人番号": houno,
            "枝番号": houeda,
            "登録年月日": reg_date,
            "事務所名称": office_name,
            "所属税理士会": self._clean(fields.get("所属税理士会", "")),
            "FAX": self._clean(fields.get("事務所FAX", "")),
            "性別": self._clean(fields.get("性別", "")),
            "生年": self._clean(fields.get("生年", "")),
            "研修受講義務の達成状況": kenshu,
            "懲戒処分": self._clean(fields.get("懲戒処分", "")),
            "報酬のある公職による業務停止期間":
                self._clean(fields.get("報酬のある公職による業務停止期間", "")),
            "主たる事務所": self._clean(fields.get("主たる事務所", "")),
            "従たる事務所": self._clean(fields.get("従たる事務所", "")),
            "取扱業務_税務": self._clean(fields.get("税務代理・税務書類の作成・税務相談", "")),
            "取扱業務_会計": self._clean(fields.get("会計業務", "")),
            "取扱業務_経営相談等": self._clean(fields.get("経営相談等", "")),
            "取扱業務_公益的業務等": self._clean(fields.get("公益的業務等", "")),
            "取扱業種": " / ".join(gyoushu),
            "対応可能言語": self._clean(fields.get("対応可能言語", "")),
            "コメント": self._clean(fields.get("コメント", "")),
        }

    # ----------------------------------------------------------------- helpers
    def _post_soup(self, url: str, data: dict) -> BeautifulSoup | None:
        """POST で HTML を取得して BeautifulSoup を返す。

        一時的な通信エラー / サーバ内部エラー画面は指数バックオフで最大 MAX_ATTEMPTS 回
        まで再試行する (再帰はしない)。全試行失敗時は None を返してスキップする。
        """
        last_error = None
        for attempt in range(MAX_ATTEMPTS):
            if attempt:
                time.sleep(min(2 ** attempt, 10))
            try:
                resp = self.session.post(url, data=data, timeout=self.TIMEOUT)
                resp.raise_for_status()
            except Exception as e:
                last_error = e
                logger.warning("通信エラー (%s/%s): %s — %s",
                               attempt + 1, MAX_ATTEMPTS, url, e)
                continue
            resp.encoding = "utf-8"
            if "内部エラーが発生しました" in resp.text:
                last_error = "サーバー内部エラー"
                logger.warning("サーバー内部エラー (%s/%s): %s %s",
                               attempt + 1, MAX_ATTEMPTS, url, data)
                continue
            return BeautifulSoup(resp.text, "html.parser")

        self.error_count += 1
        logger.warning("取得を諦めてスキップ: %s %s — %s", url, data, last_error)
        if not self.CONTINUE_ON_ERROR:
            raise RuntimeError(f"取得に失敗しました: {url} {data} — {last_error}")
        return None

    @staticmethod
    def _clean(raw: str) -> str:
        """「非表示」「-」等のプレースホルダを空文字に正規化する。"""
        text = re.sub(r"\s+", " ", (raw or "").replace("　", " ")).strip()
        return "" if text in _BLANK_VALUES else text

    @classmethod
    def _split_name_kana(cls, raw: str) -> tuple[str, str]:
        text = (raw or "").strip()
        m = _NAME_KANA_RE.match(text)
        if m:
            return cls._clean(m.group(1)), cls._clean(m.group(2))
        return cls._clean(text), ""

    @classmethod
    def _split_address(cls, raw: str) -> tuple[str, str, str]:
        """「〒683-0845　鳥取県米子市　旗ケ崎…」→ (郵便番号, 都道府県, 市区町村以降)。

        所在地を一部非表示にしている税理士は「鳥取県境港市」のように
        郵便番号や番地が欠けることがある。
        """
        text = (raw or "").replace("　", " ").strip()
        if text in _BLANK_VALUES:
            return "", "", ""
        post_code = ""
        m = _POST_CODE_RE.search(text)
        if m:
            post_code = m.group(1)
            text = (text[:m.start()] + text[m.end():]).strip()
        pref = ""
        m2 = _PREF_RE.match(text)
        if m2:
            pref = m2.group(1)
            text = text[len(pref):].strip()
        return post_code, pref, re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _to_iso_date(raw: str) -> str:
        """和暦 (例: 令和5年9月26日) を YYYY-MM-DD に変換する。失敗時は原文を返す。"""
        text = (raw or "").strip()
        if not text:
            return ""
        m = _WAREKI_RE.search(text)
        if not m:
            return text
        era, year, month, day = m.groups()
        year = 1 if year == "元" else int(year)
        return f"{_ERA_BASE[era] + year:04d}-{int(month):02d}-{int(day):02d}"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Zeirishikensaku2()
    scraper.execute("https://www.zeirishikensaku.jp/")
