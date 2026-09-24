"""
税理士情報検索サイト（日本税理士会連合会）— 税理士・税理士法人 登録者名簿

取得対象:
    - 「詳細からさがす」(/NzSearchDetail) の 税理士情報検索 / 法人情報検索 の両区分
    - 対象都道府県: 東京都・神奈川県・埼玉県・千葉県・茨城県・栃木県・群馬県・
      新潟県・山梨県・長野県・岐阜県・静岡県・愛知県・三重県 の 14 都県
    - 登録年（法人は届出年）2025 年以降のみ

取得フロー (全て静的 / requests で完結):
    1. POST /NzSearchDListPerson  (条件: Prefecture, TorokuYF=2025, TorokuYT=今年,
       Limit.Checked=true ※「検索結果100件を超えても表示する」, RowsPerPage=100)
       → 検索結果一覧 (webgrid)
    2. ページ送りは WebGrid の JS が POST に書き換えている:
       POST /NzSearchDListPerson?page=N&Search=1 (同一条件 body を再送するので
       セッション状態に依存せず安定する)
    3. 一覧の各行 <input class="no"> が登録番号。詳細は
       POST /NzSearchContentPerson {torokuno, preview=1} で取得し、1 件ごとに即 yield
       ※ preview=0 (ブラウザ既定) はサーバ側で内部エラーになるため preview=1 を使う
    4. 法人は /NzSearchDListCompany (HouTodoukeYF/YT) と
       /NzSearchContentCompany {houno, houeda, preview=1} で同じ流れ

利用規約:
    「ご利用上の注意」(/Information/Precautions) に複製・転載・公衆送信および商業利用の
    禁止条項があるが、スクレイピング/クローリング/自動取得を明示的に禁止する条項は無い。

実行方法:
    python scripts/sites/government/zeirishikensaku.py
    docker compose exec worker python /app/bin/run_flow.py --site-id zeirishikensaku
"""

import datetime
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 備考指定: 対象 14 都県
TARGET_PREFS = [
    "東京都", "神奈川県", "埼玉県", "千葉県", "茨城県", "栃木県", "群馬県",
    "新潟県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県", "三重県",
]

# 備考指定: 登録年（法人は届出年）2025 年以降
REG_YEAR_FROM = 2025

ROWS_PER_PAGE = 100

_POST_CODE_RE = re.compile(r"〒\s*(\d{3}-?\d{4})")
_TOTAL_RE = re.compile(r"total-row-count'>(\d+)<")


class Zeirishikensaku(StaticCrawler):
    """税理士情報検索サイト（日本税理士会連合会）スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "区分",
        "登録番号",
        "税理士法人番号",
        "枝番号",
        "登録年月日",
        "事務所名称",
        "所属税理士会",
        "業務停止情報",
    ]

    # ------------------------------------------------------------------ parse
    def parse(self, url: str):
        """引数 url (= sites.yml の url / .../NzSearchDetail) を唯一のルートとする。"""
        list_person_url = urljoin(url, "/NzSearchDListPerson")
        list_company_url = urljoin(url, "/NzSearchDListCompany")
        detail_person_url = urljoin(url, "/NzSearchContentPerson")
        detail_company_url = urljoin(url, "/NzSearchContentCompany")

        year_to = datetime.date.today().year

        # 都道府県 × 区分(個人→法人) の順に巡回。詳細取得ごとに即 yield する。
        for pref in TARGET_PREFS:
            # ---- 税理士（個人） --------------------------------------------
            person_cond = {
                "ConditionsPerson.Prefecture": pref,
                "ConditionsPerson.TorokuYF": str(REG_YEAR_FROM),
                "ConditionsPerson.TorokuYT": str(year_to),
                # 「検索結果100件を超えても表示する」を有効化
                "ConditionsPerson.Limit.Checked": "true",
                "RowsPerPage": str(ROWS_PER_PAGE),
            }
            yield from self._crawl(
                pref, "税理士（個人）", list_person_url, person_cond, detail_person_url
            )

            # ---- 税理士法人 --------------------------------------------------
            company_cond = {
                "ConditionsCompany.Prefecture": pref,
                "ConditionsCompany.HouTodoukeYF": str(REG_YEAR_FROM),
                "ConditionsCompany.HouTodoukeYT": str(year_to),
                "ConditionsCompany.Limit.Checked": "true",
                "RowsPerPage": str(ROWS_PER_PAGE),
            }
            yield from self._crawl(
                pref, "税理士法人", list_company_url, company_cond, detail_company_url
            )

    # ------------------------------------------------------------- list/detail
    def _crawl(self, pref: str, kubun: str, list_url: str, cond: dict, detail_url: str):
        """1 都県 × 1 区分を巡回し、詳細を 1 件取得するごとに yield する。"""
        is_company = kubun == "税理士法人"
        page = 1
        while True:
            page_url = f"{list_url}?page={page}&Search=1" if page > 1 else list_url
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
                try:
                    keys = self._row_keys(row, is_company)
                    if not keys:
                        continue
                    item = self._fetch_detail(detail_url, keys, kubun, is_company)
                    if item:
                        yield item
                except Exception as e:  # 1 件のエラーで全体を止めない
                    logger.warning("詳細の取得に失敗しskip (%s/%s): %s", pref, kubun, e)
                    continue

            # 取得件数が 1 ページ分未満なら最終ページ
            if len(rows) < ROWS_PER_PAGE:
                return
            page += 1

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

    def _fetch_detail(self, detail_url: str, keys: dict, kubun: str, is_company: bool) -> dict | None:
        soup = self._post_soup(detail_url, keys)
        if soup is None:
            return None

        # 詳細ページは <div class="row"><div class="cont-d">ラベル</div>
        #                              <div class="cont-t">値</div></div> の繰り返し
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
            name = fields.get("法人名称", "")
            kana = fields.get("法人名称カナ", "")
            reg_date = fields.get("届出年月日", "")
            office_name = fields.get("法人名称", "")
            houno = keys.get("houno", "")
            houeda = keys.get("houeda", "")
            toroku_no = ""
        else:
            # 「氏名（カナ）」形式（例: 石水　秀治（イシミズ シュウジ））
            name, kana = self._split_name_kana(fields.get("氏名（カナ）", ""))
            reg_date = fields.get("登録年月日", "")
            office_name = fields.get("事務所名称", "")
            houno = ""
            houeda = ""
            toroku_no = fields.get("登録番号", "") or keys.get("torokuno", "")

        if not name:
            return None

        post_code, pref, addr = self._split_address(fields.get("事務所所在地", ""))

        # 業務停止情報の有無（懲戒処分 / 報酬のある公職による業務停止期間）
        teishi = [
            fields.get("懲戒処分", "").strip(),
            fields.get("報酬のある公職による業務停止期間", "").strip(),
        ]
        teishi_flag = "有" if any(teishi) else "無"

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: fields.get("事務所電話番号", ""),
            Schema.EMAIL: fields.get("事務所メールアドレス", ""),
            Schema.HP: fields.get("事務所ホームページアドレス", ""),
            "区分": kubun,
            "登録番号": toroku_no,
            "税理士法人番号": houno,
            "枝番号": houeda,
            "登録年月日": reg_date,
            "事務所名称": office_name,
            "所属税理士会": fields.get("所属税理士会", ""),
            "業務停止情報": teishi_flag,
        }

    # ----------------------------------------------------------------- helpers
    def _post_soup(self, url: str, data: dict) -> BeautifulSoup | None:
        """POST で HTML を取得して BeautifulSoup を返す（内部エラー画面は None）。"""
        try:
            resp = self.session.post(url, data=data, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                logger.warning("通信エラー (スキップして継続): %s — %s", url, e)
                return None
            raise
        resp.encoding = "utf-8"
        if "内部エラーが発生しました" in resp.text:
            logger.warning("サーバー内部エラー: %s %s", url, data)
            return None
        return BeautifulSoup(resp.text, "html.parser")

    @staticmethod
    def _split_name_kana(raw: str) -> tuple[str, str]:
        m = re.match(r"^(.*?)（(.*?)）\s*$", raw.strip())
        if m:
            return m.group(1).strip(), m.group(2).strip()
        return raw.strip(), ""

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        """「〒400-0061　山梨県甲府市　荒川１丁目…」→ (郵便番号, 都道府県, 住所)。"""
        text = (raw or "").replace("　", " ").strip()
        post_code = ""
        m = _POST_CODE_RE.search(text)
        if m:
            post_code = m.group(1)
            text = text[m.end():].strip()
        pref = ""
        for p in TARGET_PREFS + ["北海道", "大阪府", "京都府"]:
            if text.startswith(p):
                pref = p
                text = text[len(p):].strip()
                break
        if not pref:
            m2 = re.match(r"^(.{2,3}[都道府県])", text)
            if m2:
                pref = m2.group(1)
                text = text[len(pref):].strip()
        return post_code, pref, re.sub(r"\s+", " ", text).strip()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Zeirishikensaku()
    scraper.execute("https://www.zeirishikensaku.jp/NzSearchDetail")
