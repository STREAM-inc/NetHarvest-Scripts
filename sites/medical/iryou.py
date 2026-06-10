"""
医療情報ネット（ナビイ） — 厚生労働省 全国医療機関検索

取得対象:
    - 全国の病院・診療所・歯科診療所等の医療機関詳細
    - 施設特定・連絡先: 正式名称 / フリガナ / 郵便番号 / 所在地 / 案内用電話番号 /
      案内用FAX番号 / ホームページURL / 取得URL
    - 開設者詳細（開設者名・フリガナ・開設者種別）/ 管理者詳細（管理者名・フリガナ）
    - 病床数: 一般病床数 / 療養病床数 / 精神病床数 / 病床数合計
      ※ 病床種別セクションが存在しない施設（診療所等）は全て None
    - 規模・診療内容: 診療科目 / 医師数（常勤）/ 看護師数（常勤）
    - 診療科目別診療時間（動的生成）:
        {曜日}({科目名}) 例: 月(内科)〜日(内科)（7カラム）: 診療時間を "08:30-12:00,14:00-18:00" 形式で格納
        {属性}({科目名}) 例: 初診予約(内科) / 予約外診察(内科) / 入院受入(内科) / 女性医師(内科)（4カラム）
        ※ 診療科目数に応じてカラムが増加する（科目ごとに11カラム）
    - 営業判断材料: 予約診療の有無 / オンライン診療 / 電子処方箋 / 電子決済 /
      院内処方 / 院外処方 / 対応外国語
    - アポ参考: 診療時間帯 / 休診日
    ※ 自由記述・コメント系テキストは著作権リスクのため取得しない。

取得フロー（一覧→詳細, Pattern B: detail 取得ごとに即 yield）:
    1. {root}/S2300/iryoSearch?keywordType=4&keyword={都道府県名}  → 検索ID(JSON) を取得
       （keywordType=4 = 所在地検索。47都道府県名で全国を網羅）
    2. {root}/S2400/initialize?id={検索ID}&page={N}&sortNo=1       → 検索結果一覧（20件/頁）
       一覧内の S2430/initialize?prefCd=&kikanCd=&kikanKbn= が各施設の詳細リンク
    3. {root}/S2430/initialize?...                                  → 施設詳細をパースして即 yield

URL 一貫性:
    parse(url) の引数 url（= sites.yml の url, 検索フォーム S2300/initialize）を唯一のルートとし、
    iryoSearch / S2400 / S2430 の各エンドポイントは url から派生させる。別 URL はハードコードしない。

実行方法:
    # ローカルテスト
    python scripts/sites/medical/iryou.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id iryou
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 47都道府県名（所在地キーワード検索 keywordType=4 用）
PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# 住所先頭から都道府県を切り出す
_PREF_PATTERN = re.compile(r"^(北海道|東京都|(?:大阪|京都)府|.{2,3}県)")

# 診療科目ごとの曜日リスト（祝日カラムは生成しない）
_DAYS = ["月", "火", "水", "木", "金", "土", "日"]

# 診療属性キー
_ATTR_KEYS = ["初診予約", "予約外診察", "入院受入", "女性医師"]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 既知の診療科目リスト（EXTRA_COLUMNS を事前宣言するために使用）
# サイト上に存在しうる診療科目をここに網羅しておく。
# 新たな科目が検出された場合は _dept_hours() 内で安全に追加される。
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_KNOWN_DEPARTMENTS = [
    "内科", "外科", "小児科", "産婦人科", "産科", "婦人科",
    "整形外科", "眼科", "耳鼻咽喉科", "皮膚科", "泌尿器科",
    "精神科", "神経科", "心療内科", "神経内科", "脳神経外科",
    "循環器科", "呼吸器科", "消化器科", "胃腸科",
    "放射線科", "麻酔科", "リハビリテーション科",
    "形成外科", "美容外科", "救急科", "総合診療科",
    "歯科", "矯正歯科", "小児歯科", "口腔外科",
    "アレルギー科", "リウマチ科",
]

# EXTRA_COLUMNS に事前登録する診療科目別カラム名を生成
# {曜日}({科目名}) / {属性}({科目名}) の全組み合わせを列挙
def _make_dept_columns(departments: list[str]) -> list[str]:
    cols: list[str] = []
    for dept in departments:
        for day in _DAYS:
            cols.append(f"{day}({dept})")
        for attr in _ATTR_KEYS:
            cols.append(f"{attr}({dept})")
    return cols

# 診療属性パターン（正規表現）
_ATTR_PATTERNS = {
    "初診予約":   r"初診[時]?予約[：:]\s*(実施|可能|不可)",
    "予約外診察": r"予約外診察[：:]\s*(実施|可能|不可)",
    "入院受入":   r"入院患者受入[：:]\s*(実施|可能|不可)",
    "女性医師":   r"女性医師外来診察[：:]\s*(実施|可能|不可)",
}

# 病床数カラムマッピング
_BED_LABELS = {
    "一般病床": "一般病床数",
    "療養病床": "療養病床数",
    "精神病床": "精神病床数",
    "計":       "病床数合計",
}


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_tel(tel: str) -> str:
    """(0299)69-0777 → 0299-69-0777 等に整形（全角→半角は Pipeline が処理）"""
    t = _clean(tel)
    if not t or t == "-":
        return ""
    t = t.replace("（", "(").replace("）", ")")
    t = t.replace("(", "").replace(")", "-")
    t = re.sub(r"-{2,}", "-", t).strip("-")
    return t


def _val(s) -> str | None:
    """'-' または空白は None。それ以外は整形済み文字列を返す。"""
    s = _clean(s)
    return None if (not s or s == "-") else s


class IryouScraper(StaticCrawler):
    """医療情報ネット（ナビイ）スクレイパー（iryou.teikyouseido.mhlw.go.jp）"""

    DELAY = 1.5
    # Schema にマッピングできない固定カラムを EXTRA として明示的に保持する。
    # 診療科目別診療時間カラム（{科目名}_月 等）は施設ごとに動的生成されるため含めない。
    # 診療科目別の動的カラムを _KNOWN_DEPARTMENTS から事前宣言
    # → スキーマ違反 ValueError を防止するため全組み合わせを EXTRA_COLUMNS に登録する
    EXTRA_COLUMNS = [
        "案内用FAX番号",
        # 開設者詳細
        "開設者名", "開設者名フリガナ", "開設者種別",
        # 管理者詳細
        "管理者名", "管理者名フリガナ",
        # 病床数（病床種別セクションが存在しない施設は None）
        "一般病床数", "療養病床数", "精神病床数", "病床数合計",
        # 規模・診療内容
        "診療科目", "医師数（常勤）", "看護師数（常勤）",
        # 営業判断材料
        "予約診療の有無", "オンライン診療", "電子処方箋", "電子決済",
        "院内処方", "院外処方", "対応外国語",
        # アポ参考
        "診療時間帯", "休診日",
        # 診療科目別診療時間（既知の科目×曜日×属性を事前登録）
        # 例: "月(内科)", "火(歯科)", "初診予約(小児科)" ...
        *_make_dept_columns(_KNOWN_DEPARTMENTS),
    ]

    # ------------------------------------------------------------------ parse

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url（検索フォーム .../juminkanja/S2300/initialize）を唯一のルートとして各 URL を派生
        if "/S2300/" in url:
            base_dir = url.rsplit("/S2300/", 1)[0] + "/"
        else:
            base_dir = url.rsplit("/", 2)[0] + "/"
        search_url = base_dir + "S2300/iryoSearch"
        list_url   = base_dir + "S2400/initialize"

        seen: set[str] = set()
        total = 0

        for pref in PREFECTURES:
            search_id = self._fetch_search_id(search_url, pref)
            if not search_id:
                self.logger.warning("検索ID取得失敗: %s", pref)
                continue

            page = 0
            counted = False
            while True:
                page_url = f"{list_url}?id={search_id}&page={page}&sortNo=1"
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                if not counted:
                    m = re.search(r"([\d,]+)\s*件", soup.get_text())
                    if m:
                        total += int(m.group(1).replace(",", ""))
                        self.total_items = total
                    counted = True

                links = soup.select('a[href*="S2430/initialize"]')
                if not links:
                    break

                new_on_page = 0
                for a in links:
                    href = a.get("href", "").strip()
                    if not href or "kikanCd=" not in href:
                        continue
                    detail_url = urljoin(page_url, href)
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)
                    new_on_page += 1
                    try:
                        item = self._scrape_detail(detail_url)
                    except Exception as e:
                        self.logger.warning("詳細取得エラー: %s — %s", detail_url, e)
                        continue
                    if item and item.get(Schema.NAME):
                        yield item

                if len(links) < 20 or new_on_page == 0:
                    break
                page += 1

    # ------------------------------------------------------------------ search

    def _fetch_search_id(self, search_url: str, pref: str) -> str | None:
        """所在地（都道府県名）で検索し、結果一覧用の検索IDを取得する"""
        params = {
            "iyakuKbn": "1",
            "lang": "ja",
            "XCHARSET": "utf-8",
            "XPARAM": "keyword",
            "keywordType": "4",
            "keyword": pref,
        }
        try:
            resp = self.session.get(search_url, params=params, timeout=self.TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            return (data.get("result") or {}).get("id")
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.logger.warning("検索ID取得エラー: %s — %s", pref, e)
                return None
            raise

    # ------------------------------------------------------------------ detail

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        ths = soup.find_all("th")

        def row(label: str, exact: bool = True):
            for th in ths:
                t = _clean(th.get_text(" "))
                if (t == label) if exact else t.startswith(label):
                    tr = th.find_parent("tr")
                    td = tr.find("td") if tr else None
                    if td is not None:
                        return _val(td.get_text(" "))
            return None

        def row_after(anchor: str, target: str):
            """anchor の th 以降で最初に現れる target の td 値（開設者/管理者のフリガナ用）"""
            hit = False
            for th in ths:
                t = _clean(th.get_text(" "))
                if not hit:
                    if t.startswith(anchor):
                        hit = True
                    continue
                if t == target or t.startswith(target):
                    tr = th.find_parent("tr")
                    td = tr.find("td") if tr else None
                    return _val(td.get_text(" ")) if td is not None else None
            return None

        def joined(label: str):
            vals: list[str] = []
            for th in ths:
                if _clean(th.get_text(" ")).startswith(label):
                    tr = th.find_parent("tr")
                    td = tr.find("td") if tr else None
                    v = _val(td.get_text(" ")) if td is not None else None
                    if v and v not in vals:
                        vals.append(v)
            return " / ".join(vals) if vals else None

        def staff_fulltime(job: str):
            for th in ths:
                if _clean(th.get_text(" ")) == job:
                    tr = th.find_parent("tr")
                    tds = tr.find_all("td") if tr else []
                    if len(tds) >= 2:
                        return _val(tds[1].get_text(" "))
            return None

        def departments():
            names: list[str] = []
            for area in soup.find_all("div", class_="ptn3DataArea"):
                for st in area.find_all("strong"):
                    t = _clean(st.get_text())
                    if t.startswith("◆"):
                        n = t[1:].strip()
                        if n and n not in names:
                            names.append(n)
            return " / ".join(names) if names else None

        # --- 施設特定（正式名称が無ければ施設詳細ページではないとみなす） ---
        name = row("正式名称", exact=True)
        if not name:
            return None

        data = {Schema.URL: url, Schema.NAME: name}

        name_kana = row("正式名称（フリガナ）", exact=True)
        if name_kana:
            data[Schema.NAME_KANA] = name_kana

        # 所在地詳細
        post = row("郵便番号", exact=True)
        if post:
            data[Schema.POST_CODE] = post
        addr = row("所在地", exact=True)
        if addr:
            m = _PREF_PATTERN.match(addr)
            if m:
                data[Schema.PREF] = m.group(1)
            data[Schema.ADDR] = addr

        # 電話番号・FAX番号 / ホームページ
        data[Schema.TEL]       = _norm_tel(row("案内用電話番号", exact=True) or "")
        data["案内用FAX番号"]  = _norm_tel(row("案内用ファクシミリ番号", exact=True) or "") or None
        hp = row("案内用ホームページアドレス", exact=True)
        if hp:
            data[Schema.HP] = hp

        # 開設者詳細
        data["開設者名"]        = row("開設者名", exact=False)
        data["開設者名フリガナ"] = row_after("開設者名", "フリガナ")
        data["開設者種別"]       = joined("開設者種別")

        # 管理者詳細
        kanrisha = row("管理者名", exact=False)
        data["管理者名"]        = kanrisha
        data["管理者名フリガナ"] = row_after("管理者名", "フリガナ")
        if kanrisha:
            data[Schema.REP_NM] = kanrisha

        # ── 病床数 ────────────────────────────────────────────────────────────
        data.update(self._bed_counts(soup))

        # 規模・診療内容
        data["診療科目"]       = departments()
        data["医師数（常勤）"] = staff_fulltime("医師")
        data["看護師数（常勤）"] = staff_fulltime("看護師")

        # ── 診療科目別診療時間（動的カラム） ──────────────────────────────────
        data.update(self._dept_hours(soup))

        # 営業判断材料
        data["予約診療の有無"] = row("予約診療の有無（診療科目全般）", exact=True)
        data["オンライン診療"] = row("オンライン診療実施の有無", exact=False)
        data["電子処方箋"]     = row("電子処方箋の発行の可否", exact=False)
        data["電子決済"]       = row("電子決済サービスへの対応", exact=True)
        data["院内処方"]       = row("院内処方の有無", exact=False)
        data["院外処方"]       = row("院外処方の有無", exact=False)
        data["対応外国語"]     = joined("対応可能な外国語名")

        # アポ参考
        data["診療時間帯"] = row("診療時間帯", exact=False)
        data["休診日"]     = row("休診日", exact=True)

        return data

    # ------------------------------------------------------------------ 病床数

    def _bed_counts(self, soup) -> dict:
        """
        病床種別・届出・許可病床数テーブルから 4 カラムを取得する。
        セクションが存在しない施設（診療所等）や値が「-」の場合は None とする。
        """
        result: dict = {k: None for k in _BED_LABELS.values()}

        for table in soup.find_all("table"):
            header_row = table.find("tr")
            if header_row is None:
                continue
            header_cells = header_row.find_all(["th", "td"])
            header_texts = [_clean(c.get_text()) for c in header_cells]

            if "一般病床" not in header_texts:
                continue

            # 列インデックスを構築
            idx_map: dict[str, int] = {}
            for i, txt in enumerate(header_texts):
                if txt in _BED_LABELS:
                    idx_map[txt] = i

            # 「届出又は許可病床数」の行を探して値を取得
            for tr in table.find_all("tr")[1:]:
                cells = tr.find_all(["th", "td"])
                if not cells:
                    continue
                row_label = _clean(cells[0].get_text(" "))
                if "届出" not in row_label and "許可" not in row_label:
                    continue
                for col_name, col_key in _BED_LABELS.items():
                    idx = idx_map.get(col_name)
                    if idx is None or idx >= len(cells):
                        continue
                    raw = _clean(cells[idx].get_text())
                    # 「0床」→「0」のように「床」サフィックスを除去
                    val = re.sub(r"床$", "", raw).strip()
                    result[col_key] = None if (not val or val == "-") else val
            break  # 病床テーブルは 1 つのみ

        return result

    # -------------------------------------------------------- 診療科目別診療時間

    def _dept_hours(self, soup) -> dict:
        """
        診療科目ごとに曜日別診療時間（7カラム）と診療属性（4カラム）を動的に生成する。

        カラム名:
            {曜日}({科目名}) 例: 月(内科)〜日(内科)  : "08:30-12:00,14:00-18:00" / "-"
            {属性}({科目名}) 例: 初診予約(内科) / 予約外診察(内科) / 入院受入(内科) / 女性医師(内科) : "実施" / "可能" / "不可" / None
        """
        result: dict = {}

        for strong in soup.find_all("strong"):
            text = _clean(strong.get_text())
            if not text.startswith("◆"):
                continue
            dept_name = text[1:].strip()
            if not dept_name:
                continue

            # ── グループ見出し（〜領域 / 〜系 等）はスキップ ────────────
            # 例: "歯科領域" "内科領域" "外科系" など診療科目ではない見出し
            _GROUP_SUFFIX = re.compile(r"(領域|系|部門|グループ)$")
            if _GROUP_SUFFIX.search(dept_name):
                self.logger.debug("グループ見出しをスキップ: %s", dept_name)
                continue

            # ── 診療属性（括弧内テキストから正規表現で抽出） ──────────────
            attrs: dict[str, str | None] = {k: None for k in _ATTR_PATTERNS}
            for ancestor in [strong.parent,
                             strong.parent.parent if strong.parent else None]:
                if ancestor is None:
                    continue
                anc_text = ancestor.get_text(" ")
                for key, pat in _ATTR_PATTERNS.items():
                    if attrs[key] is None:
                        m = re.search(pat, anc_text)
                        if m:
                            attrs[key] = m.group(1)

            # ── 次の診療時間テーブルを探す ────────────────────────────────
            day_slots: dict[str, list[str]] = {d: [] for d in _DAYS}
            dept_table = self._find_next_table(strong)

            if dept_table:
                # ヘッダー行から曜日の列インデックスを取得
                day_col_idx: dict[str, int] = {}
                for tr in dept_table.find_all("tr"):
                    cells = tr.find_all(["th", "td"])
                    texts = [_clean(c.get_text()) for c in cells]
                    if "月" in texts and "火" in texts:
                        for i, t in enumerate(texts):
                            if t in _DAYS:
                                day_col_idx[t] = i
                        break

                if day_col_idx:
                    for tr in dept_table.find_all("tr"):
                        cells = tr.find_all(["th", "td"])
                        if not cells:
                            continue
                        row_label = _clean(cells[0].get_text(" "))
                        # 「診療時間」行（「外来受付時間」行は除外）
                        if "診療時間" not in row_label or "外来受付" in row_label:
                            continue
                        for day, idx in day_col_idx.items():
                            if idx < len(cells):
                                val = _clean(cells[idx].get_text())
                                if val and val != "-":
                                    day_slots[day].append(val)

            # ── 結果カラムに書き込み ─────────────────────────────────────
            # カラム名: {曜日}({科目名}) / {属性}({科目名})
            # 値が取れない場合は "" でガードし、ValueError による停止を防ぐ
            for day in _DAYS:
                slots = day_slots.get(day, [])
                result[f"{day}({dept_name})"] = ",".join(slots) if slots else "-"
            for attr_key in _ATTR_KEYS:
                result[f"{attr_key}({dept_name})"] = attrs.get(attr_key) or ""

        return result

    @staticmethod
    def _find_next_table(strong):
        """
        <strong> の後続要素から最初の <table> を探す（2段階まで辿る）。
        次の ◆科目 に到達したら探索を打ち切る。
        """
        def _is_next_dept(el):
            return any(
                _clean(s.get_text()).startswith("◆")
                for s in el.find_all("strong")
            )

        start = strong.parent or strong
        for sibling in start.find_next_siblings():
            if sibling.name == "table":
                return sibling
            t = sibling.find("table")
            if t:
                return t
            if _is_next_dept(sibling):
                return None

        # 一段上の親の兄弟要素まで探索
        if start.parent:
            for sibling in start.parent.find_next_siblings():
                if sibling.name == "table":
                    return sibling
                t = sibling.find("table")
                if t:
                    return t
                if _is_next_dept(sibling):
                    return None
        return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = IryouScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.iryou.teikyouseido.mhlw.go.jp/znk-web/juminkanja/S2300/initialize")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")