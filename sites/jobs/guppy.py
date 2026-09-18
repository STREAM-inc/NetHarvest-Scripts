# scripts/sites/jobs/guppy.py
"""
GUPPY (グッピー, www.guppy.jp) — 全職種・事業所単位 求人スクレイパー

取得対象:
    - 57 職種コードすべてを巡回
    - 同一事業所 (勤務先名 + 正規化住所が一致) は 1 レコードに集約
    - 推定対象: 約 4,000〜10,000 事業所（主に歯科・医療系求人特化サイト）

取得カラム:
    Schema (11): URL(=初出求人URL), NAME, PREF, ADDR, HP, CAT_SITE,
                 TIME, LOB, REP_NM, EMP_NUM, OPEN_DATE
    EXTRA (21): 最寄駅, アクセス, 初出時の職種コード,
                募集職種, 募集人数, 雇用形態, 給与, 給与補足, 諸手当, 仕事内容,
                応募資格, 勤務時間・休憩, 休日休暇, 年間休日, 加入保険, 社会保険,
                選考プロセス,
                休診日, 募集求人 (勤務先情報 dl), 法人名, 本社住所 (法人情報 dl)
                （各 dl の役割は dt ラベルで判定。募集要項/応募方法の項目は部分一致で引当）

    ※ 本社住所は法人情報 dl の「住所」(郵便番号つき) で、勤務先住所 (Schema.ADDR) とは別物。
      ADDR を上書きせず独立カラムとして保持する。

取得フロー:
    for code in CATEGORY_CODES:
        for page in 1..N:                    # page=1 にカードがなければ page=2 へスキップ
            /{code}?page={page} を GET
            div.box.box-jobitem から求人 URL 収集
            各求人 URL → dl.l-def 解析
              → (勤務先名, 正規化住所) が初出なら yield

実行方法:
    python scripts/sites/jobs/guppy.py
    python bin/run_flow.py --site-id guppy
"""

import re
import sys
from pathlib import Path

root_path = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_BASE_URL = "https://www.guppy.jp"

# サイト「職種を選択」セクションから抽出した 57 職種コード
CATEGORY_CODES = [
    # 歯科
    "dds", "dh", "dt", "da",
    # 医科・薬剤
    "ic", "md", "apo", "ns", "pns", "phn", "mw",
    # 検査・技術
    "rt", "mt", "me",
    # リハビリ
    "st", "ort", "pt", "ot",
    # 心理
    "cp", "cpp",
    # 管理・補助
    "him", "ra", "na", "pa", "po",
    # 栄養・調理
    "nrd", "nu", "ck", "ks",
    # 福祉・事務
    "msw", "wc", "mc",
    # 介護
    "hh", "ccw", "csw", "psw", "cm", "fd", "swo", "spm", "fss", "ca",
    "cgw", "sw", "ls",
    # 保育
    "kt", "cw", "acw",
    # その他
    "cc", "jdr", "acu", "mas", "bwt", "th",
    # 治験・販売
    "cra", "crc", "otc",
]

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_ADDRESS_NOISE_PATTERN = re.compile(r"Googleマップで表示|Googleマップで見る")

# 募集要項 (dl.l-def[0]) / 応募方法 (dl.l-def[3]) から引き当てる EXTRA カラム。
# 値は照合候補ラベルで、先頭から順に「完全一致 → 部分一致」で探す。
# サイト側のラベル表記ゆれ（「諸手当の内訳」「選考の流れ」等）を部分一致で吸収する。
_JOB_FIELD_LABELS: dict[str, list[str]] = {
    "募集職種": ["募集職種", "職種"],
    "募集人数": ["募集人数", "採用人数", "募集数", "人数"],
    "雇用形態": ["雇用形態", "勤務形態"],
    "給与": ["給与", "給与について", "給料", "報酬", "月給"],
    "給与補足": ["給与補足", "給与の補足", "給与備考"],
    "諸手当": ["諸手当", "諸手当の内訳", "待遇・福利厚生", "福利厚生", "手当"],
    "仕事内容": ["仕事内容", "業務内容", "職務内容"],
    "応募資格": ["応募資格", "必要資格", "募集資格", "求める人材", "資格"],
    "勤務時間・休憩": ["勤務時間・休憩", "勤務時間", "就業時間", "勤務時間帯"],
    "休日休暇": ["休日休暇", "休日・休暇", "休暇", "休日"],
    "年間休日": ["年間休日"],
    "加入保険": ["加入保険", "各種保険", "保険"],
    "社会保険": ["社会保険", "社会保険完備"],
    "選考プロセス": ["選考プロセス", "選考の流れ", "選考方法", "選考"],
}

# 部分一致のときに「他カラムの正式ラベル」を横取りしないための除外集合。
# 例: 「給与」の部分一致が「給与補足」を拾ってしまうのを防ぐ。
_JOB_FIELD_EXCLUSIVE_LABELS: dict[str, set[str]] = {
    column: {
        label
        for other, labels in _JOB_FIELD_LABELS.items()
        if other != column
        for label in labels
    }
    for column in _JOB_FIELD_LABELS
}

# 勤務先情報 dl から引き当てる EXTRA カラム（表記ゆれを部分一致で吸収）
_PLACE_FIELD_LABELS: dict[str, list[str]] = {
    "休診日": ["休診日", "定休日", "休業日", "休診"],
    "募集求人": ["募集求人", "募集中の求人"],
}

# 法人情報 dl から引き当てる EXTRA カラム。
# 「本社住所」は法人情報 dl の「住所」(郵便番号つき) を指し、
# 勤務先住所 (Schema.ADDR) とは別カラムとして保持する。
_CORP_FIELD_LABELS: dict[str, list[str]] = {
    "法人名": ["法人名", "法人"],
    "本社住所": ["住所", "本社住所", "所在地"],
}

# dl.l-def の役割判定に使う dt ラベル。位置インデックス固定だと
# dl が 4 つ未満のページでカラムがずれるため、ラベルで役割を決める。
_DL_ROLE_MARKERS: list[tuple[str, tuple[str, ...]]] = [
    ("place", ("勤務先名",)),
    ("corp", ("法人名", "代表者")),
    ("apply", ("選考プロセス", "選考方法", "提出書類")),
    ("req", ("募集職種", "雇用形態", "給与")),
]

# ラベル判定が空振りしたときのフォールバック位置（従来の固定インデックス）
_DL_ROLE_FALLBACK_INDEX: dict[str, int] = {
    "req": 0,
    "place": 1,
    "corp": 2,
    "apply": 3,
}


class GuppyScraper(StaticCrawler):
    """GUPPY 全職種・事業所単位 スクレイパー"""

    DELAY = 0.7
    EXTRA_COLUMNS = [
        "最寄駅",
        "アクセス",
        "初出時の職種コード",
        *_JOB_FIELD_LABELS.keys(),
        *_PLACE_FIELD_LABELS.keys(),
        *_CORP_FIELD_LABELS.keys(),
    ]

    def parse(self, url: str):
        """
        57 職種 × 全ページを巡回し、(勤務先名, 住所) をキーに事業所単位で yield する。
        引数 `url` を起点(ルート)として利用する。
        """
        base = url.rstrip("/")

        seen_keys: set[tuple[str, str]] = set()
        total_visited = 0
        total_yielded = 0

        for code in CATEGORY_CODES:
            # ページごとに求人 URL を収集し、その場で詳細を取得して即 yield する
            # （全件収集してから yield すると最初の 1 件までが遅くなるため禁止）
            for d_url in self._iter_detail_urls(base, code):
                total_visited += 1
                try:
                    item = self._scrape_facility(d_url, code)
                    if item is None:
                        continue
                    key = self._facility_key(item)
                    if key is None or key in seen_keys:
                        continue
                    seen_keys.add(key)
                    yield item
                    total_yielded += 1
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", d_url, e)
                    continue
            self.logger.info(
                "[%s] 処理完了。訪問 %d / 累計事業所 %d",
                code, total_visited, total_yielded,
            )

        self.logger.info(
            "全体完了: 求人 %d 件アクセス → 事業所 %d 件",
            total_visited, total_yielded,
        )

    # ------------------------------------------------------------------
    # 求人URL収集
    # ------------------------------------------------------------------

    def _iter_detail_urls(self, base: str, code: str):
        """1 職種について ?page=1 から順に全ページを巡回し、求人 URL を
        重複排除しつつ 1 件ずつ yield するジェネレータ。
        page=1 にカードがない（ランディング扱い）場合は page=2 へスキップする。
        全件を貯めてから返さないことで最初の 1 件までの時間を短くする。
        """
        seen: set[str] = set()
        total = 0
        page = 1
        while True:
            list_url = f"{base}/{code}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                if page == 1:
                    # page=1 がランディング or 404 → page=2 を試みる
                    page = 2
                    continue
                break

            cards = soup.select("div.box.box-jobitem")
            if not cards:
                if page == 1:
                    # page=1 はカード 0 件のランディング → page=2 へ
                    page = 2
                    continue
                break

            new_on_page = 0
            for card in cards:
                a = card.select_one(f'a.box-jobitem-block[href^="/{code}/"]')
                if a is None:
                    a = card.select_one('a[href^="/"]')
                if not a:
                    continue
                href = a.get("href", "")
                if not href:
                    continue
                if href.startswith("/"):
                    href = base + href
                href = re.sub(r"\?.*$", "", href)
                if not re.match(rf"^{re.escape(base)}/{code}/\d+$", href):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                new_on_page += 1
                total += 1
                yield href

            self.logger.info(
                "[%s] page=%d: %d cards, %d new (累計 %d)",
                code, page, len(cards), new_on_page, total,
            )

            next_link = soup.select_one(f'a[href*="page={page + 1}"]')
            if not next_link:
                break
            page += 1

    # ------------------------------------------------------------------
    # 詳細ページ → 事業所単位レコード構築
    # ------------------------------------------------------------------

    def _scrape_facility(self, detail_url: str, code: str) -> dict | None:
        """求人詳細ページから事業所単位のレコード dict を返す。NAME 不在なら None。"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item: dict = {Schema.URL: detail_url, "初出時の職種コード": code}

        # dl.l-def は想定 4 つ: 募集要項 / 勤務先情報 / 法人情報 / 応募方法
        # dl 個数が 4 未満のページでもズレないよう dt ラベルで役割を判定する
        dls = soup.select("dl.l-def")
        dl_dicts = [self._dl_to_dict(dl) for dl in dls]
        roles = self._classify_dls(dl_dicts)
        req: dict = roles["req"]
        place: dict = roles["place"]
        corp: dict = roles["corp"]
        apply_: dict = roles["apply"]

        # 募集要項 / 応募方法 の項目（ラベル表記ゆれは部分一致で吸収）
        for column in _JOB_FIELD_LABELS:
            value = self._pick_label(req, column) or self._pick_label(apply_, column)
            if value:
                item[column] = value

        # 勤務先情報 / 法人情報 の追加項目
        for column in _PLACE_FIELD_LABELS:
            value = self._pick_label(place, column, _PLACE_FIELD_LABELS)
            if value:
                item[column] = value
        for column in _CORP_FIELD_LABELS:
            value = self._pick_label(corp, column, _CORP_FIELD_LABELS)
            if value:
                item[column] = value

        name = place.get("勤務先名", "")
        if name:
            name = re.sub(r"\s*スピード返信.*$", "", name).strip()
            item[Schema.NAME] = name

        address_raw = place.get("住所", "")
        if address_raw:
            cleaned = _ADDRESS_NOISE_PATTERN.sub("", address_raw)
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            m = _PREF_PATTERN.match(cleaned)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = cleaned[m.end():].strip()
            else:
                item[Schema.ADDR] = cleaned

        hp = place.get("ホームページ", "")
        if hp:
            m = re.search(r"https?://\S+", hp)
            item[Schema.HP] = m.group(0) if m else hp.strip()

        if place.get("業種"):
            item[Schema.CAT_SITE] = place["業種"]
        if place.get("診療時間"):
            item[Schema.TIME] = place["診療時間"]
        if place.get("最寄駅"):
            item["最寄駅"] = place["最寄駅"]
        if place.get("アクセス"):
            item["アクセス"] = place["アクセス"]

        if corp.get("事業内容"):
            item[Schema.LOB] = corp["事業内容"]
        if corp.get("代表者"):
            item[Schema.REP_NM] = corp["代表者"]
        if corp.get("従業員"):
            item[Schema.EMP_NUM] = corp["従業員"]
        # 設立年月日: 法人情報の「設立」優先、無ければ勤務先情報の「設立年」で補完
        open_date = corp.get("設立") or place.get("設立年") or place.get("設立")
        if open_date:
            item[Schema.OPEN_DATE] = open_date

        if Schema.NAME not in item:
            return None
        return item

    @staticmethod
    def _classify_dls(dl_dicts: list[dict[str, str]]) -> dict[str, dict[str, str]]:
        """dl.l-def 群を dt ラベルで役割（req/place/corp/apply）に振り分ける。

        _DL_ROLE_MARKERS のいずれかを dt ラベルが含む dl を、その役割に割り当てる
        （1 dl は 1 役割まで）。該当が無い役割は従来の位置インデックス
        （_DL_ROLE_FALLBACK_INDEX）にフォールバックする。
        """
        roles: dict[str, dict[str, str]] = {}
        used: set[int] = set()

        for role, markers in _DL_ROLE_MARKERS:
            for i, d in enumerate(dl_dicts):
                if i in used or not d:
                    continue
                if any(marker in label for label in d for marker in markers):
                    roles[role] = d
                    used.add(i)
                    break

        for role, index in _DL_ROLE_FALLBACK_INDEX.items():
            if role in roles:
                continue
            if index < len(dl_dicts) and index not in used:
                roles[role] = dl_dicts[index]
                used.add(index)
            else:
                roles[role] = {}
        return roles

    @staticmethod
    def _pick_label(
        d: dict[str, str],
        column: str,
        label_map: dict[str, list[str]] | None = None,
    ) -> str:
        """dl 辞書から column に対応する値を取り出す。

        label_map（既定は _JOB_FIELD_LABELS）の候補ラベルを先頭から順に
        「完全一致 → 部分一致（label が候補を含む / 候補が label を含む）」で照合する。
        部分一致では同じ label_map 内の他カラムの正式ラベル
        （例 給与 に対する 給与補足）を除外して値のズレを防ぐ。
        見つからなければ空文字。
        """
        if not d:
            return ""
        if label_map is None or label_map is _JOB_FIELD_LABELS:
            label_map = _JOB_FIELD_LABELS
            blocked = _JOB_FIELD_EXCLUSIVE_LABELS.get(column, set())
        else:
            blocked = {
                label
                for other, labels in label_map.items()
                if other != column
                for label in labels
            }
        candidates = label_map.get(column, [column])
        for cand in candidates:
            if d.get(cand):
                return d[cand]
        for cand in candidates:
            for label, value in d.items():
                if not value or label in blocked:
                    continue
                if cand in label or label in cand:
                    return value
        return ""

    @staticmethod
    def _facility_key(item: dict) -> tuple[str, str] | None:
        """事業所識別キー: (勤務先名, 住所) を空白除去で正規化したタプル"""
        name = item.get(Schema.NAME, "")
        addr = item.get(Schema.ADDR, "")
        if not name:
            return None
        norm_name = re.sub(r"\s+", "", name)
        norm_addr = re.sub(r"\s+", "", addr)
        return (norm_name, norm_addr)

    @staticmethod
    def _dl_to_dict(dl) -> dict[str, str]:
        """dl の dt/dd ペアを辞書化（空白は単一スペース正規化、先勝ち）"""
        result: dict[str, str] = {}
        dts = dl.find_all("dt", recursive=True)
        dds = dl.find_all("dd", recursive=True)
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True)
            if not key:
                continue
            value = dd.get_text(" ", strip=True)
            value = re.sub(r"\s+", " ", value).strip()
            if key not in result and value:
                result[key] = value
        return result


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GuppyScraper()
    scraper.execute("https://www.guppy.jp/")

    print("\n" + "=" * 60)
    print("📊 実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
