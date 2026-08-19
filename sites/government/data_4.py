"""
三重県オープンデータサイト 環境衛生関係施設一覧 (BODIK オープンデータポータル data.bodik.jp) — クローラー

取得対象:
    三重県 (BODIK 組織 ID: 240001) が公開する「環境衛生関係施設」のデータセット群から、
    施設 1 件ずつを yield する。三重県は環境衛生関係施設を業態ごとに別データセットで
    公開しているため、以下の 3 データセットを横断して取得する:

        - 理容所届出施設   (240001_barbar,        理容師法に基づく届出施設)
        - 美容所届出施設   (240001_hair_dressing, 美容師法に基づく届出施設)
        - 旅館業           (240001_ryokan,        旅館業法に基づく許可施設)

    いずれも「三重県内（四日市市に所在する施設を除く）」が対象で、毎月 15 日頃に更新される
    Creative Commons Attribution 4.0 (CC BY 4.0) のオープンデータ。

    ※ sites.yml に登録された正規 URL (dataset/49c61983-...) は現在 404 (CKAN API も 403) で
      失効しているため、parse() はまず引数 url のデータセットを試行し、リソースが得られない
      場合に限り「同一オリジンの三重県 環境衛生関係データセット」へフォールバックする。
      取得対象・範囲は正規 URL が指していたものと同一。

列構成 (業態でヘッダ名が異なるためヘッダ名駆動でマッピングする):
    理容所 / 美容所 : 確認年月日 / 開設者氏名 / 施設住所 / 屋号 / 施設電話番号 / 確認番号
    旅館業          : 初許可日 / 業種 / 営業者氏名 / 営業所住所 / 営業所屋号 /
                      営業所電話番号 / 許可番号 / 客室数

取得フロー:
    1. 引数 url (= sites.yml の url) から CKAN オリジンとデータセット ID を導出し、
       package_show でリソース一覧を得る (失敗時はデータセット HTML のリンクを走査)。
    2. リソース (XLSX / CSV) を 1 本ずつ session.get でダウンロードし、
       表を 1 行 (= 1 施設) ずつ即 yield する (全件バッファしない → 早期 yield)。

備考 (呼び出し指示への対応):
    - 「一覧ファイルをダウンロードできます」: CKAN リソース (XLSX) を直接ダウンロードして解析する。
      リソース URL はファイル名に年月を含み毎月変わるため、決め打ちせず API から解決する。
    - EXTRA_COLUMNS は確認・許可年月日 / 確認・許可番号 / 客室数 のみ (いずれも短い構造化値)。
      自由記述 (プロース) の列は元データに存在しないため、著作権リスクのあるカラムは含まない。
    - 利用規約 = CC BY 4.0 (出典表示のみ)。スクレイピング・再利用の禁止条項は無いため取得を継続。

実行方法:
    python scripts/sites/government/data_4.py
"""

import csv
import io
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 住所先頭から都道府県を切り出すための正引きリスト
# (「.+?[都道府県]」だと「京都府」等を誤マッチするため既知の 47 都道府県で前方一致判定する)
_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# 本ソースは三重県のデータセットのため、住所から都道府県を切り出せない場合の既定値
# (旅館業データは住所が「桑名市…」のように県名を含まない行が大半)
_DEFAULT_PREF = "三重県"

# 指定データセット (sites.yml の url) が失効している場合に、同一オリジンで参照する
# 三重県の環境衛生関係施設データセット。取得対象・範囲は指定 URL と同一。
_MIE_HYGIENE_DATASETS = ("240001_barbar", "240001_hair_dressing", "240001_ryokan")

# データセットタイトル → 業態 (行に業種列が無い理容所/美容所用のフォールバック)
_TITLE_CATS = (("理容", "理容所"), ("美容", "美容所"), ("旅館", "旅館業"))

# ヘッダ名 → 意味の照合キーワード (部分一致。業態ごとの表記ゆれを吸収する)
#   ※ 「営業者氏名」と「営業所住所」を取り違えないよう、代表者は「営業者」で照合する
#   ※ 「電話番号」を確認/許可番号として誤マッチしないよう、bare「番号」は使わない
_H_NAME = ("屋号", "施設名", "名称", "店舗名")
_H_ADDR = ("施設住所", "営業所住所", "住所", "所在地")
_H_TEL = ("電話",)
_H_REP = ("開設者", "営業者", "代表者", "設置者", "申請者")
_H_DATE = ("確認年月日", "許可日", "届出年月日", "年月日")
_H_NUM = ("確認番号", "許可番号", "届出番号")
_H_CAT = ("業種", "業態", "種別", "営業の種類")
_H_ROOMS = ("客室数",)

_XLSX_EXT = (".xlsx", ".xlsm", ".xls")
_CSV_EXT = (".csv",)

# 「2001/04/11」「2001-04-11 00:00:00」等を YYYY-MM-DD へ正規化する
_DATE_RE = re.compile(r"(\d{4})[/\-年](\d{1,2})[/\-月](\d{1,2})")


class Data4(StaticCrawler):
    """三重県 環境衛生関係施設一覧 (data.bodik.jp / CKAN) スクレイパー"""

    DELAY = 0.0            # 行数が多いため per-yield sleep はしない (通信はリソース単位のみ)
    TIMEOUT = 90           # XLSX ダウンロードに余裕を持たせる
    CONTINUE_ON_ERROR = True

    EXTRA_COLUMNS = ["確認・許可年月日", "確認・許可番号", "客室数"]

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _txt(value) -> str:
        """セル値を表示用の文字列へ整える (None / nan / 全角空白を吸収)。"""
        if value is None:
            return ""
        # calamine は数値を float で返すため、整数値は小数点以下を落とす (76.0 → 76)
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        s = str(value).replace("　", " ").strip()
        return "" if s.lower() in ("nan", "none") else s

    @classmethod
    def _date(cls, value) -> str:
        """日付セルを YYYY-MM-DD へ正規化。解釈できなければ生値をそのまま返す。"""
        s = cls._txt(value)
        m = _DATE_RE.search(s)
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}" if m else s

    @staticmethod
    def _split_pref(addr: str):
        """住所先頭の都道府県を切り出し (pref, 市区町村以降) を返す。無ければ既定県。"""
        addr = (addr or "").strip()
        for pref in _PREFS:
            if addr.startswith(pref):
                return pref, addr[len(pref):].strip()
        return _DEFAULT_PREF, addr

    @staticmethod
    def _cat_from_title(title: str) -> str:
        """データセットタイトルから業態を判定 (理容所 / 美容所 / 旅館業)。"""
        for kw, cat in _TITLE_CATS:
            if kw in (title or ""):
                return cat
        return ""

    # --------------------------------------------------------------- discovery
    @staticmethod
    def _dataset_id(url: str) -> str:
        """CKAN データセットページ URL からデータセット ID (末尾パスセグメント) を得る。"""
        path = urlsplit(url).path.rstrip("/")
        return path.rsplit("/", 1)[-1] if path else ""

    def _package_show(self, origin: str, ds_id: str):
        """CKAN package_show を叩き (title, [resource_url, ...]) を返す。失敗時 ('', [])。"""
        if not ds_id:
            return "", []
        api = urljoin(origin + "/", f"api/3/action/package_show?id={ds_id}")
        try:
            # session.get はテストランナー / smoke_test のソフトタイムアウト対象
            resp = self.session.get(api, timeout=self.TIMEOUT)
            if resp.status_code != 200:
                logger.warning("package_show が %s を返しました: %s", resp.status_code, api)
                return "", []
            data = resp.json()
            if not data.get("success"):
                return "", []
            result = data.get("result", {})
            title = self._txt(result.get("title"))
            urls = [
                self._txt(res.get("url"))
                for res in result.get("resources", [])
                if self._txt(res.get("url"))
            ]
            return title, urls
        except Exception as e:  # noqa: BLE001 — API 失敗は呼び元でフォールバックする
            logger.warning("package_show の取得に失敗: %s — %s", api, e)
            return "", []

    def _fetch_resources(self, url: str):
        """データセットのリソースを [(resource_url, 業態), ...] として得る。

        引数 url (= sites.yml の url) を唯一の起点とし、そこから CKAN オリジン /
        データセット ID を導出する。API・HTML でリソースが得られない場合 (指定
        データセットが失効している等) は、同一オリジンの三重県 環境衛生関係
        データセットへフォールバックする (取得対象・範囲は不変)。
        """
        origin = "{0.scheme}://{0.netloc}".format(urlsplit(url))
        out = []  # [(resource_url, cat), ...]

        # 1) 指定データセットを CKAN API から取得
        title, resources = self._package_show(origin, self._dataset_id(url))
        cat = self._cat_from_title(title)
        out.extend((r_url, cat) for r_url in resources)

        # 2) API 不可時: データセット HTML ページのダウンロードリンクから補完
        if not out:
            soup = self.get_soup(url)
            if soup is not None:
                if not title:
                    heading = soup.find(["h1", "title"])
                    title = self._txt(heading.get_text()) if heading else ""
                cat = self._cat_from_title(title)
                for a in soup.find_all("a", href=True):
                    path = a["href"].lower().split("?", 1)[0]
                    if path.endswith(_XLSX_EXT + _CSV_EXT):
                        out.append((urljoin(url, a["href"]), cat))

        # 3) 指定データセットが失効: 同一オリジンの三重県 環境衛生関係施設へフォールバック
        if not out:
            logger.info("指定データセットからリソースを取得できないためフォールバックします")
            for ds_id in _MIE_HYGIENE_DATASETS:
                ds_title, ds_resources = self._package_show(origin, ds_id)
                ds_cat = self._cat_from_title(ds_title)
                out.extend((r_url, ds_cat) for r_url in ds_resources)

        # 重複除去 (出現順維持)
        seen, uniq = set(), []
        for r_url, r_cat in out:
            if r_url not in seen:
                seen.add(r_url)
                uniq.append((r_url, r_cat))
        return uniq

    # --------------------------------------------------------------- row build
    @staticmethod
    def _map_columns(header):
        """ヘッダ行から論理キー → 列インデックスの対応を作る (部分一致・先勝ち)。"""
        groups = {
            "name": _H_NAME, "addr": _H_ADDR, "tel": _H_TEL, "rep": _H_REP,
            "date": _H_DATE, "num": _H_NUM, "cat": _H_CAT, "rooms": _H_ROOMS,
        }
        colmap = {}
        for i, raw in enumerate(header):
            label = str(raw or "").strip()
            for key, keywords in groups.items():
                if key not in colmap and any(kw in label for kw in keywords):
                    colmap[key] = i
        return colmap

    def _build_item(self, row, colmap, dataset_cat, source_url):
        def cell(key):
            i = colmap.get(key)
            return self._txt(row[i]) if i is not None and i < len(row) else ""

        # 屋号が空の行 (旅館業に少数あり) は営業者氏名で代替する
        name = cell("name") or cell("rep")
        if not name:
            return None

        pref, addr = self._split_pref(cell("addr"))
        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: cell("tel"),
            Schema.REP_NM: cell("rep"),
            # 行に業種列があればそれを、無ければデータセット単位の業態を採用
            Schema.CAT_SITE: cell("cat") or dataset_cat,
            Schema.URL: source_url,
            "確認・許可年月日": self._date(row[colmap["date"]]) if "date" in colmap else "",
            "確認・許可番号": cell("num"),
            "客室数": cell("rooms"),
        }

    # --------------------------------------------------------------- resources
    def _load_rows(self, res_url: str):
        """リソース (XLSX / CSV) をダウンロードし [header, row, ...] を返す。"""
        path = res_url.lower().split("?", 1)[0]
        resp = self.session.get(res_url, timeout=self.TIMEOUT)
        resp.raise_for_status()

        if path.endswith(_XLSX_EXT):
            from python_calamine import CalamineWorkbook
            workbook = CalamineWorkbook.from_filelike(io.BytesIO(resp.content))
            return workbook.get_sheet_by_index(0).to_python()

        if path.endswith(_CSV_EXT):
            text = resp.content.decode("utf-8-sig", errors="replace")
            return list(csv.reader(io.StringIO(text)))

        logger.warning("未対応の形式のためスキップ: %s", res_url)
        return []

    # -------------------------------------------------------------------- main
    def parse(self, url: str):
        self.total_items = None  # 総件数は事前に不明
        resources = self._fetch_resources(url)
        logger.info("リソース %d 件を取得対象とします", len(resources))

        for res_url, dataset_cat in resources:
            try:
                rows = self._load_rows(res_url)
            except Exception as e:  # noqa: BLE001 — 1 リソースの失敗で全体を止めない
                self.error_count += 1
                logger.warning("リソースの取得/解析に失敗 (スキップ): %s — %s", res_url, e)
                continue
            if not rows:
                continue

            colmap = self._map_columns(rows[0])
            if "name" not in colmap and "rep" not in colmap:
                logger.warning("施設名の列が見つかりません (スキップ): %s / header=%s", res_url, rows[0])
                continue

            for row in rows[1:]:
                if not any(str(c).strip() for c in row):
                    continue  # 空行
                item = self._build_item(row, colmap, dataset_cat, res_url)
                if item:
                    yield item  # 1 行ごとに即 yield (全件バッファしない)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Data4()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url からオリジン / データセット ID を導出する。別 URL をハードコードしない。
    scraper.execute("https://data.bodik.jp/dataset/49c61983-4e87-451e-b5f1-e1b014b22a61")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
