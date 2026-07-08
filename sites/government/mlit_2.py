"""
一般貨物自動車運送事業者 (国土交通省 各運輸局公示 事業者一覧) — 全国横断クローラー

取得対象:
    各地方運輸局 (＋沖縄総合事務局) が公示する「一般貨物自動車運送事業者一覧
    (営業所一覧)」の Excel (.xlsx/.xls) を全国分横断で取得する。
    1 行 = 1 営業所で、事業者名・事業者住所・代表者氏名・営業所名称・営業所所在地・
    保有車両数 (普通車/小型車/牽引車/被牽引車) を持つ。

取得フロー:
    一般貨物自動車運送事業者の一覧は www.mlit.go.jp 直下ではなく、8 地方運輸局＋
    北陸信越＋沖縄総合事務局の計 10 の「運輸局サイト (wwwtb.mlit.go.jp/* 及び
    ogb.go.jp)」に連邦的に分散して公示されている。起点 URL (sites.yml の url =
    国交省 自動車ページ) からはリンク到達できないため、各運輸局の一覧掲載 HTML
    ページを構造定数 (_BUREAUS) として保持し、そのページ内の Excel リンクを動的に
    抽出 → ダウンロード → 1 行ずつ即 yield する。Excel の列レイアウトは運輸局ごと
    に異なるため、列位置ではなくヘッダ名で列を特定する (position 非依存)。
    「一般貨物」判定は普通車/牽引車等の車両列の有無で行い、貨物軽自動車・国際・
    申請様式等の Excel は自動的に除外する。

利用規約:
    国土交通省ウェブサイトは「公共データ利用規約 (PDL1.0)」に準拠し、出典表示を
    条件に複製・二次利用が許可されている (スクレイピングの明示的禁止規定は無し)。
    Schema.URL に取得元 Excel の URL を記録し出典を保持する。

備考 (呼び出し指示):
    - 「全国分を集めたい」→ 10 運輸局すべてを巡回 (行レベルの追加フィルタは無し)。
    - 沖縄は国土交通省ではなく沖縄総合事務局 (ogb.go.jp)。
    - 自由記述の文章カラムは無い (全カラムが名称・コード・数値・住所)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/mlit_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id mlit_2
"""

import io
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import pandas as pd

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# 構造定数: 各運輸局の「一般貨物自動車運送事業者一覧」掲載 HTML ページ
#   ・Excel の直リンク番号 (000xxxxxx.xlsx) は更新のたびに変わるため直書きせず、
#     比較的安定な一覧掲載ページを保持し、ページ内リンクから動的に解決する。
#   ・ページ URL が変わった / 掲載が無い運輸局は 0 件になるだけで他に影響しない
#     (CONTINUE_ON_ERROR + カテゴリ判定で安全にスキップ)。
# --------------------------------------------------------------------------- #
_BUREAUS: list[tuple[str, str]] = [
    ("北海道運輸局", "https://wwwtb.mlit.go.jp/hokkaido/bunyabetsu/jidousya/kamotsu/index.html"),
    ("東北運輸局", "https://wwwtb.mlit.go.jp/tohoku/youshiki/unnsoujigyou/trk/trk.html"),
    ("関東運輸局", "https://wwwtb.mlit.go.jp/kanto/jidou_koutu/kamotu/kamotu_jigyoukaisi/index.html"),
    ("北陸信越運輸局", "https://wwwtb.mlit.go.jp/hokushin/hrt54/track/kannnai_jigyousyaichiran.html"),
    ("中部運輸局", "https://wwwtb.mlit.go.jp/chubu/koukai/koukai_index.html"),
    ("近畿運輸局", "https://wwwtb.mlit.go.jp/kinki/00001_03125.html"),
    ("中国運輸局", "https://wwwtb.mlit.go.jp/chugoku/txt/jidousyakoutsubu.html"),
    ("四国運輸局", "https://wwwtb.mlit.go.jp/shikoku/soshiki/jidousya.html"),
    ("九州運輸局", "https://wwwtb.mlit.go.jp/kyushu/00001_00824.html"),
    ("沖縄総合事務局", "https://www.ogb.go.jp/unyu"),
]

# 一覧 Excel をダウンロードする前段の絞り込み (無関係な様式・報告書の DL を避ける)
_INCLUDE_HINT = ("一覧", "現在", "事業者", "営業所")
_EXCLUDE_HINT = (
    "様式", "記載例", "記入例", "申請", "届出書", "手引", "マニュアル",
    "チェック", "新規", "処分", "国際", "利用運送", "定期報告", "報告書",
    "委任", "誓約", "見方", "運賃", "料金",
)

# 一般貨物の判定に使う車両列 (これらが 1 つでも有れば一般貨物、無ければ貨物軽等)
_VEHICLE_LABELS = ("普通車", "小型車", "牽引車", "被牽引車")

# 都道府県 (住所先頭一致で PREF を切り出す)
_PREFS = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)

# 1 運輸局ページからダウンロードする Excel の上限 (様式ページの暴発防止)
_MAX_XLSX_PER_PAGE = 15


class Mlit2(StaticCrawler):
    """国土交通省 各運輸局 一般貨物自動車運送事業者一覧 (全国) スクレイパー"""

    DELAY = 1.5
    TIMEOUT = 60  # 大きめの Excel (最大 ~2MB) のダウンロードに耐える
    CONTINUE_ON_ERROR = True

    # Schema に該当しないサイト固有カラム (全て名称/コード/数値/住所 — 自由記述は無い)
    EXTRA_COLUMNS = [
        "運輸局",        # 公示元の地方運輸局名
        "事業者番号",    # 事業者番号 (掲載がある運輸局のみ)
        "営業所名称",    # 営業所の名称
        "営業所所在地",  # 営業所の所在地
        "普通車",        # 保有車両数
        "小型車",
        "牽引車",
        "被牽引車",
    ]

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化する (nan/None は空文字、空白は正規化)。"""
        if value is None:
            return ""
        s = str(value).strip()
        if not s or s.lower() == "nan":
            return ""
        return re.sub(r"\s+", " ", s.replace("　", " ")).strip()

    @staticmethod
    def _norm(value) -> str:
        """ヘッダ照合用: 空白・改行を全除去した文字列。"""
        return re.sub(r"\s+", "", str(value)).replace("　", "")

    def _split_pref(self, address: str) -> tuple[str, str]:
        """住所先頭の都道府県を切り出す。ADDR は元の完全住所を返す。"""
        for pref in _PREFS:
            if address.startswith(pref):
                return pref, address
        return "", address

    def _want_download(self, text: str) -> bool:
        """リンク周辺テキストから、一覧 Excel をダウンロードすべきか判定する。"""
        if any(k in text for k in _EXCLUDE_HINT):
            return False
        return any(k in text for k in _INCLUDE_HINT)

    def _xlsx_links(self, soup, page_url: str) -> list[str]:
        """ページ内の一覧 Excel リンク (絶対 URL) を抽出する。"""
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not re.search(r"\.xlsx?($|\?)", href, re.I):
                continue
            abs_url = urljoin(page_url, href)
            if abs_url in seen:
                continue
            # 判定はアンカー文字列 (＋title 属性) のみで行う。親ブロックまで見ると
            # 隣接する様式・申請リンクの除外語を巻き込み一覧まで誤除外するため。
            text = a.get_text(" ", strip=True) + " " + (a.get("title") or "")
            if not self._want_download(text):
                continue
            seen.add(abs_url)
            urls.append(abs_url)
            if len(urls) >= _MAX_XLSX_PER_PAGE:
                logger.info("Excel リンクが上限 %d 件に達したため打ち切り: %s", _MAX_XLSX_PER_PAGE, page_url)
                break
        return urls

    def _read_xlsx(self, url: str) -> pd.DataFrame | None:
        """Excel を取得し DataFrame (ヘッダ無し) を返す。
        session.get はテストランナーのソフトタイムアウト対象 (get_soup と同経路)。"""
        resp = self.session.get(url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        engine = "calamine" if url.lower().rsplit("?", 1)[0].endswith("xlsx") else None
        return pd.read_excel(io.BytesIO(resp.content), engine=engine, header=None, dtype=object)

    def _build_colmap(self, header: list[str]) -> dict | None:
        """ヘッダ行 (正規化済み) から列インデックスの対応を組む。
        一般貨物一覧でなければ (事業者名 or 車両列が無ければ) None を返す。"""
        name_idx = next((i for i, h in enumerate(header) if "事業者名" in h), None)
        if name_idx is None:
            return None

        # 車両列 (一般貨物判定 + EXTRA)。"普通運行車" 等は除外し完全一致を優先。
        vehicles: dict[str, int] = {}
        for label in _VEHICLE_LABELS:
            idx = next(
                (i for i, h in enumerate(header) if h == label),
                next((i for i, h in enumerate(header) if label in h and "運行" not in h), None),
            )
            if idx is not None:
                vehicles[label] = idx
        if not vehicles:  # 貨物軽 (軽/二輪のみ) 等は対象外
            return None

        # 事業者住所: 位置 (実住所) を優先、次に郵便/電話/番号を含まない列
        addr_cands = [i for i, h in enumerate(header) if "事業者住所" in h]
        addr_idx = next((i for i in addr_cands if "位置" in header[i]), None)
        if addr_idx is None:
            addr_idx = next(
                (i for i in addr_cands if not any(k in header[i] for k in ("郵便", "電話", "番号"))),
                addr_cands[0] if addr_cands else None,
            )

        rep_idxs = [i for i, h in enumerate(header) if "代表者" in h and "氏名" in h]
        num_idx = next((i for i, h in enumerate(header) if "事業者番号" in h), None)
        onm_idx = next((i for i, h in enumerate(header) if "営業所" in h and "名称" in h), None)
        oaddr_idx = next(
            (i for i, h in enumerate(header) if "営業所" in h and ("位置" in h or "住所" in h)),
            None,
        )
        return {
            "name": name_idx,
            "addr": addr_idx,
            "rep": rep_idxs,
            "num": num_idx,
            "onm": onm_idx,
            "oaddr": oaddr_idx,
            "vehicles": vehicles,
        }

    def _iter_rows(self, xlsx_url: str, bureau: str):
        """1 つの Excel を解析し、行を dict で yield する。"""
        df = self._read_xlsx(xlsx_url)
        if df is None or df.empty:
            return

        # ヘッダ行を探す (先頭 8 行以内で「事業者名」を含む行)
        header_row = None
        for i in range(min(8, len(df))):
            if any("事業者名" in str(v) for v in df.iloc[i].tolist()):
                header_row = i
                break
        if header_row is None:
            return

        header = [self._norm(v) for v in df.iloc[header_row].tolist()]
        cmap = self._build_colmap(header)
        if cmap is None:
            logger.info("一般貨物一覧では無いためスキップ (%s): %s", bureau, xlsx_url)
            return

        def cell(row: list, idx) -> str:
            return self._txt(row[idx]) if idx is not None and idx < len(row) else ""

        for _, row in df.iloc[header_row + 1:].iterrows():
            cells = row.tolist()
            name = cell(cells, cmap["name"])
            if not name:
                continue
            try:
                address = cell(cells, cmap["addr"])
                pref, addr = self._split_pref(address)
                rep = " ".join(filter(None, (cell(cells, i) for i in cmap["rep"])))
                yield {
                    Schema.NAME: name,
                    Schema.PREF: pref,
                    Schema.ADDR: addr,
                    Schema.REP_NM: rep,
                    Schema.URL: xlsx_url,
                    "運輸局": bureau,
                    "事業者番号": cell(cells, cmap["num"]),
                    "営業所名称": cell(cells, cmap["onm"]),
                    "営業所所在地": cell(cells, cmap["oaddr"]),
                    "普通車": cell(cells, cmap["vehicles"].get("普通車")),
                    "小型車": cell(cells, cmap["vehicles"].get("小型車")),
                    "牽引車": cell(cells, cmap["vehicles"].get("牽引車")),
                    "被牽引車": cell(cells, cmap["vehicles"].get("被牽引車")),
                }
            except Exception as e:  # noqa: BLE001 — 1 行失敗でも他は継続
                logger.warning("行の解析に失敗 (スキップ) [%s]: %s — %s", bureau, name, e)
                continue

    # ------------------------------------------------------------------ #
    # entry point
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        # 🔒 url は sites.yml の正規 URL (国交省 自動車ページ = SSOT/識別子)。
        #    一般貨物一覧は各運輸局サイトに連邦的に分散しており、この url から
        #    リンク到達できないため、掲載ページは _BUREAUS 定数で保持する。
        logger.info("起点 (正規 URL): %s — 全国 %d 運輸局を巡回", url, len(_BUREAUS))

        for bureau, page_url in _BUREAUS:
            soup = self.get_soup(page_url)  # ソフトタイムアウト対象
            if soup is None:
                logger.warning("運輸局ページ取得失敗 (スキップ): %s (%s)", bureau, page_url)
                continue
            xlsx_urls = self._xlsx_links(soup, page_url)
            if not xlsx_urls:
                logger.warning("一覧 Excel リンクが見つからず (スキップ): %s (%s)", bureau, page_url)
                continue
            logger.info("%s: %d 件の一覧 Excel 候補", bureau, len(xlsx_urls))
            for xlsx_url in xlsx_urls:
                try:
                    yield from self._iter_rows(xlsx_url, bureau)
                except Exception as e:  # noqa: BLE001 — 1 ファイル失敗でも他は継続
                    logger.warning("Excel 解析に失敗 (スキップ) [%s]: %s — %s", bureau, xlsx_url, e)
                    continue


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Mlit2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.mlit.go.jp/jidosha/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
