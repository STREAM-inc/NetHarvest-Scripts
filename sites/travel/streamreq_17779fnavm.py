"""
FNAVM (モロッコ旅行代理店全国連盟) — streamreq_17779fnavm

取得対象:
    モロッコで営業する旅行代理店 (agences de voyages) の会社名 / 住所 / TEL。

■ 正規 URL (sites.yml の url) の状況 — 2026-09 調査
    http://www.fnavm.org.ma (Fédération Nationale des Agences de Voyages du Maroc)
    は **ドメインが失効済み**。.ma レジストリ (ns3.registre.ma) が権威応答として
    NXDOMAIN を返すため、名前解決の時点でアクセスできない。
    Wayback Machine に残る 2011〜2015 年のスナップショット (全 573 URL) を
    確認したが、掲載されているのは理事会メンバー (les-membres-du-bureau, 8 名) と
    ニュース記事のみで、**会員旅行代理店の一覧ページは元々存在しない**。

■ 代替データソース (依頼備考の「モロッコ観光省の登録代理店リスト等を代替探索する」に従う)
    モロッコ公式オープンデータポータル data.gov.ma (CKAN) が公開する
    観光省 (Ministère du Tourisme, de l'Artisanat et de l'Economie Sociale
    et Solidaire) の 2 データセットを突き合わせて 1 件ずつ出力する。

      (1) liste-des-agences-de-voyages-au-maroc  … 主データ (.xls / 2022-04 更新)
          シート "AgencesVoyages" 1,245 行
          列: Nom_province / Denomination / Adresse / Tel / Fax
      (2) annuaire-des-agences-de-voyages        … 補完データ (.csv / UTF-16 TSV)
          846 行、列: Raison Sociale / Adresse / Ville / Coordonnées /
                      Agréments / Année
          Coordonnées に "Tél:… Fax:… <メールアドレス>" が同居しているため
          正規表現でメール・電話を分解し、会社名の正規化キーで (1) に結合する
          (1,245 件中 794 件がマッチ)。

    リソースの実 URL は年次で差し替わるため決め打ちせず、CKAN の
    package_show API (認証不要) でデータセット slug から解決する。
    API が落ちている場合のみ、確認済みの直リンクにフォールバックする。

■ 取得できない項目
    - 各代理店の公式サイト URL: (1)(2) いずれのデータセットにも列が無く、
      観光省の公開資料にも掲載が無いため取得不能 (Schema.HP は常に空)。
      Schema.URL には出典ファイルの URL を格納する。
    - 代表者名 / 従業員数 / 設立日: 同上 (元データに無し)。

■ 利用規約
    - data.gov.ma の当該 2 データセットはいずれも
      ODbL (Open Database License) で公開されており、再利用が明示的に許諾されている。
    - robots.txt は /core/ 等の静的アセットを Allow するのみで、
      データセットのダウンロードパスを禁止していない。
    - スクレイピング/自動収集を禁止する条項は確認できなかった。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_17779fnavm.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_17779fnavm
"""

import csv
import io
import json
import logging
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import pandas as pd

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 値が実質的に「無し」を表す表記 (元データの慣習)
_NULL_TOKENS = {"", "-", "--", "neant", "néant", "nan", "none", "n/a", "na"}

# Coordonnées 列 ("Tél:524457668 Fax:524457658  foo@bar.com") の分解用
_RE_TEL = re.compile(r"T[ée]l\s*[:.]?\s*([0-9+\s().-]{6,})", re.IGNORECASE)
_RE_FAX = re.compile(r"Fax\s*[:.]?\s*([0-9+\s().-]{6,})", re.IGNORECASE)
_RE_MAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")

# 会社名の末尾に付く注記 ("(EX:VITA CLUB)" など) を落として突き合わせ精度を上げる
_RE_PAREN = re.compile(r"[(（].*?[)）]")


class Streamreq17779Fnavm(StaticCrawler):
    """モロッコ旅行代理店一覧 (観光省オープンデータ) スクレイパー"""

    # 通信はデータセット解決 + ファイル取得の計 4 回のみ。
    # 以降はメモリ上の処理なので item 間の待機は不要。
    DELAY = 1.0
    ITEM_DELAY = 0.0
    TIMEOUT = 60

    EXTRA_COLUMNS = [
        "FAX",
        "認可番号",
        "認可年",
    ]

    # CKAN データセット slug → (リソース format, 直リンクフォールバック)
    _CKAN_API = "https://data.gov.ma/data/api/3/action/package_show?id={slug}"

    _MAIN_SLUG = "liste-des-agences-de-voyages-au-maroc"
    _MAIN_FALLBACK = (
        "https://data.gov.ma/data/fr/dataset/"
        "4081e159-650f-4b56-85b6-6fb110537e93/resource/"
        "7d67e000-8ee8-49ae-8790-519f230786bd/download/liste-agences-de-voyages.xls"
    )

    _SUB_SLUG = "annuaire-des-agences-de-voyages"
    _SUB_FALLBACK = (
        "https://data.gov.ma/data/dataset/"
        "f412263c-f855-40b1-8458-7b2e56beb2bf/resource/"
        "b5d3960b-1a2e-4f85-8082-80a6374439aa/download/"
        "annuaire-des-agences-de-voyages-ministere-du-tourisme.csv"
    )

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化する (None/nan/『neant』は空文字に潰す)。"""
        if value is None:
            return ""
        s = re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()
        return "" if s.lower() in _NULL_TOKENS else s

    @staticmethod
    def _norm_tel(value: str) -> str:
        """電話番号を数字と先頭 + のみに整形する。

        補完 CSV 側は先頭 0 が欠落した 9 桁表記 (例: 524457668) で入っているため、
        モバイル/固定の識別番号 (5/6/7/8 始まり) の場合に限り 0 を補う。
        """
        s = Streamreq17779Fnavm._txt(value)
        if not s:
            return ""
        s = re.sub(r"[^\d+]", "", s)
        if len(s) == 9 and s[0] in "5678":
            s = "0" + s
        return s if re.search(r"\d{6,}", s) else ""

    @staticmethod
    def _key(name: str) -> str:
        """会社名の突き合わせキー (英数字のみ・大文字化)。"""
        return re.sub(r"[^A-Z0-9]", "", _RE_PAREN.sub(" ", name or "").upper())

    def _resolve_resource(self, slug: str, fmt: str, fallback: str) -> str:
        """CKAN package_show でリソースの実 URL を解決する (失敗時は直リンク)。"""
        api = self._CKAN_API.format(slug=slug)
        try:
            # session.get はテストランナーのソフトタイムアウト対象 (get_soup と同経路)
            resp = self.session.get(api, timeout=self.TIMEOUT)
            resp.raise_for_status()
            resources = json.loads(resp.text)["result"]["resources"]
            for res in resources:
                if (res.get("format") or "").upper() == fmt and res.get("url"):
                    return res["url"]
            if resources and resources[0].get("url"):
                return resources[0]["url"]
        except Exception as e:  # noqa: BLE001 — API 不調でも直リンクで継続する
            logger.warning("CKAN API からリソースを解決できません (直リンク使用): %s — %s", slug, e)
        return fallback

    def _load_sub(self, csv_url: str) -> dict[str, dict]:
        """補完 CSV (UTF-16 タブ区切り) を会社名キーの dict にして返す。"""
        table: dict[str, dict] = {}
        try:
            resp = self.session.get(csv_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            # 実体は BOM 付き UTF-16LE の TSV (Content-Type は text/csv を詐称)
            text = resp.content.decode("utf-16", errors="replace")
        except Exception as e:  # noqa: BLE001 — 補完データが欠けても主データは出す
            logger.warning("補完 CSV を取得できません (補完なしで継続): %s — %s", csv_url, e)
            return table

        rows = list(csv.reader(io.StringIO(text), delimiter="\t"))
        for row in rows[1:]:
            if len(row) < 6:
                continue
            name = self._txt(row[0])
            key = self._key(name)
            if not key or key in table:
                continue
            coord = self._txt(row[3])
            mail = _RE_MAIL.search(coord)
            tel = _RE_TEL.search(coord)
            fax = _RE_FAX.search(coord)
            table[key] = {
                "email": mail.group(0).lower() if mail else "",
                "tel": self._norm_tel(tel.group(1)) if tel else "",
                "fax": self._norm_tel(fax.group(1)) if fax else "",
                "agrement": self._txt(row[4]),
                "annee": self._txt(row[5]),
            }
        logger.info("補完 CSV を読み込みました: %d 件", len(table))
        return table

    def _load_main(self, xls_url: str) -> pd.DataFrame:
        """主データ .xls (旧 BIFF) を calamine エンジンで DataFrame 化する。"""
        resp = self.session.get(xls_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        return pd.read_excel(
            io.BytesIO(resp.content),
            sheet_name="AgencesVoyages",
            engine="calamine",
            dtype=object,
        )

    # ------------------------------------------------------------------ #
    # entry point
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        """引数 url (= sites.yml の url) を起点に、到達不能なら公式オープンデータへ退避する。

        Args:
            url: sites.yml に登録された FNAVM の正規 URL。

        Yields:
            dict: 旅行代理店 1 社分のレコード。
        """
        if self._fnavm_alive(url):
            # ドメインが復活した場合に気付けるようにログだけ残す。
            # 復活時の会員一覧ページ構造は未知のため、解析は追加調査後に実装する。
            logger.warning(
                "正規 URL が復旧しています: %s — 会員一覧の構造を再調査してください "
                "(本実行は観光省オープンデータで継続します)", url,
            )
        else:
            logger.info(
                "正規 URL %s はドメイン失効 (NXDOMAIN) のため、"
                "モロッコ観光省オープンデータ (data.gov.ma) にフォールバックします", url,
            )

        xls_url = self._resolve_resource(self._MAIN_SLUG, "XLS", self._MAIN_FALLBACK)
        csv_url = self._resolve_resource(self._SUB_SLUG, "CSV", self._SUB_FALLBACK)

        sub = self._load_sub(csv_url)
        df = self._load_main(xls_url)
        if df is None or df.empty:
            logger.warning("主データが空です: %s", xls_url)
            return

        self.total_items = int(len(df))

        for _, row in df.iterrows():
            name = self._txt(row.get("Denomination"))
            if not name:
                continue
            try:
                extra = sub.get(self._key(name), {})
                tel = self._norm_tel(row.get("Tel")) or extra.get("tel", "")
                fax = self._norm_tel(row.get("Fax")) or extra.get("fax", "")

                yield {
                    Schema.NAME: name,
                    Schema.PREF: self._txt(row.get("Nom_province")),
                    Schema.ADDR: self._txt(row.get("Adresse")),
                    Schema.TEL: tel,
                    Schema.EMAIL: extra.get("email", ""),
                    # 各社の公式サイト URL は元データに存在しないため常に空
                    Schema.HP: "",
                    Schema.CAT_SITE: "旅行代理店 (Agence de voyages)",
                    Schema.URL: xls_url,
                    "FAX": fax,
                    "認可番号": extra.get("agrement", ""),
                    "認可年": extra.get("annee", ""),
                }
            except Exception as e:  # noqa: BLE001 — 1 行の異常で全体を止めない
                logger.warning("行の解析に失敗 (スキップ): %s — %s", name, e)
                continue

    def _fnavm_alive(self, url: str) -> bool:
        """正規 URL が生きているかを 1 回だけ確認する (NXDOMAIN なら即座に False)。"""
        try:
            resp = self.session.get(url, timeout=self.TIMEOUT)
            return resp.status_code < 400
        except Exception as e:  # noqa: BLE001 — 到達不能は想定内 (ドメイン失効)
            logger.debug("正規 URL に到達できません: %s — %s", url, e)
            return False


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Streamreq17779Fnavm()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    現在ドメインは失効中で、parse() は観光省オープンデータへ自動フォールバックする。
    scraper.execute("http://www.fnavm.org.ma")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
