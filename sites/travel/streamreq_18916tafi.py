"""
【STREAMREQ-18916】TAFI インド旅行代理店連盟 会員ディレクトリ（インド）
 — streamreq_18916tafi

取得対象:
    TAFI (Travel Agents Federation of India / インド旅行代理店連盟) の
    会員ディレクトリ (https://tafionline.com/ トップページの Member Directory 検索)
    に掲載されている全会員 (2026-09 時点 API 総数 1,585 件 / 削除済み 15 件を除き 1,570 件)。

サイト構造 (Phase 1 調査結果):
    - トップページの Member Directory は、ページ内 JavaScript が公開 API
        POST https://api.tafionline.com/MemberDirectory/GetAllBulkData  (本文 {})
      を 1 回叩いて全件 JSON (約 5.8MB) を取得し、入力語で絞り込んで表示するだけ。
      認証・トークン・ページングは一切無い (静的 HTML 内に上記 URL が平文で埋まっている)。
    - 会員 1 件ごとの詳細ページ (個別 URL) は存在しない。
      → Schema.URL は起点 URL (= sites.yml の url) を全件に入れる。
    - したがって Playwright は不要。StaticCrawler + requests の POST 1 回で全件取得できる。
    - API エンドポイントは起点 URL のページ HTML から正規表現で抽出する
      (URL をハードコードせず、起点 URL を唯一のルートとして派生させるため)。
      抽出できなかった場合のみ、起点 URL のホストから "api.<host>" を組み立てて代替する。

API レスポンスの実測 (2026-09-30):
    - success / count / data[] の構造。data[] の 1 要素が 1 会員 (139 キー)。
    - 充足率 (IsDeleted=false の 1,570 件に対して):
        CompanyName 100% / address 99.3% / city 99.6% / state 99.6% / pincode 98.8%
        OfficePhoneNo 98.5% / OfficeMobileNo 67.0% / email 98.8% / email2 49.0%
        iata_code 98.3% (ただし内 574 件は "NA" のプレースホルダ → 実質 61.8%)
        MemberCategory 100% / ChapterName 100% / MemberCode 100%
        contact_person 99.2% / designation 97.5% / company_type 96.5%
        website 0.2% (ほぼ無い。依頼工程⑫でメールドメインから確定する前提)
        date_of_establishment 0.3% / TotalOfficeStaff 0.1% / capital_amount 0.1%
      → 出現率が低いフィールドも実装し、値が無い場合は空文字を入れる。
    - MemberCategory: Active 916 / Allied 429 / Affiliate 122 / Associate 65 /
      Honorary 46 / Overseas 5 / Gold 1。
    - ChapterName (支部): Western India 363 / Northern India 337 / Gujarat 230 ほか。

依頼 (備考) の反映:
    - 国カラムは「インド」で固定。
    - 会員区分 Allied (航空会社・ホテル・GSA 等の賛助会員) は**除外しない**。
      依頼指示どおり作業用に EXTRA「会員区分」として保持し、旅行会社以外の除外は
      後工程 (⑭) で行う。したがって parse() 側のカテゴリフィルタは実装しない。
    - TEL は +91 を含む国際表記に正規化する。
        * "0240-2335440-2342707-2365529" のように複数番号がハイフン/スラッシュで
          連結されているものは先頭 1 件のみ採用する。
          (先頭から順にトークンを連結し、国内番号 10 桁に達した時点で打ち切る。
           これにより "0181-4110000" = 1 番号 と "0240-2335440-2342707" = 3 番号 を
           取り違えない)
        * 市外局番の先頭 0 は除去し、国番号 91 の重複付与も避ける。
        * OfficePhoneNo が空の場合は OfficeMobileNo を使う。原文は
          EXTRA「電話番号(原文)」「携帯番号(原文)」に残す。
        * EXTRA「携帯」は OfficeMobileNo (67.0%) を使い、空の場合のみ同じ携帯番号の
          別カラム mobile_no で補う (結果 97.8%)。
    - Schema.TEL は共通の正規化処理で "+" が落ちる (数字とハイフンのみ許可) ため、
      "+91-" 付きの完全な国際表記は Schema.PHONE (電話番号) 側にも保持する。
    - 郵便番号 (pincode) はインドの 6 桁 PIN コード。Schema.POST_CODE は日本の
      7 桁郵便番号以外を空にする正規化が入るため、EXTRA「PINコード」に格納する。
    - URL (website) はほぼ無いため空欄が大半。工程⑫で補完される前提。

取得しないフィールド (除外):
    - PAN 番号 / GST 番号 / 各種提出書類ファイル URL (letter_of_undertaking,
      *_file 各種) / 銀行名・取引銀行 / 支払情報 (PaymentAmount 等) /
      DeviceToken・uid・OTP 等の端末/認証情報 / 承認フロー情報 (*_Approval)。
      → 依頼指示「参考カラム以外は取得直後に破棄し、保存しない」に従い、
        画面非表示の機微情報は一切保存しない。
    - principalbusiness (主たる事業内容の自由記述) / branch_address 等 →
      自由記述プロースに該当しうるため著作権リスクで除外 (充足率も 0.3%)。

除外する行:
    - IsDeleted = true の削除済みレコード (15 件)。
    - 会社名が "test" / "testing" 等のテストデータ。

名寄せ:
    海外所在の事業者のため STX 名寄せは対象外 (依頼指示)。

利用規約 / robots.txt (2026-09-30 確認):
    - https://tafionline.com/robots.txt : 404 (robots.txt の指定なし)。
    - https://tafionline.com/terms-and-conditions : 404 (利用規約ページ自体が無い)。
    - サイト内にスクレイピング/クローリングを禁止する文言は見当たらない。
    - 取得先はトップページ自身が表示に使っている認証不要の公開 API。
    → 収集継続可能と判断。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_18916tafi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_18916tafi
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# トップページの JavaScript に平文で埋まっている会員ディレクトリ API の URL。
# 起点 URL のページ HTML から抽出するためのパターン (ホストはハードコードしない)。
_API_URL_PATTERN = re.compile(
    r"https?://[A-Za-z0-9.\-]+/MemberDirectory/GetAllBulkData", re.IGNORECASE
)
# 抽出に失敗したときに起点 URL のホストから組み立てる API パス
_API_PATH = "/MemberDirectory/GetAllBulkData"

# テストデータ判定 (会社名・屋号)
_TEST_NAME_PATTERN = re.compile(r"\btest(?:ing|ed)?\b", re.IGNORECASE)

# IATA コード等の「未記入」プレースホルダ
_PLACEHOLDER_VALUES = {"na", "n.a", "n.a.", "n/a", "nil", "none", "no", "-", "--", "0"}

# メールアドレスらしき文字列
_EMAIL_PATTERN = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")

# 電話番号の区切り (ハイフン・スラッシュ・カンマ・空白など)
_TEL_SPLIT_PATTERN = re.compile(r"[^0-9]+")

# インド国内フリーダイヤル (日本の 0120/0800 とは別物。国際表記にできない)
_TOLL_FREE_PATTERN = re.compile(r"^(?:1800|1860|1861)")

# 日付 (DD/MM/YYYY)
_DATE_PATTERN = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$")

# API の POST リトライ設定 (無限リトライ禁止 / 上限到達時は raise)
_MAX_API_ATTEMPTS = 3


class StreamReq18916Tafi(StaticCrawler):
    """TAFI (インド旅行代理店連盟) 会員ディレクトリのスクレイパー"""

    DELAY = 1.0
    # 1 回の API 呼び出しで全件 (約 1,570 件) を取得するため、
    # アイテムごとの待機を入れると待ち時間だけで完走できなくなる。
    ITEM_DELAY = 0
    TIMEOUT = 60

    COUNTRY = "インド"

    EXTRA_COLUMNS = [
        "国",
        "都市",
        "PINコード",
        "携帯",
        "IATAコード",
        "会員区分",
        "支部",
        "会員コード",
        "会員ID",
        "法人形態",
        "屋号",
        "メールアドレス2",
        "会員期間開始",
        "会員期間終了",
        "電話番号(原文)",
        "携帯番号(原文)",
    ]

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        """起点 URL (= sites.yml の url) から会員ディレクトリ API を辿って全件取得する。

        1. 起点ページの HTML を取得し、埋め込まれている API URL を抽出する
        2. その API に POST {} を 1 回投げて全件 JSON を受け取る
        3. 1 件ずつ辞書化して即 yield する (全件バッファはしない)
        """
        api_url = self._resolve_api_url(url)
        self.logger.info("会員ディレクトリ API: %s", api_url)

        payload = self._post_json(api_url, url)
        records = payload.get("data") or []
        self.logger.info(
            "API 応答: count=%s / data=%d 件", payload.get("count"), len(records)
        )

        count = 0
        skipped = 0
        for record in records:
            if not isinstance(record, dict):
                continue
            if self._is_excluded(record):
                skipped += 1
                continue
            item = self._build_item(record, url)
            if item is None:
                skipped += 1
                continue
            count += 1
            yield item

        self.logger.info("取得件数: %d 件 (除外 %d 件)", count, skipped)

    # ------------------------------------------------------------------ #
    # API
    # ------------------------------------------------------------------ #
    def _resolve_api_url(self, url: str) -> str:
        """起点ページ HTML から会員ディレクトリ API の URL を取り出す。

        ページから見つからない場合のみ、起点 URL のホストから "api.<host>" を
        組み立てて代替する (URL の起点は常に引数 url)。
        """
        soup = self.get_soup(url)
        if soup is not None:
            match = _API_URL_PATTERN.search(str(soup))
            if match:
                return match.group(0)
            self.logger.warning("起点ページ内に API URL が見つかりませんでした: %s", url)

        parts = urlsplit(url)
        host = parts.netloc
        if host.startswith("www."):
            host = host[4:]
        if not host.startswith("api."):
            host = f"api.{host}"
        return f"{parts.scheme or 'https'}://{host}{_API_PATH}"

    def _post_json(self, api_url: str, referer: str) -> dict:
        """API に POST {} を投げて JSON を返す。上限付きリトライ (再帰はしない)。"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": f"{urlsplit(referer).scheme}://{urlsplit(referer).netloc}",
            "Referer": referer,
        }
        last_error: Exception | None = None
        for attempt in range(_MAX_API_ATTEMPTS):
            if attempt:
                time.sleep(min(2 ** attempt, 10))
            try:
                response = self.session.post(
                    api_url, json={}, headers=headers, timeout=self.TIMEOUT
                )
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:  # 通信エラー / JSON 解析エラー
                last_error = exc
                self.logger.warning(
                    "API 取得に失敗しました (%d/%d): %s",
                    attempt + 1,
                    _MAX_API_ATTEMPTS,
                    exc,
                )
                continue
            if not isinstance(payload, dict) or "data" not in payload:
                last_error = RuntimeError(f"想定外のレスポンス形式: {str(payload)[:200]}")
                self.logger.warning("%s", last_error)
                continue
            return payload

        raise RuntimeError(f"会員ディレクトリ API を取得できませんでした: {api_url} ({last_error})")

    # ------------------------------------------------------------------ #
    # 1 件のパース
    # ------------------------------------------------------------------ #
    def _is_excluded(self, record: dict) -> bool:
        """削除済みレコード・テストデータを除外する。"""
        if record.get("IsDeleted"):
            return True
        name = self._clean(record.get("CompanyName"))
        if not name:
            return True
        if _TEST_NAME_PATTERN.search(name):
            self.logger.info("テストデータとみなしてスキップ: %s", name)
            return True
        return False

    def _build_item(self, record: dict, url: str) -> dict | None:
        """API の 1 レコードを出力用の辞書に変換する。"""
        name = self._clean(record.get("CompanyName"))
        if not name:
            return None

        office_tel_raw = self._clean(record.get("OfficePhoneNo"))
        mobile_raw = self._clean(record.get("OfficeMobileNo")) or self._clean(
            record.get("mobile_no")
        )
        # OfficePhoneNo が空なら OfficeMobileNo を使う (依頼指示)
        tel = self._normalize_tel(office_tel_raw) or self._normalize_tel(mobile_raw)

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: self._clean(record.get("state")),
            Schema.ADDR: self._clean(record.get("address")),
            Schema.TEL: tel,
            # Schema.TEL は共通正規化で "+" が落ちるため、+91 付きはこちらに保持する
            Schema.PHONE: tel,
            Schema.EMAIL: self._first_email(record.get("email")),
            Schema.HP: self._normalize_url(record.get("website")),
            Schema.REP_NM: self._clean(record.get("contact_person")),
            Schema.POS_NM: self._clean(record.get("designation")),
            Schema.OPEN_DATE: self._normalize_date(record.get("date_of_establishment")),
            Schema.EMP_NUM: self._clean(record.get("TotalOfficeStaff")),
            Schema.CAP: self._normalize_amount(record.get("capital_amount")),
            "国": self.COUNTRY,
            "都市": self._clean(record.get("city")),
            "PINコード": self._normalize_pincode(record.get("pincode")),
            "携帯": self._normalize_tel(mobile_raw),
            "IATAコード": self._drop_placeholder(record.get("iata_code")),
            "会員区分": self._clean(record.get("MemberCategory")),
            "支部": self._clean(record.get("ChapterName")),
            "会員コード": self._clean(record.get("MemberCode")),
            "会員ID": self._clean(record.get("MemberID")),
            "法人形態": self._clean(record.get("company_type")),
            "屋号": self._clean(record.get("trading_name")),
            "メールアドレス2": self._first_email(record.get("email2")),
            "会員期間開始": self._normalize_date(record.get("MembershipStartDate")),
            "会員期間終了": self._normalize_date(record.get("MembershipEndDate")),
            "電話番号(原文)": office_tel_raw,
            "携帯番号(原文)": mobile_raw,
        }

    # ------------------------------------------------------------------ #
    # 値の整形
    # ------------------------------------------------------------------ #
    @staticmethod
    def _clean(value) -> str:
        """None / 余分な空白 (NBSP 含む) を落として文字列にする。"""
        if value is None:
            return ""
        text = str(value).replace("\xa0", " ").replace("​", "")
        return re.sub(r"\s+", " ", text).strip()

    @classmethod
    def _drop_placeholder(cls, value) -> str:
        """"NA" 等の未記入プレースホルダを空文字にする。"""
        text = cls._clean(value)
        if text.lower().rstrip(".") in _PLACEHOLDER_VALUES:
            return ""
        return text

    @classmethod
    def _first_email(cls, value) -> str:
        """複数記載 (カンマ/スラッシュ区切り) のうち先頭のメールアドレスを返す。"""
        text = cls._clean(value)
        if not text:
            return ""
        match = _EMAIL_PATTERN.search(text)
        return match.group(0) if match else ""

    @classmethod
    def _normalize_url(cls, value) -> str:
        """website の値を URL として整える (壊れた記載は捨てる)。"""
        text = cls._drop_placeholder(value)
        if not text:
            return ""
        text = text.split()[0] if " " not in text else text.replace(" ", "")
        if not re.match(r"^https?://", text, re.IGNORECASE):
            text = f"https://{text.lstrip('/')}"
        # ホスト部にドットが無い / ドメインとして成立しないものは捨てる
        host = urlsplit(text).netloc
        if not re.match(r"^[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$", host):
            return ""
        return text

    @classmethod
    def _normalize_pincode(cls, value) -> str:
        """インドの PIN コード (6 桁) を取り出す。"""
        text = cls._clean(value)
        digits = re.sub(r"\D", "", text)
        return digits if len(digits) == 6 else ""

    @classmethod
    def _normalize_date(cls, value) -> str:
        """DD/MM/YYYY を YYYY-MM-DD に変換する。"""
        text = cls._clean(value)
        match = _DATE_PATTERN.match(text)
        if not match:
            return ""
        day, month, year = match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"

    @classmethod
    def _normalize_amount(cls, value) -> str:
        """資本金 (数値) を整数文字列にする。"""
        if value in (None, ""):
            return ""
        try:
            amount = float(value)
        except (TypeError, ValueError):
            return ""
        if amount <= 0:
            return ""
        return str(int(amount))

    @classmethod
    def _normalize_tel(cls, raw) -> str:
        """インドの電話番号を +91 を含む国際表記に正規化する。

        複数番号がハイフン/スラッシュ/カンマで連結されている場合は先頭 1 件のみ採用する。
        判定は「先頭から数字トークンを連結し、国内番号 10 桁に達した時点で打ち切る」方式。
            "0181-4110000"                  → 0181 + 4110000 = 10 桁  → +91-1814110000
            "0240-2335440-2342707-2365529"  → 0240 + 2335440 = 10 桁  → +91-2402335440
            "9104010107"                    → 単独 10 桁              → +91-9104010107
        """
        text = cls._clean(raw)
        if not text:
            return ""

        tokens = [t for t in _TEL_SPLIT_PATTERN.split(text) if t]
        if not tokens:
            return ""

        digits = ""
        for token in tokens:
            digits += token
            # 国番号・国内プレフィックスを剥がした桁数で判定する
            if len(cls._strip_prefix(digits)) >= 10:
                break

        digits = cls._strip_prefix(digits)
        if not digits:
            return ""

        if _TOLL_FREE_PATTERN.match(digits):
            # インド国内フリーダイヤルは国際表記にできないためそのまま残す
            return digits

        # 10 桁を超える場合は先頭 10 桁 (= 1 番号分) のみ採用する
        if len(digits) > 10:
            digits = digits[:10]
        return f"+91-{digits}"

    @staticmethod
    def _strip_prefix(digits: str) -> str:
        """国番号 (0091 / 91) と国内プレフィックスの先頭 0 を取り除く。"""
        if digits.startswith("0091"):
            digits = digits[4:]
        elif digits.startswith("91") and len(digits) > 10:
            digits = digits[2:]
        return digits.lstrip("0")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = StreamReq18916Tafi()
    scraper.site_id = "streamreq_18916tafi"
    scraper.site_name = "【STREAMREQ-18916】TAFI インド旅行代理店連盟 会員ディレクトリ（インド）"
    scraper.execute("https://tafionline.com/")
