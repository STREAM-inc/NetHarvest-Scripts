"""
【STREAMREQ-18922】英国民間航空局 CAA「Check an ATOL」ATOL保有者一覧 (マン島)

取得対象:
    英国民間航空局 (Civil Aviation Authority) が公開する ATOL 保有者
    (パッケージ旅行の販売許可を持つ旅行会社) の連絡先一覧のうち、
    **マン島所在 (郵便番号が "IM" で始まる) の事業者のみ**。

取得フロー:
    GET https://aircraftapi.caa.co.uk/api/checkanatol/contactdetails
        -> HTTP 200 / Content-Type: application/json
           [{atolNumber, companyName, address1..address5, postCode, phone}, ...]
    ※ 依頼書では "CSV" と記載されているが、実レスポンスは JSON 配列
       (2026-09-30 実測 1,703 社)。1 リクエストで全件が返るため
       ページネーションは無い。取得後 postCode が "IM" で始まる行だけを
       採用し、1 件ずつ即 yield する。

マン島の母集団はきわめて小さく、2026-09-30 実測で 2 社のみ (水増ししない):
    10725 Isle Of Man Event Services Limited / IM3 4EB
    12155 Mann-Link Travel Ltd               / IM1 2EL

備考 (依頼書) の反映:
    - 国カラムは "マン島" で固定
    - TEL は国際表記 (+44-1624-654685 形式) に正規化する
      ※ Schema.TEL はフレームワークの正規化で "+" が落ちるため、
        "+" 付きの値は EXTRA カラム「TEL(国際表記)」にも保持する
    - 郵便番号は英国式 (IM1 2EL) で日本の 7 桁形式ではないため、
      Schema.POST_CODE の正規化で空になる。原文は EXTRA カラム
      「郵便番号(英国)」に保持する
    - 事業者ごとの詳細ページ / HP URL はこの API にも Check an ATOL の
      画面にも存在しないため、取得元 URL (API エンドポイント) を Schema.URL とする

利用規約:
    aircraftapi.caa.co.uk / www.caa.co.uk とも robots.txt は 404 (指定なし)。
    英国政府機関が公開する許可業者データで、明確なスクレイピング禁止文言は無い
    (2026-09-30 確認)。

実行方法:
    python scripts/sites/travel/streamreq_18922_caa_atol_csv.py
    python bin/run_flow.py --site-id streamreq_18922_caa_atol_csv
"""

import re
import sys
import time
from pathlib import Path
from typing import Any, Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 備考指定: マン島 (Isle of Man) 所在の事業者のみ抽出する。
# 英国の郵便番号エリアコード "IM" がマン島を表す。
TARGET_POSTCODE_PREFIX = "IM"
COUNTRY_NAME = "マン島"

# TEL 国際表記用。マン島の市外局番は 1624 (国番号 44)。
UK_COUNTRY_CODE = "44"
IOM_AREA_CODE = "1624"

MAX_ATTEMPTS = 3  # リトライ上限 (無限ループ禁止)


def _clean(value: Any) -> str:
    """API 値を CSV 用の文字列に整形する (連続空白を 1 つに畳む)。"""
    if value is None:
        return ""
    text = re.sub(r"[\s　]+", " ", str(value)).strip()
    # address5 が ", " だけ等、英数字を含まない区切り記号のみの値は空として扱う
    if text and not re.search(r"[0-9A-Za-z]", text):
        return ""
    return text


def _dedupe_parts(parts: list[str]) -> list[str]:
    """住所要素の重複 (例: "Isle of Man, Isle of Man") を順序を保って除去する。"""
    result: list[str] = []
    seen: set[str] = set()
    for part in parts:
        key = part.lower()
        if not part or key in seen:
            continue
        seen.add(key)
        result.append(part)
    return result


def _build_address(record: dict) -> str:
    """address1〜address5 を 1 本の住所文字列に連結する。

    address5 は "Surrey, Surrey" のように州名が重複していたり、
    ", " だけの空行だったりするため、カンマ単位で分解して重複を除去する。
    """
    parts: list[str] = []
    for key in ("address1", "address2", "address3", "address4", "address5"):
        line = _clean(record.get(key))
        if not line:
            continue
        parts.extend(p.strip() for p in line.split(",") if p.strip())
    return ", ".join(_dedupe_parts(parts))


def _format_phone(value: Any) -> str:
    """英国の電話番号を国際表記 (+44-...) に正規化する。

    例: "01624654685" -> "+44-1624-654685"
        "01624 664460" -> "+44-1624-664460"
    先頭が 0 の国内表記のみ変換し、判定できない値は原文をそのまま返す。
    """
    raw = _clean(value)
    if not raw:
        return ""
    if raw.startswith("+"):
        digits = re.sub(r"\D", "", raw)
        return f"+{digits}" if digits else ""
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return ""
    if digits.startswith("0"):
        national = digits[1:]
    elif digits.startswith(UK_COUNTRY_CODE) and len(digits) > 10:
        national = digits[len(UK_COUNTRY_CODE):]
    else:
        national = digits
    if not national:
        return ""
    if national.startswith(IOM_AREA_CODE):
        return f"+{UK_COUNTRY_CODE}-{IOM_AREA_CODE}-{national[len(IOM_AREA_CODE):]}"
    return f"+{UK_COUNTRY_CODE}-{national}"


class CaaAtolIsleOfManScraper(StaticCrawler):
    """CAA ATOL 保有者一覧 (マン島所在のみ) スクレイパー"""

    DELAY = 1.0
    ITEM_DELAY = 0  # 1 リクエストで全件取得するため、アイテムごとの待機は不要

    EXTRA_COLUMNS = [
        "ATOL番号",
        "国",
        "郵便番号(英国)",
        "TEL(国際表記)",
        "TEL(原文)",
        "住所1",
        "住所2",
        "住所3",
        "住所4",
        "住所5",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # url は sites.yml の正規 URL (= ATOL 連絡先一覧エンドポイント) をそのまま使う
        records = self._get_json(url)
        if not isinstance(records, list):
            self.logger.error("想定外のレスポンス形式です: %s", type(records))
            return

        self.logger.info("ATOL 保有者 (全件): %d 社", len(records))

        seen: set[str] = set()
        matched = 0
        for record in records:
            if not isinstance(record, dict):
                continue
            post_code = _clean(record.get("postCode"))
            # 備考指定のフィルター: 郵便番号が IM (マン島) で始まる行だけを採用する
            if not post_code.upper().startswith(TARGET_POSTCODE_PREFIX):
                continue

            atol_number = _clean(record.get("atolNumber"))
            name = _clean(record.get("companyName"))
            if not name:
                continue
            key = atol_number or name
            if key in seen:
                continue
            seen.add(key)
            matched += 1

            phone_raw = _clean(record.get("phone"))
            phone_intl = _format_phone(phone_raw)

            yield {
                Schema.URL: url,
                Schema.NAME: name,
                Schema.ADDR: _build_address(record),
                # 英国式郵便番号のため、フレームワークの正規化では空になる。
                # 原文は EXTRA カラム「郵便番号(英国)」に保持する。
                Schema.POST_CODE: post_code,
                Schema.TEL: phone_intl,
                Schema.CAT_SITE: "ATOL保有者",
                "ATOL番号": atol_number,
                "国": COUNTRY_NAME,
                "郵便番号(英国)": post_code,
                "TEL(国際表記)": phone_intl,
                "TEL(原文)": phone_raw,
                "住所1": _clean(record.get("address1")),
                "住所2": _clean(record.get("address2")),
                "住所3": _clean(record.get("address3")),
                "住所4": _clean(record.get("address4")),
                "住所5": _clean(record.get("address5")),
            }

        self.total_items = matched
        self.logger.info("マン島 (郵便番号 IM*) 所在: %d 社", matched)

    # ------------------------------------------------------------------
    # JSON 取得 (上限付きリトライ / 無限ループ禁止)
    # ------------------------------------------------------------------
    def _get_json(self, api_url: str) -> Any:
        last_error: Exception | None = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = self.session.get(
                    api_url,
                    headers={"Accept": "application/json, text/csv, */*"},
                    timeout=self.TIMEOUT,
                )
                response.raise_for_status()
                return response.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                last_error = e
                self.logger.warning(
                    "API 取得失敗 (%d/%d): %s — %s", attempt + 1, MAX_ATTEMPTS, api_url, e
                )
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"API 取得に {MAX_ATTEMPTS} 回失敗: {api_url}") from last_error


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CaaAtolIsleOfManScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://aircraftapi.caa.co.uk/api/checkanatol/contactdetails")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
