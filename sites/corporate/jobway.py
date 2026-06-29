"""
Jobway (中小企業家同友会 共同求人サイト) — 掲載企業の会社情報クローラー

取得対象:
    - Jobway に掲載されている全エリア (47都道府県) の全企業の会社概要

取得フロー:
    1. 公開 JSON API `GET /api/member/companylist` で全都道府県の掲載企業
       (id / name / url) を一括列挙する (CSRF 不要・ページネーション不要)。
    2. 各企業について
         - `GET /api/member/company/header?idcompany={id}`      … 名称・カナ・勤務地
         - `GET /api/member/company/companydata?idcompany={id}`  … 会社概要一式
       を取得し、1 社取得するごとに即 yield する (Pattern B / 早期 yield)。

備考対応:
    呼び出し時の備考 (業種/事業内容/全従業員数/従業員数内訳/代表者名/設立/資本金/
    売上高/事業所/関連会社/郵便番号/所在地/電話番号/FAX/メール/HP) で示された
    「とれるカラムはすべて取得」「全エリア取得」の方針に従い、上記 API から取得可能な
    構造化フィールドを網羅的に取得する。フィルター指示は無いため絞り込みはしない。
    PR 用の自由記述・画像 (cap*/cont*/appeals/frees) は著作権リスクのため除外。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/jobway.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id jobway
"""

import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


def _s(v) -> str:
    """API 値を安全な文字列へ。None / 文字列 "None" / "null" / 空白のみは "" に正規化。"""
    if v is None:
        return ""
    text = str(v).strip()
    if text in ("", "None", "null", "NULL"):
        return ""
    return text


class JobwayScraper(StaticCrawler):
    """Jobway (jobway.jp) 共同求人サイト 掲載企業情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "業種コード",
        "従業員数_男性",
        "従業員数_女性",
        "従業員数_パート男性",
        "従業員数_パート女性",
        "従業員数_備考",
        "事業所",
        "関連会社",
        "FAX番号",
        "勤務地",
    ]

    def prepare(self):
        # 各 API は通常のブラウザ UA で 200 を返すが、SPA と同じく AJAX ヘッダも付与しておく
        self.session.headers.update({"X-Requested-With": "XMLHttpRequest"})

    def _get_json(self, api_url: str) -> dict | None:
        """API を GET し data 部分の dict を返す。失敗時は None。

        self.session.get をそのまま使うため、テストランナーのソフトタイムアウトで中断可能。
        """
        try:
            resp = self.session.get(api_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:  # noqa: BLE001 — 個別企業のエラーはスキップして継続
            self.logger.warning("API 取得失敗 (スキップ): %s — %s", api_url, e)
            return None
        if not isinstance(payload, dict) or payload.get("st") != "ok":
            self.logger.warning("API 応答が ok でない: %s", api_url)
            return None
        data = payload.get("data")
        return data if isinstance(data, dict) else None

    def _collect_company_ids(self, api_base: str) -> list[dict]:
        """companylist API から全都道府県の掲載企業 (id / name / url) を列挙する。"""
        data = self._get_json(urljoin(api_base, "/api/member/companylist"))
        if not data:
            return []
        companies: list[dict] = []
        items = data.get("items") or {}
        # items は {連番: {pref, items:[{id,name,url}, ...]}} の入れ子構造
        groups = items.values() if isinstance(items, dict) else items
        for group in groups:
            if not isinstance(group, dict):
                continue
            for company in group.get("items", []) or []:
                cid = company.get("id")
                if cid is None:
                    continue
                companies.append(
                    {
                        "id": cid,
                        "name": _s(company.get("name")),
                        "url": _s(company.get("url")),
                    }
                )
        return companies

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルートとし、同一オリジンから API ベースを導出する (SSOT = sites.yml の url)
        origin = urlsplit(url)
        api_base = f"{origin.scheme}://{origin.netloc}"

        companies = self._collect_company_ids(api_base)
        self.total_items = len(companies)
        self.logger.info("companylist から %d 社を列挙しました", len(companies))

        for company in companies:
            cid = company["id"]
            try:
                detail_url = urljoin(api_base, f"/company/view/{cid}")

                header = self._get_json(
                    urljoin(api_base, f"/api/member/company/header?idcompany={cid}")
                ) or {}
                data = self._get_json(
                    urljoin(api_base, f"/api/member/company/companydata?idcompany={cid}")
                ) or {}

                # companydata が取れなければ会社概要が無いので companylist の名前だけでも残す
                name = _s(header.get("name")) or company["name"]
                if not name and not data:
                    continue

                # 住所 (addr1 + addr2)
                addr = " ".join(p for p in (_s(data.get("addr1")), _s(data.get("addr2"))) if p).strip()

                # 資本金 (sumcapital は万円単位)
                cap = _s(data.get("sumcapital"))
                if cap:
                    cap = f"{cap}万円"

                # 勤務地 (header.plwork は [{plwork: "埼玉県、東京都..."}])
                plwork_list = header.get("plwork") or []
                plwork = " / ".join(
                    _s(p.get("plwork")) for p in plwork_list
                    if isinstance(p, dict) and _s(p.get("plwork"))
                )

                # HP は companydata.url を優先、無ければ companylist の url
                hp = _s(data.get("url")) or company["url"]

                yield {
                    Schema.URL: detail_url,
                    Schema.NAME: name,
                    Schema.NAME_KANA: _s(header.get("kana")),
                    Schema.CAT_SITE: _s(data.get("category")),
                    Schema.LOB: _s(data.get("business")),
                    Schema.REP_NM: _s(data.get("president")),
                    Schema.OPEN_DATE: _s(data.get("establish")),
                    Schema.CAP: cap,
                    Schema.EMP_NUM: _s(data.get("numall")),
                    Schema.POST_CODE: _s(data.get("postal")),
                    Schema.PREF: _s(data.get("pref")),
                    Schema.ADDR: addr,
                    Schema.TEL: _s(data.get("tel1")),
                    Schema.EMAIL: _s(data.get("companymail")),
                    Schema.HP: hp,
                    Schema.SALES: _s(data.get("saletext")),
                    # --- EXTRA ---
                    "業種コード": _s(data.get("cdtype")),
                    "従業員数_男性": _s(data.get("numman")),
                    "従業員数_女性": _s(data.get("numlady")),
                    "従業員数_パート男性": _s(data.get("numpman")),
                    "従業員数_パート女性": _s(data.get("numplady")),
                    "従業員数_備考": _s(data.get("empetc")),
                    "事業所": _s(data.get("branch")),
                    "関連会社": _s(data.get("associated")),
                    "FAX番号": _s(data.get("fax")),
                    "勤務地": plwork,
                }
            except Exception as e:  # noqa: BLE001
                self.error_count += 1
                self.logger.warning("企業 id=%s の取得でエラー (スキップ): %s", cid, e)
                continue


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JobwayScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jobway.jp/index/current")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
