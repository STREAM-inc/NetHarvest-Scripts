"""
彩JOB（サイジョブ） — 埼玉の会社で働こう｜人を生かす会社の求人サイト (kyujin-saitama.com)

取得対象:
    - 2026-2027年度の参画企業リスト（全27社）。各企業の詳細ページから
      会社名・業種・採用職種・採用問合せ TEL/メール・会社 HP・会社案内ムービー・
      Jobway 掲載ページを取得する。

取得フロー:
    一覧ページ (/2026-2027list/) は静的な WordPress ページで、
    各企業は figure.wp-block-image > a の画像カードとして並ぶ（ページネーション無し）。
    各カードの href（例: /moriya-mortors/）が企業の詳細ページ。
    詳細ページを1件取得するごとに即 yield する (Pattern B)。

    名称 (Schema.NAME) は詳細ページ見出し h2 の1行目（会社名）を採用する。
    業種は h2 の「-○○業-」span から、採用職種は「採用職種：…」span から取得する。

    代表者・住所・資本金・従業員数・設立等の企業概要は詳細ページ上では
    画像 (PDF をラスタライズした JPG) 内にのみ存在し、HTML テキストとしては
    存在しない。画像には alt 属性も PDF テキスト層も無く、本環境には OCR も
    入っていないため「画像からのテキスト一致」では取得できない。

    代わりに、各詳細ページに必ず存在する「Jobway掲載ページ」リンク
    (https://www.jobway.jp/company/view/{id}) から id を取り出し、Jobway が
    公開する JSON API を呼んで企業概要を補完する:
        GET https://www.jobway.jp/api/member/company/companydata?idcompany={id}
    レスポンス (st=ok, data=...) に 代表者(president)・設立(establish)・
    資本金(sumcapital, 万円)・従業員数(numall)・事業内容(business)・
    住所(postal/pref/addr1/addr2)・TEL(tel1)・FAX(fax)・会社メール(companymail)・
    会社URL(url) がプレーンテキストで含まれており、画像 OCR より遥かに高精度。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/job_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id job_2
"""

import json
import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema


def _clean(text: str) -> str:
    """空白・改行を1スペースに正規化する。"""
    return re.sub(r"\s+", " ", text or "").strip()


# 詳細ページの問合せボタン（アンカーテキスト → 取得先カラム）
_LINK_HP = "会社ホームページ"
_LINK_MOVIE = "会社案内ムービー"
_LINK_JOBWAY = "Jobway掲載ページ"

# Jobway 公開 API（企業概要 JSON）。Jobway 掲載ページの id を idcompany に渡す。
_JOBWAY_API = "https://www.jobway.jp/api/member/company/companydata"
# 詳細ページの Jobway リンク (https://www.jobway.jp/company/view/14934?...) から id 抽出
_JOBWAY_ID_RE = re.compile(r"jobway\.jp/company/view/(\d+)")


def _normalize_postal(postal) -> str:
    """郵便番号を 7桁 "123-4567" 形式に整形する。桁数が想定外ならそのまま返す。"""
    digits = re.sub(r"\D", "", str(postal or ""))
    if len(digits) == 7:
        return f"{digits[:3]}-{digits[3:]}"
    return digits


def _normalize_establish(establish) -> str:
    """設立日 "1964年7月15日" を "1964-07-15" に整形する。

    年のみ・年月のみのケースは取れた範囲だけ返す（"1964", "1964-07"）。
    パースできない場合は元文字列をそのまま返す。
    """
    s = _clean(str(establish or ""))
    if not s:
        return ""
    m = re.search(r"(\d{4})\s*年(?:\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?)?", s)
    if not m:
        return s
    year, month, day = m.group(1), m.group(2), m.group(3)
    parts = [year]
    if month:
        parts.append(f"{int(month):02d}")
        if day:
            parts.append(f"{int(day):02d}")
    return "-".join(parts)


def _normalize_capital(sumcapital) -> str:
    """資本金（API は万円単位の整数）を "1,000万円" 形式に整形する。"""
    if sumcapital is None or str(sumcapital).strip() == "":
        return ""
    try:
        return f"{int(sumcapital):,}万円"
    except (TypeError, ValueError):
        return _clean(str(sumcapital))


class KyujinSaitamaCrawler(StaticCrawler):
    """彩JOB（サイジョブ） スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "採用職種",
        "採用関連お問合せメール",
        "会社案内ムービー",
        "Jobway掲載ページ",
        "FAX",
    ]

    def parse(self, url: str):
        soup = self.get_soup(url)

        # 一覧ページの企業カード（画像リンク）。同一スラッグの重複を除外しつつ順序維持。
        detail_urls = []
        seen = set()
        for a in soup.select("figure.wp-block-image > a[href]"):
            href = urllib.parse.urljoin(url, a.get("href", "").strip())
            m = re.match(r"^https://kyujin-saitama\.com/([^/]+)/$", href)
            if not m or m.group(1) == "2026-2027list":
                continue
            if href in seen:
                continue
            seen.add(href)
            detail_urls.append(href)

        self.total_items = len(detail_urls)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("詳細ページの取得に失敗: %s (%s)", detail_url, exc)
                continue
            if item:
                yield item

    def _fetch_jobway(self, jobway_url: str) -> dict:
        """Jobway 掲載リンクから企業概要 JSON を取得して dict で返す。

        画像内にしか無い 代表者・住所・資本金・従業員数・設立 等を、
        Jobway 公開 API (companydata) のプレーンテキストから補完するための処理。
        取得失敗・id 抽出失敗時は空 dict を返す（詳細ページの情報のみで yield 継続）。
        """
        m = _JOBWAY_ID_RE.search(jobway_url or "")
        if not m:
            return {}
        idcompany = m.group(1)
        try:
            resp = self.session.get(
                _JOBWAY_API,
                params={"idcompany": idcompany},
                headers={
                    "Accept": "application/json",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
            payload = resp.json()
        except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
            self.logger.warning("Jobway API の取得に失敗: %s (%s)", jobway_url, exc)
            return {}

        if not isinstance(payload, dict) or payload.get("st") != "ok":
            return {}
        data = payload.get("data") or {}
        if not isinstance(data, dict):
            return {}

        # 住所: 都道府県を除いた市区町村以降 (addr1 + addr2) を Schema.ADDR に。
        addr = _clean(f"{data.get('addr1') or ''}{data.get('addr2') or ''}")
        numall = data.get("numall")
        result = {
            Schema.REP_NM: _clean(str(data.get("president") or "")),
            Schema.OPEN_DATE: _normalize_establish(data.get("establish")),
            Schema.CAP: _normalize_capital(data.get("sumcapital")),
            Schema.EMP_NUM: "" if numall in (None, "") else _clean(str(numall)),
            Schema.LOB: _clean(str(data.get("business") or "")),
            Schema.POST_CODE: _normalize_postal(data.get("postal")),
            Schema.ADDR: addr,
            Schema.PREF: _clean(str(data.get("pref") or "")),
            Schema.EMAIL: _clean(str(data.get("companymail") or "")),
            "FAX": _clean(str(data.get("fax") or "")),
            "_tel": _clean(str(data.get("tel1") or "")),
            "_url": _clean(str(data.get("url") or "")),
        }
        return result

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None
        body = soup.select_one(".entry-body") or soup

        h2 = body.select_one("h2.wp-block-heading")

        # 見出し h2 は <br> 区切りの3行構成:
        #   1行目: 会社名
        #   2行目: -○○業-          （業種）
        #   3行目: 採用職種：…       （採用職種）
        # ページにより span のネストが異なる（2 span / 1 span 内で <br> 区切り）ため、
        # span 単位ではなく <br> 区切りの行単位でパースする。
        name = ""
        cat_site = ""
        recruit_job = ""
        if h2 is not None:
            lines = [l.strip() for l in h2.get_text("\n").split("\n") if l.strip()]
            if lines:
                name = _clean(lines[0])
            for line in lines[1:]:
                line = _clean(line)
                if line.startswith("採用職種"):
                    recruit_job = re.sub(r"^採用職種[：:]\s*", "", line)
                elif not cat_site:
                    # "-サービス業-" のような業種表記。前後のハイフン類を除去。
                    cat_site = line.strip("-－‐―ー ")

        if not name and soup.title:
            # フォールバック: タイトル先頭（"｜" より前）
            name = _clean(soup.title.get_text()).split("｜")[0].strip()
        if not name:
            return None

        # 採用関連 TEL / メール
        tel = ""
        tel_a = body.select_one('a[href^="tel:"]')
        if tel_a:
            tel = _clean(urllib.parse.unquote(tel_a.get("href", "")[len("tel:"):]))

        email = ""
        mail_a = body.select_one('a[href^="mailto:"]')
        if mail_a:
            email = _clean(urllib.parse.unquote(mail_a.get("href", "")[len("mailto:"):]))

        # 各種ボタンリンク（アンカーテキストで判別）
        hp = ""
        movie = ""
        jobway = ""
        for a in body.select("a[href]"):
            label = _clean(a.get_text())
            href = a.get("href", "").strip()
            if not href:
                continue
            if label == _LINK_HP and not hp:
                hp = href
            elif label == _LINK_MOVIE and not movie:
                movie = href
            elif label == _LINK_JOBWAY and not jobway:
                jobway = href

        # Jobway 公開 API から企業概要（代表者・住所・資本金・従業員数・設立 等）を補完。
        # 画像内にしか無い情報を OCR ではなく構造化 JSON で取得する。
        jw = self._fetch_jobway(jobway)
        jw_tel = jw.pop("_tel", "")
        jw_url = jw.pop("_url", "")

        item = {
            Schema.NAME: name,
            # PREF は Jobway の pref を優先（無ければ「埼玉の会社」サイトの既定値）。
            Schema.PREF: jw.get(Schema.PREF) or "埼玉県",
            # TEL は採用問合せ番号を優先し、無ければ会社代表番号 (tel1) で補完。
            Schema.TEL: tel or jw_tel,
            # HP は詳細ページのボタンを優先し、無ければ Jobway の会社 URL で補完。
            Schema.HP: hp or jw_url,
            Schema.CAT_SITE: cat_site,
            Schema.URL: url,
            "採用職種": recruit_job,
            "採用関連お問合せメール": email,
            "会社案内ムービー": movie,
            "Jobway掲載ページ": jobway,
        }
        # Jobway 由来の概要カラム（PREF は上で処理済みなので除外して上書き）。
        for key, value in jw.items():
            if key == Schema.PREF:
                continue
            if value:
                item[key] = value
        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KyujinSaitamaCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://kyujin-saitama.com/2026-2027list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
