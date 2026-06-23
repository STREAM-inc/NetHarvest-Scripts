"""
シゴトガイド (sgnavi.com) — 北海道アルバイト・社員系求人情報スクレイパー

取得対象:
    - 北海道エリアの求人情報 (https://www.sgnavi.com/hokkaido/job-list/)
    - 各求人詳細ページ (/hokkaido/jobs/{id}/) の募集要項・会社データ

取得フロー (一覧 → 詳細, Pattern B: detail 取得ごとに即 yield):
    1. 一覧ページを ?page=N で巡回し、詳細リンク (/hokkaido/jobs/{id}/) を抽出
    2. 詳細ページの JSON-LD (JobPosting) + description の [ラベル] ブロック + HTML をパース
    3. 1 件取得するごとに即 yield (全件収集 → 一括 yield はしない)
    4. 詳細リンクが 0 件のページに到達したら終了

備考対応:
    - [Column] が会社ごとにバラバラなため、description 内の [ラベル] を辞書化して
      必要なラベルだけ拾う方式。存在しないラベルは空文字。
    - 「仕事内容」「求める人物像」「応募方法」「応募後の流れ」は自由記述プロースのため
      著作権リスクを考慮して取得しない (備考に明示的な取得許可がないため除外)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/https_www_sgnavi_com.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id https_www_sgnavi_com
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 詳細ページの ID 抽出 (/hokkaido/jobs/{id}/ — エリア部は url から派生)
_JOB_HREF = re.compile(r"/jobs/[A-Za-z0-9]+/?$")

# 電話番号パターン
_TEL = re.compile(r"0\d{1,4}[-(]\d{1,4}[-)]\d{3,4}")

# 雇用形態 (schema.org employmentType → 日本語)
_EMP_TYPE = {
    "PART_TIME": "アルバイト・パート",
    "FULL_TIME": "正社員",
    "CONTRACTOR": "契約社員",
    "TEMPORARY": "派遣・短期",
    "INTERN": "インターン",
    "PER_DIEM": "日雇い",
    "OTHER": "その他",
}

# description の [ラベル] → EXTRA カラム名 (構造化された短い項目のみ)
# 仕事内容 / 求める人物像 / 応募方法 / 応募後の流れ は自由記述プロースのため除外
_EXTRA_LABELS = {
    "最寄り駅": "最寄り駅",
    "受動喫煙防止措置": "受動喫煙防止措置",
    "採用予定人数": "採用予定人数",
    "期間の定め": "期間の定め",
    "期間": "期間",
    "時間": "勤務時間",
    "勤務日数": "勤務日数",
    "休暇": "休暇",
    "給与": "給与",
    "諸手当": "諸手当",
    "福利厚生": "福利厚生",
    "昇給・賞与": "昇給・賞与",
    "試用期間": "試用期間",
}


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _frag_text(html_fragment: str) -> str:
    """description の値断片 (<br /> 含む) をテキスト化して整形する。"""
    return _clean(BeautifulSoup(html_fragment, "html.parser").get_text(" / "))


class HttpsWwwSgnaviComScraper(StaticCrawler):
    """シゴトガイド (sgnavi.com) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "職種",
        "雇用形態",
        "勤務先",
        "最寄り駅",
        "受動喫煙防止措置",
        "採用予定人数",
        "期間の定め",
        "期間",
        "勤務時間",
        "勤務日数",
        "休暇",
        "給与",
        "諸手当",
        "福利厚生",
        "昇給・賞与",
        "試用期間",
        "掲載終了日",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1
        while True:
            list_url = f"{url}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s", list_url)
                break

            detail_urls = self._collect_detail_urls(soup, url)
            new_urls = [u for u in detail_urls if u not in seen]
            if not new_urls:
                self.logger.info("ページ %d で新規求人なし。巡回終了。", page)
                break

            for detail_url in new_urls:
                seen.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細パース失敗 %s: %s", detail_url, e)
                    continue
                if item and item.get(Schema.NAME):
                    yield item

            page += 1

    def _collect_detail_urls(self, soup: BeautifulSoup, root_url: str) -> list[str]:
        urls: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if _JOB_HREF.search(href):
                urls.append(urljoin(root_url, href))
        return list(dict.fromkeys(urls))

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        data: dict = {Schema.URL: detail_url}

        jp = self._find_jobposting(soup)
        if jp:
            org = jp.get("hiringOrganization") or {}
            if isinstance(org, dict):
                name = _clean(org.get("name"))
                if name:
                    data[Schema.NAME] = name

            title = _clean(jp.get("title"))
            if title:
                data["職種"] = title

            emp = jp.get("employmentType")
            if emp:
                emp_list = emp if isinstance(emp, list) else [emp]
                data["雇用形態"] = " / ".join(_EMP_TYPE.get(e, str(e)) for e in emp_list)

            valid = _clean(jp.get("validThrough"))
            if valid:
                data["掲載終了日"] = valid

            loc = jp.get("jobLocation") or {}
            addr = loc.get("address") if isinstance(loc, dict) else None
            if isinstance(addr, dict):
                region = _clean(addr.get("addressRegion"))
                locality = _clean(addr.get("addressLocality"))
                street = _clean(addr.get("streetAddress"))
                postal = _clean(addr.get("postalCode"))
                if region:
                    data[Schema.PREF] = region
                if postal:
                    data[Schema.POST_CODE] = postal
                full_addr = (locality + street).strip()
                if full_addr:
                    data[Schema.ADDR] = full_addr

            # description の [ラベル] ブロックを辞書化
            labels = self._parse_description(_clean_keep_br(jp.get("description", "")))
            if labels.get("事業内容"):
                data[Schema.LOB] = labels["事業内容"]
            if labels.get("休日"):
                data[Schema.HOLIDAY] = labels["休日"]
            # 勤務先 (掲載されている勤務先名称/所在地表記) — EXTRA として保持
            if labels.get("勤務先"):
                data["勤務先"] = labels["勤務先"]
            for src_label, col in _EXTRA_LABELS.items():
                if labels.get(src_label):
                    data[col] = labels[src_label]

        # NAME フォールバック (JSON-LD が無い/組織名欠落時)
        if not data.get(Schema.NAME):
            h1 = soup.find(["h1", "h2"])
            if h1:
                data[Schema.NAME] = _clean(h1.get_text(" "))

        tel = self._extract_tel(soup)
        if tel:
            data[Schema.TEL] = tel

        return data if data.get(Schema.NAME) else None

    @staticmethod
    def _find_jobposting(soup: BeautifulSoup) -> dict | None:
        for tag in soup.find_all("script", type="application/ld+json"):
            txt = tag.string or tag.get_text() or ""
            if not txt.strip():
                continue
            try:
                obj = json.loads(txt)
            except Exception:  # noqa: BLE001
                continue
            for o in (obj if isinstance(obj, list) else [obj]):
                if isinstance(o, dict) and o.get("@type") == "JobPosting":
                    return o
        return None

    @staticmethod
    def _parse_description(desc: str) -> dict:
        """description ("[ラベル]値[ラベル]値...") を {ラベル: 整形済み値} に分解する。"""
        result: dict = {}
        if not desc:
            return result
        parts = re.split(r"\[([^\]]+)\]", desc)
        # parts = [先頭ゴミ, ラベル1, 値1, ラベル2, 値2, ...]
        it = iter(parts[1:])
        for label, value in zip(it, it):
            label = _clean(label)
            text = _frag_text(value)
            if label and text and label not in result:
                result[label] = text
        return result

    def _extract_tel(self, soup: BeautifulSoup) -> str:
        text = soup.get_text("\n")
        idx = text.find("連絡先")
        if idx >= 0:
            m = _TEL.search(text[idx: idx + 60])
            if m:
                return m.group(0)
        m = _TEL.search(text)
        return m.group(0) if m else ""


def _clean_keep_br(s) -> str:
    """description は <br /> を保持したまま返す (ブロック内の改行整形用)。"""
    return s or ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HttpsWwwSgnaviComScraper()
    # 🔒 sites.yml に登録する url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://www.sgnavi.com/hokkaido/job-list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
