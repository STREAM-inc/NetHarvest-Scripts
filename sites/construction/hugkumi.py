"""
HUGKUMI（はぐくみ）(hugkumi-life.jp) — 注文住宅・工務店一覧

取得対象:
    - 東北・北陸 10 県 (青森2, 岩手3, 宮城4, 秋田5, 山形6, 福島7, 新潟15, 富山16, 石川17, 福井18)
      の都道府県別工務店一覧に掲載された工務店 (計約1,078件)

取得フロー:
    ルート(url = const-area/pref/select.php?kcd=2) の kcd パラメータを差し替えて対象 10 県の一覧を導出
      → 各一覧ページ (ページネーション無し・全件1ページ) の h2 > a[href*="detail/index.php?id="] を収集
      → 詳細ページ (detail/index.php?id=N) を取得して 1 件ずつ即 yield

備考:
    - 詳細の項目は div.unit > p.title(ラベル) + p.text(値)。項目は工務店ごとに有無が異なる。
    - 「注文住宅の金額の目安（建物のみ）」は多くのページで HTML コメントアウトされている
      (画面非表示)。備考で取得指定があるため、コメント内の div.unit も補助的に解析して取得する。
    - 同一工務店が複数県の一覧に掲載されるため詳細 ID で重複除外する。
    - 都道府県は所在地から導出 (本社所在地が対象10県外の工務店もある)。
    - 紹介文 (p.description)・アフター保障・アクセス説明は自由記述のため取得しない (著作権配慮)。

実行方法:
    python scripts/sites/construction/hugkumi.py
    docker compose exec worker python /app/bin/run_flow.py --site-id hugkumi
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 対象県の kcd (備考で指定された 10 県のみ)
_TARGET_KCDS = ["2", "3", "4", "5", "6", "7", "15", "16", "17", "18"]

_PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")
_ZIP_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_RE = re.compile(r"TEL[：:]\s*([0-9０-９\-－()（）]+)")
_FAX_RE = re.compile(r"FAX[：:]\s*([0-9０-９\-－()（）]+)")


class HugkumiScraper(StaticCrawler):
    """HUGKUMI（はぐくみ）工務店スクレイパー"""

    DELAY = 3.0

    EXTRA_COLUMNS = [
        "FAX",
        "建設業許可番号",
        "建築可能エリア",
        "注文住宅の金額の目安（建物のみ）",
        "坪単価",
        "掲載都道府県",
        "詳細ページURL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for list_url in self._prefecture_urls(url):
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧取得失敗: %s", list_url)
                continue
            list_pref = self._list_pref(soup)
            links = []
            for a in soup.select('h2 a[href*="detail/index.php?id="]'):
                href = urljoin(list_url, a["href"])
                detail_id = parse_qs(urlparse(href).query).get("id", [""])[0]
                if detail_id and detail_id not in seen:
                    seen.add(detail_id)
                    links.append(href)
            self.logger.info("%s: %d件 (%s)", list_pref, len(links), list_url)

            for detail_url in links:
                item = self._parse_detail(detail_url, list_pref)
                if item:
                    yield item

    def _prefecture_urls(self, url: str) -> list[str]:
        """ルート url の kcd を差し替えて対象 10 県の一覧 URL を導出 (ルート県を先頭に)。"""
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        root_kcd = qs.get("kcd", [""])[0]
        kcds = [root_kcd] if root_kcd in _TARGET_KCDS else []
        kcds += [k for k in _TARGET_KCDS if k not in kcds]
        urls = []
        for kcd in kcds:
            qs["kcd"] = [kcd]
            urls.append(urlunparse(parsed._replace(query=urlencode(qs, doseq=True))))
        return urls

    @staticmethod
    def _list_pref(soup) -> str:
        title = soup.title.get_text(strip=True) if soup.title else ""
        m = _PREF_RE.match(title)
        return m.group(1) if m else ""

    def _parse_detail(self, detail_url: str, list_pref: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            self.logger.warning("詳細取得失敗: %s", detail_url)
            return None

        fields = self._collect_units(soup)

        name = fields.get("会社名", "")
        if not name:
            h1 = soup.select_one("h1")
            name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        zip_code, pref, addr = self._split_address(fields.get("所在地", ""))
        contact = fields.get("連絡先", "")
        tel_m = _TEL_RE.search(contact)
        fax_m = _FAX_RE.search(contact)

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: zip_code,
            Schema.ADDR: addr,
            Schema.TEL: tel_m.group(1) if tel_m else "",
            Schema.HP: fields.get("ホームページ", ""),
            Schema.CAP: fields.get("資本金", ""),
            Schema.REP_NM: fields.get("代表者", ""),
            Schema.URL: detail_url,
            "FAX": fax_m.group(1) if fax_m else "",
            "建設業許可番号": fields.get("建設業許可番号", ""),
            "建築可能エリア": fields.get("建築可能エリア", ""),
            "注文住宅の金額の目安（建物のみ）": fields.get("注文住宅の金額の目安（建物のみ）", ""),
            "坪単価": fields.get("坪単価", ""),
            "掲載都道府県": list_pref,
            "詳細ページURL": detail_url,
        }

    def _collect_units(self, soup) -> dict[str, str]:
        """div.unit の p.title/p.text を辞書化。表示中の値を優先し、コメントアウト分で補完。"""
        fields: dict[str, str] = {}
        scope = soup.select_one(".area_detail_about_info") or soup
        self._read_units(scope, fields)
        for comment in scope.find_all(string=lambda s: isinstance(s, bs4.Comment)):
            if "unit" in comment:
                hidden: dict[str, str] = {}
                self._read_units(bs4.BeautifulSoup(comment, "html.parser"), hidden)
                for k, v in hidden.items():
                    fields.setdefault(k, v)
        return fields

    @staticmethod
    def _read_units(scope, fields: dict[str, str]) -> None:
        for unit in scope.select("div.unit"):
            title = unit.select_one("p.title")
            text = unit.select_one("p.text")
            if not title or not text:
                continue
            label = title.get_text(strip=True)
            value = re.sub(r"\s+", " ", text.get_text(" ", strip=True)).strip()
            if label and label not in fields:
                fields[label] = value

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        raw = raw.replace("　", " ").strip()
        zip_code = ""
        m = _ZIP_RE.match(raw)
        if m:
            zip_code = m.group(1)
            if "-" not in zip_code:
                zip_code = f"{zip_code[:3]}-{zip_code[3:]}"
            raw = raw[m.end():].strip()
        pref = ""
        pm = _PREF_RE.match(raw)
        if pm:
            pref = pm.group(1)
            raw = raw[pm.end():].strip()
        return zip_code, pref, raw


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HugkumiScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://hugkumi-life.jp/const-area/pref/select.php?kcd=2")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
