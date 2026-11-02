"""
対象サイト: https://www.l-posi.jp/search_result.html
とかち仕事情報 webプラスワン（l-posi）

構造:
  - 一覧: /search_result.html (GET で全件=「求人をすべて表示」)。50件単位で
    「次の50件を表示」は hidden form (job_search_form) の page=N を POST する方式。
  - 詳細リンク: 写真部分の a[href] (写真無しの求人は onclick="window.open('...')" のみ)
    → 両方を拾うため一覧 HTML から /job/YYYY/MM/jobNNN.html を正規表現で抽出する。
  - 詳細: table.job-company (職種/雇用形態), table.gaiyou (連絡先/会社名/住所/会社概要/ホームページ)。
    会社概要は「■項目／値」の半構造テキストで、設立/創業/従業員数/事業内容/本社TEL を含むことがある。
  - 利用規約: スクレイピングの明示的禁止条項なし (転載・販売・営利目的利用の禁止のみ)。
"""
import re
import time
import urllib.parse
from typing import Generator

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_DETAIL_RE = re.compile(r"https?://www\.l-posi\.jp/job/\d{4}/\d{2}/job\d+\.html|/job/\d{4}/\d{2}/job\d+\.html")
_TOTAL_RE = re.compile(r"全\s*([\d,]+)\s*件中")
_TEL_RE = re.compile(r"0\d{1,4}[-‐－ー]\d{1,4}[-‐－ー]\d{3,4}")
_ZIP_RE = re.compile(r"〒?\s*(\d{3})[-‐－ー](\d{4})")
_PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")
PAGE_SIZE = 50
MAX_PAGES = 100


class WebLPosiScraper(StaticCrawler):
    """とかち仕事情報 webプラスワン（l-posi） スクレイパー"""
    DELAY = 4.0  # 備考「アクセス間隔は十分に空けること」
    EXTRA_COLUMNS = ["求人タイトル", "雇用形態", "本社TEL", "創業", "店舗名"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1
        soup = self.get_soup(url)
        while soup is not None and page <= MAX_PAGES:
            links = self._detail_links(soup, url)
            new_links = [u for u in links if u not in seen]
            for detail_url in new_links:
                seen.add(detail_url)
                item = self._parse_detail(detail_url)
                if item:
                    yield item

            total = self._total(soup)
            if not new_links or total is None or page * PAGE_SIZE >= total:
                break
            page += 1
            soup = self._post_page(url, soup, page)

    # ---- 一覧 ----
    def _detail_links(self, soup: bs4.BeautifulSoup, base_url: str) -> list[str]:
        # 検索結果ブロック (pagenate より前) に限定し、サイドバー等の求人リンク混入を防ぐ
        html = str(soup)
        cut = html.find('class="pagenate"')
        if cut > 0:
            html = html[:cut]
        links: list[str] = []
        for m in _DETAIL_RE.finditer(html):
            u = urllib.parse.urljoin(base_url, m.group(0))
            if u not in links:
                links.append(u)
        return links

    @staticmethod
    def _total(soup: bs4.BeautifulSoup) -> int | None:
        mes = soup.select_one(".pagenate .mes")
        m = _TOTAL_RE.search(mes.get_text() if mes else soup.get_text())
        return int(m.group(1).replace(",", "")) if m else None

    def _post_page(self, url: str, soup: bs4.BeautifulSoup, page: int) -> bs4.BeautifulSoup | None:
        """「次の50件を表示」: job_search_form の hidden 値を引き継いで page=N を POST する。"""
        form = soup.select_one("form#job_search_form")
        data = {}
        if form:
            for inp in form.select("input[name]"):
                data[inp["name"]] = inp.get("value", "")
        data["page"] = str(page)
        action = urllib.parse.urljoin(url, form.get("action", url)) if form else url
        time.sleep(self.DELAY)
        resp = self.session.post(action, data=data, timeout=self.TIMEOUT)
        resp.raise_for_status()
        if "charset=" not in resp.headers.get("Content-Type", "").lower():
            resp.encoding = resp.apparent_encoding
        return bs4.BeautifulSoup(resp.text, "html.parser")

    # ---- 詳細 ----
    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None
        fields: dict[str, bs4.Tag] = {}
        for tr in soup.select("table.job-company tr, table.gaiyou tr"):
            th, td = tr.find("th"), tr.find("td")
            if th and td:
                fields.setdefault(th.get_text(strip=True), td)

        def text(key: str) -> str:
            td = fields.get(key)
            return re.sub(r"\s+", " ", td.get_text(" ", strip=True)).strip() if td else ""

        name = text("会社名")
        if not name:
            h1 = soup.find("h1")
            name = h1.get_text(strip=True) if h1 else ""

        post_code, pref, addr = self._split_address(text("住所"))
        overview = self._parse_overview(fields.get("会社概要"))

        tel_m = _TEL_RE.search(text("連絡先"))
        hp_td = fields.get("ホームページ")
        hp = ""
        if hp_td:
            a = hp_td.find("a", href=True)
            hp = a["href"].strip() if a else hp_td.get_text(strip=True)

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel_m.group(0) if tel_m else "",
            Schema.HP: hp,
            Schema.OPEN_DATE: overview["設立"],
            Schema.EMP_NUM: overview["従業員数"],
            Schema.LOB: overview["事業内容"],
            "求人タイトル": text("職種"),
            "雇用形態": text("雇用形態"),
            "本社TEL": overview["本社TEL"],
            "創業": overview["創業"],
            "店舗名": text("店舗名"),
        }

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        post_code = ""
        m = _ZIP_RE.search(raw)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
            raw = raw[:m.start()] + raw[m.end():]
        # 道案内 "★帯広畜産大学前の通りを…" / 末尾の "(第一自動車学校様前…)" を除去
        raw = re.split(r"[★※]", raw)[0]
        addr = re.sub(r"\s*[（(][^（）()]*[）)]\s*$", "", raw).strip()
        m = _PREF_RE.match(addr)
        if m:
            pref = m.group(1)
            addr = addr[m.end():].strip()
        else:
            # 十勝地域の求人誌で住所は道名省略が通例 (帯広市…/芽室町…)
            pref = "北海道" if addr else ""
        return post_code, pref, addr

    @staticmethod
    def _parse_overview(td: bs4.Tag | None) -> dict[str, str]:
        """会社概要の「■項目／値」から構造化項目だけを抜き出す (自由記述の PR 文は取らない)。"""
        out = {"設立": "", "創業": "", "従業員数": "", "事業内容": "", "本社TEL": ""}
        if td is None:
            return out
        raw = td.get_text("\n", strip=True)
        for sec in raw.split("■")[1:]:
            head, sep, body = sec.partition("／")
            if not sep:
                head, body = sec.split("\n", 1)[0], ""
            head = head.strip()
            # 区切り線以降は PR 文なので捨てる
            body = re.split(r"ー{5,}|-{5,}", body)[0]
            lines = [re.sub(r"^[・〇]\s*", "", ln.strip()) for ln in body.split("\n")]
            # 電話番号だけの行 (本社TEL) は値から除外し、後段の本社TEL フォールバックで拾う
            lines = [ln for ln in lines if ln and not _TEL_RE.fullmatch(ln)]
            value = re.sub(r"\s+", " ", " ".join(lines)).strip()
            if head.startswith("設立") and not out["設立"]:
                out["設立"] = value
            elif head.startswith("創業") and not out["創業"]:
                out["創業"] = value
            elif head in ("従業員数", "社員数") and not out["従業員数"]:
                out["従業員数"] = value.lstrip("★")
            elif head == "事業内容" and not out["事業内容"]:
                out["事業内容"] = "、".join(lines)
            elif head == "TEL" and not out["本社TEL"]:
                m = _TEL_RE.search(value)
                out["本社TEL"] = m.group(0) if m else ""
        if not out["本社TEL"]:
            # "■015-574-3104" や末尾に番号のみの行で記載されるケース
            m = _TEL_RE.search(raw)
            if m:
                out["本社TEL"] = m.group(0)
        return out


if __name__ == "__main__":
    scraper = WebLPosiScraper()
    scraper.execute("https://www.l-posi.jp/search_result.html")
