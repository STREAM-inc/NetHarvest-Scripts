"""
対象サイト: https://square.jfa.jp/squarePtnServlet/TeamSearch
JFA SQUARE チーム検索（日本サッカー協会）

- 検索は GET パラメータで再現可能（静的取得可）。都道府県は select_area_prefecture_id=002..。
  募集年齢 qualifying_age_id は複数指定すると AND 条件になり「小学生のみ募集」等が落ちるため
  サーバー側では絞らず、一覧カード（td.cell_focus = 募集中の年齢）と詳細で判定する。
- 一覧は 1 ページ 20 件。ページ送りは sei_submit=PAGER&cp=11&page=N の GET で取得可。
- 詳細は TeamSearch?np=10000|19&team_id={id}（np はチームにより異なるので一覧のリンクをそのまま使う）。
- 対象: 東北・北陸 10 県 × サッカー/フットサル × 募集年齢に未就学児〜中学生年代を含むチーム。
"""
import math
import re
import urllib.parse
from typing import Generator

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 青森・岩手・宮城・秋田・山形・福島・新潟・富山・石川・福井
_PREF_CODES = ["002", "003", "004", "005", "006", "007", "017", "018", "019", "020"]
_TARGET_COMPETITIONS = {"サッカー", "フットサル"}
_TARGET_AGES = {"未就学児", "小学校低学年", "小学校中学年", "小学校高学年", "中学生年代"}
_PAGE_SIZE = 20

_TEAM_ID_RE = re.compile(r"SeiJump\(window,'([^']*team_id=(\d+)[^']*)'")
_PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)\s*(.*)$")


class JfaSquareScraper(StaticCrawler):
    """JFA SQUARE チーム検索 スクレイパー"""
    DELAY = 3.0  # 待機時間（秒）
    EXTRA_COLUMNS = [
        "活動競技",
        "募集年齢",
        "募集性別",
        "主な活動日",
        "活動頻度",
        "JFA加盟状況",
        "チーム特徴",
        "活動場所",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url.split("?")[0]
        seen: set[str] = set()

        for pref_code in _PREF_CODES:
            page = 1
            total_pages = 1
            while page <= total_pages:
                soup = self.get_soup(self._search_url(base, pref_code, page))
                if soup is None:
                    break
                if page == 1:
                    total_pages = self._total_pages(soup)

                for detail_url, team_id in self._detail_links(soup, base):
                    if team_id in seen:
                        continue
                    seen.add(team_id)
                    item = self._parse_detail(detail_url)
                    if item:
                        yield item
                page += 1

    # ------------------------------------------------------------------
    # 一覧
    # ------------------------------------------------------------------
    def _search_url(self, base: str, pref_code: str, page: int) -> str:
        params: list[tuple[str, str]] = [("np", "11")]
        if page == 1:
            params += [("cp", "0"), ("sei_submit", "SUBMIT")]
        else:
            params += [("cp", "11"), ("sei_submit", "PAGER"), ("page", str(page))]
        params.append(("select_area_prefecture_id", pref_code))
        params.append(("ss", "0"))
        return f"{base}?{urllib.parse.urlencode(params)}"

    @staticmethod
    def _total_pages(soup: bs4.BeautifulSoup) -> int:
        hit = soup.select_one(".block_hit h3 span")
        if not hit:
            return 1
        total = int(re.sub(r"\D", "", hit.get_text()) or 0)
        return max(1, math.ceil(total / _PAGE_SIZE))

    @staticmethod
    def _detail_links(soup: bs4.BeautifulSoup, base: str) -> list[tuple[str, str]]:
        """一覧カードのうち備考の条件（競技・募集年齢）を満たすチームの詳細リンクを返す。"""
        links = []
        for card in soup.select(".block_search_result table"):
            a = card.select_one("a[id*='inquire_team_by_key_button']")
            m = _TEAM_ID_RE.search(a.get("onclick", "")) if a else None
            if not m:
                continue
            comps: set[str] = set()
            for th in card.select("tbody th"):
                if th.get_text(strip=True) == "活動競技":
                    td = th.find_next_sibling("td")
                    comps = {c.strip() for c in td.get_text().split(",")} if td else set()
                    break
            ages = {td.get_text(strip=True) for td in card.select("td.cell_focus")}
            if comps and not _TARGET_COMPETITIONS & comps:
                continue
            if not _TARGET_AGES & ages:
                continue
            links.append((urllib.parse.urljoin(base, m.group(1)), m.group(2)))
        return links

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        fields: dict[str, bs4.Tag] = {}
        email = ""
        for th in soup.select("th.SeiTH"):
            label = th.get_text(strip=True)
            td = th.find_next_sibling("td")
            if label and td is not None and label not in fields:
                fields[label] = td
        for td in soup.select("td.SeiTD"):
            text = td.get_text(" ", strip=True)
            if text.startswith("メールアドレス"):
                email = text.replace("メールアドレス", "", 1).strip()
                break

        def val(label: str) -> str:
            td = fields.get(label)
            return re.sub(r"\s+", " ", td.get_text(" ", strip=True)) if td else ""

        competitions = val("活動競技")
        ages = val("募集する年齢")
        # 備考のフィルター: サッカー/フットサル かつ 未就学児〜中学生年代を募集するチームのみ
        if not _TARGET_COMPETITIONS & {c.strip() for c in competitions.split(",")}:
            return None
        if not _TARGET_AGES & {a.strip() for a in ages.split(",")}:
            return None

        names = [n.strip() for n in val("チーム名").split(" / ")]
        name = names[0] if names else ""
        kana = names[1] if len(names) > 1 else ""

        pref, addr = "", val("チーム所在地")
        m = _PREF_RE.match(addr)
        if m:
            pref, addr = m.group(1), m.group(2)

        hp, insta, fb, x = "", "", "", ""
        hp_td = fields.get("ホームページ")
        hrefs = [a.get("href", "").strip() for a in hp_td.select("a[href]")] if hp_td else []
        if not hrefs and hp_td:
            hrefs = re.findall(r"https?://\S+", hp_td.get_text(" ", strip=True))
        for href in hrefs:
            low = href.lower()
            if "instagram.com" in low:
                insta = insta or href
            elif "facebook.com" in low:
                fb = fb or href
            elif "twitter.com" in low or "//x.com" in low:
                x = x or href
            else:
                hp = hp or href

        features = ""
        feat_td = fields.get("チームの特徴")
        if feat_td:
            features = ",".join(s.strip() for s in feat_td.stripped_strings if s.strip())

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.HP: hp,
            Schema.INSTA: insta,
            Schema.FB: fb,
            Schema.X: x,
            Schema.EMAIL: email,
            Schema.CAT_SITE: competitions,
            "活動競技": competitions,
            "募集年齢": ages,
            "募集性別": val("募集する性別"),
            "主な活動日": val("主な活動日"),
            "活動頻度": val("活動頻度"),
            "JFA加盟状況": val("JFA加盟"),
            "チーム特徴": features,
            "活動場所": val("活動場所詳細（地図）"),
        }


if __name__ == "__main__":
    scraper = JfaSquareScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://square.jfa.jp/squarePtnServlet/TeamSearch")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
