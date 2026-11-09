import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.base import CrawlAbortedError
from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://entrenet.jp"
LIST_BASE = "https://entrenet.jp/dokuritsu/?cbn=top_src_fw"
MAX_PAGES = 100
# 詳細ページの取得にこの回数だけ連続で失敗したら打ち切る (WAF ブロック中に叩き続けないため)
MAX_CONSECUTIVE_FAILURES = 3


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[\r\n\t]+", " ", str(s)).strip()


class EntreNetScraper(StaticCrawler):
    """アントレ 独立・開業支援 スクレイパー（entrenet.jp）"""

    DELAY = 3.0
    # 取得失敗が続いても (yield しなくても) 実取得の間隔を 3 秒空ける (基盤が待つ)。
    # 間隔はリクエスト単位で保証されるので、アイテム間の追加待機は不要
    REQUEST_INTERVAL = 3.0
    ITEM_DELAY = 0.0
    EXTRA_COLUMNS = ["代表者", "資本金", "売上", "従業員数", "設立日", "事業内容", "FAX", "メール"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        failures = 0
        for detail_url in detail_urls:
            soup = self.get_soup(detail_url.rstrip("/") + "/kigyou/")
            if soup is None:
                # 取得失敗 (WAF 応答・通信エラー) だけを数える。抽出失敗は数えない
                failures += 1
                if failures >= MAX_CONSECUTIVE_FAILURES:
                    raise CrawlAbortedError(
                        f"詳細ページの取得に {failures} 回連続で失敗したため打ち切ります "
                        f"(WAF ブロックの可能性): 最後の URL={detail_url}"
                    )
                continue
            failures = 0
            item = self._extract_detail(detail_url, soup)
            if item:
                yield item

    def _collect_detail_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for page in range(1, MAX_PAGES + 1):
            page_url = f"{LIST_BASE}&page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                # 一覧の途中で取れないと詳細 URL が欠けた結果を納品してしまうため打ち切る
                # (最終ページの判定は下の「一覧が無い / 新規 0 件」で行う)
                raise CrawlAbortedError(f"一覧ページの取得に失敗したため打ち切ります: {page_url}")

            container = soup.select_one(
                "body > article > main > div:nth-of-type(2) > section > ul"
            )
            if not container:
                break

            found = 0
            for a in container.select("a.detailBtn"):
                href = a.get("href", "")
                if "/dplan/" in href:
                    full = href if href.startswith("http") else BASE_URL + href
                    if not full.endswith("/"):
                        full += "/"
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)
                        found += 1

            if found == 0:
                break
        return urls

    def _extract_detail(self, url: str, soup) -> dict | None:
        data = {Schema.URL: url}

        name_tag = soup.select_one("h2.kigyouName span")
        if name_tag:
            data[Schema.NAME] = _clean(name_tag.get_text())

        sections = soup.select("section.kigyouData")
        profile = next(
            (s for s in sections if s.find("h3") and "会社プロフィール" in s.find("h3").get_text()), None
        )
        achievement = next(
            (s for s in sections if s.find("h3") and "実績" in s.find("h3").get_text()), None
        )
        contact = next(
            (s for s in sections if s.find("h3") and "お問い合わせ" in s.find("h3").get_text()), None
        )

        def extract_dl(section) -> dict[str, str]:
            if not section:
                return {}
            return {
                _clean(dt.get_text()): _clean(dd.get_text("\n", strip=True))
                for dt, dd in zip(section.find_all("dt"), section.find_all("dd"))
            }

        prof = extract_dl(profile)
        ach = extract_dl(achievement)
        con = extract_dl(contact)

        addr = _clean(
            prof.get("本社・支社\n事業所・製造所", "") or
            prof.get("本社・支社事業所・製造所", "") or
            con.get("所在地", "")
        )
        if addr:
            data[Schema.ADDR] = addr
        if con.get("電話番号"):
            data[Schema.TEL] = con["電話番号"]
        if prof.get("代表者氏名"):
            data[Schema.REP_NM] = prof["代表者氏名"]
        if prof.get("資本金・総資産"):
            data[Schema.CAP] = prof["資本金・総資産"]
        if ach.get("売上高"):
            data[Schema.SALES] = ach["売上高"]
        if prof.get("従業員数"):
            data[Schema.EMP_NUM] = prof["従業員数"]
        if prof.get("設立"):
            data["設立日"] = prof["設立"]
        if prof.get("事業内容"):
            data["事業内容"] = prof["事業内容"]
        if con.get("FAX番号"):
            data["FAX"] = con["FAX番号"]
        if con.get("E-mailアドレス"):
            data["メール"] = con["E-mailアドレス"]
        if con.get("自社ホームページ"):
            data[Schema.HP] = con["自社ホームページ"]

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    EntreNetScraper().execute("https://entrenet.jp/dokuritsu/?cbn=top_src_fw")
