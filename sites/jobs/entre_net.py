import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://entrenet.jp"
LIST_BASE = "https://entrenet.jp/dokuritsu/?cbn=top_src_fw"
MAX_PAGES = 100
# 連続でこの回数だけ取得に失敗したら打ち切る (WAF ブロック中に叩き続けないため)
MAX_CONSECUTIVE_FAILURES = 3


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[\r\n\t]+", " ", str(s)).strip()


class EntreNetScraper(StaticCrawler):
    """アントレ 独立・開業支援 スクレイパー（entrenet.jp）"""

    # 待機はリクエスト単位で _get() が行う。基盤のアイテム間待機は二重になるので無効化
    DELAY = 3.0
    ITEM_DELAY = 0.0
    EXTRA_COLUMNS = ["代表者", "資本金", "売上", "従業員数", "設立日", "事業内容", "FAX", "メール"]

    _last_request_at: float = 0.0

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        failures = 0
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item is None:
                failures += 1
                if failures >= MAX_CONSECUTIVE_FAILURES:
                    self.logger.error(
                        "詳細ページの取得に %d 回連続で失敗したため打ち切ります "
                        "(WAF ブロックの可能性): 最後の URL=%s",
                        failures, detail_url,
                    )
                    return
                continue
            failures = 0
            yield item

    def _get(self, url: str):
        """DELAY 秒の間隔を空けて get_soup() する (キャッシュリプレイ時は待たない)。

        基盤の待機はアイテムを yield したときにしか入らないため、取得失敗が続くと
        無待機で連続アクセスになる。リクエスト単位で間隔を保証する。
        """
        if self._last_request_at:
            remain = self.DELAY - (time.monotonic() - self._last_request_at)
            if remain > 0:
                time.sleep(remain)
        soup = self.get_soup(url)
        if not self._last_fetch_from_cache:
            self._last_request_at = time.monotonic()
        return soup

    def _collect_detail_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for page in range(1, MAX_PAGES + 1):
            page_url = f"{LIST_BASE}&page={page}"
            soup = self._get(page_url)
            if soup is None:
                self.logger.warning("一覧ページの取得に失敗したため収集を打ち切ります: %s", page_url)
                break

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

    def _scrape_detail(self, url: str) -> dict | None:
        company_url = url.rstrip("/") + "/kigyou/"
        soup = self._get(company_url)
        if soup is None:
            return None

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
