import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(r"(北海道|東京都|大阪府|京都府|.{2,3}県)")
_POST_RE = re.compile(r"〒?(\d{3}-?\d{4})")


class OKurumaScraper(StaticCrawler):
    """おくるまドットコム 自動車関連店舗情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["FAX", "一言コメント"]

    SHOP_ID_MAX = 900

    def parse(self, url: str) -> Generator[dict, None, None]:
        """連番URL https://www.o-kuruma.com/shop_{N}.html を順に巡回する。

        欠番(退店済み)の ID が多いため early termination は行わず、1..SHOP_ID_MAX を
        全件走査する。欠番はテンプレのみの空ページが返るので _scrape_detail が
        None を返し自然にスキップされる。
        """
        base = url.rstrip("/")
        self.total_items = self.SHOP_ID_MAX
        for shop_id in range(1, self.SHOP_ID_MAX + 1):
            shop_url = f"{base}/shop_{shop_id}.html"
            item = self._scrape_detail(shop_url)
            if item is None:
                continue
            yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # dl.data 内は <dd>label</dd><dt>value</dt> の順序で登場する(通常と逆)
        for dl in soup.select("dl.data"):
            current_label: str | None = None
            for child in dl.find_all(["dd", "dt"], recursive=False):
                if child.name == "dd":
                    current_label = child.get_text(strip=True)
                elif child.name == "dt" and current_label is not None:
                    value_text = child.get_text(separator="\n", strip=True)
                    self._assign_field(data, current_label, value_text)
                    current_label = None

        # 一言コメント: <h3>一言コメント</h3> の直後の <p> またはテキストブロック
        comment_el = soup.find(
            lambda tag: tag.name in ("h3", "h4", "dt") and tag.get_text(strip=True) == "一言コメント"
        )
        if comment_el:
            next_el = comment_el.find_next(["p", "dd", "div"])
            if next_el:
                comment = next_el.get_text(separator="\n", strip=True)
                if comment and comment != "一言コメント":
                    data["一言コメント"] = comment

        if not data.get(Schema.NAME):
            return None
        return data

    def _assign_field(self, data: dict, label: str, value: str) -> None:
        if not value:
            return

        if "店舗名称" in label:
            data[Schema.NAME] = value

        elif "住所" in label:
            flat = value.replace("\n", "")
            m_post = _POST_RE.search(flat)
            if m_post:
                post = m_post.group(1)
                if "-" not in post:
                    post = f"{post[:3]}-{post[3:]}"
                data[Schema.POST_CODE] = post
                addr = _POST_RE.sub("", flat).strip()
            else:
                addr = flat
            m_pref = _PREF_RE.search(addr)
            if m_pref:
                data[Schema.PREF] = m_pref.group(1)
            data[Schema.ADDR] = addr

        elif "電話" in label or "TEL" in label.upper():
            parts = re.split(r"[\s/／]+", value)
            parts = [p.strip() for p in parts if p.strip()]
            if parts:
                data[Schema.TEL] = parts[0]
            if len(parts) >= 2:
                data["FAX"] = parts[1]

        elif "FAX" in label.upper():
            data["FAX"] = value

        elif "営業時間" in label:
            data[Schema.TIME] = value.replace("\n", " ")

        elif "定休日" in label:
            data[Schema.HOLIDAY] = value.replace("\n", " ")

        elif "ホームページ" in label or "HP" in label.upper() or "URL" in label.upper():
            data[Schema.HP] = value.splitlines()[0].strip()

        elif "ジャンル" in label or "業種" in label or "カテゴリ" in label:
            genres = [g.strip() for g in value.split("\n") if g.strip()]
            data[Schema.CAT_SITE] = " / ".join(genres)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    OKurumaScraper().execute("https://www.o-kuruma.com")
