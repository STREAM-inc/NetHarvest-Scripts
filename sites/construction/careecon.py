"""
CAREECON採用 (recruit.careecon.jp) — 建設業界向け採用ページの企業情報スクレイパー

取得対象:
    - BRANU株式会社が運営する採用ページ作成サービス「CAREECON採用」に
      掲載されている企業の個別ページ (/co/<slug>) 全件 (約1,900件)
    - 会社名・本社所在地 (郵便番号/都道府県/住所)・代表者名 (役職)・設立年月・
      社員数・事業内容・公式ホームページURL・募集職種 など

取得フロー:
    一覧ページは存在せず (ルート /co/ は 404)、列挙は sitemap のみ:
        https://recruit.careecon.jp/sitemap_top_1.xml
            -> <loc>https://recruit.careecon.jp/co/<slug></loc> が約1,919件
    sitemap から企業ページ URL を 1 件ずつ取得し、取得できた時点で即 yield する。
    sitemap の URL は引数 url (= sites.yml の url) から urljoin で組み立てる。

ページ構造 (Rails の SSR。静的取得で全項目が取れる):
    section#company_info
        .p-index-company__description__title__names__name   -> 会社名
        .p-index-company__description__title__names__url     -> 公式ホームページURL (無い企業あり)
        .p-index-company__description__info__item            -> ラベル/値のペア
            __name = 本社所在地 / 代表取締役 / 設立年月 / 社員数 / 事業内容 / 保有資格
            __text = 値 (本社所在地のみ <span>〒...</span><span>住所</span> の 2 要素)
    section#recruits
        .c-job-listings-card__description__title             -> 募集職種 (求人タイトル)
        .c-job-listings-card__tags (caption=業種)            -> サイト定義業種

備考 (依頼元指示):
    - ページネーション・絞り込みは不要 (sitemap から全 URL を直接列挙)
    - TEL はサイト内に記載が無いため空欄 (24件サンプルで tel: リンク 0 件)
    - 公式ホームページURL が無い企業は該当列を空欄にする

利用規約:
    careecon.jp の利用規約 (第7条 禁止事項) にスクレイピング/クローリングの
    明示的な禁止条項は無い。「本サービスに過度な負荷をかける行為」が
    禁止されているため DELAY=1.0 秒で巡回する。

実行方法:
    python scripts/sites/construction/careecon.py
    python bin/run_flow.py --site-id careecon
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 企業ページ URL の列挙元 (一覧ページが存在しないため sitemap が唯一の列挙手段)
SITEMAP_PATH = "/sitemap_top_1.xml"
COMPANY_PATH_RE = re.compile(r"^/co/[^/]+/?$")

MAX_ATTEMPTS = 3        # sitemap 取得のリトライ上限 (無限ループ禁止)
SKIP_SLUGS = {"hogehogehoge"}   # 運営側のテスト用ページ

PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# 企業情報ブロックのラベル (サンプル24件での出現率)
LABEL_ADDR = "本社所在地"    # 100%
LABEL_REP = "代表取締役"     # 100% (役職名そのものがラベルになる)
LABEL_FOUNDED = "設立年月"   # 67%
LABEL_EMP = "社員数"         # 100%
LABEL_LOB = "事業内容"       # 38%
LABEL_LICENSE = "保有資格"   # 4% (出現率は低いが実装する)

# 代表者ラベルの表記ゆれ (「代表取締役」以外が来ても代表者として扱う)
REP_LABEL_RE = re.compile(r"代表|社長|理事長|会長|オーナー")


def _clean(text: str | None) -> str:
    """CSV 用に空白を 1 つに畳んで前後を落とす。"""
    if not text:
        return ""
    return re.sub(r"[\s　]+", " ", text).strip()


def _split_address(raw: str) -> tuple[str, str, str]:
    """「〒183-0051 東京都府中市栄町3丁目15-1」を (郵便番号, 都道府県, 住所) に分解する。"""
    text = _clean(raw)
    post_code = ""
    m = re.search(r"〒?\s*(\d{3}-?\d{4})", text)
    if m:
        post_code = m.group(1)
        if len(post_code) == 7:
            post_code = f"{post_code[:3]}-{post_code[3:]}"
        text = _clean(text[: m.start()] + text[m.end():])
    for pref in PREFECTURES:
        if text.startswith(pref):
            return post_code, pref, _clean(text[len(pref):])
    return post_code, "", text


def _format_founded(raw: str) -> str:
    """「2016年11月」を「2016-11」に正規化する (日が無いので日は付けない)。"""
    text = _clean(raw)
    if not text:
        return ""
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})?\s*月?\s*(\d{1,2})?\s*日?", text)
    if not m:
        return text
    year, month, day = m.group(1), m.group(2), m.group(3)
    if month and day:
        return f"{year}-{int(month):02d}-{int(day):02d}"
    if month:
        return f"{year}-{int(month):02d}"
    return year


class CareeconRecruitScraper(StaticCrawler):
    """CAREECON採用 (掲載企業) スクレイパー"""

    DELAY = 1.0   # 規約の「過度な負荷をかける行為」禁止に配慮

    EXTRA_COLUMNS = [
        "企業ID",
        "募集職種",
        "募集件数",
        "保有資格",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_company_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("sitemap から企業ページ %d 件を取得対象にします", self.total_items)

        for detail_url in detail_urls:
            try:
                soup = self.get_soup(detail_url)
            except requests.exceptions.RequestException as e:
                self.logger.warning("企業ページ取得失敗 (スキップ): %s (%s)", detail_url, e)
                continue
            if soup is None:
                continue

            item = self._build_item(detail_url, soup)
            if item:
                # 1 件ずつ即 yield する (全件収集してからの一括 yield は禁止)
                yield item

    # ------------------------------------------------------------------
    # 列挙: sitemap から /co/<slug> 形式の企業ページ URL を集める
    # ------------------------------------------------------------------
    def _collect_company_urls(self, url: str) -> list[str]:
        sitemap_url = urljoin(url, SITEMAP_PATH)
        xml = self._fetch_sitemap(sitemap_url)

        seen: set[str] = set()
        results: list[str] = []
        for loc in re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml):
            # 引数 url を基準に正規化し、/co/<slug> 形式だけを対象にする
            absolute = urljoin(url, loc)
            path = re.sub(r"^https?://[^/]+", "", absolute)
            if not COMPANY_PATH_RE.match(path):
                continue
            slug = path.strip("/").split("/")[-1]
            if slug in SKIP_SLUGS or absolute in seen:
                continue
            seen.add(absolute)
            results.append(absolute)
        return results

    def _fetch_sitemap(self, sitemap_url: str) -> str:
        """sitemap XML を取得する。失敗時は上限付きでリトライし、全滅なら例外を送出する。"""
        last_error: Exception | None = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
                response.raise_for_status()
                response.encoding = response.apparent_encoding or "utf-8"
                return response.text
            except requests.exceptions.RequestException as e:
                last_error = e
                self.logger.warning(
                    "sitemap 取得失敗 (%d/%d): %s", attempt + 1, MAX_ATTEMPTS, e
                )
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"sitemap の取得に {MAX_ATTEMPTS} 回失敗しました: {sitemap_url}") from last_error

    # ------------------------------------------------------------------
    # 詳細ページ 1 件 -> 1 レコード
    # ------------------------------------------------------------------
    def _build_item(self, detail_url: str, soup: bs4.BeautifulSoup) -> dict | None:
        section = soup.select_one("section#company_info")
        if section is None:
            self.logger.warning("企業情報セクションが見つかりません: %s", detail_url)
            return None

        name_el = section.select_one(".p-index-company__description__title__names__name")
        name = _clean(name_el.get_text()) if name_el else ""
        if not name:
            self.logger.warning("会社名を取得できません: %s", detail_url)
            return None

        hp_el = section.select_one(".p-index-company__description__title__names__url")
        # 公式ホームページを掲載していない企業は該当列を空欄にする
        hp = _clean(hp_el.get("href")) if hp_el else ""

        fields = self._extract_fields(section)

        post_code, pref, addr = _split_address(fields.get(LABEL_ADDR, ""))

        rep_label, rep_name = "", ""
        for label, value in fields.items():
            if REP_LABEL_RE.search(label):
                rep_label, rep_name = label, value
                break

        job_titles, job_genres = self._extract_jobs(soup)

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: "",   # サイト上に電話番号の掲載が無い
            Schema.REP_NM: rep_name,
            Schema.POS_NM: rep_label,
            Schema.EMP_NUM: fields.get(LABEL_EMP, ""),
            Schema.LOB: fields.get(LABEL_LOB, ""),
            Schema.OPEN_DATE: _format_founded(fields.get(LABEL_FOUNDED, "")),
            Schema.HP: hp,
            Schema.CAT_SITE: "、".join(job_genres),
            "企業ID": detail_url.rstrip("/").split("/")[-1],
            "募集職種": " / ".join(job_titles),
            "募集件数": str(len(job_titles)),
            "保有資格": fields.get(LABEL_LICENSE, ""),
        }

    def _extract_fields(self, section: bs4.Tag) -> dict[str, str]:
        """企業情報ブロックのラベル/値ペアを辞書化する。"""
        fields: dict[str, str] = {}
        for item in section.select(".p-index-company__description__info__item"):
            label_el = item.select_one(".p-index-company__description__info__item__name")
            value_el = item.select_one(".p-index-company__description__info__item__text")
            label = _clean(label_el.get_text()) if label_el else ""
            if not label or value_el is None:
                continue
            # 本社所在地は <span>〒...</span><span>住所</span> の 2 要素に分かれている
            spans = value_el.select(".p-index-company__description__address span")
            if spans:
                value = _clean(" ".join(s.get_text() for s in spans))
            else:
                value = _clean(value_el.get_text(" "))
            if value and label not in fields:
                fields[label] = value
        return fields

    def _extract_jobs(self, soup: bs4.BeautifulSoup) -> tuple[list[str], list[str]]:
        """募集中の求人から募集職種 (求人タイトル) と業種タグを集める。"""
        titles: list[str] = []
        genres: list[str] = []
        for card in soup.select("section#recruits .c-job-listings-card"):
            title_el = card.select_one(".c-job-listings-card__description__title")
            title = _clean(title_el.get_text()) if title_el else ""
            if title and title not in titles:
                titles.append(title)
            for tags in card.select(".c-job-listings-card__tags"):
                caption_el = tags.select_one(".c-job-listings-card__tags__caption span")
                if not caption_el or _clean(caption_el.get_text()) != "業種":
                    continue
                for tag in tags.select(".c-job-listings-card__tags__items__item__text"):
                    genre = _clean(tag.get_text())
                    if genre and genre not in genres:
                        genres.append(genre)
        return titles, genres


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CareeconRecruitScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://recruit.careecon.jp/co/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
