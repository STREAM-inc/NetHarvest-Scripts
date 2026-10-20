# scripts/sites/construction/andreform_2.py
"""
&リフォーム（アンドリフォーム）加盟会社 (andreform.jp) — 全国 47 都道府県版

対象サイト: https://andreform.jp/fukuoka

取得対象 (備考指示):
    - 粒度 = 会社 (/company/{id} 単位)
    - 全国 47 都道府県の一覧ページ /{県ローマ字}?page=N を最終ページまで巡回
    - 一覧内の /company/{id}/{n} 形式リンク (口コミページ) は会社 ID に正規化
    - 同一会社が複数県の一覧に出るため会社 ID で重複排除し、詳細は 1 回だけ取得
    - TEL・公式 HP はサイト上に無いので取得しない
    - /cont/ 配下は robots.txt で Disallow のためアクセスしない

取得フロー (Pattern B: 詳細 1 件取得ごとに即 yield):
    引数 url (例: https://andreform.jp/fukuoka) の県を先頭に、
    同じホストの残り 46 都道府県一覧 /{slug}?page=N を順に巡回
      → 一覧カード .switch-target-company .company-box から会社 ID を抽出
      → 詳細 /company/{id} を 1 件取得するごとに yield

ページネーション:
    ol.pagination の li.next に href がある間だけ次ページへ進む
    (最終ページは class="next end" + href 空)。カード 0 件でも打ち切る。

カラム補足:
    - 営業時間 / 定休日: サイト上 (会社詳細・口コミページとも) に項目が存在しないため常に空
    - 施工実績カテゴリ: 「工事の実績ランキング」(No.順の工事種別)。掲載の無い会社は空
    - 事業内容: 「得意な工事」タグ
    - 掲載一覧の県: その会社を最初に見つけた都道府県一覧ページの県名
    - 「特長とポイント」「得意な工事本文」「対応可能エリアの説明文」「口コミ本文」は
      自由記述の長文のため、著作権リスクを避けて取得しない

実行方法:
    python scripts/sites/construction/andreform_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id andreform_2
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class Andreform2Scraper(StaticCrawler):
    """&リフォーム 加盟会社 スクレイパー (全国 47 都道府県)"""

    DELAY = 1.0  # 備考: リクエスト間隔 1 秒以上

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    EXTRA_COLUMNS = [
        "会社ID",
        "掲載一覧の県",
        "施工実績カテゴリ",
        "資格",
        "建設業許可証",
        "加入保険",
        "提携ローン",
        "アクセス方法",
    ]

    # 一覧ページのスラッグ → 都道府県名 (サイトの綴り: 群馬 = gumma)
    PREF_SLUGS = {
        "hokkaido": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
        "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県", "ibaraki": "茨城県",
        "tochigi": "栃木県", "gumma": "群馬県", "saitama": "埼玉県", "chiba": "千葉県",
        "tokyo": "東京都", "kanagawa": "神奈川県", "niigata": "新潟県", "toyama": "富山県",
        "ishikawa": "石川県", "fukui": "福井県", "yamanashi": "山梨県", "nagano": "長野県",
        "gifu": "岐阜県", "shizuoka": "静岡県", "aichi": "愛知県", "mie": "三重県",
        "shiga": "滋賀県", "kyoto": "京都府", "osaka": "大阪府", "hyogo": "兵庫県",
        "nara": "奈良県", "wakayama": "和歌山県", "tottori": "鳥取県", "shimane": "島根県",
        "okayama": "岡山県", "hiroshima": "広島県", "yamaguchi": "山口県", "tokushima": "徳島県",
        "kagawa": "香川県", "ehime": "愛媛県", "kochi": "高知県", "fukuoka": "福岡県",
        "saga": "佐賀県", "nagasaki": "長崎県", "kumamoto": "熊本県", "oita": "大分県",
        "miyazaki": "宮崎県", "kagoshima": "鹿児島県", "okinawa": "沖縄県",
    }

    # 想定外のページ送りで無限ループしないための上限 (1 県あたり)
    MAX_PAGES = 200

    _PREF_RE = re.compile(r"^\s*(北海道|東京都|(?:京都|大阪)府|.{2,3}県)\s*(.*)$", re.S)
    # 「?」化けの原因になる各種ハイフン/ダッシュ/マイナス → 半角「-」
    _HYPHEN_RE = re.compile(r"[\u2010-\u2015\u2212\uFF0D]")
    # 数字に挟まれた長音記号「ー」(ｰ 含む) の誤用 → 半角「-」
    _CHOON_RE = re.compile(r"(?<=[0-9０-９])[ー－ｰ](?=[0-9０-９])")
    _ERA_BASE = {"明治": 1867, "大正": 1911, "昭和": 1925, "平成": 1988, "令和": 2018}

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        引数 url の県一覧を先頭に全国 47 都道府県の一覧を巡回し、会社詳細を 1 件ずつ yield する。

        Args:
            url: 都道府県一覧 URL (例: https://andreform.jp/fukuoka)
        """
        root = urljoin(url, "/")
        start_slug = urlparse(url).path.strip("/").split("/")[0]

        areas: list[tuple[str, str]] = []
        if start_slug in self.PREF_SLUGS:
            areas.append((url.split("?")[0], self.PREF_SLUGS[start_slug]))
        for slug, pref_name in self.PREF_SLUGS.items():
            if slug != start_slug:
                areas.append((urljoin(root, slug), pref_name))

        seen_ids: set[str] = set()
        for area_url, pref_name in areas:
            self.logger.info("エリア巡回開始: %s (%s)", pref_name, area_url)
            for page in range(1, self.MAX_PAGES + 1):
                list_url = area_url if page == 1 else f"{area_url}?page={page}"
                soup = self.get_soup(list_url)
                if soup is None:
                    self.logger.warning("一覧ページ取得失敗: %s", list_url)
                    break

                cards = soup.select(".switch-target-company .company-box")
                if not cards:
                    self.logger.info("一覧にカードが無いため終了: %s", list_url)
                    break

                for card in cards:
                    info = self._parse_card(card, root)
                    if info is None or info["id"] in seen_ids:
                        continue
                    seen_ids.add(info["id"])
                    row = self._parse_detail(info, pref_name)
                    if row:
                        yield row

                if not self._has_next(soup):
                    break

    # ------------------------------------------------------------------
    # 一覧
    # ------------------------------------------------------------------
    def _parse_card(self, card, root: str) -> dict | None:
        """一覧カードから会社 ID・会社名・住所・タグ・口コミ評価を取り出す"""
        company_id = ""
        for a in card.select("a[href]"):
            # /company/{id}/{n} (口コミページ) も会社 ID に正規化する
            m = re.search(r"/company/(\d+)", a.get("href") or "")
            if m:
                company_id = m.group(1)
                break
        if not company_id:
            return None

        name_el = card.select_one(".com-head .name h3 a") or card.select_one(".com-head .name h3")
        addr_el = card.select_one(".com-head .name > p")
        score_el = card.select_one(".score .point-num")
        review_el = card.select_one(".score .review-num")
        tags = [s.get_text(strip=True) for s in card.select(".com-head .name .tag span")]

        return {
            "id": company_id,
            "url": urljoin(root, f"company/{company_id}"),
            "name": name_el.get_text(" ", strip=True) if name_el else "",
            "raw_addr": addr_el.get_text(" ", strip=True) if addr_el else "",
            "tags": [t for t in tags if t],
            "score": score_el.get_text(strip=True) if score_el else "",
            "review": review_el.get_text(strip=True) if review_el else "",
        }

    @staticmethod
    def _has_next(soup) -> bool:
        """ol.pagination の「次へ」に有効なリンクがあるか"""
        nxt = soup.select_one("ol.pagination li.next")
        if nxt is None or "end" in (nxt.get("class") or []):
            return False
        a = nxt.select_one("a[href]")
        return bool(a and (a.get("href") or "").strip())

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _parse_detail(self, info: dict, area_pref: str) -> dict | None:
        soup = self.get_soup(info["url"])

        table: dict[str, str] = {}
        tags: list[str] = []
        ranking = ""
        if soup is not None:
            table = self._basic_info(soup)
            tags = [s.get_text(strip=True) for s in soup.select(".specialty > .tag span")]
            tags = [t for t in tags if t]
            ranking = self._ranking(soup)
            if not info["score"]:
                el = soup.select_one(".score .point-num")
                info["score"] = el.get_text(strip=True) if el else ""
            if not info["review"]:
                el = soup.select_one(".score .review-num")
                info["review"] = el.get_text(strip=True) if el else ""
        else:
            self.logger.warning("詳細ページ取得失敗 (一覧の情報のみで出力): %s", info["url"])

        name = table.get("業者名") or info["name"]
        if not name:
            return None

        pref, addr = self._split_address(table.get("住所") or info["raw_addr"])
        post_code = re.sub(r"^〒\s*", "", table.get("郵便番号", "")).strip()
        if not re.search(r"\d", post_code):
            post_code = ""  # 「-」等の未登録プレースホルダ

        row = {
            Schema.URL: info["url"],
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.REP_NM: table.get("代表者名", ""),
            Schema.LOB: " / ".join(tags or info["tags"]),
            Schema.OPEN_DATE: self._to_iso_date(table.get("設立年月日", "")),
            Schema.SCORES: info["score"],
            Schema.REV_SCR: info["review"],
            Schema.TIME: table.get("営業時間", ""),
            Schema.HOLIDAY: table.get("定休日", ""),
            "会社ID": info["id"],
            "掲載一覧の県": area_pref,
            "施工実績カテゴリ": ranking,
            "資格": table.get("資格", ""),
            "建設業許可証": table.get("建設業許可証", ""),
            "加入保険": table.get("加入保険", ""),
            "提携ローン": table.get("提携ローン", ""),
            "アクセス方法": table.get("アクセス方法", ""),
        }
        return {k: self._normalize_hyphen(v) for k, v in row.items()}

    # ------------------------------------------------------------------
    # パーツ抽出
    # ------------------------------------------------------------------
    @classmethod
    def _normalize_hyphen(cls, value):
        """各種ハイフン・ダッシュ・マイナス・数字間の長音記号を半角「-」に揃える"""
        if not isinstance(value, str) or not value:
            return value
        return cls._CHOON_RE.sub("-", cls._HYPHEN_RE.sub("-", value))

    @staticmethod
    def _basic_info(soup) -> dict:
        """「〇〇の基本情報」テーブルの <th>ラベル</th><td>値</td> を辞書化する"""
        fields: dict[str, str] = {}
        block = soup.select_one(".com-info.block table") or soup.select_one(".com-info table")
        if block is None:
            return fields
        for tr in block.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th is None or td is None:
                continue
            label = re.sub(r"\s+", "", th.get_text(" ", strip=True))
            # 「地図で見る」等のボタンは値ではないので除去する
            for btn in td.select(".btn, script, style"):
                btn.decompose()
            fields[label] = re.sub(r"[ \t　]+", " ", td.get_text(" ", strip=True)).strip()
        return fields

    @staticmethod
    def _ranking(soup) -> str:
        """工事の実績ランキング (No.順の工事種別) を「 / 」連結で返す"""
        ul = soup.select_one(".specialty ul.ranking")
        if ul is None:
            return ""
        items = []
        for li in ul.select("li"):
            span = li.select_one("span")
            if span is not None:
                span.extract()  # 「No.1」等の順位ラベル
            text = li.get_text(" ", strip=True)
            if text:
                items.append(text)
        return " / ".join(items)

    @classmethod
    def _split_address(cls, raw: str) -> tuple[str, str]:
        """住所文字列を都道府県と市区町村以降に分割する"""
        text = re.sub(r"\s+", " ", (raw or "")).strip()
        if not text:
            return "", ""
        m = cls._PREF_RE.match(text)
        if m:
            return m.group(1), cls._dedupe_address(m.group(1), m.group(2).strip())
        return "", text

    @staticmethod
    def _dedupe_address(pref: str, rest: str) -> str:
        """住所二重記載 (例:「東京都多摩市東京都多摩市関戸…」) を 1 回だけ畳む"""
        idx = rest.find(pref)
        if idx <= 0:
            return rest
        head, tail = rest[:idx], rest[idx + len(pref):]
        return tail if tail.startswith(head) else rest

    @classmethod
    def _to_iso_date(cls, raw: str) -> str:
        """「2006年7月1日」「平成14年1月」→ ISO 形式。年月のみは YYYY-MM、不明形式はそのまま"""
        text = (raw or "").strip()
        if not text:
            return ""
        m = re.search(r"(明治|大正|昭和|平成|令和)\s*(元|\d{1,2})\s*年", text)
        if m:
            year = cls._ERA_BASE[m.group(1)] + (1 if m.group(2) == "元" else int(m.group(2)))
            text = text[:m.start()] + f"{year}年" + text[m.end():]
        m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
        if m:
            return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月", text)
        if m:
            return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}"
        m = re.search(r"(\d{4})\s*年", text)
        if m:
            return f"{int(m.group(1)):04d}"
        return text


if __name__ == "__main__":
    scraper = Andreform2Scraper()
    scraper.execute("https://andreform.jp/fukuoka")
