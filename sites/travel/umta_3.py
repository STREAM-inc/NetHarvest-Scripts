"""
UMTA会員名簿 (ミャンマー旅行代理店協会) — umta_3

取得対象:
    Member Directory (https://www.umtanet.org/index.php/en/members/membership-directory)
    ミャンマー観光省 (Ministry of Hotels & Tourism) 認可の旅行代理店・
    ツアーオペレーター等 UMTA 会員の一覧。

サイト構造 (Phase 1 調査結果):
    - Joomla 製の静的 1 ページ。ページネーション・検索フォーム・絞り込みは一切無い
      (本文 div[itemprop="articleBody"] 内の単一 <table> に約 254 行)。
    - 各行は 2 セル構成: [ロゴ画像セル][会社名セル]。どちらのセルのリンクも
      会員企業の自社サイト URL (約 184 行)。リンクが無い行は href="#"。
    - UMTA サイト内に会員の詳細ページは存在せず、住所・電話番号・メールの
      掲載も一切無い (協会自身の連絡先が contact-us にあるのみ)。
      → 備考で要求された「住所 / TEL」は、会員企業の自社サイトから
        構造化された箇所に限って補完する。
    - 利用規約ページは存在しない (フッターは Web 制作者表記のみ)。
      robots.txt は /administrator/ 等の管理系のみ Disallow で、
      /index.php/en/members/ は許可。

取得フロー:
    1. ルート URL (= sites.yml の url) を 1 回 GET し、本文テーブルの行を走査
    2. 会社名 / 自社サイト URL (HP) / ロゴ画像 URL を取得
    3. HP がある会員のみ、そのトップページを 1 回 GET して
       TEL / メール / 住所 / SNS を補完。埋まらない項目がある場合に限り
       同一ドメインの contact ページを最大 MAX_SUB_PAGES 件だけ追加取得
    4. 1 件ごとに即 yield (Pattern B)

注意:
    - 住所は誤検出を避けるため、JSON-LD / microdata の PostalAddress か
      「Address:」等のラベル付きテキストで、かつミャンマー国内を示す語
      (Yangon / Township / Myanmar 等) を含む場合のみ採用する。
    - 会員企業の自社サイトは応答が遅い / 消滅しているものが多いため、
      外部サイトへのアクセスは短いタイムアウトで行い、失敗しても空文字で継続する。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/umta_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id umta_3
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
import requests

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 一覧テーブルのヘッダー行などを弾くための語
_HEADER_WORDS = {"company", "company name", "member", "members", "name", "no", "no.", "logo"}

# href="#" / javascript: 等のダミーリンク
_PLACEHOLDER_PATTERN = re.compile(r"^(?:#|javascript:|tel:|about:blank)", re.IGNORECASE)

# ミャンマー国内住所であることの確からしさを担保するキーワード
_MYANMAR_HINT_PATTERN = re.compile(
    r"(Myanmar|Burma|Yangon|Rangoon|Mandalay|Naypyidaw|Naypyitaw|Nay\s*Pyi\s*Taw|"
    r"Bagan|Nyaung\s*U|Nyaung\s*Shwe|Taunggyi|Township|Mawlamyine|Bago|Pathein|"
    r"Inle|Heho|Myeik|Dawei|Kalaw|Monywa|Magway|Sittwe|Loikaw|Hpa\s*-?\s*an)",
    re.IGNORECASE,
)
# 住所らしさを判定する構成要素 (番地・階・通り・タウンシップ等)
_ADDR_TOKEN_PATTERN = re.compile(
    r"(?i)(No\.?\s*\(?\d|Room|Floor|Bldg|Building|Street|St\.|Road|Rd\.|Lane|"
    r"Avenue|Ave\.|Township|Quarter|Ward|Tower|Condo|Plaza)"
)
# ラベル付き住所テキスト
_ADDR_LABEL_PATTERN = re.compile(
    r"(?i)(?:Address|Head\s*Office|Office\s*Address|Location)\s*[:：]\s*([^\n\r]{12,200})"
)
# contact / about ページ候補の判定
_CONTACT_HINT_PATTERN = re.compile(r"(?i)(contact|about|company-profile|aboutus)")
# 明らかに電話番号でない tel: リンク
_TEL_NG_PATTERN = re.compile(r"^\+?0+$")

# SNS
_SNS_PATTERNS = {
    Schema.FB: re.compile(r"(?i)^https?://(?:www\.|m\.|web\.)?facebook\.com/"),
    Schema.INSTA: re.compile(r"(?i)^https?://(?:www\.)?instagram\.com/"),
}
_SNS_NG_PATTERN = re.compile(
    r"(?i)(sharer|share\.php|/share|/plugins/|intent/|/tr\?|dialog/)"
)


class UmtaMemberDirectoryScraper(StaticCrawler):
    """UMTA 会員名簿 (Member Directory) のクローラー。"""

    # 一覧は 1 ページのみ。会員サイトへの外部アクセスがあるため軽めのウェイト
    DELAY = 0.5
    ITEM_DELAY = 0.0
    TIMEOUT = 30
    CONTINUE_ON_ERROR = True

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    # 会員企業の自社サイト取得用 (遅い / 消滅サイトが多いので短めに)
    MEMBER_SITE_TIMEOUT = 8
    # トップページで埋まらなかったときに追加で見る contact ページ数
    MAX_SUB_PAGES = 1

    EXTRA_COLUMNS = ["ロゴ画像URL", "連絡先取得元URL"]

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str):
        """一覧テーブルを 1 行ずつ処理し、1 件ごとに即 yield する。"""
        soup = self.get_soup(url)
        if soup is None:
            raise RuntimeError(f"一覧ページを取得できませんでした: {url}")

        rows = self._select_rows(soup)
        if not rows:
            raise RuntimeError(f"会員テーブルが見つかりませんでした: {url}")

        self.logger.info("会員テーブル行数: %s", len(rows))

        seen: set[tuple[str, str]] = set()
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            logo_cell, name_cell = tds[0], tds[-1]

            name = re.sub(r"\s+", " ", name_cell.get_text(" ", strip=True)).strip()
            if not name or name.lower() in _HEADER_WORDS:
                continue

            hp = self._extract_hp(url, name_cell) or self._extract_hp(url, logo_cell)

            key = (name.casefold(), hp)
            if key in seen:
                continue
            seen.add(key)

            logo = ""
            img = logo_cell.find("img", src=True)
            if img and (img["src"] or "").strip():
                logo = urljoin(url, img["src"].strip())

            contact = self._collect_contact(hp)

            yield {
                Schema.URL: url,
                Schema.NAME: name,
                Schema.ADDR: contact["addr"],
                Schema.TEL: contact["tel"],
                Schema.EMAIL: contact["email"],
                Schema.HP: hp,
                Schema.FB: contact[Schema.FB],
                Schema.INSTA: contact[Schema.INSTA],
                "ロゴ画像URL": logo,
                "連絡先取得元URL": contact["source"],
            }

    # ------------------------------------------------------------------
    # 一覧ページ
    # ------------------------------------------------------------------
    @staticmethod
    def _select_rows(soup: bs4.BeautifulSoup) -> list[bs4.Tag]:
        """本文内で最も行数の多いテーブルの行を返す。"""
        body = soup.select_one('div[itemprop="articleBody"]') or soup.select_one("#zo2-component")
        scope = body if body is not None else soup

        best: list[bs4.Tag] = []
        for table in scope.find_all("table"):
            rows = [tr for tr in table.find_all("tr") if len(tr.find_all("td")) >= 2]
            if len(rows) > len(best):
                best = rows
        # 装飾用の小さなテーブルを誤検出しないよう最低行数を設ける
        return best if len(best) >= 5 else []

    @staticmethod
    def _extract_hp(base_url: str, cell: bs4.Tag) -> str:
        """セル内リンクから会員企業の自社サイト URL を取り出して正規化する。"""
        for a in cell.find_all("a", href=True):
            href = (a["href"] or "").strip()
            if not href or _PLACEHOLDER_PATTERN.match(href) or href.startswith("mailto:"):
                continue

            # 「/www.example.com」のようなスキーム欠落リンクの救済
            if href.startswith("/www.") or href.startswith("www."):
                href = "https://" + href.lstrip("/")
            elif not href.startswith(("http://", "https://")):
                href = urljoin(base_url, href)

            netloc = urlparse(href).netloc.lower()
            # UMTA 自身のページ (自社サイトではない) は採用しない
            if not netloc or "umtanet.org" in netloc:
                continue
            return href
        return ""

    # ------------------------------------------------------------------
    # 会員自社サイトからの連絡先補完
    # ------------------------------------------------------------------
    def _empty_contact(self) -> dict:
        return {
            "tel": "",
            "email": "",
            "addr": "",
            Schema.FB: "",
            Schema.INSTA: "",
            "source": "",
        }

    def _collect_contact(self, hp: str) -> dict:
        """会員企業の自社サイトから TEL / メール / 住所 / SNS を補完する。

        トップページで埋まらない項目がある場合のみ、同一ドメインの contact /
        about ページを最大 MAX_SUB_PAGES 件だけ追加で参照する。
        """
        result = self._empty_contact()
        if not hp:
            return result

        soup = self._fetch_member_soup(hp)
        if soup is None:
            return result

        self._merge_contact(result, soup, hp)
        if self._is_filled(result):
            return result

        for sub_url in self._contact_links(soup, hp)[: self.MAX_SUB_PAGES]:
            sub_soup = self._fetch_member_soup(sub_url)
            if sub_soup is None:
                continue
            self._merge_contact(result, sub_soup, sub_url)
            if self._is_filled(result):
                break
        return result

    @staticmethod
    def _is_filled(result: dict) -> bool:
        """住所・TEL・メールが揃っていれば追加ページを見る必要は無い。"""
        return bool(result["tel"] and result["email"] and result["addr"])

    def _merge_contact(self, result: dict, soup: bs4.BeautifulSoup, page_url: str) -> None:
        """未取得の項目だけを埋め、埋まった場合は取得元ページを記録する。"""
        found = False
        values = {
            "tel": self._pick_tel(soup),
            "email": self._pick_email(soup),
            "addr": self._pick_addr(soup),
        }
        values.update(self._pick_sns(soup))
        for key, value in values.items():
            if not result[key] and value:
                result[key] = value
                found = True
        if found and not result["source"]:
            result["source"] = page_url

    def _fetch_member_soup(self, url: str) -> bs4.BeautifulSoup | None:
        """外部サイトを短いタイムアウトで取得する (失敗しても None で継続)。"""
        try:
            resp = self.session.get(
                url, timeout=self.MEMBER_SITE_TIMEOUT, allow_redirects=True
            )
        except requests.exceptions.RequestException as e:
            self.logger.warning("会員サイト取得失敗 %s: %s", url, e)
            return None

        if resp.status_code != 200:
            self.logger.warning("会員サイト HTTP %s: %s", resp.status_code, url)
            return None

        content_type = resp.headers.get("Content-Type", "").lower()
        if "html" not in content_type:
            return None
        if "charset=" not in content_type:
            resp.encoding = resp.apparent_encoding

        # 極端に大きいページはパースコストが高いので先頭のみ見る
        return bs4.BeautifulSoup(resp.text[:500_000], "html.parser")

    @staticmethod
    def _contact_links(soup: bs4.BeautifulSoup, base_url: str) -> list[str]:
        """同一ドメインの contact / about ページ候補 URL を返す。"""
        base_host = urlparse(base_url).netloc.lower()
        candidates: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = (a["href"] or "").strip()
            if not href or href.startswith(("mailto:", "tel:", "javascript:", "#")):
                continue
            label = a.get_text(" ", strip=True)
            if not _CONTACT_HINT_PATTERN.search(href) and not _CONTACT_HINT_PATTERN.search(label):
                continue
            absolute = urljoin(base_url, href).split("#")[0]
            if urlparse(absolute).netloc.lower() != base_host:
                continue
            if absolute in seen or absolute.rstrip("/") == base_url.rstrip("/"):
                continue
            seen.add(absolute)
            candidates.append(absolute)
        # contact 系を about 系より優先する
        candidates.sort(key=lambda u: 0 if re.search(r"(?i)contact", u) else 1)
        return candidates

    # ------------------------------------------------------------------
    # 各項目の抽出
    # ------------------------------------------------------------------
    @staticmethod
    def _pick_tel(soup: bs4.BeautifulSoup) -> str:
        for a in soup.select('a[href^="tel:"]'):
            raw = re.sub(r"[\s()]", "", a["href"].split(":", 1)[1].strip())
            if len(re.sub(r"\D", "", raw)) >= 7 and not _TEL_NG_PATTERN.match(raw):
                return raw
        # tel: リンクが無い場合はラベル付きテキストから拾う
        text = soup.get_text("\n", strip=True)
        m = re.search(r"(?i)(?:Tel|Phone|Hotline|Ph)\s*[:：]\s*([+\d][\d\s\-+/,()]{6,40})", text)
        if m:
            value = re.sub(r"\s+", " ", m.group(1)).strip(" ,/-")
            if len(re.sub(r"\D", "", value)) >= 7:
                return value
        return ""

    @staticmethod
    def _pick_email(soup: bs4.BeautifulSoup) -> str:
        for a in soup.select('a[href^="mailto:"]'):
            addr = a["href"].split(":", 1)[1].split("?")[0].strip()
            if "@" in addr and not addr.lower().startswith(("example@", "your@", "name@")):
                return addr
        m = re.search(
            r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
            soup.get_text(" ", strip=True),
        )
        if m and not m.group(0).lower().startswith(("example@", "your@", "name@")):
            return m.group(0)
        return ""

    @classmethod
    def _pick_addr(cls, soup: bs4.BeautifulSoup) -> str:
        # 1) JSON-LD の PostalAddress
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except (ValueError, TypeError):
                continue
            addr = cls._addr_from_jsonld(data)
            if addr:
                return addr

        # 2) microdata (itemprop)
        parts = []
        for prop in ("streetAddress", "addressLocality", "addressRegion", "addressCountry"):
            tag = soup.find(attrs={"itemprop": prop})
            if tag:
                value = tag.get("content") or tag.get_text(" ", strip=True)
                if value:
                    parts.append(value.strip())
        if parts:
            candidate = cls._normalize_addr(", ".join(parts))
            if cls._looks_like_myanmar_addr(candidate):
                return candidate

        # 3) 「Address:」等のラベル付きテキスト
        text = soup.get_text("\n", strip=True)
        for m in _ADDR_LABEL_PATTERN.finditer(text):
            candidate = cls._normalize_addr(m.group(1))
            if cls._looks_like_myanmar_addr(candidate):
                return candidate

        # 4) <address> タグ
        for tag in soup.find_all("address"):
            candidate = cls._normalize_addr(tag.get_text(" ", strip=True))
            if cls._looks_like_myanmar_addr(candidate):
                return candidate
        return ""

    @classmethod
    def _addr_from_jsonld(cls, data) -> str:
        """JSON-LD を再帰的に辿って PostalAddress を組み立てる。"""
        if isinstance(data, list):
            for item in data:
                addr = cls._addr_from_jsonld(item)
                if addr:
                    return addr
            return ""
        if not isinstance(data, dict):
            return ""

        for key in ("@graph", "mainEntity", "itemListElement"):
            if key in data:
                addr = cls._addr_from_jsonld(data[key])
                if addr:
                    return addr

        node = data.get("address")
        if isinstance(node, str):
            candidate = cls._normalize_addr(node)
            return candidate if cls._looks_like_myanmar_addr(candidate) else ""
        if isinstance(node, list):
            return cls._addr_from_jsonld({"address": node[0]}) if node else ""
        if isinstance(node, dict):
            parts = [
                str(node.get(k, "")).strip()
                for k in ("streetAddress", "addressLocality", "addressRegion",
                          "postalCode", "addressCountry")
            ]
            candidate = cls._normalize_addr(", ".join(p for p in parts if p))
            if cls._looks_like_myanmar_addr(candidate):
                return candidate
        return ""

    @staticmethod
    def _normalize_addr(value: str) -> str:
        value = re.sub(r"\s+", " ", value or "").strip()
        return value.strip(" ,;|/-")

    @staticmethod
    def _looks_like_myanmar_addr(value: str) -> bool:
        """ミャンマー国内住所らしいかを 2 条件で判定する (誤検出防止)。"""
        if not (12 <= len(value) <= 200):
            return False
        if not _MYANMAR_HINT_PATTERN.search(value):
            return False
        return bool(_ADDR_TOKEN_PATTERN.search(value))

    @staticmethod
    def _pick_sns(soup: bs4.BeautifulSoup) -> dict:
        """公式 SNS アカウント URL (Facebook / Instagram) を拾う。"""
        found = {Schema.FB: "", Schema.INSTA: ""}
        for a in soup.find_all("a", href=True):
            href = (a["href"] or "").strip()
            if _SNS_NG_PATTERN.search(href):
                continue
            for column, pattern in _SNS_PATTERNS.items():
                if not found[column] and pattern.match(href):
                    # プロフィール URL のみ採用 (末尾がドメインだけのものは除外)
                    path = urlparse(href).path.strip("/")
                    if path:
                        found[column] = href.split("?")[0]
            if all(found.values()):
                break
        return found


if __name__ == "__main__":
    scraper = UmtaMemberDirectoryScraper()
    scraper.execute("https://www.umtanet.org/index.php/en/members/membership-directory")
