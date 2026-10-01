"""
＠Press (atpress.ne.jp) — プレスリリース単位で配信企業情報を抽出するクローラー

取得単位:
    プレスリリース 1 本 = 1 レコード (同一企業が複数リリースを出していれば複数行になる)
    ※ 企業単位で名寄せ済みの会社概要を取る既存クローラー (corporate/press / site_id: press) とは
      取得単位・情報源が異なる。こちらは「どの企業がいつ何のジャンルで配信したか」を
      リリース単位で押さえ、リリース本文末尾の会社概要欄から企業属性を補完する。

取得フロー (一覧 → 詳細 / Pattern B: 1 件取得ごとに即 yield):
    引数 url (トップページ = sites.yml の url) を唯一のルートとして使う。
      1. トップページ上の新着プレスリリースカード (/news/{id}) を列挙 (最初の 1 件を数秒で yield)
      2. 続いて sitemap.xml → sitemap-news.xml?page=N を辿り、過去リリースを網羅列挙
      3. 各リリース詳細ページを取得し、その場で 1 件 yield

リリース詳細ページの構造 (2026-09 時点 / Next.js SSR・requests で静的取得可):
    - JSON-LD NewsArticle
        description "<配信企業名>のプレスリリース:<タイトル>" → 名称 (配信企業名)
        datePublished (UTC) → 配信日 (JST に変換)
        articleSection → 業種ジャンル (フォールバック)
    - BreadcrumbList position 3 → 配信企業名 (JSON-LD description が無い場合の予備)
    - span#published-at "2026年9月30日 15:30" → 配信日 (表示値を優先)
    - 発行者リンク: a[href^="/b/"] (ブランドページ) / RSC ペイロードの publisherTargetUrl
      (旧プラン企業は /news/search?q=...&search_mode=pr_publisher_name) → 企業ページURL
    - RSC ペイロードの prGenreClassName → 業種ジャンル (複数可)
    - 本文末尾の「会社概要欄」(【社名】本社：/社名：/代表者：/設立：/資本金：/TEL：/URL：)
      → 所在地・代表者名・設立・資本金・電話番号・公式URL
      ※ 会社概要欄はリリース本文中の任意項目のため、記載が無いリリースでは空文字になる
      ※ 1 リリースに複数社の概要欄が並ぶ場合があるため、社名が配信企業名と一致する
        ブロックを優先して採用する

除外フィールド (著作権リスク回避):
    リリース本文・リード文・事業内容などの自由記述プロース (JSON-LD articleBody /
    description 本体) は取得しない。構造化された短いラベル・数値のみを対象とする。

備考 (取得カラム指定) の対応:
    名称=Schema.NAME / 代表者名=Schema.REP_NM / プレスリリースURL=EXTRA(+Schema.URL) /
    企業ページURL=EXTRA / 配信日=EXTRA / 業種ジャンル=Schema.CAT_SITE /
    所在地=Schema.PREF+Schema.ADDR / 設立=Schema.OPEN_DATE / 資本金=Schema.CAP /
    電話番号=Schema.TEL / 公式URL=Schema.HP
    絞り込み条件の指示は無いため parse() にフィルタは実装しない。

実行方法:
    python scripts/sites/corporate/press_2.py
    python bin/run_flow.py --site-id press_2
"""

import json
import re
import sys
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# --- EXTRA カラム (Schema に該当定数が無いサイト固有カラム) ---
_COL_RELEASE_URL = "プレスリリースURL"
_COL_COMPANY_URL = "企業ページURL"
_COL_PUB_DATE = "配信日"

# リリース ID (/news/636007, /news/8219605)
_RELEASE_RE = re.compile(r"/news/(\d{5,})(?:[\"/?#]|$)")
# RSC ペイロード中の発行者ページ URL (エスケープ有無の両方に対応)
_TARGET_URL_RE = re.compile(r'publisherTargetUrl\\?":\\?"([^"\\]+)')
# RSC ペイロード中の業種ジャンル名
_GENRE_RE = re.compile(r'prGenreClassName\\?":\\?"([^"\\]+)')
# 表示上の配信日時 "2026年9月30日 15:30"
_PUB_TEXT_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日(?:\s*(\d{1,2}):(\d{2}))?")
# JST (JSON-LD datePublished は UTC)
_JST = timezone(timedelta(hours=9))

# 会社概要欄のラベル行 (NFKC 正規化後に判定する)
_LABEL_LINE_RE = re.compile(
    r"^[【\[\(]?\s*"
    r"(社名|商号|会社名|企業名|名称|本社所在地|所在地|本社|住所|代表者名|代表者|代表取締役|"
    r"設立年月日|設立|創業|資本金|電話番号|電話|TEL|Tel|HP|URL|ホームページ|ウェブサイト)"
    r"\s*[】\]\)]?\s*[:：]\s*(.*)$"
)
# ラベル → 取り込み先キー
_LABEL_MAP = {
    "社名": "name", "商号": "name", "会社名": "name", "企業名": "name", "名称": "name",
    "本社所在地": "addr", "所在地": "addr", "本社": "addr", "住所": "addr",
    "代表者名": "rep", "代表者": "rep", "代表取締役": "rep",
    "設立年月日": "founded", "設立": "founded", "創業": "founded",
    "資本金": "capital",
    "電話番号": "tel", "電話": "tel", "TEL": "tel", "Tel": "tel",
    "HP": "hp", "URL": "hp", "ホームページ": "hp", "ウェブサイト": "hp",
}
# 本文の終わり (これ以降はカテゴリ・タグ・関連リリース等のサイト UI)
_BODY_END_MARKERS = ("カテゴリ", "タグ", "シェア", "配信企業へのお問い合わせ", "この発行者のリリース")
# 代表者名から役職を切り出す
_POS_RE = re.compile(
    r"^(.*(?:取締役|社長|会長|理事長|代表社員|園長|学長|CEO|COO|CFO|CTO|代表)"
    r"(?:\s*兼\s*[A-Z]{2,4})?)\s+(\S.*)$"
)
# 会社概要欄の見出し行 (【株式会社◯◯】)
_HEADING_RE = re.compile(r"^【[^】]{2,40}】$")
# 電話番号らしさ
_TEL_RE = re.compile(r"\+?\d[\d\-\(\)\s]{7,}\d")
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# sitemap-news.xml の巡回上限 (1 ファイル 48,000 URL 程度)
_MAX_SITEMAP_FILES = 11


class Press2(StaticCrawler):
    """＠Press (atpress.ne.jp) プレスリリース単位スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [_COL_RELEASE_URL, _COL_COMPANY_URL, _COL_PUB_DATE]

    def parse(self, url: str):
        # 任意のクエリで分割実行・期間絞り込みができる (Prefect の url パラメータで渡す):
        #   nh_shard=K/N  … リリースIDを N で割った余りが K-1 のものだけ取る (並列実行用)
        #   nh_since=YYYY-MM-DD … サイトマップの lastmod がこの日以降のものだけ取る
        # クエリ無しなら従来どおり全件。
        root, shard_k, shard_n, since = self._parse_run_options(url)
        seen: set[str] = set()
        for release_url in self._iter_release_urls(root, since):
            rid = release_url.rstrip("/").rsplit("/", 1)[-1]
            if rid in seen:
                continue
            seen.add(rid)
            if shard_n > 1 and int(rid) % shard_n != shard_k - 1:
                continue
            try:
                item = self._scrape_release(release_url)
            except Exception as exc:  # noqa: BLE001 個別失敗はログして継続
                self.logger.warning("skip release %s: %s", release_url, exc)
                continue
            if item:
                yield item

    # ------------------------------------------------------------------ 列挙
    @staticmethod
    def _parse_run_options(url: str) -> tuple[str, int, int, str]:
        parts = urlsplit(url)
        qs = parse_qs(parts.query)
        root = f"{parts.scheme}://{parts.netloc}{parts.path or '/'}"
        shard_k, shard_n = 1, 1
        if "nh_shard" in qs:
            k, n = qs["nh_shard"][0].split("/", 1)
            shard_k, shard_n = int(k), int(n)
            if not 1 <= shard_k <= shard_n:
                raise ValueError(f"nh_shard が不正です: {qs['nh_shard'][0]}")
        since = qs.get("nh_since", [""])[0]
        if since:
            datetime.strptime(since, "%Y-%m-%d")  # 形式チェック
        return root, shard_k, shard_n, since

    def _iter_release_urls(self, root: str, since: str = ""):
        """トップページの新着 → sitemap-news.xml の順にリリース URL を列挙する。

        since 指定時は lastmod がそれより前の URL を除く。サイトマップは新しい順なので、
        lastmod が since を下回った時点で以降のファイルも含めて打ち切る。
        """
        soup = self.get_soup(root)
        if soup is not None:
            for rid in dict.fromkeys(_RELEASE_RE.findall(str(soup))):
                yield urljoin(root, f"/news/{rid}")

        index = self.get_soup(urljoin(root, "/sitemap.xml"))
        if index is None:
            return
        sitemaps = [
            loc.get_text(strip=True)
            for loc in index.find_all("loc")
            if "sitemap-news" in loc.get_text(strip=True)
        ]
        for sm_url in sitemaps[:_MAX_SITEMAP_FILES]:
            sm = self.get_soup(urljoin(root, sm_url))
            if sm is None:
                continue
            for loc in sm.find_all("loc"):
                if since:
                    lastmod = loc.find_next_sibling("lastmod")
                    if lastmod is None or lastmod.get_text(strip=True)[:10] < since:
                        return
                m = _RELEASE_RE.search(loc.get_text(strip=True) + "/")
                if m:
                    yield urljoin(root, f"/news/{m.group(1)}")

    # ------------------------------------------------------------------ 詳細
    def _scrape_release(self, release_url: str) -> dict | None:
        soup = self.get_soup(release_url)
        if soup is None:
            return None
        raw = str(soup)
        article, crumbs = self._extract_ld(soup)

        name = self._extract_publisher_name(article, crumbs)
        if not name:
            return None

        overview = self._extract_company_overview(soup, name)
        pref, addr = self._split_address(overview.get("addr", ""))
        rep, pos = self._split_rep(overview.get("rep", ""))

        return {
            Schema.URL: release_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: overview.get("tel", ""),
            Schema.REP_NM: rep,
            Schema.POS_NM: pos,
            Schema.CAP: overview.get("capital", ""),
            Schema.CAT_SITE: self._extract_genres(raw, article),
            Schema.HP: overview.get("hp", ""),
            Schema.OPEN_DATE: overview.get("founded", ""),
            _COL_RELEASE_URL: release_url,
            _COL_COMPANY_URL: self._extract_company_url(soup, raw, release_url),
            _COL_PUB_DATE: self._extract_published_at(soup, article),
        }

    # --------------------------------------------------------------- JSON-LD
    @staticmethod
    def _extract_ld(soup) -> tuple[dict, dict]:
        """JSON-LD から NewsArticle と BreadcrumbList を取り出す。"""
        article: dict = {}
        crumbs: dict = {}
        for s in soup.find_all("script", type="application/ld+json"):
            text = s.string or s.get_text() or ""
            if not text.strip():
                continue
            try:
                data = json.loads(text)
            except (ValueError, TypeError):
                continue
            for it in data if isinstance(data, list) else [data]:
                if not isinstance(it, dict):
                    continue
                if it.get("@type") == "NewsArticle" and not article:
                    article = it
                elif it.get("@type") == "BreadcrumbList" and not crumbs:
                    crumbs = it
        return article, crumbs

    @staticmethod
    def _extract_publisher_name(article: dict, crumbs: dict) -> str:
        """配信企業名を取得する (description の接頭辞 → パンくず 3 番目)。"""
        desc = str(article.get("description") or "")
        m = re.match(r"(.+?)のプレスリリース\s*[:：]", desc)
        if m:
            return m.group(1).strip()
        for it in crumbs.get("itemListElement") or []:
            if not isinstance(it, dict) or it.get("position") != 3:
                continue
            obj = it.get("item")
            if isinstance(obj, dict):
                return str(obj.get("name") or "").strip()
        return ""

    @staticmethod
    def _extract_genres(raw: str, article: dict) -> str:
        """業種ジャンル (フード・飲食 / ビジネス など) を " / " 区切りで返す。"""
        genres = list(dict.fromkeys(_GENRE_RE.findall(raw)))
        if not genres:
            section = str(article.get("articleSection") or "").strip()
            genres = [section] if section else []
        return " / ".join(g for g in genres if g)

    @staticmethod
    def _extract_company_url(soup, raw: str, release_url: str) -> str:
        """企業ページ URL (ブランドページ /b/{code} または発行者検索ページ)。"""
        a = soup.select_one('a[href^="/b/"]')
        if a and a.get("href"):
            return urljoin(release_url, a["href"])
        m = _TARGET_URL_RE.search(raw)
        if m:
            target = m.group(1).replace("\\u0026", "&").replace("&", "&")
            return urljoin(release_url, target)
        return ""

    @staticmethod
    def _extract_published_at(soup, article: dict) -> str:
        """配信日時を "YYYY-MM-DD HH:MM" 形式で返す。"""
        span = soup.select_one("#published-at")
        if span:
            m = _PUB_TEXT_RE.search(span.get_text(" ", strip=True))
            if m:
                y, mo, d, hh, mm = m.groups()
                date = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
                return f"{date} {int(hh):02d}:{mm}" if hh else date
        iso = article.get("datePublished")
        if iso:
            try:
                dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
            except ValueError:
                return str(iso)
            if dt.tzinfo is not None:
                dt = dt.astimezone(_JST)
            return dt.strftime("%Y-%m-%d %H:%M")
        return ""

    # --------------------------------------------------------- 会社概要欄解析
    @classmethod
    def _extract_company_overview(cls, soup, publisher: str) -> dict:
        """リリース本文末尾の会社概要欄からラベル/値を取り出す。

        「社名：」が配信企業名と一致するブロックを最優先し、次に項目数の多い
        ブロックを採用する (1 リリースに複数社の概要欄が並ぶことがあるため)。
        """
        lines = cls._body_lines(soup)
        # 【株式会社◯◯】のような見出し行はブロックの区切りとして扱う
        heads = {i for i, ln in enumerate(lines) if _HEADING_RE.match(ln)}
        labeled: list[tuple[int, str, str]] = []
        for idx, line in enumerate(lines):
            m = _LABEL_LINE_RE.match(cls._label_probe(line))
            if not m:
                continue
            key = _LABEL_MAP.get(m.group(1).replace(" ", ""))
            if not key:
                continue
            value = m.group(2).strip()
            if not value and idx + 1 < len(lines):
                nxt = lines[idx + 1]
                # "URL：" の次行に値が来るレイアウトに対応 (ラベル行は拾わない)
                if nxt and not _LABEL_LINE_RE.match(cls._label_probe(nxt)):
                    value = nxt.strip()
            if value:
                labeled.append((idx, key, value))
        if not labeled:
            return {}

        # 近接するラベル行をブロックにまとめる
        blocks: list[dict[str, str]] = []
        current: dict[str, str] = {}
        prev_idx = None
        for idx, key, value in labeled:
            if prev_idx is not None and (
                idx - prev_idx > 4                                   # 行が離れた
                or any(prev_idx < h < idx for h in heads)             # 別会社の見出しを跨いだ
                or (key == "name" and "name" in current)              # 社名が 2 つ目に出た
            ):
                blocks.append(current)
                current = {}
            current.setdefault(key, value)
            prev_idx = idx
        blocks.append(current)

        def score(block: dict) -> int:
            s = len(block)
            bname = block.get("name", "")
            if bname and publisher:
                if bname == publisher:
                    s += 20
                elif publisher in bname or bname in publisher:
                    s += 5
            return s

        return cls._clean_overview(max(blocks, key=score))

    @staticmethod
    def _label_probe(line: str) -> str:
        """ラベル判定用に、コロンより前の空白を除去した行を返す。

        会社概要欄のラベルは "本　社：" "社　名：" のように字間を空けて書かれることが
        多く (NFKC 正規化で半角スペースになる)、そのままではラベル名と一致しない。
        """
        if ":" not in line:
            return line
        head, rest = line.split(":", 1)
        return head.replace(" ", "") + ":" + rest

    @staticmethod
    def _body_lines(soup) -> list[str]:
        """本文相当のテキスト行 (NFKC 正規化済み) を返す。"""
        text = unicodedata.normalize("NFKC", soup.get_text("\n"))
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        # カテゴリ以降はサイト UI (関連リリース等) なので打ち切る
        for i, ln in enumerate(lines):
            if ln in _BODY_END_MARKERS and i > 5:
                return lines[:i]
        return lines

    @staticmethod
    def _clean_overview(block: dict) -> dict:
        out = {k: re.sub(r"\s+", " ", v).strip(" :：") for k, v in block.items()}
        tel = out.get("tel", "")
        if tel:
            m = _TEL_RE.search(tel)
            out["tel"] = m.group(0).strip() if m else ""
        hp = out.get("hp", "")
        if hp and not hp.lower().startswith("http"):
            m = re.search(r"https?://\S+", hp)
            if m:
                out["hp"] = m.group(0)
            elif "." in hp and " " not in hp:
                out["hp"] = "https://" + hp
            else:
                out["hp"] = ""
        return out

    @staticmethod
    def _split_address(value: str) -> tuple[str, str]:
        value = re.sub(r"^〒?\s*\d{3}-?\d{4}\s*", "", value).strip()
        if not value:
            return "", ""
        m = _PREF_RE.match(value)
        if m:
            return m.group(1), value[m.end():].strip()
        return "", value

    @staticmethod
    def _split_rep(value: str) -> tuple[str, str]:
        """「代表取締役社長 山田 太郎」→ (代表者名, 役職) に分ける。"""
        if not value:
            return "", ""
        m = _POS_RE.match(value)
        if m:
            return m.group(2).strip(), m.group(1).strip()
        return value, ""


if __name__ == "__main__":
    scraper = Press2()
    scraper.execute("https://www.atpress.ne.jp/")
