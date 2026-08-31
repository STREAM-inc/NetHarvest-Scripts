"""
外国人労働者ドットコム (gai-rou.com) — 登録支援機関 / 監理団体 / 送出機関ディレクトリ

取得対象:
    - 登録支援機関  (/shien/{id}/)       約 11,420 件
    - 監理団体      (/kanri/{id}/)       約  3,674 件
    - 送出機関      (/okuridashi/{slug}/) 約  3,058 件
    合計 約 18,150 件

取得フロー:
    - 一覧ページの検索 UI は使わず、サイトマップインデックス (sitemap.xml) から
      kanri-sitemapN.xml / shien-sitemapN.xml / okuridashi-sitemapN.xml を辿る。
      これらのサブサイトマップは全件が詳細ページ URL のみで構成されている。
    - 3 種別をラウンドロビンで交互に巡回し、詳細ページを 1 件取得するごとに即 yield する。

ページ構造:
    - main#main 直下の div.box_dantai_list が情報ブロック (h3 = セクション見出し)。
      同ページ下部の「関連団体」リストにも同クラスが現れるが、そちらは入れ子が深いため
      recursive=False で直下のみを対象にして除外する。
    - 各ブロック内の dl.dl_dantai_list > dt/dd がラベル/値のペア。

備考 / 方針:
    - TEL は「掲載されている電話番号を全て記録する」方針。メイン欄と送出機関の
      「日本国内窓口」欄の番号をすべて収集し、Schema.TEL に先頭、EXTRA「電話番号（全件）」に全件を格納。
    - 住所は事務所所在地。送出機関は海外拠点住所がそのまま入る。
    - 「支援業務内容」「備考」は自由記述のプロースのため著作権リスク回避で除外。
    - 利用規約 (https://www.gai-rou.com/terms/) にスクレイピング/クローリングの明示禁止条項は無い。
      robots.txt も /wp-admin/ 以外は許可。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/gai_rou.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id gai_rou
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# サイトマップ内の投稿タイプ → サイト上の「種別」表記
_TYPE_LABELS = {
    "shien": "登録支援機関",
    "kanri": "監理団体",
    "okuridashi": "送出機関",
}
# 巡回順 (ラウンドロビン)
_TYPES = ["shien", "kanri", "okuridashi"]

_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(r"^\s*(" + _PREF + r")")
_POST_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})")

# 名称に使われるラベル (投稿タイプごとに表記が異なる)
_NAME_LABELS = ("団体名", "機関名", "申請者の氏名又は名称", "名称")
# 電話番号として扱うラベル
_TEL_LABELS = ("電話番号", "TEL", "携帯電話番号", "電話")
# 公式サイトとして扱うラベル
_HP_LABELS = ("URL", "公式サイト", "ホームページ", "ＵＲＬ")
# メールアドレスとして扱うラベル
_MAIL_LABELS = ("メールアドレス", "メール", "E-mail")


class GaiRou(StaticCrawler):
    """外国人労働者ドットコム 登録支援機関/監理団体/送出機関 スクレイパー"""

    DELAY = 0.5

    EXTRA_COLUMNS = [
        "電話番号（全件）",
        "FAX",
        "団体種別",
        "2号移行対応職種",
        "対象国",
        "対応可能言語",
        "登録番号",
        "登録年月日",
        "許可日",
        "許可期限",
        "許可期間",
        "支援業務開始予定日",
        "事務所所在地",
        "日本国内窓口_責任者氏名",
        "日本国内窓口_電話番号",
        "日本国内窓口_メール",
    ]

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        """サイトマップから 3 種別の詳細 URL を辿り、1 件ずつ yield する。"""
        sitemaps = self._sitemap_index(url)

        # 種別ごとに「詳細 URL を遅延生成するジェネレーター」を用意し、交互に消費する
        streams = {t: self._detail_urls(sitemaps.get(t, [])) for t in _TYPES}
        seen: set[str] = set()

        while streams:
            for site_type in list(_TYPES):
                gen = streams.get(site_type)
                if gen is None:
                    continue
                detail_url = next(gen, None)
                if detail_url is None:
                    del streams[site_type]
                    continue
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                item = self._parse_detail(detail_url, site_type)
                if item:
                    yield item

    # ------------------------------------------------------------------ #
    # 列挙 (サイトマップ)
    # ------------------------------------------------------------------ #
    def _sitemap_index(self, url: str) -> dict[str, list[str]]:
        """sitemap.xml を読み、投稿タイプ別にサブサイトマップ URL を分類して返す。"""
        index_url = urljoin(url, "sitemap.xml")
        result: dict[str, list[str]] = {t: [] for t in _TYPES}

        locs = self._sitemap_locs(index_url)
        if not locs:
            logger.warning("サイトマップインデックスを取得できません: %s", index_url)
            return result

        for loc in locs:
            name = urlparse(loc).path.rsplit("/", 1)[-1]
            for site_type in _TYPES:
                # 例: shien-sitemap.xml / shien-sitemap2.xml
                if name.startswith(f"{site_type}-sitemap"):
                    result[site_type].append(loc)
                    break

        for site_type in _TYPES:
            # sitemap2, sitemap10 ... を数値順に整列させる
            result[site_type].sort(key=_sitemap_sort_key)
            logger.info("%s: サブサイトマップ %d 本", _TYPE_LABELS[site_type], len(result[site_type]))
        return result

    def _detail_urls(self, sitemap_urls: list[str]):
        """サブサイトマップを 1 本ずつ取得し、詳細 URL を遅延生成する。"""
        for sitemap_url in sitemap_urls:
            yield from self._sitemap_locs(sitemap_url)

    def _sitemap_locs(self, url: str) -> list[str]:
        """サイトマップ XML を取得し <loc> の値を列挙する (get_soup のキャッシュ/リトライを共有)。"""
        soup = self.get_soup(url)
        if soup is None:
            return []
        return [loc.get_text(strip=True) for loc in soup.find_all("loc") if loc.get_text(strip=True)]

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #
    def _parse_detail(self, detail_url: str, site_type: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        main = soup.select_one("main#main")
        if main is None:
            logger.warning("main#main が見つかりません: %s", detail_url)
            return None

        # main 直下のブロックのみ (下部の「関連団体」リストは入れ子が深いので除外される)
        boxes = main.find_all("div", class_="box_dantai_list", recursive=False)
        if not boxes:
            logger.warning("情報ブロックが見つかりません: %s", detail_url)
            return None

        # セクション見出しごとに {ラベル: 値} を集める
        sections: list[tuple[str, dict[str, str], dict[str, object]]] = []
        for box in boxes:
            h3 = box.find("h3")
            title = _text(h3) if h3 else ""
            fields: dict[str, str] = {}
            nodes: dict[str, object] = {}
            for dl in box.find_all("dl", class_="dl_dantai_list", recursive=False):
                dt, dd = dl.find("dt"), dl.find("dd")
                if dt is None or dd is None:
                    continue
                label = _text(dt)
                # 「団体種別について」等の注記リンクはラベル値から除く
                for note in dd.select("a.notice_yuryo_link"):
                    note.decompose()
                if label and label not in fields:
                    fields[label] = _text(dd)
                    nodes[label] = dd
            sections.append((title, fields, nodes))

        # 「日本国内窓口」だけは同じラベルが重複するので分離して扱う
        jp_desk = next((f for t, f, _ in sections if "日本国内窓口" in t), {})
        main_fields: dict[str, str] = {}
        main_nodes: dict[str, object] = {}
        for title, fields, nodes in sections:
            if "日本国内窓口" in title:
                continue
            for k, v in fields.items():
                main_fields.setdefault(k, v)
                main_nodes.setdefault(k, nodes[k])

        name = _first(main_fields, _NAME_LABELS) or _page_title(soup)
        if not name:
            logger.warning("名称を取得できません: %s", detail_url)
            return None

        addr = main_fields.get("住所", "")
        post_code = main_fields.get("郵便番号", "")
        if not post_code:
            m = _POST_RE.search(addr)
            if m:
                post_code = f"{m.group(1)}-{m.group(2)}"

        pref = main_fields.get("所在都道府県", "")
        if not pref:
            m = _PREF_RE.search(addr)
            pref = m.group(1) if m else ""

        # 電話番号: 掲載されている番号を全て収集する (絞り込みは後段クレンジング)
        tels: list[str] = []
        for label in _TEL_LABELS:
            _collect_tels(main_nodes.get(label), main_fields.get(label, ""), tels)
        jp_tel = jp_desk.get("電話番号", "") or jp_desk.get("TEL", "")
        for t in _split_multi(jp_tel):
            if t not in tels:
                tels.append(t)

        hp = _link_or_text(main_nodes, main_fields, _HP_LABELS)
        email = _link_or_text(main_nodes, main_fields, _MAIL_LABELS, scheme="mailto:")

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tels[0] if tels else "",
            Schema.REP_NM: main_fields.get("責任者氏名", ""),
            Schema.CAT_SITE: _TYPE_LABELS.get(site_type, ""),
            Schema.HP: hp,
            Schema.EMAIL: email,
            "電話番号（全件）": " / ".join(tels),
            "FAX": main_fields.get("FAX番号", "") or main_fields.get("FAX", ""),
            "団体種別": main_fields.get("団体種別", ""),
            "2号移行対応職種": _job_categories(main_nodes.get("2号移行対応職種")),
            "対象国": main_fields.get("受け入れ国", "") or main_fields.get("送出国", ""),
            "対応可能言語": main_fields.get("対応可能言語", ""),
            "登録番号": main_fields.get("登録番号", ""),
            "登録年月日": main_fields.get("登録年月日", ""),
            "許可日": main_fields.get("許可日", ""),
            "許可期限": main_fields.get("許可期限", ""),
            "許可期間": main_fields.get("許可期間", ""),
            "支援業務開始予定日": main_fields.get("支援業務開始予定日", ""),
            "事務所所在地": _offices(main_nodes.get("事務所所在地")),
            "日本国内窓口_責任者氏名": jp_desk.get("責任者氏名", ""),
            "日本国内窓口_電話番号": jp_tel,
            "日本国内窓口_メール": jp_desk.get("メール", "") or jp_desk.get("メールアドレス", ""),
        }


# ---------------------------------------------------------------------- #
# ヘルパー
# ---------------------------------------------------------------------- #
def _sitemap_sort_key(loc: str) -> int:
    """shien-sitemap.xml → 1, shien-sitemap2.xml → 2 の順に並べる。"""
    m = re.search(r"sitemap(\d*)\.xml", loc)
    return int(m.group(1)) if m and m.group(1) else 1


def _text(node) -> str:
    if node is None:
        return ""
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()


def _first(fields: dict[str, str], labels) -> str:
    for label in labels:
        if fields.get(label):
            return fields[label]
    return ""


def _page_title(soup) -> str:
    """dt/dd から名称を取れない場合の予備 (<title> の先頭部分)。"""
    title = soup.find("title")
    if not title:
        return ""
    return re.split(r"\s*[|｜]\s*", _text(title))[0].strip()


def _split_multi(value: str) -> list[str]:
    """「A / B」「A、B」形式で併記された値を分割する (空要素は捨てる)。"""
    parts = re.split(r"\s*[/／、,]\s*", value or "")
    return [p.strip() for p in parts if p.strip()]


def _collect_tels(dd, text_value: str, out: list[str]) -> None:
    """dd 内の tel: リンクと表示テキストの両方から電話番号を集める。"""
    candidates: list[str] = []
    if dd is not None:
        for a in dd.select('a[href^="tel:"]'):
            candidates.extend(_split_multi(a.get("href", "")[4:]))
    candidates.extend(_split_multi(text_value))
    for tel in candidates:
        if tel and tel not in out:
            out.append(tel)


def _link_or_text(nodes: dict, fields: dict, labels, scheme: str = "") -> str:
    """URL / メールは a[href] を優先し、無ければ表示テキストを使う。"""
    for label in labels:
        dd = nodes.get(label)
        if dd is not None:
            for a in dd.find_all("a", href=True):
                href = a["href"].strip()
                if scheme:
                    if href.lower().startswith(scheme):
                        value = href[len(scheme):].strip()
                        if value:
                            return value
                    continue
                if href.lower().startswith(("http://", "https://")):
                    return href
        if fields.get(label):
            return fields[label]
    return ""


def _job_categories(dd) -> str:
    """2号移行対応職種 (dl.dl_kanri_cat_list) を「分類:職種,職種」形式に整形する。"""
    if dd is None:
        return ""
    groups = []
    for dl in dd.find_all("dl", class_="dl_kanri_cat_list"):
        cat = _text(dl.find("dt"))
        jobs = [_text(s) for s in dl.select("dd span")]
        jobs = [j for j in jobs if j]
        if cat and jobs:
            groups.append(f"{cat}:{'、'.join(jobs)}")
        elif cat:
            groups.append(cat)
    return " / ".join(groups) if groups else _text(dd)


def _offices(dd) -> str:
    """事務所所在地 (複数拠点あり) を「名称 〒郵便番号 住所」形式で連結する。"""
    if dd is None:
        return ""
    boxes = dd.select("div.shien_office_box")
    if not boxes:
        return _text(dd)
    offices = []
    for box in boxes:
        name = _text(box.select_one(".office_name"))
        address = _text(box.select_one(".office_address"))
        line = " ".join(p for p in (name, address) if p)
        if line:
            offices.append(line)
    return " / ".join(offices)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = GaiRou()
    scraper.execute("https://www.gai-rou.com/")
