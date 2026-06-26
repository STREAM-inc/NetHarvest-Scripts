"""
nextinjapan (外国人求人サイト NINJA) — nextinjapan.com/jobs

取得対象:
    - 外国人向け求人の全件。企業情報 (会社名・HP・業種・設立・資本金・従業員数・
      代表者・所在地) と求人条件 (勤務時間・休日・募集職種・雇用形態・給与・使用言語等) を取得する。

取得フロー:
    一覧ページ (?page=N) → 各カードの詳細リンク (/jobs/{id}) を辿る。
    企業・求人情報は詳細ページの .card-table (.ct-row > .left/.right) から取得し、
    求人タイトル・求人種別・必要言語は詳細ページ上部の見出しから取得する。
    給与は一覧カードの短い表記 (例: 年収300〜400万円) を採用 (詳細は長文のため)。
    ※詳細は一覧取得で確立した同一セッション (Cookie) で取得するため安定する。

    Pattern B: 詳細を1件取得するごとに即 yield (早期 yield / テスト実行のタイムアウト回避)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/nextinjapan.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id nextinjapan
"""

import re
import sys
import urllib.parse
from pathlib import Path

import urllib3

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_RE = re.compile(r"〒\s*([\d]{3}-?[\d]{4})")
_ADDR_IN_TEXT_RE = re.compile(r"住所[：:]\s*([^\n]+?)(?:\s*アクセス|$)")


class NextinjapanCrawler(StaticCrawler):
    """nextinjapan (外国人求人サイト NINJA) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル",
        "求人種別",
        "必要言語",
        "募集職種",
        "雇用形態",
        "給与",
        "売上高",
        "受動喫煙対策",
        "日本語使用割合",
        "有料職業紹介事業許可番号",
        "役職・部署",
    ]

    def _setup(self):
        """セッション初期化。

        nextinjapan.com は配信サーバが中間 (intermediate) 証明書を返さないため、
        コンテナ実行環境の CA バンドルでは証明書チェーンを検証できず
        `SSL: CERTIFICATE_VERIFY_FAILED (unable to get local issuer certificate)` で
        全リクエストが失敗する。その結果 get_soup() が常に None を返し、一覧ページが
        取得できず yield 0 件になっていた (セレクタは live と一致しており正常)。

        サーバ側の証明書チェーン不備が原因のため、このサイトに限り TLS 検証を無効化する。
        """
        super()._setup()
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session.verify = False

    def parse(self, url: str):
        page = 1
        while True:
            list_url = f"{url}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break
            cards = soup.select("div.card-row")
            if not cards:
                break

            # 初回ページで総件数を進捗表示用に設定 (例: "全 4962 件中、...")
            if page == 1:
                for el in soup.select("[class*=result]"):
                    m = re.search(r"全\s*([\d,]+)\s*件", el.get_text(" ", strip=True))
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))
                        break

            for card in cards:
                try:
                    link = card.select_one('a.btn.btn-dark[href*="/jobs/"]')
                    if not link or not link.get("href"):
                        continue
                    detail_url = urllib.parse.urljoin(url, link["href"])

                    # 給与は一覧カードの短い表記を採用 (詳細は長文プロースのため除外)
                    salary = ""
                    for row in card.select(".info-row"):
                        sp = row.select_one(".img-area span")
                        val = row.select_one(".info-area")
                        if sp and val and sp.get_text(strip=True) == "給与":
                            salary = val.get_text(" ", strip=True)
                            break

                    item = self._scrape_detail(detail_url)
                    if item:
                        item["給与"] = salary
                        yield item
                except Exception as e:
                    self.logger.warning(f"page {page}: card skip — {e}")
                    continue
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # .card-table の全 .ct-row を ラベル→値 dict 化。
        # ラベルが複数回現れる場合 (紹介会社求人で会社情報が2表) は、
        # 先勝ちにしつつ空欄なら後の非空値で補完する。
        info: dict[str, str] = {}
        for row in soup.select(".ct-row"):
            left = row.select_one(".left")
            right = row.select_one(".right")
            if not left:
                continue
            label = left.get_text(" ", strip=True)
            value = right.get_text(" ", strip=True) if right else ""
            if label not in info or (not info[label] and value):
                info[label] = value

        def g(key: str) -> str:
            return info.get(key, "")

        # 求人タイトル・求人種別・必要言語 (ページ上部の見出し)
        job_title_el = soup.select_one(".job-title")
        job_title = job_title_el.get_text(" ", strip=True) if job_title_el else ""
        company_type_el = soup.select_one(".company-type")
        company_type = (
            company_type_el.get_text(" ", strip=True) if company_type_el else ""
        )
        languages = " / ".join(
            el.get_text(" ", strip=True)
            for el in soup.select(".language-title")
            if el.get_text(strip=True)
        )

        # 代表者役職/氏名 → POS_NM / REP_NM に分割
        rep_raw = g("代表者役職/氏名")
        rep_pos, rep_name = "", ""
        if rep_raw:
            parts = [p.strip() for p in re.split(r"[／/]", rep_raw, maxsplit=1)]
            if len(parts) == 2:
                rep_pos, rep_name = parts[0], parts[1]
            else:
                rep_name = rep_raw

        # 住所・郵便番号・都道府県
        post_code, addr, pref = "", "", ""
        jigyousho = g("事業所")
        kinmuchi = g("勤務地")
        addr_source = jigyousho or kinmuchi
        if jigyousho:
            pc = _POST_CODE_RE.search(jigyousho)
            if pc:
                post_code = pc.group(1)
            addr = _POST_CODE_RE.sub("", jigyousho).strip()
        elif kinmuchi:
            m_addr = _ADDR_IN_TEXT_RE.search(kinmuchi)
            if m_addr:
                addr = m_addr.group(1).strip()
        # 都道府県は所在地候補から抽出
        m_pref = _PREF_PATTERN.search(addr_source)
        if m_pref:
            pref = m_pref.group(1)
            if addr:
                m_a = _PREF_PATTERN.search(addr)
                if m_a:
                    addr = addr[m_a.start():].strip()

        return {
            Schema.URL: url,
            Schema.NAME: g("会社名"),
            Schema.HP: g("ホームページ"),
            Schema.CAT_SITE: g("業種"),
            Schema.OPEN_DATE: g("設立"),
            Schema.CAP: g("資本金"),
            Schema.EMP_NUM: g("従業員数"),
            Schema.REP_NM: rep_name,
            Schema.POS_NM: rep_pos,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TIME: g("勤務時間"),
            Schema.HOLIDAY: g("休日・休暇"),
            "求人タイトル": job_title,
            "求人種別": company_type,
            "必要言語": languages,
            "募集職種": g("募集職種"),
            "雇用形態": g("雇用形態"),
            "給与": "",  # parse() で一覧カードの値を上書き
            "売上高": g("売上高"),
            "受動喫煙対策": g("受動喫煙対策"),
            "日本語使用割合": g("日本語使用割合"),
            "有料職業紹介事業許可番号": g("有料職業紹介事業許可番号"),
            "役職・部署": g("役職・部署"),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NextinjapanCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://nextinjapan.com/jobs")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
