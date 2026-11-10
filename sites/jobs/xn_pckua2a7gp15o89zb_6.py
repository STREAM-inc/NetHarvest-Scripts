"""
求人ボックス（アルバイト・パート 14都府県・掲載30日以内）スクレイパー

対象サイト: https://xn--pckua2a7gp15o89zb.com/アルバイト・パート-東京都新宿区の仕事?u=1

取得対象:
- 14都府県（東京都・神奈川県・埼玉県・千葉県・三重県・滋賀県・京都府・大阪府・兵庫県・
  奈良県・和歌山県・岐阜県・愛知県・福岡県）の「アルバイト・パート」求人
- 「アルバイト・パート-{都道府県}{市区町村}の仕事」（新着フィルタ無し）を市区町村ごとに
  最大ページまで巡回する。政令指定都市は区単位で検索する（1検索あたりの件数を抑えるため）。

取得カラム:
- 名称 (会社名) / 都道府県 / 住所 / 勤務地 / 職種 / 雇用形態 / 求人タイトル / 給与
- 新着表示 (「新着」「5時間前」「3日前」等) / 求人掲載日 / 求人票URL / 取得日 / 検索市区町村

備考:
- 求人掲載日は新着表示から算出する（新着・○分前・○時間前 = 取得日、○日前 = 取得日の○日前）。
  新着表示はおおむね 2 週間以内の求人にしか付かないため、表示が無いカードは埋め込み JSON の
  updatedAt (日付部分) を求人掲載日とする（「14日以上前」もそれより古い updatedAt があれば採用）。求人掲載日が取得日の 30 日より前の求人は出力しない。
- サイトの掲載日フィルタ (u) は 1=本日 / 2=3日以内 / 3=7日以内 しかなく (u=30 は 404)、
  sort=date 等の新着順ソートも無い (無視され関連順のまま)。そのため正規 URL の ?u=1 は外して
  全件の一覧を辿る。並びが日付順ではないので「30 日より前に達したら即打ち切り」はできず、
  通常枠に 30 日以内の求人が 1 件も無いページが STALE_PAGES ページ続いた時点で
  30 日より前の求人帯に達したとみなし、その市区町村を打ち切る。
- 一覧の p-ad-item* カードはスポンサー枠（/rd/ 経由・並びが新着順でない・他地域の求人も混ざる）。
  1 ページの過半を占め検索地域の実求人も多いため出力はする (掲載区分=広告) が、
  30 日打ち切り判定には通常枠のカードだけを使う。都道府県が対象14都府県外の求人は出力しない。
- robots.txt で /jb/ ・ /rd/ ・ /api/ は Disallow のため、詳細ページには遷移せず一覧のみで完結する。
  求人票URL は埋め込み JSON の rdUrl (/jb/ /jbn/ /jbi/) をパーマリンクとして記録するだけ。
- ページ送りは「次のページへ」(a.c-pager_btn--next = ?pg=N) を同一セッションで辿る
  (クッキー無しで ?pg=2 を直接叩くと 0 件になるため)。
- 市区町村一覧はサイト上で静的に公開されていない (エリア選択モーダルは robots 対象外の JS が
  動的に描画する) ため、14都府県の市区町村をこのファイルに定数で持つ。
  検索結果の見出し (h1) に市区町村名が含まれない場合 (サイト側で未対応の地名) はスキップする。
- 同じ求人が隣接市区町村の検索にも現れるため、uniqueId で全体重複除去する。
- Cloudflare 配下のため並列化はせず、1 リクエストごとに DELAY 秒待機する。
  bot 対策の回避 (ヘッダ偽装・Cookie 流用・CAPTCHA 回避等) は行わない。
"""

import html
import json
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


def _wards(city: str, wards: str) -> str:
    """政令指定都市の区を「市名+区名」の空白区切りにする。"""
    return " ".join(city + w for w in wards.split())


# 対象14都府県の市区町村 (政令指定都市は区単位)
MUNICIPALITIES: dict[str, str] = {
    "東京都": (
        "千代田区 中央区 港区 新宿区 文京区 台東区 墨田区 江東区 品川区 目黒区 大田区 世田谷区 "
        "渋谷区 中野区 杉並区 豊島区 北区 荒川区 板橋区 練馬区 足立区 葛飾区 江戸川区 "
        "八王子市 立川市 武蔵野市 三鷹市 青梅市 府中市 昭島市 調布市 町田市 小金井市 小平市 "
        "日野市 東村山市 国分寺市 国立市 福生市 狛江市 東大和市 清瀬市 東久留米市 武蔵村山市 "
        "多摩市 稲城市 羽村市 あきる野市 西東京市 瑞穂町 日の出町 檜原村 奥多摩町 大島町 "
        "利島村 新島村 神津島村 三宅村 御蔵島村 八丈町 青ヶ島村 小笠原村"
    ),
    "神奈川県": " ".join([
        _wards("横浜市", "鶴見区 神奈川区 西区 中区 南区 保土ケ谷区 磯子区 金沢区 港北区 戸塚区 "
                       "港南区 旭区 緑区 瀬谷区 栄区 泉区 青葉区 都筑区"),
        _wards("川崎市", "川崎区 幸区 中原区 高津区 多摩区 宮前区 麻生区"),
        _wards("相模原市", "緑区 中央区 南区"),
        "横須賀市 平塚市 鎌倉市 藤沢市 小田原市 茅ヶ崎市 逗子市 三浦市 秦野市 厚木市 大和市 "
        "伊勢原市 海老名市 座間市 南足柄市 綾瀬市 葉山町 寒川町 大磯町 二宮町 中井町 大井町 "
        "松田町 山北町 開成町 箱根町 真鶴町 湯河原町 愛川町 清川村",
    ]),
    "埼玉県": " ".join([
        _wards("さいたま市", "西区 北区 大宮区 見沼区 中央区 桜区 浦和区 南区 緑区 岩槻区"),
        "川越市 熊谷市 川口市 行田市 秩父市 所沢市 飯能市 加須市 本庄市 東松山市 春日部市 "
        "狭山市 羽生市 鴻巣市 深谷市 上尾市 草加市 越谷市 蕨市 戸田市 入間市 朝霞市 志木市 "
        "和光市 新座市 桶川市 久喜市 北本市 八潮市 富士見市 三郷市 蓮田市 坂戸市 幸手市 "
        "鶴ヶ島市 日高市 吉川市 ふじみ野市 白岡市 伊奈町 三芳町 毛呂山町 越生町 滑川町 嵐山町 "
        "小川町 川島町 吉見町 鳩山町 ときがわ町 横瀬町 皆野町 長瀞町 小鹿野町 東秩父村 美里町 "
        "神川町 上里町 寄居町 宮代町 杉戸町 松伏町",
    ]),
    "千葉県": " ".join([
        _wards("千葉市", "中央区 花見川区 稲毛区 若葉区 緑区 美浜区"),
        "銚子市 市川市 船橋市 館山市 木更津市 松戸市 野田市 茂原市 成田市 佐倉市 東金市 旭市 "
        "習志野市 柏市 勝浦市 市原市 流山市 八千代市 我孫子市 鴨川市 鎌ケ谷市 君津市 富津市 "
        "浦安市 四街道市 袖ケ浦市 八街市 印西市 白井市 富里市 南房総市 匝瑳市 香取市 山武市 "
        "いすみ市 大網白里市 酒々井町 栄町 神崎町 多古町 東庄町 九十九里町 芝山町 横芝光町 "
        "一宮町 睦沢町 長生村 白子町 長柄町 長南町 大多喜町 御宿町 鋸南町",
    ]),
    "三重県": (
        "津市 四日市市 伊勢市 松阪市 桑名市 鈴鹿市 名張市 尾鷲市 亀山市 鳥羽市 熊野市 いなべ市 "
        "志摩市 伊賀市 木曽岬町 東員町 菰野町 朝日町 川越町 多気町 明和町 大台町 玉城町 度会町 "
        "大紀町 南伊勢町 紀北町 御浜町 紀宝町"
    ),
    "滋賀県": (
        "大津市 彦根市 長浜市 近江八幡市 草津市 守山市 栗東市 甲賀市 野洲市 湖南市 高島市 "
        "東近江市 米原市 日野町 竜王町 愛荘町 豊郷町 甲良町 多賀町"
    ),
    "京都府": " ".join([
        _wards("京都市", "北区 上京区 左京区 中京区 東山区 下京区 南区 右京区 伏見区 山科区 西京区"),
        "福知山市 舞鶴市 綾部市 宇治市 宮津市 亀岡市 城陽市 向日市 長岡京市 八幡市 京田辺市 "
        "京丹後市 南丹市 木津川市 大山崎町 久御山町 井手町 宇治田原町 笠置町 和束町 精華町 "
        "南山城村 京丹波町 伊根町 与謝野町",
    ]),
    "大阪府": " ".join([
        _wards("大阪市", "都島区 福島区 此花区 西区 港区 大正区 天王寺区 浪速区 西淀川区 東淀川区 "
                       "東成区 生野区 旭区 城東区 阿倍野区 住吉区 東住吉区 西成区 淀川区 鶴見区 "
                       "住之江区 平野区 北区 中央区"),
        _wards("堺市", "堺区 中区 東区 西区 南区 北区 美原区"),
        "岸和田市 豊中市 池田市 吹田市 泉大津市 高槻市 貝塚市 守口市 枚方市 茨木市 八尾市 "
        "泉佐野市 富田林市 寝屋川市 河内長野市 松原市 大東市 和泉市 箕面市 柏原市 羽曳野市 "
        "門真市 摂津市 高石市 藤井寺市 東大阪市 泉南市 四條畷市 交野市 大阪狭山市 阪南市 "
        "島本町 豊能町 能勢町 忠岡町 熊取町 田尻町 岬町 太子町 河南町 千早赤阪村",
    ]),
    "兵庫県": " ".join([
        _wards("神戸市", "東灘区 灘区 兵庫区 長田区 須磨区 垂水区 北区 中央区 西区"),
        "姫路市 尼崎市 明石市 西宮市 洲本市 芦屋市 伊丹市 相生市 豊岡市 加古川市 赤穂市 "
        "西脇市 宝塚市 三木市 高砂市 川西市 小野市 三田市 加西市 丹波篠山市 養父市 丹波市 "
        "南あわじ市 朝来市 淡路市 宍粟市 加東市 たつの市 猪名川町 多可町 稲美町 播磨町 市川町 "
        "福崎町 神河町 太子町 上郡町 佐用町 香美町 新温泉町",
    ]),
    "奈良県": (
        "奈良市 大和高田市 大和郡山市 天理市 橿原市 桜井市 五條市 御所市 生駒市 香芝市 葛城市 "
        "宇陀市 山添村 平群町 三郷町 斑鳩町 安堵町 川西町 三宅町 田原本町 曽爾村 御杖村 高取町 "
        "明日香村 上牧町 王寺町 広陵町 河合町 吉野町 大淀町 下市町 黒滝村 天川村 野迫川村 "
        "十津川村 下北山村 上北山村 川上村 東吉野村"
    ),
    "和歌山県": (
        "和歌山市 海南市 橋本市 有田市 御坊市 田辺市 新宮市 紀の川市 岩出市 紀美野町 かつらぎ町 "
        "九度山町 高野町 湯浅町 広川町 有田川町 美浜町 日高町 由良町 印南町 みなべ町 日高川町 "
        "白浜町 上富田町 すさみ町 那智勝浦町 太地町 古座川町 北山村 串本町"
    ),
    "岐阜県": (
        "岐阜市 大垣市 高山市 多治見市 関市 中津川市 美濃市 瑞浪市 羽島市 恵那市 美濃加茂市 "
        "土岐市 各務原市 可児市 山県市 瑞穂市 飛騨市 本巣市 郡上市 下呂市 海津市 岐南町 笠松町 "
        "養老町 垂井町 関ケ原町 神戸町 輪之内町 安八町 揖斐川町 大野町 池田町 北方町 坂祝町 "
        "富加町 川辺町 七宗町 八百津町 白川町 東白川村 御嵩町 白川村"
    ),
    "愛知県": " ".join([
        _wards("名古屋市", "千種区 東区 北区 西区 中村区 中区 昭和区 瑞穂区 熱田区 中川区 港区 "
                         "南区 守山区 緑区 名東区 天白区"),
        "豊橋市 岡崎市 一宮市 瀬戸市 半田市 春日井市 豊川市 津島市 碧南市 刈谷市 豊田市 安城市 "
        "西尾市 蒲郡市 犬山市 常滑市 江南市 小牧市 稲沢市 新城市 東海市 大府市 知多市 知立市 "
        "尾張旭市 高浜市 岩倉市 豊明市 日進市 田原市 愛西市 清須市 北名古屋市 弥富市 みよし市 "
        "あま市 長久手市 東郷町 豊山町 大口町 扶桑町 大治町 蟹江町 飛島村 阿久比町 東浦町 "
        "南知多町 美浜町 武豊町 幸田町 設楽町 東栄町 豊根村",
    ]),
    "福岡県": " ".join([
        _wards("北九州市", "門司区 若松区 戸畑区 小倉北区 小倉南区 八幡東区 八幡西区"),
        _wards("福岡市", "東区 博多区 中央区 南区 西区 城南区 早良区"),
        "大牟田市 久留米市 直方市 飯塚市 田川市 柳川市 八女市 筑後市 大川市 行橋市 豊前市 "
        "中間市 小郡市 筑紫野市 春日市 大野城市 宗像市 太宰府市 古賀市 福津市 うきは市 宮若市 "
        "嘉麻市 朝倉市 みやま市 糸島市 那珂川市 宇美町 篠栗町 志免町 須恵町 新宮町 久山町 "
        "粕屋町 芦屋町 水巻町 岡垣町 遠賀町 小竹町 鞍手町 桂川町 筑前町 東峰村 大刀洗町 大木町 "
        "広川町 香春町 添田町 糸田町 川崎町 大任町 赤村 福智町 苅田町 みやこ町 吉富町 上毛町 "
        "築上町",
    ]),
}

TARGET_PREFS = tuple(MUNICIPALITIES)

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_SEARCH_PATH = re.compile(r"^/?(?P<prefix>.*-)?(?P<area>[^-]+?)の仕事$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_value(v) -> str:
    """埋め込み JSON の 'None' / 'null' 文字列を空に正規化する。"""
    s = _clean(v)
    return "" if s in ("None", "null", "undefined") else s


def _extract_pref(text: str) -> str:
    m = _PREF_PATTERN.search(_clean(text))
    return m.group(1) if m else ""


def _strip_pref(addr: str) -> str:
    addr = _clean(addr)
    m = _PREF_PATTERN.match(addr)
    return addr[m.end():].strip() if m else addr


def _safe_json_loads(text: str) -> dict:
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except (ValueError, TypeError):
        return {}


def posted_date(label: str, today: date) -> date | None:
    """新着表示から求人掲載日を求める。解釈できない表示は None。

    新着・○分前・○時間前 → 取得日 / ○日前 → 取得日の○日前 /
    ○週間前・○ヶ月前・○年前 → それぞれ 7 日・30 日・365 日換算。
    """
    label = _clean(label)
    if not label:
        return None
    if re.search(r"新着|たった今|秒前|分前|時間前|本日|今日", label):
        return today
    if "昨日" in label:
        return today - timedelta(days=1)
    m = re.search(r"(\d+)\s*(日|週間|週|[ヶヵケか]?月|年)", label)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    if unit == "日":
        days = n
    elif unit.startswith("週"):
        days = n * 7
    elif unit.endswith("月"):
        days = n * 30
    else:
        days = n * 365
    return today - timedelta(days=days)


def _iso_date(s: str) -> date | None:
    """'2026-10-07 16:43:28.000' 形式の先頭日付を date にする。"""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


class KyujinBoxBaitoPart14PrefScraper(StaticCrawler):
    """求人ボックス（アルバイト・パート 14都府県・掲載30日以内）スクレイパー"""

    DELAY = 3.0  # 1 リクエストごとの待機秒数 (Cloudflare 配下のため間隔を空ける)
    ITEM_DELAY = 0  # 待機はリクエスト単位で行う (一覧 1 ページから複数件を yield するため)

    EXTRA_COLUMNS = [
        "勤務地",
        "職種",
        "雇用形態",
        "求人タイトル",
        "給与",
        "新着表示",
        "求人掲載日",
        "求人票URL",
        "取得日",
        "検索市区町村",
        "掲載区分",
    ]

    # 求人掲載日がこの日数より前の求人に達したら、その市区町村を打ち切る
    RECENT_DAYS = 30
    # 通常枠に 30 日以内の求人が無いページがこの数だけ続いたら、その市区町村を打ち切る
    STALE_PAGES = 3
    # 1 市区町村あたりの最大ページ数 (安全弁。通常は「次のページへ」が無くなった時点で終了)
    MAX_PAGES = 1000

    def parse(self, url: str) -> Generator[dict, None, None]:
        self._today = date.today()
        self._cutoff = self._today - timedelta(days=self.RECENT_DAYS)
        self._seen_ids: set[str] = set()
        self._last_fetch_was_network = False

        for area, list_url in self._area_urls(url):
            yield from self._crawl_area(area, list_url)

    # -------------------------------------------------------------- URL 組み立て
    def _area_urls(self, url: str) -> list[tuple[str, str]]:
        """(検索市区町村, 一覧URL) を巡回順に返す。

        引数 url を唯一のルートとし、url のパス中の地名部分 (例: 東京都新宿区) を差し替える。
        クエリは引き継ぐが、新着フィルタ u (?u=1 = 本日の新着) とページ番号 pg は外す。
        """
        parts = urlsplit(url)
        m = _SEARCH_PATH.match(unquote(parts.path))
        prefix = (m.group("prefix") or "") if m else "アルバイト・パート-"
        root_area = m.group("area") if m else ""
        params = [(k, v) for k, v in parse_qsl(parts.query) if k not in ("u", "pg")]
        query = f"?{urlencode(params)}" if params else ""

        pairs: list[tuple[str, str]] = [(root_area, urljoin(url, parts.path) + query)]
        seen = {root_area}
        for pref, names in MUNICIPALITIES.items():
            for name in names.split():
                area = pref + name
                if area in seen:
                    continue
                seen.add(area)
                path = "/" + quote(f"{prefix}{area}の仕事")
                pairs.append((area, urljoin(url, path) + query))
        return pairs

    # ------------------------------------------------------------------ 取得
    def _fetch(self, url: str):
        if self._last_fetch_was_network:
            time.sleep(self.DELAY)
        soup = self.get_soup(url)
        # リプレイ (キャッシュヒット) 時は待機不要
        self._last_fetch_was_network = not getattr(self, "_last_fetch_from_cache", False)
        return soup

    # ------------------------------------------------------------------ 一覧巡回
    def _crawl_area(self, area: str, list_url: str) -> Generator[dict, None, None]:
        next_url = list_url
        stale_pages = 0
        for page in range(1, self.MAX_PAGES + 1):
            soup = self._fetch(next_url)
            if soup is None:
                self.logger.warning("[%s] 一覧取得失敗: %s", area, next_url)
                return

            if page == 1 and area and not self._is_area_page(soup, area):
                self.logger.warning("[%s] 検索結果がこの地名に対応していないためスキップ", area)
                return

            cards = soup.select("section.p-result_card")
            if not cards:
                self.logger.info("[%s] %dページ目: 求人なし、終了", area, page)
                return

            kept = 0
            fresh_normal = 0
            for card in cards:
                item = self._parse_card(card, area, next_url)
                if not item:
                    continue
                posted = item.pop("_posted")
                if posted is not None and posted < self._cutoff:
                    continue
                # 広告枠は並びが検索地域・日付と無関係なため打ち切り判定に使わない
                if item["掲載区分"] == "通常":
                    fresh_normal += 1
                uid = item.pop("_unique_id")
                if uid in self._seen_ids:
                    continue
                self._seen_ids.add(uid)
                if item[Schema.PREF] and item[Schema.PREF] not in TARGET_PREFS:
                    continue
                kept += 1
                yield item

            self.logger.info(
                "[%s] %dページ目: 求人%d件 / 通常枠30日以内%d件 / 出力%d件",
                area, page, len(cards), fresh_normal, kept,
            )
            stale_pages = 0 if fresh_normal else stale_pages + 1
            if stale_pages >= self.STALE_PAGES:
                self.logger.info(
                    "[%s] %dページ連続で通常枠に%d日以内の求人なし、打ち切り",
                    area, stale_pages, self.RECENT_DAYS,
                )
                return

            next_link = soup.select_one("a.c-pager_btn--next")
            href = _clean(next_link.get("href")) if next_link else ""
            if not href:
                return
            next_url = urljoin(next_url, href)

    @staticmethod
    def _is_area_page(soup, area: str) -> bool:
        h1 = soup.select_one("h1")
        heading = re.sub(r"\s+", "", h1.get_text()) if h1 else ""
        return not heading or area in heading

    # ------------------------------------------------------------------ カード解析
    def _parse_card(self, card, area: str, list_url: str) -> dict | None:
        a_tag = card.select_one("a.p-result_title_link")
        if not a_tag:
            return None
        preview = self._extract_preview_json(a_tag)
        unique_id = _norm_value(preview.get("uniqueId")) or _clean(a_tag.get("data-target-focus"))
        if not unique_id:
            return None

        title = _norm_value(preview.get("formatTitle") or preview.get("title")) or _clean(
            a_tag.get_text(" ", strip=True)
        )
        work_area = _norm_value(preview.get("workArea")) or self._text(card, ".p-result_area")
        new_label = self._text(card, ".p-result_updatedAt_hyphen") or self._text(
            card, ".p-result_new"
        )
        posted = posted_date(new_label, self._today)
        updated = _iso_date(_norm_value(preview.get("updatedAt")))
        if posted is None:
            posted = updated
        elif "以上" in new_label and updated and updated < posted:
            # 「14日以上前」は上限側の目安でしかないため、より古い updatedAt を優先する
            posted = updated
        is_ad = any(cls.startswith("p-ad-item") for cls in card.get("class") or [])

        return {
            Schema.URL: list_url,
            Schema.NAME: _norm_value(preview.get("company")) or self._text(
                card, ".p-result_company"
            ),
            Schema.PREF: _extract_pref(work_area),
            Schema.ADDR: _strip_pref(work_area),
            "勤務地": work_area,
            "職種": _norm_value(preview.get("jobType")),
            "雇用形態": _norm_value(preview.get("employType"))
            or self._text(card, ".p-result_employType"),
            "求人タイトル": title,
            "給与": _norm_value(preview.get("payment")) or self._text(card, ".p-result_pay"),
            "新着表示": new_label,
            "求人掲載日": posted.isoformat() if posted else "",
            "求人票URL": self._job_url(preview, unique_id, list_url),
            "取得日": self._today.isoformat(),
            "検索市区町村": area,
            "掲載区分": "広告" if is_ad else "通常",
            "_posted": posted,
            "_unique_id": unique_id,
        }

    @staticmethod
    def _extract_preview_json(a_tag) -> dict:
        raw = a_tag.get("data-func-show-arg", "")
        if not raw:
            return {}
        inner = _safe_json_loads(raw).get("json")
        if isinstance(inner, dict):
            return inner
        if isinstance(inner, str):
            return _safe_json_loads(html.unescape(inner))
        return {}

    @staticmethod
    def _job_url(preview: dict, unique_id: str, list_url: str) -> str:
        """求人票のパーマリンク。/jb/ は robots Disallow のため記録のみで取得はしない。"""
        rd_url = _norm_value(preview.get("rdUrl"))
        if rd_url and not rd_url.startswith("/rd"):
            return urljoin(list_url, rd_url)
        return urljoin(list_url, f"/jb/{unique_id}")

    @staticmethod
    def _text(root, selector: str) -> str:
        el = root.select_one(selector)
        return _clean(el.get_text(" ", strip=True)) if el else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KyujinBoxBaitoPart14PrefScraper()
    scraper.execute("https://xn--pckua2a7gp15o89zb.com/アルバイト・パート-東京都新宿区の仕事?u=1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
