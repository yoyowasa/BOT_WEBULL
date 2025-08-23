# Polygon REST（Basicプラン想定）から日足を取得し、EODスクリーナに必要な列を計算して返すモジュールです。
# 目的（小学生向け）：株の「昨日までの成績表（日足）」をたくさん集めて、点数をつける前の“材料”をそろえます。
# 使い道：nightly_screen.py がこれを呼び、apply_hard_filters → compute_scores_basic へ渡します。
# 根拠：Runbookの providers/polygon_rest.py という責務分担と、スクリーナPDFの“基本8割”指標。  

from __future__ import annotations
from datetime import date, timedelta
from typing import Iterable, Dict, Any
import requests  # REST呼び出し
import pandas as pd  # 日足の集計・指標計算
import numpy as np   # 数値計算（ATRなど）
from loguru import logger  # エラーログ
from tenacity import retry, wait_exponential, stop_after_attempt  # 一時的な失敗の再試行

# ---- 内部ヘルパ：HTTP -------------------------------------------------------------------------

def _daterange_for(days: int = 400) -> tuple[str, str]:
    # 何をする関数？：過去N日ぶんの日付レンジ（ISO文字列）を作ります（営業日じゃない日も含め広めに）。
    start = (date.today() - timedelta(days=days)).isoformat()
    end = date.today().isoformat()
    return start, end

def _session(api_key: str) -> requests.Session:
    # 何をする関数？：Polygon用の共通セッション（ヘッダ付き）を作ります。
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {api_key}"})
    return s

@retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
def _get_json(s: requests.Session, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    # 何をする関数？：GETしてJSONを返します。429/一時失敗は指数バックオフで再試行します。
    r = s.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _fetch_aggs_1d(s: requests.Session, symbol: str, days: int = 400) -> pd.DataFrame:
    # 何をする関数？：1日足（1/day）をまとめて取得し、DataFrame化します。
    start, end = _daterange_for(days)
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
    js = _get_json(s, url, {"adjusted": "true", "sort": "asc", "limit": 500})
    results = js.get("results") or []
    if not results:
        logger.warning("polygon: empty results for {}", symbol)
        return pd.DataFrame()
    df = pd.DataFrame(results)
    # 統一：列名を短く（o/h/l/c/v, t=ms）に合わせ、欠損は落とす
    want = {"o": "o", "h": "h", "l": "l", "c": "c", "v": "v", "t": "t"}
    df = df.rename(columns=want)[list(want.keys())]
    df = df.dropna()
    return df

# ---- 指標計算（“基本8割”に必要な列） ------------------------------------------------------------

def _features_from_aggs(symbol: str, daily: pd.DataFrame) -> Dict[str, Any]:
    """
    何をする関数？：
      - 日足DataFrameから、EODスクリーナに必要な列を計算して1行の辞書にまとめます。
      - 計算するもの：PDC/PDH/PDL、ATR14、EMA20/50、Inside/NR7、52w高、20日平均出来高/ドル出来高、Pivot(P)。
    """
    if daily.empty or len(daily) < 20:
        return {}

    # EMA・ATR の計算
    close = daily["c"].astype(float)
    high = daily["h"].astype(float)
    low = daily["l"].astype(float)
    vol = daily["v"].astype(float)

    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()

    tr = pd.concat(
        [
            (high - low),
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr14 = tr.ewm(alpha=1 / 14, adjust=False).mean()

    # 最終日（=前日）で指標を集約
    last = len(close) - 1
    pdc = float(close.iloc[last])
    pdh = float(high.iloc[last])
    pdl = float(low.iloc[last])
    ema20_v = float(ema20.iloc[last])
    ema50_v = float(ema50.iloc[last])
    atr14_v = float(atr14.iloc[last])

    # Inside/NR7
    if last >= 1:
        prev_h, prev_l = float(high.iloc[last - 1]), float(low.iloc[last - 1])
        is_inside = (pdh <= prev_h) and (pdl >= prev_l)
    else:
        is_inside = False
    rng = (high - low).astype(float)
    is_nr7 = bool(rng.iloc[last] <= rng.tail(7).min())

    # 52週高（足りないときは取得期間内の最大）
    high_52w = float(high.tail(252).max() if len(high) >= 252 else high.max())

    # 平均出来高（株・ドル）
    avg_volume20 = float(vol.tail(20).mean())
    avg_dollar_vol20 = float((vol.tail(20) * close.tail(20)).mean())

    # Pivot(P)（前日値）
    pivot_p = (pdh + pdl + pdc) / 3.0

    # フロートは参照APIがプランにより取得不可のことが多いので、欠損時は“中庸”の 30M を仮置き（後で精緻化）。
    est_float = 30_000_000.0

    return {
        "symbol": symbol,
        "close": pdc,  # EODは“前日終値”を次日の基準にする
        "pdc": pdc,
        "pdh": pdh,
        "pdl": pdl,
        "ema20": ema20_v,
        "ema50": ema50_v,
        "atr14": atr14_v,
        "is_inside_day": is_inside,
        "is_nr7": is_nr7,
        "high_52w": high_52w,
        "avg_volume20": avg_volume20,
        "avg_dollar_vol20": avg_dollar_vol20,
        "pivot_p": pivot_p,
        "float": est_float,
    }

def fetch_eod_dataset(symbols: Iterable[str], api_key: str, days: int = 400) -> pd.DataFrame:
    """
    何をする関数？：
      - 複数銘柄の1日足をPolygonから取得し、“基本8割”用の特徴量を計算して DataFrame で返します。
      - 失敗した銘柄はスキップし、全体は“止めずに”続行します（あとで雛形にフォールバック可能）。  :contentReference[oaicite:4]{index=4}
    使い方：
      df = fetch_eod_dataset(["AAPL","TSLA"], api_key)
    """
    sess = _session(api_key)
    rows = []
    for sym in symbols:
        try:
            daily = _fetch_aggs_1d(sess, sym, days=days)
            feat = _features_from_aggs(sym, daily)
            if feat:
                rows.append(feat)
            else:
                logger.warning("polygon: insufficient data for {}", sym)
        except Exception as e:
            logger.error("polygon: failed {} ({})", sym, e)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
