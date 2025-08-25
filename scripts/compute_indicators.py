# WSで保存した NDJSON（bars）から、1分バー＋VWAP＋AVWAP(9:30)＋ORB(5m) を計算して data/bars/ に保存します。
# 役割は Runbookの compute_indicators（ORB/VWAP/AVWAP）と一致させています。  :contentReference[oaicite:4]{index=4}

from __future__ import annotations
from pathlib import Path           # 入出力パス操作
from datetime import datetime, time
import pandas as pd                # 集計と指標計算に使う
from loguru import logger          # ログ（共通ルールで data/logs/bot.log へ）

from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists  # .env を先に読む（API/設定）  :contentReference[oaicite:5]{index=5}
from rh_pdc_daytrade.utils.logutil import configure_logging       # ログ初期化（冪等）
from rh_pdc_daytrade.utils.configutil import load_config          # config.yaml のロード
from rh_pdc_daytrade.utils.timeutil import get_et_tz              # ET日付の決定（tzdata+フォールバック）  :contentReference[oaicite:6]{index=6}
from rh_pdc_daytrade.utils.io import write_parquet, write_csv     # Parquet/CSVの標準保存口  :contentReference[oaicite:7]{index=7}

def _bars_ndjson_path(channel: str = "bars") -> Path:
    """何をする関数？：ET日付の NDJSON（bars_YYYYMMDD.ndjson）のパスを返します。"""
    et_date = datetime.now(get_et_tz()).strftime("%Y%m%d")
    return Path("data") / "stream" / f"{channel}_{et_date}.ndjson"

def _read_bars_ndjson(p: Path, symbols: list[str]) -> pd.DataFrame:
    """
    何をする関数？：
      - NDJSON（1行=1メッセージ）を読み、必要なキー（S,t,o,h,l,c,v）だけを取り出して DataFrame にします。
      - 指定の symbols に含まれるものだけに絞ります。ファイルが無ければ空DataFrameを返します。
    """
    import orjson  # この関数内でのみ使う高速JSON
    if not p.exists():
        logger.warning("bars ndjson not found: {}", p)
        return pd.DataFrame(columns=["symbol", "et", "o", "h", "l", "c", "v"])

    def _parse_ts(val: int) -> pd.Timestamp:
        # 受信 t（エポック）が ns/us/ms/s のどれでも安全に ET に変換します。
        v = int(val)
        unit = "ns" if v > 1_000_000_000_000_000_000 else "us" if v > 1_000_000_000_000 else "ms" if v > 1_000_000_000 else "s"
        return pd.to_datetime(v, unit=unit, utc=True).tz_convert(get_et_tz())

    rows = []
    with open(p, "rb") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                m = orjson.loads(line)
            except Exception:
                continue
            if not isinstance(m, dict) or m.get("type") != "bar":
                continue
            s = str(m.get("S") or "").upper()
            if symbols and s not in symbols:
                continue
            rows.append({
                "symbol": s,
                "et": _parse_ts(m.get("t")),
                "o": float(m.get("o", 0.0)),
                "h": float(m.get("h", 0.0)),
                "l": float(m.get("l", 0.0)),
                "c": float(m.get("c", 0.0)),
                "v": float(m.get("v", 0.0)),
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(["symbol", "et"], kind="mergesort").reset_index(drop=True)

def _compute_vwap(df: pd.DataFrame) -> pd.DataFrame:
    """何をする関数？：当日累積の VWAP を計算して列 'vwap' を追加します（簡易版：終値×出来高で近似）。"""
    if df.empty:
        df["vwap"] = []
        return df
    g = df.groupby("symbol", as_index=False, sort=False)
    df["pv"] = df["c"] * df["v"]
    df["cum_pv"] = g["pv"].cumsum()
    df["cum_v"] = g["v"].cumsum()
    df["vwap"] = df["cum_pv"] / df["cum_v"]
    return df.drop(columns=["pv", "cum_pv", "cum_v"])

def _compute_avwap(df: pd.DataFrame, anchor: str = "09:30:00") -> pd.DataFrame:
    """
    何をする関数？：**9:30:00（ET）アンカー**以降の出来高加重平均（AVWAP）を計算し 'avwap' を追加します。
    AVWAPの概念は戦略PDFどおり、寄り時刻を基準に累積します。  :contentReference[oaicite:8]{index=8}
    """
    if df.empty:
        df["avwap"] = []
        return df
    anc_t = time.fromisoformat(anchor)
    after_anchor = df["et"].dt.time >= anc_t
    df["pv_a"] = df["c"] * df["v"] * after_anchor
    df["v_a"] = df["v"] * after_anchor
    g = df.groupby("symbol", as_index=False, sort=False)
    df["cum_pv_a"] = g["pv_a"].cumsum()
    df["cum_v_a"] = g["v_a"].cumsum()
    df["avwap"] = df["cum_pv_a"] / df["cum_v_a"]
    return df.drop(columns=["pv_a", "v_a", "cum_pv_a", "cum_v_a"])

def _compute_orb_5m(df: pd.DataFrame) -> pd.DataFrame:
    """
    何をする関数？：**9:30–9:35（ET）の5本**で ORB 高値/安値を計算し、銘柄ごとの1行サマリを返します。  :contentReference[oaicite:9]{index=9}
    戻り値：symbol, orb_high, orb_low
    """
    if df.empty:
        return pd.DataFrame(columns=["symbol", "orb_high", "orb_low"])
    t = df["et"].dt.time
    m = (t >= time(9, 30)) & (t < time(9, 35))
    base = df.loc[m, ["symbol", "h", "l"]]
    if base.empty:
        return pd.DataFrame(columns=["symbol", "orb_high", "orb_low"])
    agg = base.groupby("symbol").agg(orb_high=("h", "max"), orb_low=("l", "min")).reset_index()
    return agg

def _save_outputs(df_1m: pd.DataFrame, summary: pd.DataFrame) -> tuple[Path, Path]:
    """
    何をする関数？：
      - 計算した 1分バー（VWAP/AVWAP付き）と、銘柄ごとの ORB/VWAP/AVWAP の**当日スナップショット**を保存します。
      - 保存先：data/bars/bars_1m_YYYYMMDD.parquet / indicators_YYYYMMDD.parquet（CSVも同名で保存）。  :contentReference[oaicite:10]{index=10}
    """
    et_date = datetime.now(get_et_tz()).strftime("%Y%m%d")
    out_dir = Path("data") / "bars"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1分バー
    p1 = out_dir / f"bars_1m_{et_date}.parquet"
    write_parquet(df_1m, p1)

    # スナップショット（銘柄×1行：最新の vwap/avwap と ORB）
    latest = (df_1m.sort_values(["symbol", "et"])
                    .groupby("symbol")
                    .tail(1)[["symbol", "vwap", "avwap"]])
    snap = latest.merge(summary, on="symbol", how="left")
    p2 = out_dir / f"indicators_{et_date}.parquet"
    write_parquet(snap, p2)
    # 人が見る用に CSV も保存
    write_csv(df_1m, out_dir / f"bars_1m_{et_date}.csv")
    write_csv(snap,  out_dir / f"indicators_{et_date}.csv")
    return p1, p2

def main() -> int:
    """
    何をする関数？：
      - .env → ログ → 設定 を読み、当日の NDJSON（bars_YYYYMMDD.ndjson）を読み込み、
        VWAP / AVWAP(9:30) / ORB(5m) を計算して data/bars/ に保存します。  :contentReference[oaicite:11]{index=11}
    使い方：
      poetry run python scripts/compute_indicators.py
    """
    load_dotenv_if_exists()
    logfile = configure_logging()
    cfg = load_config()
    logger.info("compute_indicators: start (logfile={})", logfile)

    # ウォッチ対象（ws_run と同じく watchlist を優先・無ければ全件許容）
    # ここでは NDJSON 内のシンボルで自動的に絞られるため、空でもOK。
    ndjson_path = _bars_ndjson_path("bars")
    # symbols は空にして「ファイル内の全銘柄」を対象に（将来は cfg のA/Bに合わせて渡せます）
    df = _read_bars_ndjson(ndjson_path, symbols=[])

    if df.empty:
        logger.warning("no bars to compute ({}). Is it outside regular hours?", ndjson_path)
        return 0  # 市場時間外は bars が0でも正常（Runbookの想定）  :contentReference[oaicite:12]{index=12}

    df = _compute_vwap(df)
    df = _compute_avwap(df, anchor=cfg.get("strategy", {}).get("avwap_anchor", "09:30:00"))
    orb = _compute_orb_5m(df)
    p1, p2 = _save_outputs(df, orb)
    logger.info("indicators saved: {} , {}", p1, p2)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
