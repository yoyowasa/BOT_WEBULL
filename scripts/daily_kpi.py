# 紙の約定ログ（data/logs/executions.csv）から当日分を集計し、kpi_daily.csv に upsert します。
# 役割：ランブックの「ログ→日次KPI」フローの最小実装（当日ぶんを1行で更新）。  :contentReference[oaicite:3]{index=3}

from __future__ import annotations
from pathlib import Path                # 入出力パスの扱い
import pandas as pd                     # CSVの読込と集計に使う
from loguru import logger               # ログ（data/logs/bot.log へ集約）

from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists   # 何をする関数？：.envを先に読む  :contentReference[oaicite:4]{index=4}
from rh_pdc_daytrade.utils.logutil import configure_logging        # 何をする関数？：ログ初期化（冪等）  :contentReference[oaicite:5]{index=5}
from rh_pdc_daytrade.utils.configutil import load_config           # 何をする関数？：config.yaml を読む（将来の閾値参照）  :contentReference[oaicite:6]{index=6}
from rh_pdc_daytrade.utils.timeutil import get_et_tz              # 何をする関数？：ETタイムゾーンで“きょう”を決める  :contentReference[oaicite:7]{index=7}
from datetime import datetime                                     # 当日ET日付の決定に使う

def _paths() -> tuple[Path, Path]:
    """
    何をする関数？：
      - 約定ログCSV（executions.csv）と、出力KPI（kpi_daily.csv）のパスを返します。
      - 置き場所はランブック準拠（data/logs/）。  :contentReference[oaicite:8]{index=8}
    """
    logs = Path("data") / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    return (logs / "executions.csv", logs / "kpi_daily.csv")

def _today_et_str() -> str:
    """何をする関数？：ETの“きょう”を YYYYMMDD 文字列で返します。"""
    return datetime.now(get_et_tz()).strftime("%Y%m%d")

def _read_today_executions(p_exec: Path) -> pd.DataFrame:
    """
    何をする関数？：
      - executions.csv があれば読み、当日（ET）の行だけを返します。
      - 列が足りない場合（qty等）はここで追加し、以降の集計が止まらないようにします。  
    """
    if not p_exec.exists():
        logger.info("daily_kpi: executions.csv が見つかりません（初回/未約定の可能性）")
        return pd.DataFrame(columns=[
            "date","timestamp_et","symbol","setup","entry_type","qty","entry_price","tp_price","sl_price","notes"
        ])
    df = pd.read_csv(p_exec)
    today = _today_et_str()
    df = df[df["date"].astype(str) == today].copy()
    # 無い列は追加（0/空）しておく
    for col, default in (("qty", 0), ("entry_price", None), ("tp_price", None), ("sl_price", None)):
        if col not in df.columns:
            df[col] = default
    # 型を整える（集計で使う列だけ）
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
    for c in ("entry_price","tp_price","sl_price"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _compute_kpi_today(df: pd.DataFrame) -> dict:
    """
    何をする関数？：
      - 当日ぶんの簡易KPIを1行の dict にまとめます（紙トレ最小版）。
      - 出す項目：
        件数 / A・B内訳 / 平均entry / 平均TP幅[%] / 平均SL幅[%] / 平均qty / 想定リスク合計USD。
      - 想定リスク合計 = ∑ max(entry − SL, 0) × qty。  :contentReference[oaicite:3]{index=3}
    """
    today = _today_et_str()
    if df.empty:
        return {
            "date": today, "trades": 0, "setup_A": 0, "setup_B": 0,
            "avg_entry_price": None, "avg_tp_pct": None, "avg_sl_pct": None,
            "avg_qty": None, "total_risk_usd": None
        }

    trades = len(df)
    setup_A = int((df.get("setup","") == "A").sum()) if "setup" in df.columns else 0
    setup_B = int((df.get("setup","") == "B").sum()) if "setup" in df.columns else 0

    avg_entry = float(df["entry_price"].mean()) if "entry_price" in df.columns else None

    if {"entry_price","tp_price"}.issubset(df.columns):
        tp_pct = ((df["tp_price"] - df["entry_price"]) / df["entry_price"]).replace([pd.NA, pd.NaT], None).dropna()*100.0
        avg_tp = float(tp_pct.mean()) if not tp_pct.empty else None
    else:
        avg_tp = None

    if {"entry_price","sl_price"}.issubset(df.columns):
        sl_pct = ((df["entry_price"] - df["sl_price"]) / df["entry_price"]).replace([pd.NA, pd.NaT], None).dropna()*100.0
        avg_sl = float(sl_pct.mean()) if not sl_pct.empty else None
    else:
        avg_sl = None

    avg_qty = float(df["qty"].mean()) if "qty" in df.columns and trades > 0 else None

    # 想定リスク金額：entry−SL が負なら0として切り上げ、qtyを掛けて合計（紙トレの管理用KPI）。  :contentReference[oaicite:4]{index=4}
    if {"entry_price","sl_price","qty"}.issubset(df.columns):
        per_trade_risk = (df["entry_price"] - df["sl_price"]).clip(lower=0) * df["qty"]
        total_risk = float(per_trade_risk.sum())
    else:
        total_risk = None

    return {
        "date": today, "trades": trades, "setup_A": setup_A, "setup_B": setup_B,
        "avg_entry_price": avg_entry, "avg_tp_pct": avg_tp, "avg_sl_pct": avg_sl,
        "avg_qty": avg_qty, "total_risk_usd": total_risk
    }


def _upsert_kpi_row(row: dict, p_kpi: Path) -> Path:
    """
    何をする関数？：
      - 既存の kpi_daily.csv を読み（date列は**文字列**で統一）、
        同じ日付の行を一度削除 → 今回の1行を追加 → 日付で昇順ソートして保存します。
      - 空DataFrameとのconcatで将来のWarningが出ないよう、空のときはconcatを使わずにそのまま代入します。  :contentReference[oaicite:1]{index=1}
    """
    cols = ["date","trades","setup_A","setup_B","avg_entry_price","avg_tp_pct","avg_sl_pct","avg_qty","total_risk_usd"]

    if p_kpi.exists():
        df = pd.read_csv(p_kpi, dtype={"date": str})  # 何をする行？：dateを必ず文字列として読む
        # 無い列は追加してから並びを固定（後方互換のため）
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]
        # 同日行を除外（文字列比較で安全）
        df = df[df["date"].astype(str) != str(row["date"])].copy()
    else:
        df = pd.DataFrame(columns=cols)

    # 追加する1行をDataFrame化（列を揃え、dateは文字列で統一）
    new_row = {c: row.get(c, None) for c in cols}
    new_row["date"] = str(new_row["date"])
    df_new = pd.DataFrame([new_row], columns=cols)

    # 空のときはconcatを避けて将来の仕様変更に備える
    if df.empty:
        out_df = df_new
    else:
        out_df = pd.concat([df, df_new], ignore_index=True)

    # 8桁YYYYMMDDの文字列として並び替え（辞書順＝日付順）
    out_df["date"] = out_df["date"].astype(str)
    out_df = out_df.sort_values("date")

    out_df.to_csv(p_kpi, index=False)
    return p_kpi



def main() -> int:
    """
    何をする関数？：
      - .env → ログ → config を読み、当日の executions.csv を集計して kpi_daily.csv に upsert します。
      - 入力なし（0件）でも正常終了（運用フロー上、signalsが無い日はあり得ます）。  :contentReference[oaicite:12]{index=12}
    使い方：
      poetry run python scripts/daily_kpi.py
    """
    load_dotenv_if_exists()
    logfile = configure_logging()
    load_config()  # いまは閾値未使用でも、将来の拡張に備えて読み込んでおく  :contentReference[oaicite:13]{index=13}

    p_exec, p_kpi = _paths()
    df_today = _read_today_executions(p_exec)
    row = _compute_kpi_today(df_today)
    out = _upsert_kpi_row(row, p_kpi)
    logger.info("daily_kpi: {} のKPIを更新しました（logfile={}）", row["date"], logfile)
    logger.info("daily_kpi: output => {}", out)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
