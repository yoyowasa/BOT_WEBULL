# 夜間EODスクリーニングの計算ロジック（ハードフィルタ＋スコア）をまとめたモジュールです。
# 目的：PDFの「基本8割」の条件で A/B 用スコアを計算し、上位銘柄を選べるようにする箱。  :contentReference[oaicite:3]{index=3}
# A=ORB+VWAP, B=AVWAP 押し目という戦略の骨子に一致（同日いずれか一方のみ運用）。            :contentReference[oaicite:4]{index=4}

from __future__ import annotations
from typing import Iterable, Tuple, Dict
import numpy as np               # 数値計算（スコア算出/クリップ）
import pandas as pd              # テーブル計算（フィルタ/スコア列を追加）

# ---- ヘルパ：必須列が無い時でも「止めずに」空列で補う -----------------------------------------
def _ensure_columns(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    # 何をする関数？：必要な列が無ければ NaN で作っておき、計算途中で落ちないようにします。
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

# ---- ハードフィルタ ---------------------------------------------------------------------------
def apply_hard_filters(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    何をする関数？：
      PDFの“まず落とす条件”でブーリアン列を作り、最後に pass_all（全条件OK）を付けます。       :contentReference[oaicite:5]{index=5}
      条件：$2–$20、20日平均出来高/ドル出来高、ATR14%=4–12%、Close>EMA20>EMA50、Float=10–60M。
    使い方：
      filtered = apply_hard_filters(df, cfg); df_pass = filtered[filtered["pass_all"]]
    期待する列（無い場合は NaN 補完）：
      close, avg_volume20, avg_dollar_vol20, atr14, ema20, ema50, float, high_52w, pdc, pdh, pdl,
      is_inside_day, is_nr7, pivot_p
    """
    scr = cfg.get("screening", {})
    df = _ensure_columns(df, [
        "close", "avg_volume20", "avg_dollar_vol20", "atr14", "ema20", "ema50",
        "float", "high_52w", "pdc", "pdh", "pdl", "is_inside_day", "is_nr7", "pivot_p"
    ])

    price_min = float(scr.get("price_min", 2.0))
    price_max = float(scr.get("price_max", 20.0))
    atr_min   = float(scr.get("atr_pct_min", 0.04))
    atr_max   = float(scr.get("atr_pct_max", 0.12))
    vol_min   = float(scr.get("min_avg_volume", 1_000_000))
    dvol_min  = float(scr.get("min_avg_dollar_vol", 5_000_000))
    flt_min   = float(scr.get("float_min", 10_000_000))
    flt_max   = float(scr.get("float_max", 60_000_000))

    # 価格帯
    df["ok_price"] = (df["close"] >= price_min) & (df["close"] <= price_max)
    # 流動性（出来高＆ドル出来高）
    df["ok_liquidity"] = (df["avg_volume20"] >= vol_min) & (df["avg_dollar_vol20"] >= dvol_min)
    # ATR%（中心域 4–12%）
    df["atr_pct"] = df["atr14"] / df["close"]
    df["ok_volatility"] = (df["atr_pct"] >= atr_min) & (df["atr_pct"] <= atr_max)
    # トレンド整合：Close > EMA20 > EMA50
    df["ok_trend"] = (df["close"] > df["ema20"]) & (df["ema20"] > df["ema50"])
    # フロート帯域：10–60M
    df["ok_float"] = (df["float"] >= flt_min) & (df["float"] <= flt_max)

    # すべて満たすか
    df["pass_all"] = df[["ok_price", "ok_liquidity", "ok_volatility", "ok_trend", "ok_float"]].all(axis=1)
    return df

# ---- スコア計算（A=基本、B=基本+リテスト加点） --------------------------------------------------
def compute_scores_basic(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    何をする関数？：
      “基本8割”の線形和でスコアを作ります。Aは基本スコア、Bは Pivot 近接の再テスト加点を足します。   :contentReference[oaicite:6]{index=6}
      重みはランブックの例（pdh/atr/dollar_vol/trend/compression/float）を使用。                       :contentReference[oaicite:7]{index=7}
    使い方：
      df = compute_scores_basic(df, cfg); df.nlargest(20, "score_A")
    期待する列（無い場合は NaN 補完）：
      close, pdh, is_inside_day, is_nr7, avg_dollar_vol20, atr14, ema20, ema50, float, high_52w, pivot_p
    """
    w = (cfg.get("scoring", {}) or {}).get("weights", {}) or {}
    w_pdh = float(w.get("pdh", 0.30))
    w_atr = float(w.get("atr", 0.20))
    w_dol = float(w.get("dollar_vol", 0.15))
    w_trd = float(w.get("trend", 0.15))
    w_cmp = float(w.get("compression", 0.10))
    w_flt = float(w.get("float", 0.10))

    df = _ensure_columns(df, [
        "close", "pdh", "is_inside_day", "is_nr7", "avg_dollar_vol20",
        "atr14", "ema20", "ema50", "float", "high_52w", "pivot_p"
    ])

    # ---- 個別スコアの定義（PDFに準拠） ---------------------------------------------------------  :contentReference[oaicite:8]{index=8}
    # PDH 近接（2%以内を高評価）
    dist_pdh = (df["pdh"] - df["close"]) / df["close"]
    df["pdh_score"] = np.clip((0.02 - dist_pdh) / 0.02, 0.0, 1.0)

    # ATR%：4–12% を三角でピーク 8%
    atr_pct = df["atr14"] / df["close"]
    a, b, c = 0.04, 0.08, 0.12
    left = (atr_pct - a) / (b - a)
    right = (c - atr_pct) / (c - b)
    df["atr_score"] = np.clip(np.minimum(left, right), 0.0, 1.0)

    # ドル出来高：$5M→0.5、$20M→1.0 に漸近（単純な区分線形）
    dvol = df["avg_dollar_vol20"]
    df["dollar_vol_score"] = np.clip((dvol - 5_000_000) / (15_000_000), 0.0, 1.0)
    df.loc[dvol >= 20_000_000, "dollar_vol_score"] = 1.0
    df.loc[(dvol > 0) & (dvol < 5_000_000), "dollar_vol_score"] = 0.5 * (dvol / 5_000_000)

    # トレンド：Close>EMA20>EMA50=1、Close>EMA20=0.5、その他0
    df["trend_score"] = 0.0
    df.loc[(df["close"] > df["ema20"]) & (df["ema20"] > df["ema50"]), "trend_score"] = 1.0
    df.loc[(df["close"] > df["ema20"]) & ~(df["ema20"] > df["ema50"]), "trend_score"] = 0.5

    # 圧縮（Inside/NR7 なら1、それ以外0）
    df["compression_score"] = np.where((df["is_inside_day"] == True) | (df["is_nr7"] == True), 1.0, 0.0)

    # フロート：10–60M=1、60–120M=0.6、その他0.2
    fl = df["float"]
    df["float_score"] = 0.2
    df.loc[(fl >= 10_000_000) & (fl <= 60_000_000), "float_score"] = 1.0
    df.loc[(fl > 60_000_000) & (fl <= 120_000_000), "float_score"] = 0.6

    # ---- 基本スコア（A用） ----------------------------------------------------------------------
    df["score"] = (
        w_pdh * df["pdh_score"] +
        w_atr * df["atr_score"] +
        w_dol * df["dollar_vol_score"] +
        w_trd * df["trend_score"] +
        w_cmp * df["compression_score"] +
        w_flt * df["float_score"]
    )
    df["score_A"] = df["score"]  # A=基本

    # ---- B用加点：Pivot 近接（|Close-P|/Close ≤ 0.5% 付近を高評価） -----------------------------
    # retest_score = 1 - clip(|Close-P|/Close / 0.005, 0..1)
    with np.errstate(divide="ignore", invalid="ignore"):
        pivot_dist = np.abs(df["close"] - df["pivot_p"]) / df["close"]
    df["retest_score"] = 1.0 - np.clip(pivot_dist / 0.005, 0.0, 1.0)
    df["score_B"] = df["score"] + 0.15 * df["retest_score"]  # PDFの例に合わせて+0.15を加点       :contentReference[oaicite:9]{index=9}

    return df

# ---- 上位N件の選出（A/B） ----------------------------------------------------------------------
def rank_watchlists(df: pd.DataFrame, top_n: int = 20) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    何をする関数？：
      pass_all=True の行から、A/B それぞれスコア順に上位 N 件を返します（列は symbol, score_* のみ）。
    使い方：
      topA, topB = rank_watchlists(df_scored, 20)
    """
    base = df[df.get("pass_all", True) == True].copy()
    cols = [c for c in ["symbol", "score_A", "score_B"] if c in base.columns]
    topA = base.sort_values("score_A", ascending=False).head(top_n)[cols]
    topB = base.sort_values("score_B", ascending=False).head(top_n)[cols]
    return topA, topB
