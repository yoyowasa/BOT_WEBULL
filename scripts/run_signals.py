# A/Bシグナルを生成して data/signals/ にJSONを書き出すスクリプトです。
# A：ORB(5m)高値ブレイク＋VWAP上キープ（Stop‑Limitでブレイク追随）  :contentReference[oaicite:3]{index=3}
# B：AVWAP(9:30アンカー)付近（±0.3%）での反発（Limitで押し目拾い）  

from __future__ import annotations
from pathlib import Path                     # 入出力のパス操作
from datetime import datetime, time                # 生成時刻（ET）を記録
import os                                    # 環境変数（RUN_MODE等）
import math                                  # 価格丸め
import orjson                                # JSON高速出力
import pandas as pd                          # 1分バー/指標の読み込み
from loguru import logger                    # 共通ログ

from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists   # 何をする関数？：.envを先に読む  :contentReference[oaicite:5]{index=5}
from rh_pdc_daytrade.utils.logutil import configure_logging        # 何をする関数？：ログ初期化
from rh_pdc_daytrade.utils.configutil import load_config           # 何をする関数？：config.yaml を読む  :contentReference[oaicite:6]{index=6}
from rh_pdc_daytrade.utils.timeutil import get_et_tz               # 何をする関数？：ETのtzinfoを取得（フォールバック付）  :contentReference[oaicite:7]{index=7}
from rh_pdc_daytrade.risk.sizing import calc_qty_from_risk  # 何をする関数？：リスク％から数量を計算する。  :contentReference[oaicite:3]{index=3}

def _today_str() -> str:
    """何をする関数？：ET日付の文字列 YYYYMMDD を返します。"""
    return datetime.now(get_et_tz()).strftime("%Y%m%d")

def _paths_for_today() -> tuple[Path, Path]:
    """何をする関数？：当日分bars/indicatorsのParquetパスを返します。"""
    d = _today_str()
    return (Path("data") / "bars" / f"bars_1m_{d}.parquet",
            Path("data") / "bars" / f"indicators_{d}.parquet")

def _read_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    何をする関数？：
      - 1分バー（VWAP/AVWAP列含む）と当日スナップショット（vwap/avwap/orb_high/low）を読み込みます。
      - 片方でも無ければ空で返して“止めません”（運用フローの前提）。  :contentReference[oaicite:8]{index=8}
    """
    p_bars, p_ind = _paths_for_today()
    df_bars = pd.read_parquet(p_bars) if p_bars.exists() else pd.DataFrame()
    df_ind  = pd.read_parquet(p_ind)  if p_ind.exists()  else pd.DataFrame()
    return df_bars, df_ind

def _price_round(x: float) -> float:
    """何をする関数？：小型株の価格丸め（2桁）を行います（ざっくり）。"""
    return round(float(x), 2)

def _mk_bracket(entry: float, cfg: dict) -> dict:
    """
    何をする関数？：
      - config.yaml の bracket設定（TP/SL/半利確→建値）から、価格を具体化して返します。  :contentReference[oaicite:9]{index=9}
    """
    b = (cfg.get("bracket") or {})
    tps = b.get("take_profit_pct", [0.05, 0.10])
    slp = float(b.get("stop_loss_pct", 0.025))
    be  = bool(b.get("move_to_breakeven_after_first_tp", True))
    tp_price = _price_round(entry * (1 + float(tps[0])))
    sl_price = _price_round(entry * (1 - slp))
    return {"takeProfitPrice": tp_price, "stopLossPrice": sl_price, "moveToBreakevenOnTP": be}

def _active_watchlist(cfg: dict) -> set[str] | None:
    """
    何をする関数？：
      - config.strategy.active_setup（A/B）に対応する data/eod/watchlist_{A|B}.json を開き、
        "symbols" の文字列リストを set で返します。ファイルが無ければ None（= 全件許可）。  :contentReference[oaicite:1]{index=1}
    """
    from pathlib import Path           # この関数内だけで使うため関数内importにします
    import orjson                      # 同上（遅延インポートで起動を止めない）
    setup = str((cfg.get("strategy") or {}).get("active_setup", "A")).strip().upper()
    p = Path("data") / "eod" / f"watchlist_{setup}.json"
    if not p.exists():
        return None
    try:
        data = orjson.loads(p.read_bytes())
        syms = [s for s in data.get("symbols", []) if isinstance(s, str)]
        return set(syms) if syms else None
    except Exception:
        return None

def _compute_qty(entry_price: float, sl_price: float, cfg: dict) -> int:
    """
    何をする関数？：
      - config.risk.account_size_usd と risk_per_trade_pct を使って数量（整数）を返します。
      - 口座×リスク％ ÷ (entry−SL) で計算し、負やゼロは 0 にします。  :contentReference[oaicite:4]{index=4}
    使い方：
      qty = _compute_qty(10.16, 9.91, cfg)
    """
    risk_cfg = cfg.get("risk") or {}
    account = float(risk_cfg.get("account_size_usd", 10_000.0))    # 無指定なら $10k を仮定
    r_pct   = float(risk_cfg.get("risk_per_trade_pct", 0.005))     # 0.5%/trade が既定  :contentReference[oaicite:5]{index=5}
    try:
        return int(calc_qty_from_risk(entry_price, sl_price, account, r_pct))
    except Exception:
        return 0

def _already_exists(out_dir: Path, setup: str, symbol: str, entry_price: float) -> bool:
    """
    何をする関数？：
      - 同日・同セットアップ・同銘柄で“ほぼ同じエントリ価格（±0.1%）”のJSONがあるかを簡易チェックします。
      - 冪等性を確保し、重複シグナルの量産を防ぎます（エラー整理メモの方針）。  :contentReference[oaicite:10]{index=10}
    """
    if not out_dir.exists():
        return False
    for p in out_dir.glob(f"{_today_str()}__{setup}_{symbol}_*.json"):
        try:
            js = orjson.loads(p.read_bytes())
            ep = float(js.get("entry", {}).get("price", float("nan")))
            if math.isfinite(ep) and abs(ep - entry_price) / entry_price <= 0.001:
                return True
        except Exception:
            continue
    return False

def _gen_A(df_bars: pd.DataFrame, df_ind: pd.DataFrame, cfg: dict) -> list[dict]:
    """
    何をする関数？：
      - A：**ORB(5m)高値ブレイク＋VWAP上キープ**を「9:30–10:30 ET の全バー」を順に見て、
        最初に満たした1回だけシグナル化（Stop‑Limit）します。  :contentReference[oaicite:2]{index=2}
      具体条件：
        前足Close < ORB高値 かつ 今足Close ≥ ORB高値 かつ 今足Close ≥ 今足VWAP
    """
    allowed = _active_watchlist(cfg)  # 何をする行？：前夜のwatchlist（A/B）に載っている銘柄だけ許可。無ければ全件許可。  :contentReference[oaicite:1]{index=1}

    if df_bars.empty or df_ind.empty:
        return []
    ind = df_ind.set_index("symbol")
    out: list[dict] = []
    win_s, win_e = time(9, 30), time(10, 30)  # 勝負時間  :contentReference[oaicite:3]{index=3}

    for sym, g in df_bars.groupby("symbol", sort=False):
        if allowed and sym not in allowed:  # 何をする行？：ウォッチ外はスキップ（同日A/B混在を防ぐ運用ガード）。  :contentReference[oaicite:5]{index=5}
            continue

        if sym not in ind.index:
            continue
        # 勝負時間に絞る
        g = g[(g["et"].dt.time >= win_s) & (g["et"].dt.time < win_e)].reset_index(drop=True)
        if len(g) < 2:
            continue

        orb_hi = float(ind.loc[sym, "orb_high"])
        # 9:30→10:30 を順番に見て「初回クロス」を検出
        for i in range(1, len(g)):
            prev_c = float(g.iloc[i - 1]["c"])
            now_c  = float(g.iloc[ i    ]["c"])
            now_vw = float(g.iloc[ i    ].get("vwap", now_c))
            if (prev_c < orb_hi) and (now_c >= orb_hi) and (now_c >= now_vw):
                stop  = _price_round(orb_hi * 1.002)        # PDH+0.2%（Stop）  :contentReference[oaicite:4]{index=4}
                limit = _price_round(stop   * 1.003)        # +0.3%（Limit）
                br = _mk_bracket(limit, cfg)                # ブラケットは設定から  :contentReference[oaicite:5]{index=5}
                qty = _compute_qty(limit, br["stopLossPrice"], cfg)  # 何をする行？：リスク％から数量を出す。  :contentReference[oaicite:6]{index=6}

                out.append({
                    "date": _today_str(),
                    "symbol": sym,
                    "setup": "A",
                    "entryType": "stop_limit",
                    "qty": qty,
                    "entry": {"stop": stop, "limit": limit, "price": limit},
                    "bracket": br,
                    "notes": "A: ORB breakout + VWAP above (first hit in window)",
                })
                break  # その銘柄は1回だけ
    return out


def _gen_B(df_bars: pd.DataFrame, df_ind: pd.DataFrame, cfg: dict) -> list[dict]:
    """
    何をする関数？：
      - B：**AVWAP(9:30)±0.3%付近の反発**を「9:30–10:30 ET の全バー」から初回だけ拾い、Limitで出力。  :contentReference[oaicite:6]{index=6}
      具体条件：
        前足Close < 前足AVWAP かつ 今足Close ≥ 今足AVWAP かつ 乖離 ≤ 0.3%
    """
    allowed = _active_watchlist(cfg)  # 何をする行？：前夜のwatchlist（A/B）に載っている銘柄だけ許可。無ければ全件許可。  :contentReference[oaicite:3]{index=3}

    if df_bars.empty or df_ind.empty:
        return []
    out: list[dict] = []
    win_s, win_e = time(9, 30), time(10, 30)  # 勝負時間  :contentReference[oaicite:7]{index=7}

    for sym, g in df_bars.groupby("symbol", sort=False):
        if allowed and sym not in allowed:  # 何をする行？：ウォッチ外はスキップ（同日A/B混在を防ぐ運用ガード）。  :contentReference[oaicite:5]{index=5}
            continue

        g = g[(g["et"].dt.time >= win_s) & (g["et"].dt.time < win_e)].reset_index(drop=True)
        if len(g) < 2:
            continue
        for i in range(1, len(g)):
            prev_c = float(g.iloc[i - 1]["c"])
            prev_av = float(g.iloc[i - 1].get("avwap", float("nan")))
            now_c  = float(g.iloc[i]["c"])
            now_av = float(g.iloc[i].get("avwap", float("nan")))
            if not (math.isfinite(prev_av) and math.isfinite(now_av) and now_av > 0):
                continue
            near = abs(now_c - now_av) / now_av <= 0.003
            crossed = (prev_c < prev_av) and (now_c >= now_av)
            if crossed and near:
                price = _price_round(now_av)
                br = _mk_bracket(price, cfg)                # ブラケットは設定から  :contentReference[oaicite:8]{index=8}
                qty = _compute_qty(price, br["stopLossPrice"], cfg)  # 何をする行？：リスク％から数量を出す。  :contentReference[oaicite:8]{index=8}

                out.append({
                    "date": _today_str(),
                    "symbol": sym,
                    "setup": "B",
                    "entryType": "limit",
                    "qty": qty,
                    "entry": {"price": price},
                    "bracket": br,
                    "notes": "B: AVWAP(9:30) pullback bounce (first hit in window)",
                })
                break  # その銘柄は1回だけ
    return out


def _write_signals(signals: list[dict], out_dir: Path) -> list[Path]:
    """
    何をする関数？：
      - シグナルを 1ファイル=1JSON で書き出します（重複は簡易スキップ）。  :contentReference[oaicite:14]{index=14}
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for sig in signals:
        sym = sig["symbol"]
        setup = sig["setup"]
        entry_price = float(sig["entry"].get("price") or sig["entry"].get("limit") or 0.0)
        if entry_price and _already_exists(out_dir, setup, sym, entry_price):
            logger.info("skip duplicate signal: {} {}", setup, sym)
            continue
        ts = datetime.now(get_et_tz()).strftime("%H%M%S")
        p = out_dir / f"{_today_str()}__{setup}_{sym}_{ts}.json"
        p.write_bytes(orjson.dumps(sig, option=orjson.OPT_INDENT_2))
        paths.append(p)
    return paths

def main() -> int:
    """
    何をする関数？：
      - .env→ログ→config を読み、当日bars/indicatorsをもとに A/B シグナルJSONを data/signals/ に出力します。
      - 同日は A/B どちらか片方の運用が原則（config.strategy.active_setup を尊重、ただし ACTIVE_SETUP で一時上書き可）。
    使い方：
      poetry run python scripts/run_signals.py
    """
    load_dotenv_if_exists()
    logfile = configure_logging()
    cfg = load_config()

    # --- A/B の決定（環境変数で一時上書き可） ----------------------------
    setup_cfg = ((cfg.get("strategy") or {}).get("active_setup", "A")).upper()
    env_setup = (os.environ.get("ACTIVE_SETUP") or "").strip().upper()
    if env_setup in {"A", "B"} and env_setup != setup_cfg:
        logger.info("override setup: {} -> {} (ACTIVE_SETUP)", setup_cfg, env_setup)
        setup = env_setup
        # _active_watchlist() 側でも同じセットアップを見るよう、cfgにも反映
        cfg.setdefault("strategy", {})["active_setup"] = setup
    else:
        setup = setup_cfg

    # --- 入力の決定（今日 or フォールバック） ----------------------------
    from pathlib import Path
    import glob, re

    # 今日の想定パス
    p_bars_today, p_ind_today = _paths_for_today()
    have_today = p_bars_today.exists() and p_ind_today.exists()

    df_bars: pd.DataFrame
    df_ind: pd.DataFrame
    bars_path: str
    indicators_path: str

    if have_today:
        bars_path, indicators_path = str(p_bars_today), str(p_ind_today)
        df_bars = pd.read_parquet(bars_path)
        df_ind  = pd.read_parquet(indicators_path)
        logger.info(
            "signals inputs loaded: rows={} symbols={} (bars='{}', ind='{}')",
            len(df_bars), df_bars["symbol"].nunique() if not df_bars.empty else 0,
            os.path.basename(bars_path), os.path.basename(indicators_path),
        )
    else:
        allow_fb = (os.environ.get("ALLOW_BARS_FALLBACK", "1") != "0")
        if not allow_fb:
            logger.warning("inputs not ready (bars/indicators) and fallback disabled. compute_indicators を先に実行してください。")
            return 0

        # bars_1m_*.parquet を新しい順に見て、同日付の indicators_*.parquet がある最初の組を採用
        cands = sorted(glob.glob("data/bars/bars_1m_*.parquet"), key=os.path.getmtime, reverse=True)
        fb_bars, fb_ind = None, None
        for bp in cands:
            m = re.search(r"(\d{8})", bp)
            if not m:
                continue
            d = m.group(1)
            ip = f"data/bars/indicators_{d}.parquet"
            if os.path.exists(ip):
                fb_bars, fb_ind = bp, ip
                break

        if not (fb_bars and fb_ind):
            logger.warning("inputs not ready (bars/indicators). compute_indicators を先に実行してください。")
            return 0

        bars_path, indicators_path = fb_bars, fb_ind
        logger.warning(
            "inputs not found for today -> fallback to latest: {} , {}",
            os.path.basename(bars_path), os.path.basename(indicators_path),
        )
        df_bars = pd.read_parquet(bars_path)
        df_ind  = pd.read_parquet(indicators_path)
        logger.info(
            "signals inputs loaded: rows={} symbols={} (bars='{}', ind='{}')",
            len(df_bars), df_bars["symbol"].nunique() if not df_bars.empty else 0,
            os.path.basename(bars_path), os.path.basename(indicators_path),
        )

    # --- シグナル生成 ------------------------------------------------------
    out_dir = Path("data") / "signals"
    if setup == "A":
        signals = _gen_A(df_bars, df_ind, cfg)
    else:
        signals = _gen_B(df_bars, df_ind, cfg)

    paths = _write_signals(signals, out_dir)

    # 各シグナルの内容をINFOに
    for _sig in signals:
        entry = (_sig.get("entry") or {})
        br = (_sig.get("bracket") or {})
        logger.info(
            "signal: {} {} {} @ {} | qty={} | TP={} SL={}",
            _sig.get("date",""), _sig.get("setup",""), _sig.get("symbol",""),
            entry.get("price") or entry.get("limit") or entry.get("stop") or "",
            _sig.get("qty",""), br.get("takeProfitPrice",""), br.get("stopLossPrice",""),
        )

    logger.info("run_signals: {} file(s) written (logfile={})", len(paths), logfile)
    return 0





if __name__ == "__main__":
    raise SystemExit(main())
