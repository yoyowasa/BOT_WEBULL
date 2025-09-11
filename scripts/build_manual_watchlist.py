#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 目的：sp500/nasdaq/nyse のユニバースから、価格帯・ドル出来高・ATR%で絞って
#       configs/manual_watchlist.txt を自動生成するワンショットツール

import os
import sys
from pathlib import Path  # ファイル探索と入出力に使う
import argparse           # コマンドライン引数の受け取りに使う
import pandas as pd       # EOD特徴量の読み込みとフィルタに使う

try:
    import yaml           # symbols.yml からユニバースを読むために使う
except Exception:
    yaml = None           # 未インストールでも動く（TXTフォールバック有効）

def _read_yaml_symbols(symbols_file: str, group: str) -> list[str]:
    # 何をする関数か：symbols.yml から group のティッカー配列を読み出す（無ければ空リスト）
    if yaml is None:
        return []
    p = Path(symbols_file)
    if not p.exists():
        return []
    data = yaml.safe_load(p.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        return []
    # よくある2形態に対応：{"sp500":[...]} または {"groups":{"sp500":[...]}}
    if group in data:
        arr = data.get(group) or []
    else:
        groups = data.get('groups', {}) or {}
        arr = groups.get(group, []) or []
    return [str(s).strip().upper() for s in arr if str(s).strip()]

def _read_txt_universe(group: str) -> list[str]:
    # 何をする関数か：configs/universe_{group}.txt からティッカーを読み出す（無ければ空）
    p = Path(f"configs/universe_{group}.txt")
    if not p.exists():
        return []
    out: list[str] = []
    for line in p.read_text(encoding='utf-8').splitlines():
        s = line.strip().upper()
        if not s or s.startswith('#'):
            continue
        out.append(s)
    return out

def _unique(seq: list[str]) -> list[str]:
    # 何をする関数か：重複を削除（順序は維持）
    seen = set()
    out: list[str] = []
    for s in seq:
        if s not in seen:
            out.append(s); seen.add(s)
    return out

def _latest_eod_file(eod_dir: str) -> Path | None:
    # 何をする関数か：最新の eod_features_YYYYMMDD.(parquet|csv) を探す
    d = Path(eod_dir)
    cands = list(d.glob("eod_features_*.parquet")) + list(d.glob("eod_features_*.csv"))
    if not cands:
        return None
    return max(cands, key=lambda p: p.stat().st_mtime)  # 最終更新が最新のもの

def _load_eod_df(path: Path) -> pd.DataFrame:
    # 何をする関数か：EOD特徴量を読み込む（parquet/csv どちらでも可）
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)

def _pick_symbol_col(df: pd.DataFrame) -> str | None:
    # 何をする関数か：シンボル列名を推定（存在しなければ None）
    for c in ["symbol","ticker","Symbol","Ticker","sym"]:
        if c in df.columns:
            return c
    return None

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    # 何をする関数か：候補名のうち最初に見つかった列名を返す
    for n in candidates:
        if n in df.columns:
            return n
    return None

def _apply_filters(df: pd.DataFrame, tickers: list[str],
                   min_price: float|None, max_price: float|None,
                   min_dollar_vol: float|None, min_atr_pct: float|None) -> pd.DataFrame:
    # 何をする関数か：価格帯・ドル出来高・ATR% の各しきい値でフィルタする
    sym_col = _pick_symbol_col(df)
    if sym_col is None:
        # 列名が見つからない場合は、ティッカーだけ返すための最小DFを作る
        return pd.DataFrame({ "symbol": tickers })[["symbol"]]

    sub = df[df[sym_col].str.upper().isin([s.upper() for s in tickers])].copy()

    price_col = _find_col(sub, ["close","Close","c","adj_close"])
    vol_col   = _find_col(sub, ["volume","Volume","v","avg_volume20","avg_vol20"])
    atr_col   = _find_col(sub, ["atr14","atr","ATR14","ATR"])

    if price_col is not None:
        if min_price is not None:
            sub = sub[sub[price_col] >= float(min_price)]
        if max_price is not None:
            sub = sub[sub[price_col] <= float(max_price)]

    # ドル出来高 = close × volume（volume系が無い場合はこの条件はスキップ）
    if price_col is not None and vol_col is not None and min_dollar_vol is not None:
        sub = sub[(sub[price_col] * sub[vol_col]) >= float(min_dollar_vol)]

    # ATR% = ATR / close × 100（列が無い場合はスキップ）
    if price_col is not None and atr_col is not None and min_atr_pct is not None:
        atr_pct = (sub[atr_col] / sub[price_col]) * 100.0
        sub = sub[atr_pct >= float(min_atr_pct)]

    # 並べ替え：ドル出来高があれば降順、無ければ価格降順、無ければシンボル昇順
    if price_col is not None and vol_col is not None:
        sub = sub.assign(_dv=sub[price_col]*sub[vol_col]).sort_values("_dv", ascending=False)
    elif price_col is not None:
        sub = sub.sort_values(price_col, ascending=False)
    else:
        sub = sub.sort_values(sym_col)

    return sub[[sym_col]]

def main():
    # 何をする関数か：ユニバース結合→EODでフィルタ→manual_watchlist.txt を出力する
    ap = argparse.ArgumentParser(description="Build manual watchlist from universe groups.")
    ap.add_argument("--universe", default="sp500+nasdaq+nyse",
                    help="例: sp500 / nasdaq / nyse / sp500+nasdaq など '+' で結合")
    ap.add_argument("--limit", type=int, default=30, help="上位から何件を書き出すか")
    ap.add_argument("--min-price", type=float, default=5.0, help="最低株価（close）")
    ap.add_argument("--max-price", type=float, default=500.0, help="最高株価（close）")
    ap.add_argument("--min-dollar-vol", type=float, default=3e7, help="最低ドル出来高（close×volume）")
    ap.add_argument("--min-atr-pct", type=float, default=1.0, help="最低ATR%%（ATR/close×100）")
    ap.add_argument("--symbols-file", default="configs/symbols.yml", help="ユニバース定義のYAML")
    ap.add_argument("--output", default="configs/manual_watchlist.txt", help="書き出し先TXT")
    ap.add_argument("--eod-dir", default=os.environ.get("EOD_DIR","data/eod"), help="EODファイルの場所")
    args = ap.parse_args()

    # 1) ユニバースを読み込む（YAMLが空のときは configs/universe_{name}.txt に自動フォールバック）
    groups = [g.strip() for g in args.universe.split("+") if g.strip()]
    all_syms: list[str] = []
    for g in groups:
        syms = _read_yaml_symbols(args.symbols_file, g)
        if not syms:
            alt = _read_txt_universe(g)
            if not alt:
                print(f"[WARN] group '{g}' is empty in YAML and no configs/universe_{g}.txt found.", file=sys.stderr)
            syms = alt
        all_syms.extend(syms)
    all_syms = _unique([s for s in all_syms if s])

    if not all_syms:
        print("[ERROR] universe is empty. Provide symbols.yml groups or configs/universe_*.txt.", file=sys.stderr)
        sys.exit(2)

    # 2) 最新のEODを見つけて、可能ならフィルタを適用（列が無い条件は自動スキップ）
    latest = _latest_eod_file(args.eod_dir)
    if latest is None:
        print(f"[WARN] no eod_features files under {args.eod_dir}. Output by membership only.", file=sys.stderr)
        picked = all_syms[:args.limit]
    else:
        try:
            df = _load_eod_df(latest)
            sub = _apply_filters(
                df, all_syms,
                args.min_price, args.max_price,
                args.min_dollar_vol, args.min_atr_pct
            )
            picked = [str(s) for s in sub.iloc[:args.limit, 0].tolist()]
        except Exception as e:
            print(f"[WARN] failed to apply filters ({type(e).__name__}): {e}. Output by membership only.", file=sys.stderr)
            picked = all_syms[:args.limit]

    # 3) TXTに書き出し（先頭に使用条件をコメントとして残す）
    out = Path(args.output)
    lines: list[str] = []
    lines.append("# manual_watchlist generated by build_manual_watchlist.py")
    lines.append(f"# universe={args.universe} limit={args.limit} "
                 f"min_price={args.min_price} max_price={args.max_price} "
                 f"min_dollar_vol={args.min_dollar_vol} min_atr_pct={args.min_atr_pct}")
    lines.extend(picked)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[INFO] wrote {len(picked)} symbols -> {out}")

if __name__ == "__main__":
    main()
