#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 目的：
#   - Russell 2000 の銘柄CSV（symbolのみ）を出力する
#   - 同ユニバースから条件で絞った「小型株100銘柄」のCSVを出力する
# 入力：
#   - symbols.yml のグループ（例: russell2000）または --input で渡すCSV/TXT（代替）
#   - EOD_DIR にある eod_features_*.parquet / .csv（あれば価格・出来高・ATR%で絞込）
# 出力：
#   - configs/universe_russell2000.csv
#   - configs/universe_russell_smallcap100.csv

import os            # 環境変数（EOD_DIR）やパス結合に使用
import sys           # エラー終了に使用
import csv           # CSV入出力に使用
import argparse      # コマンド引数の処理に使用
from pathlib import Path  # ファイル・ディレクトリ操作に使用

# 何をするimportか：YAMLが無くても動くようにtry-import（YAML未導入の環境でもTXT/CSV入力で動作）
try:
    import yaml
except Exception:
    yaml = None

# 何をするimportか：pandasが無い環境でも“所属だけで出力”できるようにtry-import
try:
    import pandas as pd
except Exception:
    pd = None


def _safe_unique_upper(seq: list[str]) -> list[str]:
    # 何をする関数か：文字列リストを大文字化し、順序を保ったまま重複除去する
    out, seen = [], set()
    for s in seq:
        t = str(s).strip().upper()
        if t and t not in seen:
            seen.add(t); out.append(t)
    return out


def _read_yaml_group(symbols_file: str, group: str) -> list[str]:
    # 何をする関数か：symbols.yml から group のティッカー配列を読み出す（一般的な2形態に対応）
    if yaml is None:
        return []
    p = Path(symbols_file)
    if not p.exists():
        return []
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        return []
    if group in data:
        raw = data.get(group) or []
    else:
        raw = (data.get("groups") or {}).get(group) or []
    return _safe_unique_upper([x for x in raw if str(x).strip()])


def _read_input_txt_or_csv(path: str) -> list[str]:
    # 何をする関数か：TXT/CSV からティッカーを読み込む（CSVは 'symbol' または 'ticker' 列優先、無ければ先頭列）
    p = Path(path)
    if not p.exists():
        return []
    if p.suffix.lower() == ".csv":
        rows = list(csv.reader(p.read_text(encoding="utf-8").splitlines()))
        if not rows:
            return []
        header = [c.strip().lower() for c in rows[0]]
        has_header = any(h.isalpha() for h in header)
        data_rows = rows[1:] if has_header else rows
        col_idx = 0
        for cand in ("symbol", "ticker"):
            if cand in header:
                col_idx = header.index(cand); break
        vals = []
        for row in data_rows:
            if not row:
                continue
            s = (row[col_idx] if col_idx < len(row) else "").strip()
            if not s or s.startswith("#"):
                continue
            vals.append(s)
        return _safe_unique_upper(vals)
    else:
        vals = []
        for line in p.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            vals.append(s)
        return _safe_unique_upper(vals)

def _fetch_russell2000_from_ishares() -> list[str]:
    # 何をする関数か：iShares IWM の保有銘柄CSVをダウンロードし、Ticker列からティッカーを抽出して返す
    import urllib.request  # この関数内だけで使う：HTTPダウンロードに使用
    import io              # この関数内だけで使う：メモリ上でテキスト化するために使用
    url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = resp.read().decode("utf-8-sig", errors="ignore")  # 何をする行？：BOM/文字化けに備えてデコード
        rows = list(csv.reader(io.StringIO(data)))  # 何をする行？：CSVとして読み込む（ヘッダ有無どちらにも対応）
        if not rows:
            return []
        header = [c.strip().lower() for c in rows[0]]
        has_header = any(h.isalpha() for h in header)
        data_rows = rows[1:] if has_header else rows
        # 何をする行？：Ticker列の候補を探す（無ければ先頭列）
        col_idx = 0
        for cand in ("ticker", "ticker symbol", "symbol"):
            if cand in header:
                col_idx = header.index(cand); break
        vals: list[str] = []
        for row in data_rows:
            if not row:
                continue
            s = (row[col_idx] if col_idx < len(row) else "").strip()
            if not s or s.startswith("#"):
                continue
            vals.append(s)
        return _safe_unique_upper(vals)  # 何をする行？：大文字化＋重複除去
    except Exception:
        return []  # 何をする行？：失敗時は空（呼び出し側でエラー処理）

def _latest_eod_file(eod_dir: str) -> Path | None:
    # 何をする関数か：EOD_DIR から最新の eod_features_*.parquet / .csv を見つける
    d = Path(eod_dir)
    cands = list(d.glob("eod_features_*.parquet")) + list(d.glob("eod_features_*.csv"))
    if not cands:
        return None
    return max(cands, key=lambda p: p.stat().st_mtime)


def _load_eod_df(path: Path):
    # 何をする関数か：EOD特徴量をDataFrameで読み込む（pandasが無ければ None を返す）
    if pd is None or path is None:
        return None
    try:
        if path.suffix == ".parquet":
            return pd.read_parquet(path)
        else:
            return pd.read_csv(path)
    except Exception:
        return None


def _pick_col(df, candidates: list[str]) -> str | None:
    # 何をする関数か：複数候補の中から存在する列名を返す（無ければNone）
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _filter_smallcap(df, universe: list[str], min_price: float, max_price: float,
                     min_dollar_vol: float, min_atr_pct: float, limit: int) -> list[str]:
    # 何をする関数か：
    #   - EODがあれば：価格帯・ドル出来高・ATR% で絞り、ドル出来高の降順で上位 limit 件を返す
    #   - EODが無ければ：ユニバースの先頭から limit 件を返す（所属のみ）
    if not universe:
        return []
    if df is None:
        return universe[:limit]

    # シンボル列を推定
    sym_col = _pick_col(df, ["symbol", "ticker", "Symbol", "Ticker", "sym"])
    if sym_col is None:
        return universe[:limit]

    sub = df[df[sym_col].str.upper().isin(universe)].copy()

    price_col = _pick_col(sub, ["close", "Close", "c", "adj_close"])
    vol_col   = _pick_col(sub, ["volume", "Volume", "v", "avg_volume20", "avg_vol20"])
    atr_col   = _pick_col(sub, ["atr14", "ATR14", "atr", "ATR"])

    # 価格帯フィルタ
    if price_col is not None:
        if min_price is not None:
            sub = sub[sub[price_col] >= float(min_price)]
        if max_price is not None:
            sub = sub[sub[price_col] <= float(max_price)]

    # ドル出来高（price×volume）フィルタ
    if price_col is not None and vol_col is not None and min_dollar_vol is not None:
        sub = sub[(sub[price_col] * sub[vol_col]) >= float(min_dollar_vol)]

    # ATR% フィルタ（ATR/close×100）
    if price_col is not None and atr_col is not None and min_atr_pct is not None:
        atr_pct = (sub[atr_col] / sub[price_col]) * 100.0
        sub = sub[atr_pct >= float(min_atr_pct)]

    # 並び順（ドル出来高があれば降順、無ければ価格降順、無ければシンボル昇順）
    if price_col is not None and vol_col is not None:
        sub = sub.assign(_dv=sub[price_col] * sub[vol_col]).sort_values("_dv", ascending=False)
    elif price_col is not None:
        sub = sub.sort_values(price_col, ascending=False)
    else:
        sub = sub.sort_values(sym_col)

    picked = [str(s).strip().upper() for s in sub[sym_col].tolist() if str(s).strip()]
    return picked[:limit] if picked else universe[:limit]


def _write_csv_symbols(path: str, symbols: list[str]) -> None:
    # 何をする関数か：symbol列だけのCSVを書き出す（UTF-8、ヘッダあり）
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol"])
        for s in symbols:
            w.writerow([s])


def main():
    # 何をする関数か：
    #   - ユニバース（russell2000）を取得（YAML or 入力CSV/TXT）
    #   - 全体CSVを書き出し
    #   - EODがあれば条件で小型株を抽出し、100件CSVを書き出し
    ap = argparse.ArgumentParser(description="Export Russell2000 universe and SmallCap100 CSVs.")
    ap.add_argument("--group", default="russell2000", help="symbols.ymlのグループ名（例：russell2000）")
    ap.add_argument("--symbols-file", default="configs/symbols.yml", help="ユニバース定義のYAMLパス")
    ap.add_argument("--fetch", choices=["iwm"], default="", help="オンラインでユニバース取得：'iwm' = iShares IWM holdings（指定時のみ使用）")

    ap.add_argument("--input", default="", help="YAMLが空のときの代替入力（CSV/TXTのパス）")
    ap.add_argument("--out-r2000", default="configs/universe_russell2000.csv", help="Russell2000全体を書き出すCSVパス")
    ap.add_argument("--out-smallcap", default="configs/universe_russell_smallcap100.csv", help="小型株100を書き出すCSVパス")
    ap.add_argument("--limit", type=int, default=100, help="小型株の件数（既定100）")
    ap.add_argument("--min-price", type=float, default=2.0, help="最低株価（close、既定2）")
    ap.add_argument("--max-price", type=float, default=100.0, help="最高株価（close、既定100）")
    ap.add_argument("--min-dollar-vol", type=float, default=1.5e7, help="最低ドル出来高（close×volume、既定1500万）")
    ap.add_argument("--min-atr-pct", type=float, default=1.0, help="最低ATR%%（ATR/close×100、既定1.0）")
    ap.add_argument("--eod-dir", default=os.environ.get("EOD_DIR", "data/eod"), help="EOD特徴量の保存ディレクトリ")
    args = ap.parse_args()

    # 1) ユニバースの取得（YAML→代替入力の順）
    universe = _read_yaml_group(args.symbols_file, args.group) if yaml is not None else []
    if not universe and args.fetch == "iwm":
        universe = _fetch_russell2000_from_ishares()  # 何をする行？：YAML/入力が空ならオンラインから取得
    if not universe:
        print(f"[ERROR] universe is empty (group={args.group}, input={args.input or '-'}, fetch={args.fetch or '-'})", file=sys.stderr); sys.exit(2)


    universe = _safe_unique_upper(universe)
    _write_csv_symbols(args.out_r2000, universe)
    print(f"[INFO] wrote {len(universe)} symbols -> {args.out_r2000}")

    # 2) EODから条件で小型株を抽出（無ければ所属だけで先頭N件）
    latest = _latest_eod_file(args.eod_dir)
    df = _load_eod_df(latest) if latest else None
    smallcap = _filter_smallcap(
        df, universe,
        args.min_price, args.max_price,
        args.min_dollar_vol, args.min_atr_pct,
        args.limit
    )
    _write_csv_symbols(args.out_smallcap, smallcap)
    print(f"[INFO] wrote {len(smallcap)} symbols -> {args.out_smallcap}")


if __name__ == "__main__":
    main()
