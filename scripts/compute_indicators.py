# WSで保存した NDJSON（bars）から、1分バー＋VWAP＋AVWAP(9:30)＋ORB(5m) を計算して data/bars/ に保存します。
# 役割は Runbookの compute_indicators（ORB/VWAP/AVWAP）と一致させています。  :contentReference[oaicite:4]{index=4}

from __future__ import annotations
from pathlib import Path           # 入出力パス操作
from datetime import datetime, timezone, time
import pandas as pd                # 集計と指標計算に使う
from loguru import logger          # ログ（共通ルールで data/logs/bot.log へ）
import os        # 何をする行？：ファイル存在確認・パス操作（fallbackで使用）
import glob      # 何をする行？：bars_*.ndjson を列挙するため（fallbackで使用）
import json  # 何をする行？：NDJSONを読む入口（json.loads）をフックするために使う標準ライブラリ。
_original_json_loads = json.loads  # 何をする行？：本物の loads を退避。後でラッパーから必ず参照するため。

from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists  # .env を先に読む（API/設定）  :contentReference[oaicite:5]{index=5}
from rh_pdc_daytrade.utils.logutil import configure_logging       # ログ初期化（冪等）
from rh_pdc_daytrade.utils.configutil import load_config          # config.yaml のロード
from rh_pdc_daytrade.utils.timeutil import get_et_tz              # ET日付の決定（tzdata+フォールバック）  :contentReference[oaicite:6]{index=6}
from rh_pdc_daytrade.utils.io import write_parquet, write_csv     # Parquet/CSVの標準保存口  :contentReference[oaicite:7]{index=7}

_original_read_json = pd.read_json  # 何をする行？：元の pandas.read_json を退避。以後のラッパーから“本物”を確実に呼べるようにする。

def _to_epoch_seconds(ts):
    """この関数は、どんな時刻形式（文字列/秒/ms/ns/datetime）でもUTCのエポック秒(int)にそろえるための関数です。"""
    if isinstance(ts, (int, float)):  # 数値なら桁で秒/ミリ/ナノを判定
        s = int(ts)
        if s > 1_000_000_000_000_000_000:  # ナノ秒(≈19桁) → 秒
            return s // 1_000_000_000
        if s > 1_000_000_000_000:          # ミリ秒(≈13桁) → 秒
            return s // 1_000
        return s                             # すでに秒
    if isinstance(ts, str):                  # 文字列（例: '2025-08-28T09:02:00Z'）
        iso = ts.replace('Z', '+00:00')      # 'Z' は +00:00 と等価にして fromisoformat で読める形に
        return int(datetime.fromisoformat(iso).timestamp())
    # datetime 等：UTC で timestamp → 秒
    return int(ts.replace(tzinfo=timezone.utc).timestamp())

def _bars_ndjson_path(channel: str = "bars") -> Path:
    """何をする関数？：ET日付の NDJSON（bars_YYYYMMDD.ndjson）のパスを返します。"""
    et_date = datetime.now(get_et_tz()).strftime("%Y%m%d")  # 何をする行？：入力NDJSONは“いまのET日付”で探す（ここではdf_1mは使わない）


    stream_dir = Path(os.environ.get("STREAM_DIR", str(Path("data") / "stream")))  # 何をする行？：barsの参照先を環境変数で一元化（未設定はリポ内 data/stream）
    return Path(stream_dir) / f"{channel}_{et_date}.ndjson"  # 何をする行？：当日のNDJSONファイルのフルパスを返す


def _read_bars_ndjson(p: Path, symbols: list[str]) -> pd.DataFrame:
    """
    何をする関数？：
      - NDJSON（1行=1メッセージ）を読み、必要なキー（S,t,o,h,l,c,v）だけを取り出して DataFrame にします。
      - 指定の symbols に含まれるものだけに絞ります。ファイルが無ければ空DataFrameを返します。
    """
    import orjson  # この関数内でのみ使う高速JSON
    if not p.exists():
# 何をする行？：環境変数でフォールバックの有効/無効を切替（本番で前日データ誤参照を防ぐ）
        _allow_fb = os.environ.get("ALLOW_BARS_FALLBACK", "1").lower()
        if _allow_fb not in ("1", "true", "yes", "on"):
            logger.warning(f"bars ndjson not found: {p} (fallback disabled)")  # 何をする行？：無効化されていることを明示
            return pd.DataFrame(columns=["symbol", "et", "o", "h", "l", "c", "v"])  # 何をする行？：空DFで正常終了（後段はスキップ）

        # 何をする行？：今日のbarsが無いとき、候補ディレクトリから“最新bars”を探して p を差し替える
        search_dirs = [Path("data") / "stream"]  # 何をする行？：通常の保存先
        ext_dir = os.environ.get("STREAM_DIR", r"E:\data\stream")  # 何をする行？：外部保存先（環境変数優先、無ければE:\data\stream）
        if ext_dir:
            search_dirs.append(Path(ext_dir))

        candidates = []
        for root in search_dirs:
            if root and root.exists():
                # 何をする行？：bars_*.ndjson を更新時刻の新しい順に集める
                candidates.extend(sorted(root.glob("bars_*.ndjson"),
                                        key=lambda q: q.stat().st_mtime,
                                        reverse=True))

        if candidates:
            logger.warning(f"bars ndjson not found: {p} -> fallback to latest: {candidates[0]}")  # 何をする行？：切替先をログに出す
            p = candidates[0]  # 何をする行？：ここが肝心。以降はこの実在ファイルを読む
        else:
            logger.warning(f"bars ndjson not found: {p}")  # 何をする行？：見つからなかったことを記録
            logger.warning("no bars to compute (searched: {})".format(", ".join(str(x) for x in search_dirs)))  # 何をする行？：探した場所も記録
            return pd.DataFrame(columns=["symbol", "et", "o", "h", "l", "c", "v"])  # 何をする行？：空DFで正常終了（後段はスキップ）



    def _parse_ts(val):
        # 何をする関数？：Alpacaの 't' を ns/us/ms/s の数値 or ISO文字列('...Z') どちらでも
        # 安全に「ETタイムスタンプ(pd.Timestamp, tz=America/New_York)」へ変換する。
        tz = get_et_tz()

        # 1) 数値エポック（桁数で単位を推定）
        if isinstance(val, (int, float)):
            v = int(val)
            # 10^18台=ns, 10^15台=us, 10^12台=ms, それ以外=s
            if v >= 1_000_000_000_000_000_000:
                dt = pd.to_datetime(v, unit="ns", utc=True)
            elif v >= 1_000_000_000_000_000:
                dt = pd.to_datetime(v, unit="us", utc=True)
            elif v >= 1_000_000_000_000:
                dt = pd.to_datetime(v, unit="ms", utc=True)
            else:
                dt = pd.to_datetime(v, unit="s", utc=True)
            return dt.tz_convert(tz)

        # 2) ISO文字列（例：'2025-08-28T09:02:00Z'）
        if isinstance(val, str):
            s = val.strip().replace("Z", "+00:00")  # 'Z' を UTC オフセットに正規化
            try:
                dt = pd.to_datetime(s, utc=True)
                return dt.tz_convert(tz)
            except Exception:
                # 予備策：文字列中の数字だけを拾って再判定（ログに載せるほどではないので黙ってNaT可）
                digits = "".join(ch for ch in s if ch.isdigit())
                if digits:
                    try:
                        return _parse_ts(int(digits))  # 再帰で数値ルートへ
                    except Exception:
                        return pd.NaT
                return pd.NaT

        # 3) それ以外は欠損扱い
        return pd.NaT




    logger.info(f"reading bars ndjson: {p}")  # 何をする行？：実際に読み込むbarsファイルのフルパスをログに出して原因切り分けを容易にする

    rows = []
    with open(p, "rb") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                m = orjson.loads(line)
            except Exception:
                continue
            # 何をする行？：JSON1件が辞書かを確認した上で、IEXの"T"か"type"のどちらかを取り、bar（b/ bar）だけを通す。
            tmark = (m.get("T") or m.get("type"))
            if not isinstance(m, dict) or tmark not in ("b", "bar"): 
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
    et_date = df_1m["et"].dt.date.min().strftime("%Y%m%d")  # 何をする行？：保存ファイルの日付を“実際に読み込んだバーのET日付”に合わせる
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

    def _norm_ns(x):
        # 何をする関数？：'t' が数値/文字列どちらでも、UTCの“nsエポック整数”に正規化する。
        try:
            if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit()):
                return int(x)  # 既に数値ならそのまま
            return int(pd.to_datetime(x, utc=True).value)  # '2025-08-28T09:02:00Z' → ns
        except Exception:
            return None  # 変換できないものは欠損（後段でdropされても落ちない）

    def _json_loads_patch(s, *a, **k):
        # 何をする関数？：json.loads の結果（辞書）に 't' があれば、必ず nsエポック整数に直す“入口ガード”。
        obj = _original_json_loads(s, *a, **k)
        if isinstance(obj, dict) and "t" in obj:
            obj["t"] = _norm_ns(obj["t"])
        return obj

    json.loads = _json_loads_patch  # 何をする行？：このプロセス中だけ入口を“安全版”に差し替え（他ファイル不変）。

    # 何をする行？：どの読み込み経路（json/orjson/pandas）でも 't' を“ISO/整数どちらでも”nsエポック整数へ正規化する“入口フック”。
    try:
        import json as _json                     # 何をする行？：NDJSONを標準jsonで読む経路をフックするため。
        import orjson as _orjson                 # 何をする行？：orjsonで読む経路をフックするため。
        import pandas as _pd                     # 何をする行？：pd.read_json経路も安全化するため。
        from pandas import to_datetime as _to_dt # 何をする行？：ISO文字列→UTCのnsエポックへ変換するため。

        def _norm_ns(x):
            # 何をする関数？：'t' が数値/文字列どちらでも nsエポック整数に統一する。
            try:
                if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit()):
                    return int(x)  # 既に数値ならそのまま
                return int(_to_dt(x, utc=True).value)  # 例：'2025-08-28T09:02:00Z' → nsエポック
            except Exception:
                return None  # 変換不能は欠損（後段でdrop可）

        # --- json.loads をフック（行ごと loads する実装向け） ---
        _orig_json_loads = _json.loads
        def _json_loads_patch(s, *a, **k):
            obj = _orig_json_loads(s, *a, **k)
            if isinstance(obj, dict) and "t" in obj:
                try: obj["t"] = _norm_ns(obj["t"])
                except Exception: pass
            return obj
        _json.loads = _json_loads_patch

        # --- orjson.loads をフック（orjson 経路でも同様に正規化） ---
        _orig_orjson_loads = _orjson.loads
        def _orjson_loads_patch(s):
            obj = _orig_orjson_loads(s)
            if isinstance(obj, dict) and "t" in obj:
                try: obj["t"] = _norm_ns(obj["t"])
                except Exception: pass
            return obj
        _orjson.loads = _orjson_loads_patch

        # --- pandas.read_json をフック（lines=True で読む経路向け） ---
        _orig_pd_read_json = _pd.read_json
        def _pd_read_json_patch(*a, **k):
            df = _orig_pd_read_json(*a, **k)
            try:
                if hasattr(df, "columns") and "t" in df.columns:
                    df["t"] = df["t"].map(_norm_ns)
                    df = df.dropna(subset=["t"]).astype({"t": "int64"})  # 以降の計算を安定化
            except Exception:
                pass
            return df
        _pd.read_json = _pd_read_json_patch
    except Exception:
        # 何をする行？：環境差でフックに失敗しても本処理は継続（ログは main 側で出る）。
        pass

# 何をする行？：保険として“この後に読み込むどのDataFrameにも” t があれば再正規化しておく。
    def _coerce_t_inplace(df): 
        if hasattr(df, "columns") and "t" in df.columns: df["t"] = df["t"].map(_to_ns_epoch).dropna().astype("int64")


    # 何をする行？：IEXバーの 't' を“ISO/整数どちらでも”nsエポック整数に統一する安全パッチ（このプロセス限定）。
    def _to_ns_epoch(x):
        try:
            if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit()):
                return int(x)  # 既に整数ならそのまま
            return int(pd.to_datetime(x, utc=True).value)  # '2025-08-28T09:02:00Z' → nsエポック
        except Exception:
            return None  # 変換不能は欠損（後段でdrop）

    def _read_json_with_t_fix(*args, **kwargs):
        df = _original_read_json(*args, **kwargs)  # 何をする行？：まず通常どおり読み込む（退避しておいた“本物”を使用）。
        _coerce_t_inplace(df)                      # 何をする行？：t列をISO/整数どちらでもnsエポック整数に正規化（呼び出し漏れを根絶）。
        return df                                  # 何をする行？：以後の指標計算が同じ前提で進むように、正規化後のdfを返す。



    pd.read_json = _read_json_with_t_fix  # 何をする行？：このスクリプト内だけ read_json を安全版に差し替え

    # ウォッチ対象（ws_run と同じく watchlist を優先・無ければ全件許容）
    # ここでは NDJSON 内のシンボルで自動的に絞られるため、空でもOK。
    ndjson_path = _bars_ndjson_path("bars")
    # symbols は空にして「ファイル内の全銘柄」を対象に（将来は cfg のA/Bに合わせて渡せます）
    df = _read_bars_ndjson(ndjson_path, symbols=[])
    logger.info(f"bars loaded: rows={len(df)} symbols={(0 if df.empty else df['symbol'].nunique())}")  # 何をする行？：読み込んだ行数と銘柄数を表示して“受信不足”をすぐ判定できるようにする


    if df.empty:
        return 0  # 市場時間外は bars が0でも正常（Runbookの想定）  :contentReference[oaicite:12]{index=12}

    df = _compute_vwap(df)
    df = _compute_avwap(df, anchor=cfg.get("strategy", {}).get("avwap_anchor", "09:30:00"))
    orb = _compute_orb_5m(df)
    p1, p2 = _save_outputs(df, orb)
    logger.info("indicators saved: {} , {}", p1, p2)
    return 0


if __name__ == "__main__":
    # 何をする行？：どんな例外でも“ログを残して成功終了(0)”にする安全弁。
    # こうしておくと、市場時間外や入力欠如でもパイプライン（signals/place_orders）を止めません。:contentReference[oaicite:1]{index=1}
    import sys, traceback  # 何をする行？：例外の詳細をログに残すためだけに、ここで限定import。

    try:
        code = main()  # 何をする行？：通常フローを実行。
    except FileNotFoundError as e:
        # 何をする行？：バー未生成やNDJSON不在など“想定内の欠落”は警告だけ出して成功扱い。
        from loguru import logger
        logger.warning("compute_indicators: missing input {}; treat as outside-regular-hours and skip.", getattr(e, "filename", ""))
        sys.exit(0)
    except Exception as e:
        # 何をする行？：その他の例外も“運用継続”を優先し、エラー内容を記録して成功終了。
        from loguru import logger
        logger.error("compute_indicators: non-fatal error: {}", e)
        logger.debug("stack:\n{}", traceback.format_exc())
        sys.exit(0)
    else:
        # 何をする行？：main() が正常に返した場合は、そのコードで終了（通常は 0）。
        raise SystemExit(code)

