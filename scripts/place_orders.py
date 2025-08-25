# signals/*.json を読み、「紙トレ（ログ記録のみ）」で処理し、処理済みを signals/sent/ へ移動します。
# 後でWebull公式SDKに差し替える場合でも、この“箱”のI/O（読み出し・移動）はそのまま使えます。 :contentReference[oaicite:3]{index=3}

from __future__ import annotations
from pathlib import Path                  # 入出力パス操作（signals/ 配下の移動に使う）
from datetime import datetime             # ET時刻の記録（ログやファイル名用）
import os                                 # RUN_MODEの参照（paper/live）
import orjson                             # JSONの高速読込
from loguru import logger                 # 共通ログ（data/logs/bot.logへ集約）

from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists  # 何をする関数？：.envを先に読む  :contentReference[oaicite:4]{index=4}
from rh_pdc_daytrade.utils.logutil import configure_logging       # 何をする関数？：ログ初期化（冪等）  :contentReference[oaicite:5]{index=5}
from rh_pdc_daytrade.utils.configutil import load_config          # 何をする関数？：config.yamlを読む（RUN_MODE等）  :contentReference[oaicite:6]{index=6}
from rh_pdc_daytrade.utils.timeutil import get_et_tz             # 何をする関数？：ETのtzinfoを得る（フォールバック付）  :contentReference[oaicite:7]{index=7}

def _dirs() -> tuple[Path, Path, Path]:
    """
    何をする関数？：
      - signals/（未処理）, signals/sent/（処理済み）, signals/failed/（失敗）の各フォルダPathを返します。
    """
    base = Path("data") / "signals"
    sent = base / "sent"
    failed = base / "failed"
    base.mkdir(parents=True, exist_ok=True)
    sent.mkdir(parents=True, exist_ok=True)
    failed.mkdir(parents=True, exist_ok=True)
    return base, sent, failed

def _exec_log_path() -> Path:
    """
    何をする関数？：
      - 約定ログCSV（data/logs/executions.csv）のパスを返し、
        1) 無ければフォルダ作成＋ヘッダー行を書きます。
        2) 既にあっても列が古い場合（qtyが無い等）は、安全に“列を増やして”ヘッダーを更新します。  
    """
    import csv  # この関数内だけで使うため関数内インポートにします
    p = Path("data") / "logs" / "executions.csv"
    p.parent.mkdir(parents=True, exist_ok=True)

    new_cols = [
        "date", "timestamp_et", "symbol", "setup",
        "entry_type", "qty", "entry_price", "tp_price", "sl_price", "notes"
    ]

    if not p.exists():
        with open(p, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(new_cols)
        return p

    # 既存ファイルがある場合はヘッダーを確認し、足りない列を補って並びも揃え直します（後方互換のため）。
    with open(p, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))

    if not rows:
        with open(p, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(new_cols)
        return p

    old_cols = rows[0]
    if old_cols == new_cols:
        return p

    # マッピングを作り、無い列は空欄で埋めて新しい並びに書き換えます（安全に“列追加”）。  :contentReference[oaicite:4]{index=4}
    index_of = {name: (old_cols.index(name) if name in old_cols else None) for name in new_cols}
    tmp = p.with_suffix(".csv.tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(new_cols)
        for row in rows[1:]:
            out = []
            for name in new_cols:
                idx = index_of[name]
                out.append(row[idx] if (idx is not None and idx < len(row)) else "")
            w.writerow(out)
    tmp.replace(p)
    return p

def _append_strategy_entry(sig: dict) -> Path:
    """
    何をする関数？：
      - 紙トレの“エントリー明細”を data/logs/strategy.csv に1行追記します（EXIT/スリッページ等は将来拡張）。  :contentReference[oaicite:4]{index=4}
    """
    import csv  # この関数内だけで使うため関数内インポートにします
    p = _strategy_log_path()
    ts = datetime.now(get_et_tz())
    entry = sig.get("entry", {}) or {}
    br = sig.get("bracket", {}) or {}
    qty = sig.get("qty", "")
    with open(p, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            ts.strftime("%Y%m%d"),
            ts.strftime("%H:%M:%S"),
            sig.get("symbol", ""),
            sig.get("setup", ""),
            sig.get("entryType", ""),
            entry.get("price") or entry.get("limit") or entry.get("stop") or "",
            qty,
            br.get("takeProfitPrice", ""),
            br.get("stopLossPrice", ""),
            sig.get("notes", ""),
            "", "", "", "", ""  # exit_time/exit_price/R/slippage/spread は今は空（将来更新）  :contentReference[oaicite:5]{index=5}
        ])
    return p

def _strategy_log_path() -> Path:
    """
    何をする関数？：
      - トレード明細ログCSV（data/logs/strategy.csv）のパスを返し、無ければヘッダーを作ります。
      - 列はRunbook準拠（ENTRY中心・EXIT等は将来拡張）。  :contentReference[oaicite:2]{index=2}
    """
    import csv  # この関数内だけで使うため関数内インポートにします
    p = Path("data") / "logs" / "strategy.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "date", "entry_time_et", "symbol", "setup",
        "entry_type", "entry_price", "qty",
        "tp_price", "sl_price", "notes",
        "exit_time_et", "exit_price", "R", "slippage_pct", "spread_pct"
    ]
    if not p.exists():
        with open(p, "w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow(cols)
    else:
        # 既存に列不足があれば“列を増やして”ヘッダーを揃えます（後方互換）。  :contentReference[oaicite:3]{index=3}
        with open(p, "r", encoding="utf-8", newline="") as f:
            rows = list(csv.reader(f))
        if not rows or rows[0] != cols:
            index_of = {name: (rows[0].index(name) if rows and name in rows[0] else None) for name in cols}
            tmp = p.with_suffix(".csv.tmp")
            with open(tmp, "w", encoding="utf-8", newline="") as f:
                w = csv.writer(f); w.writerow(cols)
                for r in rows[1:] if rows else []:
                    out = [(r[index_of[c]] if (index_of[c] is not None and index_of[c] < len(r)) else "") for c in cols]
                    w.writerow(out)
            tmp.replace(p)
    return p


def _list_signal_files(base: Path) -> list[Path]:
    """
    何をする関数？：
      - 未処理の *.json を新しい順にリストアップします。
    """
    return sorted(base.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)

def _load_signal(p: Path) -> dict | None:
    """
    何をする関数？：
      - シグナルJSONを辞書で読み出します（壊れていたら None を返してスキップ）。
    """
    try:
        return orjson.loads(p.read_bytes())
    except Exception as e:
        logger.error("signal load error: {} ({})", p, e)
        return None

def _append_execution_csv(sig: dict) -> Path:
    """
    何をする関数？：
      - 紙トレの発注内容を data/logs/executions.csv に1行追記します（ET時刻で記録）。
      - きょうからは数量 qty も保存します（KPIや振り返りの材料）。  :contentReference[oaicite:5]{index=5}
    """
    import csv  # この関数内だけで使うため関数内インポートにします
    p = _exec_log_path()
    ts = datetime.now(get_et_tz())
    entry = sig.get("entry", {}) or {}
    br = sig.get("bracket", {}) or {}
    qty = sig.get("qty", "")
    with open(p, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            ts.strftime("%Y%m%d"),
            ts.strftime("%H:%M:%S"),
            sig.get("symbol", ""),
            sig.get("setup", ""),
            sig.get("entryType", ""),
            qty,
            entry.get("price") or entry.get("limit") or entry.get("stop") or "",
            br.get("takeProfitPrice", ""),
            br.get("stopLossPrice", ""),
            sig.get("notes", ""),
        ])
    return p


def _log_paper(sig: dict) -> bool:
    """
    何をする関数？：
      - 紙トレとして、発注内容（銘柄/価格/数量/ブラケット等）をログに出し、
        executions.csv と strategy.csv の両方に追記します（Runbook準拠）。  :contentReference[oaicite:6]{index=6}
    """
    sym = sig.get("symbol")
    setup = sig.get("setup")
    entry = sig.get("entry", {})
    br = sig.get("bracket", {})
    logger.info("PAPER ORDER {} {} @ {} | bracket: TP={} SL={} notes={}",
                setup, sym, entry, br.get("takeProfitPrice"), br.get("stopLossPrice"), sig.get("notes"))
    p1 = _append_execution_csv(sig)      # 何をする行？：約定ログ（KPI入力）に追記。  :contentReference[oaicite:7]{index=7}
    p2 = _append_strategy_entry(sig)     # 何をする行？：明細ログ（将来のR/スリッページ集計）に追記。  :contentReference[oaicite:8]{index=8}
    logger.info("paper execution logged: {} ; strategy entry logged: {}", p1, p2)
    return True



def _move_after(p: Path, sent_dir: Path, failed_dir: Path, ok: bool) -> Path:
    """
    何をする関数？：
      - 処理結果に応じて signals/sent/ または signals/failed/ へファイルを移動します。
      - 同名があれば末尾に _1, _2... を付けて衝突を避けます。 :contentReference[oaicite:9]{index=9}
    """
    target_dir = sent_dir if ok else failed_dir
    name = p.name
    dest = target_dir / name
    i = 1
    while dest.exists():
        stem = p.stem
        suffix = p.suffix
        dest = target_dir / f"{stem}_{i}{suffix}"
        i += 1
    p.replace(dest)
    return dest

def main() -> int:
    """
    何をする関数？：
      - .env→ログ→config を読み、signals/*.json を順に処理（紙トレ：ログ出力）し、sent/ へ移動します。
      - “同一signalsの再処理防止”のため、成功・失敗をフォルダで分けて保存します。 :contentReference[oaicite:10]{index=10}
    使い方：
      poetry run python scripts/place_orders.py
    """
    load_dotenv_if_exists()
    logfile = configure_logging()
    cfg = load_config()
    mode = (cfg.get("runtime") or {}).get("mode", os.getenv("RUN_MODE", "paper")).lower()

    base, sent, failed = _dirs()
    files = _list_signal_files(base)

    if not files:
        logger.info("place_orders: no signals (logfile={})", logfile)
        return 0

    logger.info("place_orders: start ({} file[s], mode={})", len(files), mode)
    placed = 0
    for f in files:
        sig = _load_signal(f)
        if not isinstance(sig, dict):
            _move_after(f, sent, failed, ok=False)
            continue

        # いまは“紙トレ”のみ。liveは後でWebull SDKに差し替え。
        ok = _log_paper(sig)
        _move_after(f, sent, failed, ok=ok)
        if ok:
            placed += 1

    logger.info("place_orders: done placed={} / total={}", placed, len(files))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
