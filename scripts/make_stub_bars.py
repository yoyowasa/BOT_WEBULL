# オフ時間の動作確認用：「ET 9:30 から N 分分」のダミー1分バーを作り、NDJSONに追記します。
# 目的：市場が閉まっていても compute_indicators の計算（VWAP/AVWAP/ORB）を確認できるようにする。  :contentReference[oaicite:2]{index=2}

from __future__ import annotations
from pathlib import Path            # ファイル保存先の扱い（ここでは使わないが型のため）
from datetime import datetime, time, timedelta  # ET時刻の生成に使う
from loguru import logger           # ログ（共通ルールで data/logs/bot.log へ）

from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists   # 何をする関数？：.envを読む
from rh_pdc_daytrade.utils.logutil import configure_logging        # 何をする関数？：ログを初期化
from rh_pdc_daytrade.utils.configutil import load_config, load_symbols  # 何をする関数？：設定と銘柄を読む
from rh_pdc_daytrade.utils.timeutil import get_et_tz              # 何をする関数？：ETタイムゾーンを得る
from rh_pdc_daytrade.providers.alpaca_iex_ws import append_ndjson # 何をする関数？：NDJSONへ1行追記（既存実装を再利用）

def _to_ns(dt: datetime) -> int:
    """
    何をする関数？：
      - awareな日時を「エポック**ナノ秒**」に直して返します。
      - compute_indicators のタイムスタンプ推定ロジックが“ns”を最優先で認識するため、nsで出します。
    """
    return int(dt.timestamp() * 1_000_000_000)

def build_stub_bars(symbols: list[str], start: str = "09:30:00", minutes: int = 10) -> list[dict]:
    """
    何をする関数？：
      - ETの当日 9:30:00 から、指定分数（既定10分）の**1分バー**を合成して返します。
      - フォーマットは IEX Bar 互換（type/S/t/o/h/l/c/v）。ORB/VWAP/AVWAPが計算できる“素直な形”です。
    使い方：
      recs = build_stub_bars(["AAPL","TSLA"], minutes=10)
    """
    tz = get_et_tz()
    base_date = datetime.now(tz).date()
    start_t = time.fromisoformat(start)
    start_dt = datetime.combine(base_date, start_t, tzinfo=tz)

    out: list[dict] = []
    for sym in symbols:
        # 銘柄ごとに少しだけベース価格をずらす（10ドル台）。以後、毎分0.01ドルずつ上向きにします。
        base = 10.0 + (sum(map(ord, sym)) % 50) / 1000.0
        for i in range(minutes):
            ts = start_dt + timedelta(minutes=i)
            o = base + 0.01 * i
            c = o + 0.01
            h = max(o, c) + 0.02
            l = min(o, c) - 0.02
            v = 1000 + 10 * i
            out.append({
                "type": "bar", "S": sym, "t": _to_ns(ts),
                "o": round(o, 4), "h": round(h, 4), "l": round(l, 4), "c": round(c, 4), "v": int(v)
            })
    return out

def write_stub_bars(records: list[dict]) -> None:
    """
    何をする関数？：
      - 合成した1分バーを **data/stream/bars_YYYYMMDD.ndjson** に**1行ずつ追記**します。
      - 既存の append_ndjson("bars", rec) を使います（保存先とファイル名規則は共通）。  :contentReference[oaicite:3]{index=3}
    使い方：
      write_stub_bars(recs)
    """
    for rec in records:
        append_ndjson("bars", rec)

def main() -> int:
    """
    何をする関数？：
      - .env → ログ → config を読み、symbols.yml の quick_test から銘柄を取り、スタブbarを生成→保存します。
    使い方：
      poetry run python scripts/make_stub_bars.py
    """
    load_dotenv_if_exists()
    logfile = configure_logging()
    cfg = load_config()

    syms = load_symbols("quick_test", cfg["data"]["symbols_file"]) or ["AAPL", "TSLA", "AMD", "NVDA"]
    recs = build_stub_bars(syms, minutes=10)
    write_stub_bars(recs)
    logger.info("stub bars written: {} symbols x {} minutes (logfile={})", len(syms), 10, logfile)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
