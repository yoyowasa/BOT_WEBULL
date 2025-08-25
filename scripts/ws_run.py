# 場中WSの起動スクリプトです。
# 目的：.env → ログ設定 → 設定/ウォッチリスト読込 → Alpaca(iex) WS接続（bars保存）を一本化します。  :contentReference[oaicite:3]{index=3}

from __future__ import annotations
from pathlib import Path  # パス操作（watchlistの場所を扱う）
import os                 # 環境変数（ALPACA_FEED / WS_RUN_SECONDS）取得
from loguru import logger # 共通ログ（data/logs/bot.log に集約）  :contentReference[oaicite:4]{index=4}

from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists   # .env 自動読込（最初に呼ぶ）  :contentReference[oaicite:5]{index=5}
from rh_pdc_daytrade.utils.logutil import configure_logging        # ログ初期化（冪等）  :contentReference[oaicite:6]{index=6}
from rh_pdc_daytrade.utils.configutil import load_config, load_symbols  # 設定/銘柄の共通ローダ  :contentReference[oaicite:7]{index=7}
from rh_pdc_daytrade.providers.alpaca_iex_ws import connect_and_stream  # IEX WSへ接続・bars保存  :contentReference[oaicite:8]{index=8}

def _watchlist_path(setup: str) -> Path:
    """
    何をする関数？：
      - 戦略A/Bに応じて、既定のウォッチリスト（data/eod/watchlist_A/B.json）のパスを返します。  :contentReference[oaicite:9]{index=9}
    """
    s = "A" if str(setup).upper() != "B" else "B"
    return Path("data") / "eod" / f"watchlist_{s}.json"

def _read_watchlist_symbols(p: Path) -> list[str]:
    """
    何をする関数？：
      - watchlist_X.json から symbols配列を読み込み、重複除去＋大文字統一して返します。
      - 無ければ空配列を返し、上位へフォールバックさせます（“止めない”ため）。  :contentReference[oaicite:10]{index=10}
    """
    if not p.exists():
        logger.warning("watchlist not found: {}", p)
        return []
    import orjson  # 関数内でしか使わないためここでインポート（高速JSON読込）
    try:
        payload = orjson.loads(p.read_bytes())
        raw = payload.get("symbols") or []
    except Exception as e:
        logger.error("watchlist parse error: {} ({})", p, e)
        return []
    result, seen = [], set()
    for s in raw:
        sym = str(s).strip().upper()
        if sym and sym not in seen:
            seen.add(sym)
            result.append(sym)
    return result

def _pick_symbols(cfg: dict) -> list[str]:
    """
    何をする関数？：
      - 優先：data/eod/watchlist_{A|B}.json（config.strategy.active_setupに従う）。  :contentReference[oaicite:11]{index=11}
      - 次点：configs/symbols.yml の quick_test グループ。
      - 最後：["AAPL","TSLA","AMD","NVDA"]（最小の動作確認用）。  :contentReference[oaicite:12]{index=12}
    """
    setup = (cfg.get("strategy", {}) or {}).get("active_setup", "A")
    wl = _watchlist_path(setup)
    syms = _read_watchlist_symbols(wl)
    if not syms:
        syms = load_symbols("quick_test", cfg["data"]["symbols_file"])
    if not syms:
        logger.warning("fallback to default quick list (AAPL,TSLA,AMD,NVDA)")
        syms = ["AAPL", "TSLA", "AMD", "NVDA"]
    return syms

def _load_session_symbols(cfg: dict) -> list[str]:
    """
    何をする関数？：
      - config.strategy.active_setup（A or B）を見て、data/eod/watchlist_{A|B}.json の "symbols" を返します。
      - ファイルが無い・壊れている時は symbols.yml の quick_test → それも無ければ ['AAPL','TSLA','AMD','NVDA'] に戻します。  
    """
    from pathlib import Path         # この関数内だけで使うため関数内importにします
    import orjson                    # 同上（他所へ影響させない）
    setup = (cfg.get("strategy") or {}).get("active_setup", "A").upper()
    wl_path = Path("data") / "eod" / f"watchlist_{setup}.json"
    if wl_path.exists():
        try:
            data = orjson.loads(wl_path.read_bytes())
            syms = [s for s in data.get("symbols", []) if isinstance(s, str)]
            if syms:
                return syms
        except Exception:
            from loguru import logger
            logger.warning("ws_run: failed to read {} ; fallback to symbols.yml / defaults", wl_path)

    # symbols.yml の quick_test グループへフォールバック（運用メモ準拠）  :contentReference[oaicite:3]{index=3}
    try:
        from rh_pdc_daytrade.utils.configutil import load_symbols
        syms = load_symbols("quick_test", cfg["data"]["symbols_file"])
        if syms:
            return syms
    except Exception:
        pass

    # 最後の砦（固定4銘柄）。“止めない”方針。  :contentReference[oaicite:4]{index=4}
    return ["AAPL", "TSLA", "AMD", "NVDA"]

def main() -> int:
    """
    何をする関数？：
      - .env読込 → ログ設定 → 設定読込 → ウォッチリスト決定 → Alpaca(iex) WS接続 を実行します。  :contentReference[oaicite:13]{index=13}
      - WSの実行時間は WS_RUN_SECONDS（環境変数）で秒数指定可。未指定なら継続実行します。
    使い方：
      poetry run python scripts/ws_run.py
      （お試しは $env:WS_RUN_SECONDS='10' で10秒間だけ実行）
    """
    load_dotenv_if_exists()                 # まず .env を適用（APIキー・FEEDなど）  :contentReference[oaicite:14]{index=14}
    logfile = configure_logging()           # data/logs/bot.log に出力  :contentReference[oaicite:15]{index=15}
    cfg = load_config()                     # configs/config.yaml を読み込み  :contentReference[oaicite:16]{index=16}
    syms = _load_session_symbols(cfg)  # 何をする行？：前夜のwatchlist（A/B）から当日の購読銘柄を決める。無ければ安全Fallback。  :contentReference[oaicite:5]{index=5}

    syms = _pick_symbols(cfg)
    if not syms:
        logger.error("no symbols to subscribe; exiting")
        return 1

    feed = os.getenv("ALPACA_FEED", "iex")  # 既定は iex（無料でまず運用）  :contentReference[oaicite:17]{index=17}
    run_seconds = None
    val = os.getenv("WS_RUN_SECONDS", "").strip()
    if val.isdigit():
        run_seconds = int(val)

    logger.info("ws_run start: feed={} symbols={} (logfile={})", feed, syms, logfile)
    return connect_and_stream(syms, feed=feed, run_seconds=run_seconds)

if __name__ == "__main__":
    raise SystemExit(main())
