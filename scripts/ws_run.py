# 場中WSの起動スクリプトです。
# 目的：.env → ログ設定 → 設定/ウォッチリスト読込 → Alpaca(iex) WS接続（bars保存）を一本化します。  :contentReference[oaicite:3]{index=3}

from __future__ import annotations
from pathlib import Path  # パス操作（watchlistの場所を扱う）
import json               # サブスクライブ送信用のペイロード生成に使用
try:
    import yaml           # symbols.yml からユニバースを読む（無ければ手動/JSONのみで動く）
except Exception:
    yaml = None
import os                 # 環境変数（ALPACA_FEED / WS_RUN_SECONDS）取得
from loguru import logger # 共通ログ（data/logs/bot.log に集約）  :contentReference[oaicite:4]{index=4}
from threading import Thread  # 何をする行？：WSを別スレッドで実行し、規定秒でメインを必ず返すために使用

from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists   # .env 自動読込（最初に呼ぶ）  :contentReference[oaicite:5]{index=5}
from rh_pdc_daytrade.utils.logutil import configure_logging        # ログ初期化（冪等）  :contentReference[oaicite:6]{index=6}
from rh_pdc_daytrade.utils.configutil import load_config, load_symbols  # 設定/銘柄の共通ローダ  :contentReference[oaicite:7]{index=7}
from threading import Thread  # 何をする行？：WSを別スレッドで実行し、規定秒でメインを確実に返すために使用

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
      - config.strategy.active_setup（A/B）に対応する data/eod/watchlist_{A|B}.json の "symbols" を返します。
      - ファイル無し・空・壊れのときは symbols.yml の quick_test → 最後に固定4銘柄へ“安全フォールバック”。
      - どのファイルを使ったか／何銘柄読めたかをログに出し、原因調査を簡単にします。  :contentReference[oaicite:2]{index=2}
    """
    from pathlib import Path            # 関数内だけで使うのでここに書く（ルール準拠）
    import orjson                       # 同上（遅延インポートで起動失敗を防ぐ）

    # 0) 何をするブロック？：
    #    環境変数 WATCHLIST_FILE / MANUAL_WATCHLIST が指す手動TXTを“最優先”で読む。
    #    空行・#コメントをスキップし、重複を除いた配列が得られたら即returnする。
    manual = os.environ.get("WATCHLIST_FILE") or os.environ.get("MANUAL_WATCHLIST")
    if manual:
        _mf = Path(manual)
        if _mf.exists():
            try:
                _syms, _seen = [], set()
                for line in _mf.read_text(encoding="utf-8").splitlines():
                    s = line.strip().upper()
                    if not s or s.startswith("#"):
                        continue
                    if s not in _seen:
                        _seen.add(s); _syms.append(s)
                if _syms:
                    logger.info("ws_run: using manual watchlist {} ({} symbols)", _mf, len(_syms))
                    return _syms
                else:
                    logger.warning("ws_run: manual watchlist {} has no symbols; fallback", _mf)
            except Exception as e:
                logger.warning("ws_run: manual watchlist read failed ({}); fallback", e)
        else:
            logger.warning("ws_run: manual watchlist not found: {}", _mf)

    # A/Bの余計な空白や小文字を吸収（"A "→"A" など）。  :contentReference[oaicite:3]{index=3}
    setup = str((cfg.get("strategy") or {}).get("active_setup", "A")).strip().upper()
    wl_path = Path("data") / "eod" / f"watchlist_{setup}.json"

    # 1) 前夜のwatchlistを読む
    if wl_path.exists():
        try:
            data = orjson.loads(wl_path.read_bytes())  # 何をする行？：watchlist JSONを読み込む（配列形式/辞書形式の両方に対応）
            raw = (data if isinstance(data, list) else (data.get("symbols") or data.get("tickers") or [])) if isinstance(data, (list, dict)) else []  # 何をする行？：配列ならそのまま／辞書なら "symbols"→"tickers" の順で読む
            syms = [str(s).strip().upper() for s in raw if isinstance(s, str) and str(s).strip()]  # 何をする行？：文字列だけを大文字整形して抽出（空行・無効値は除外）

            if syms:
                logger.info("ws_run: using {} ({} symbols) for setup={}", wl_path, len(syms), setup)
                return syms
            else:
                logger.warning("ws_run: {} has no 'symbols' or empty; fallback to symbols.yml", wl_path)
        except Exception as e:
            logger.warning("ws_run: failed to parse {} ({}); fallback to symbols.yml", wl_path, e)

    # 2) symbols.yml の quick_test へフォールバック（運用手順の既定）。  :contentReference[oaicite:4]{index=4}
    try:
        from rh_pdc_daytrade.utils.configutil import load_symbols
        syms = load_symbols("quick_test", cfg["data"]["symbols_file"])
        if syms:
            logger.info("ws_run: fallback to symbols.yml quick_test ({} symbols)", len(syms))
            return syms
    except Exception as e:
        logger.warning("ws_run: load_symbols failed ({}); fallback to defaults", e)

    # 3) 最後の砦（固定4銘柄）。“止めない”運用。  :contentReference[oaicite:5]{index=5}
    defaults = ["AAPL", "TSLA", "AMD", "NVDA"]
    logger.info("ws_run: using defaults {} (no watchlist/symbols.yml available)", defaults)
    return defaults


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
    # 何をする行？：IEXは最大30銘柄までなので、必要なら安全にトリミングします。  :contentReference[oaicite:2]{index=2}
    import os  # この関数内でしか使わないため関数内importにします
    feed = os.getenv("ALPACA_FEED", "").lower() or str((cfg.get("runtime") or {}).get("provider_realtime", "")).lower()
    if "iex" in feed and len(syms) > 30:
        logger.warning("ws_run: iex feed allows up to 30 symbols; trimming from {} to 30", len(syms))
        syms = syms[:30]

    logger.info("ws_run: using symbols decided by _load_session_symbols ({} symbols)", len(syms))  # 何をする行？：上書きをやめ、直前で決まった購読銘柄の件数だけ通知する

    if not syms:
        logger.error("no symbols to subscribe; exiting")
        return 1

    feed = os.getenv("ALPACA_FEED", "iex")  # 何をする行？：使用するリアルタイムfeed（既定はiex）を決定

    # 何をする行？：WSの実行秒数を確定（env優先／未指定→90秒／75秒未満→90秒に底上げ）
    _env = os.getenv("WS_RUN_SECONDS", "").strip()  # 何をする行？：環境変数から実行秒数を読む（空なら後段でデフォルト化）
    run_seconds = int(_env) if _env.isdigit() else 0  # 何をする行？：未定義を避けて確実に初期化（0ならデフォルト適用）
    if run_seconds < 90: run_seconds = 90  # 何をする行？：短すぎるとバー不足なので下限を90秒に固定

    stream_dir = os.environ.get("STREAM_DIR") or os.path.join("data", "stream")  # 何をする行？：barsの保存先を環境変数で一元化（未設定はリポ内 data/stream）
    os.makedirs(stream_dir, exist_ok=True)                                        # 何をする行？：保存ディレクトリを必ず用意（無ければ作成）
    os.environ["STREAM_DIR"] = os.path.abspath(stream_dir)                         # 何をする行？：下位モジュール（WS/ロック/保存）にも同じ場所を伝える
    logger.info("STREAM_DIR={}", os.environ["STREAM_DIR"])                         # 何をする行？：どこに書くかをログで明示（切り分け用）
    logger.info("ws lock (expected): {}", os.path.join(os.environ["STREAM_DIR"], ".alpaca_ws.lock"))  # 何をする行？：WSが使う想定のロックパスを見える化（将来の誤検知を早期発見）
    legacy_lock = os.path.join(r"E:\data\stream", ".alpaca_ws.lock")  # 何をする行？：過去バージョンの固定ロック場所を指す
    if os.path.exists(legacy_lock) and os.path.abspath(os.environ["STREAM_DIR"]) != os.path.abspath(os.path.dirname(legacy_lock)):
        try:
            os.remove(legacy_lock)  # 何をする行？：STREAM_DIR外に残った“古いロック”を削除して接続スキップを防ぐ
            logger.warning(f"removed legacy ws lock: {legacy_lock}")
        except Exception as e:
            logger.warning(f"failed to remove legacy ws lock: {legacy_lock} ({e})")

    
    from rh_pdc_daytrade.providers.alpaca_iex_ws import connect_and_stream  # 何をする行？：STREAM_DIR設定後にimportして、単一実行ロックと保存先を同じ環境変数で解決させる

    logger.info("ws_run start: feed={} symbols={} run_seconds={} (logfile={})", feed, syms, run_seconds, logfile)  # 何をする行？：確定した秒数を開始ログに出す
    def _runner():  # 何をする関数？：WS接続ループの起動ラッパー（STREAM_DIR設定後にimportさせる）
        from rh_pdc_daytrade.providers.alpaca_iex_ws import connect_and_stream  # 何をする行？：環境が整った後に限定してimport
        connect_and_stream(syms, feed=feed, run_seconds=run_seconds)  # 何をする行？：WS本体を起動

    t = Thread(target=_runner, daemon=True)  # 何をする行？：WSをデーモンスレッドで実行
    t.start(); t.join(run_seconds)           # 何をする行？：規定秒だけ待機してメイン処理を確実に返す
    logger.info("ws watchdog: timeout reached ({}s); returning to caller", run_seconds)  # 何をする行？：タイムアウト到達を記録
    return 0  # 何をする行？：この時点でmainを終了し、次工程（指標→シグナル）へ必ず進ませる

    def _runner():  # 何をする関数？：WS本体（connect_and_stream）を別スレッドで実行する
        connect_and_stream(syms, feed=feed, run_seconds=run_seconds)

    t = Thread(target=_runner, daemon=True)  # 何をする行？：規定秒で必ず制御を返すためのウォッチドッグ
    t.start(); t.join(run_seconds)           # 何をする行？：run_seconds 経過まで待機し、超えたらメインを返す
    logger.info("ws watchdog: timeout reached ({}s); returning to caller", run_seconds)  # 何をする行？：タイムアウト到達を記録
    return 0  # 何をする行？：ここでmainを終了し、次工程（compute_indicators→run_signals）へ必ず進ませる






if __name__ == "__main__":
    raise SystemExit(main())
