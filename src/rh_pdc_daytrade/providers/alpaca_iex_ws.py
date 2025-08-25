# Alpaca Market Data (feed=iex) の WebSocket に接続し、bars を data/stream に NDJSON で保存する最小プロバイダです。
# 目的：Phase-1（無料枠）のリアルタイム層として bars を安定取得して“止めずに保存する”箱を用意する。  :contentReference[oaicite:6]{index=6}
# 仕様メモ：IEX Bar は {"T":"b","S":"AAPL","t":..., "o":..., "h":..., "l":..., "c":..., "v":...} 形式（資料の想定）。  :contentReference[oaicite:7]{index=7}

from __future__ import annotations
import asyncio                      # 非同期WSループ
from pathlib import Path            # 保存先のパス操作
from datetime import datetime       # ET日付でファイル名を付ける
import os                           # APIキー・FEEDの参照
import json                         # 認証/購読メッセージ送信用（テキスト）
import orjson                       # 受信データの高速書き込み（バイナリ）
import websockets                   # WebSocketクライアント（^12系）
from loguru import logger           # ログ（共通ポリシー）

from rh_pdc_daytrade.utils.timeutil import get_et_tz  # ET日付の安定取得（tzdata+フォールバック）  :contentReference[oaicite:8]{index=8}

def ws_url(feed: str = "iex") -> str:
    """何をする関数？：feed名（iex/sip/delayed_sip）から Alpaca WS エンドポイントURLを返します。"""
    f = (feed or "iex").lower()
    if f not in {"iex", "sip", "delayed_sip"}:
        f = "iex"
    return f"wss://stream.data.alpaca.markets/v2/{f}"

def stream_dir() -> Path:
    """何をする関数？：標準の保存先 data/stream を返し、無ければ作ります（Runbook準拠）。"""  # :contentReference[oaicite:9]{index=9}
    root = Path(__file__).resolve().parents[3]
    d = root / "data" / "stream"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _ndjson_path(channel: str) -> Path:
    """何をする関数？：チャンネル名（bars等）と ET日付から、出力ファイルパスを作ります。"""
    et = datetime.now(get_et_tz()).strftime("%Y%m%d")
    return stream_dir() / f"{channel}_{et}.ndjson"

def append_ndjson(channel: str, obj: dict) -> None:
    """何をする関数？：1レコードを NDJSON として追記保存します（バイナリ高速出力）。"""
    p = _ndjson_path(channel)
    with open(p, "ab") as f:
        f.write(orjson.dumps(obj))
        f.write(b"\n")

def standardize_bar(msg: dict) -> dict:
    """何をする関数？：IEXのBarメッセージを標準キーに整えます（T/S/t/o/h/l/c/v をそのまま使用）。"""  # :contentReference[oaicite:10]{index=10}
    return {
        "type": "bar",
        "S": msg.get("S"),  # シンボル
        "t": msg.get("t"),  # タイムスタンプ（ns/μs）
        "o": msg.get("o"),
        "h": msg.get("h"),
        "l": msg.get("l"),
        "c": msg.get("c"),
        "v": msg.get("v"),
    }

def build_subscribe(symbols: list[str]) -> dict:
    """何をする関数？：bars購読のサブスクJSONを作ります（まずはbarsのみ）。"""
    return {"action": "subscribe", "bars": symbols}

async def _stream_once(symbols: list[str], key: str, secret: str, feed: str = "iex") -> None:
    """何をする関数？：WSへ接続→認証→購読→受信ループ→NDJSON保存を1回の接続で実行します。"""
    url = ws_url(feed)
    async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
        # 認証（"authenticated" を受信するまで待つ）
        await ws.send(json.dumps({"action": "auth", "key": key, "secret": secret}))
        authenticated = False
        while True:
            frame = await ws.recv()
            logger.info("alpaca auth reply: {}", frame)
            try:
                pl = json.loads(frame)
            except Exception:
                continue
            msgs = pl if isinstance(pl, list) else [pl]
            for m in msgs:
                if m.get("T") == "error":
                    logger.error("alpaca auth error: {}", m)
                    return
                if m.get("T") == "success" and str(m.get("msg")).lower() == "authenticated":
                    authenticated = True
                    break
            if authenticated:
                break

        # 購読（barsのみ：まずはbarsを安定保存する最小構成）
        await ws.send(json.dumps(build_subscribe(symbols)))
        sub_resp = await ws.recv()
        logger.info("alpaca subscription reply: {}", sub_resp)


        # 受信ループ：配列または単発メッセージの両方に対応
        while True:
            raw = await ws.recv()
            try:
                payload = json.loads(raw)
            except Exception:
                logger.warning("non-JSON frame skipped")
                continue

            msgs = payload if isinstance(payload, list) else [payload]
            for m in msgs:
                typ = m.get("T")
                if typ == "b":  # bar
                    rec = standardize_bar(m)
                    append_ndjson("bars", rec)
                elif typ in {"success", "error"}:
                    # 成功/エラーの管理系はログに残して継続
                    logger.info("alpaca control: {}", m)
                else:
                    # 今はbars以外は無視（将来 trades/quotes を追加）
                    continue

def connect_and_stream(symbols: list[str], feed: str = "iex", run_seconds: int | None = None) -> int:
    """
    何をする関数？：
      - APIキー（ALPACA_KEY_ID/ALPACA_SECRET_KEY）と feed を使って IEX WS に接続し、barsを保存します。
      - キー未設定のときは警告して 0 を返し、処理を終えます（“止めない”運用方針）。  :contentReference[oaicite:11]{index=11}
    使い方：
      connect_and_stream(["AAPL","TSLA"], feed="iex")
    戻り値：0=正常終了
    """
    key = os.getenv("ALPACA_KEY_ID", "").strip()
    secret = os.getenv("ALPACA_SECRET_KEY", "").strip()
    if not key or not secret:
        logger.warning("ALPACA_KEY_ID/ALPACA_SECRET_KEY is empty; skipping WS connect.")
        return 0

    async def runner():
        # run_seconds が指定されていればその時間でキャンセル（テスト用）
        task = asyncio.create_task(_stream_once(symbols, key, secret, feed=feed))
        if run_seconds and run_seconds > 0:
            await asyncio.sleep(run_seconds)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.info("ws cancelled after {} seconds", run_seconds)
        else:
            await task

    asyncio.run(runner())
    return 0
