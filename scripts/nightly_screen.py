# 夜間EODスクリーニングの“雛形”スクリプトです。
# 目的：
#  - Polygonキーや本処理が未接続でも、毎晩のウォッチリスト(A/B)を最低限生成して「止まらない」ようにする。
#  - 出力先は Runbook準拠の data/eod/ 配下（watchlist_A.json / watchlist_B.json）。  :contentReference[oaicite:3]{index=3}
# 今後：
#  - この箱に Polygon REST → ハードフィルタ → スコアリング（PDFのルール）を実装していきます。          :contentReference[oaicite:4]{index=4}

from __future__ import annotations
from pathlib import Path                  # 出力フォルダの作成とパス操作に使う
from datetime import datetime             # 生成時刻（ET）を記録するために使う
import os                                 # POLYGON_API_KEY の有無を確認するために使う
import orjson                             # 高速にJSONを書き出すために使う
from loguru import logger                 # ログ出力（共通ルールに従う）
import pandas as pd  # スコア計算の中間表（DataFrame）を扱うために使う
from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists   # .envの自動読込（先頭で呼ぶ）  :contentReference[oaicite:5]{index=5}
from rh_pdc_daytrade.utils.logutil import configure_logging        # ログを data/logs/bot.log に集約  :contentReference[oaicite:6]{index=6}
from rh_pdc_daytrade.utils.configutil import load_config, load_symbols  # config/symbols の読込     :contentReference[oaicite:7]{index=7}

# “EODロジック箱”から、ハードフィルタとスコア計算・ランキング関数を呼び出します。  :contentReference[oaicite:1]{index=1}
from rh_pdc_daytrade.screening.eod_screen import (
    apply_hard_filters,      # 何をする関数？：価格/出来高/ATR%/トレンド/フロートで合否を付ける
    compute_scores_basic,    # 何をする関数？：基本8割の線形和で A/B スコアを出す
    rank_watchlists          # 何をする関数？：A/B の上位N銘柄を選ぶ
)
from rh_pdc_daytrade.utils.io import write_parquet, write_csv  # 何をする関数？：EOD特徴量のParquet/CSV保存用（標準の保存口）。  :contentReference[oaicite:2]{index=2}
from rh_pdc_daytrade.utils.timeutil import get_et_tz               # ET時刻の安定取得（tzdataフォールバック）  :contentReference[oaicite:8]{index=8}

def write_watchlists_stub(symbols: list[str], out_dir: Path) -> tuple[Path, Path]:
    """
    何をする関数？：
      - EODの本処理がまだでも、最小限のウォッチリスト(A/B)をJSONで出力します。
      - 生成時刻（ET）とメモを入れ、後工程の確認ができる形にします。
    使い方：
      a_path, b_path = write_watchlists_stub(["AAPL","TSLA"], Path("data/eod"))
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(get_et_tz()).isoformat(),
        "symbols": symbols,
        "notes": "stub: polygon key missing or EOD fetch not yet implemented"
    }
    a_path = out_dir / "watchlist_A.json"
    b_path = out_dir / "watchlist_B.json"
    a_path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
    b_path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
    return a_path, b_path

def write_watchlists_ranked(topA, topB, out_dir: Path) -> tuple[Path, Path]:
    """
    何をする関数？：
      - A/B の「順位付きウォッチリスト」を Runbook準拠の data/eod/ に JSON で書き出します。
      - 引数は **pandas.DataFrame** でも **list[str]** でも受け付け、呼び出し元の違いを吸収します。  :contentReference[oaicite:2]{index=2}
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    def _to_payload(x):
        """
        何をする関数？：
          - DataFrame または list[str] から、["symbols"], ["top"] フィールド用の配列を作ります。
        """
        if isinstance(x, pd.DataFrame):
            # DataFrame：symbol列があればそれを採用。無ければ先頭列をシンボル相当として扱う。
            if "symbol" in x.columns:
                symbols = [str(s) for s in x["symbol"].tolist()]
            else:
                symbols = [str(s) for s in x.iloc[:, 0].tolist()]
            top_list = x.to_dict(orient="records")
        else:
            # list[str]：そのままsymbolsにし、topは簡易レコード化。
            symbols = [str(s) for s in list(x)]
            top_list = [{"symbol": s} for s in symbols]
        return symbols, top_list

    symsA, topA_list = _to_payload(topA)  # 何をする行？：Aのsymbols配列とスコア明細を作る
    symsB, topB_list = _to_payload(topB)  # 何をする行？：Bのsymbols配列とスコア明細を作る

    payloadA = {
        "generated_at": datetime.now(get_et_tz()).isoformat(),
        "symbols": symsA,
        "top": topA_list,
        "notes": "ranked by nightly_screen (list/DataFrame both supported)"
    }
    payloadB = {
        "generated_at": datetime.now(get_et_tz()).isoformat(),
        "symbols": symsB,
        "top": topB_list,
        "notes": "ranked by nightly_screen (list/DataFrame both supported)"
    }

    a_path = out_dir / "watchlist_A.json"
    b_path = out_dir / "watchlist_B.json"
    a_path.write_bytes(orjson.dumps(payloadA, option=orjson.OPT_INDENT_2))
    b_path.write_bytes(orjson.dumps(payloadB, option=orjson.OPT_INDENT_2))
    return a_path, b_path


def build_df_stub(symbols: list[str]) -> pd.DataFrame:
    """
    何をする関数？：
      - Polygon REST がまだでも、EODロジック（フィルタ＆スコア）を試せる最小DataFrameを作ります。
      - 値は“無難な既定”で、PDFの基本8割の条件を概ね通過するように置いています。  :contentReference[oaicite:3]{index=3}
    使い方：
      df = build_df_stub(["AAPL","TSLA"])
    """
    rows = []
    for s in symbols:
        close = 10.0
        rows.append({
            "symbol": s,
            "close": close,                     # 基準価格
            "pdc": close * 0.99,                # 前日終値（ここでは近い値）
            "pdh": close * 1.01,                # 前日高値（+1%で“接近”判定に入る）
            "pdl": close * 0.95,                # 前日安値
            "avg_volume20": 2_000_000,          # 20日平均出来高（≥1M）  :contentReference[oaicite:4]{index=4}
            "avg_dollar_vol20": 30_000_000,     # 20日平均ドル出来高（≥$5M）  :contentReference[oaicite:5]{index=5}
            "atr14": close * 0.08,              # ATR14=8%（適正域4–12%の中心）  :contentReference[oaicite:6]{index=6}
            "ema20": close * 0.97,              # Close>EMA20>EMA50 を満たす値  :contentReference[oaicite:7]{index=7}
            "ema50": close * 0.94,
            "float": 30_000_000,                # フロート 10–60M（推奨域）  :contentReference[oaicite:8]{index=8}
            "is_inside_day": True,              # 圧縮サイン（Inside/NR7）  :contentReference[oaicite:9]{index=9}
            "is_nr7": True,
            "pivot_p": close * 0.98,            # ピボット近接テスト用
            "high_52w": close * 1.2             # 上値余地の参考（ここでは未使用でも列は用意）
        })
    return pd.DataFrame(rows)

def save_eod_features(df: pd.DataFrame, out_dir: Path) -> tuple[Path, Path]:
    """
    何をする関数？：
      - “その日のEOD特徴量（df）” を data/eod/ に Parquet/CSV で保存します（Runbook準拠の標準保存）。  :contentReference[oaicite:3]{index=3}
      - ファイル名は eod_features_YYYYMMDD.*（ET日付）で揃えます。
    使い方：
      p_parq, p_csv = save_eod_features(df, Path("data/eod"))
    """
    et_date = datetime.now(get_et_tz()).strftime("%Y%m%d")
    out_dir.mkdir(parents=True, exist_ok=True)
    p_parq = out_dir / f"eod_features_{et_date}.parquet"
    p_csv  = out_dir / f"eod_features_{et_date}.csv"
    write_parquet(df, p_parq)  # 何をする関数？：Parquetで高速・省容量に保存（標準形式）。  :contentReference[oaicite:4]{index=4}
    write_csv(df, p_csv)       # 何をする関数？：人が確認しやすいCSVも同時に保存。
    return p_parq, p_csv

def main() -> int:
    """
    何をする関数？：
      - .env / ログ / config を読み込みます。
      - Polygonキーが空でも止めずに、symbols.yml の quick_test グループから“最小ウォッチリスト”を出力します。
      - 将来はここに Polygon REST → ハードフィルタ → スコアリングを実装します（PDFの規則に準拠）。  :contentReference[oaicite:9]{index=9}
    使い方：
      poetry run python scripts/nightly_screen.py
    """
    # 1) 環境変数とログの準備（全スクリプトの冒頭で呼ぶ運用）  :contentReference[oaicite:10]{index=10}
    load_dotenv_if_exists()
    logfile = configure_logging()
    logger.info("nightly_screen: start (logfile={})", logfile)

    # 2) 設定と銘柄グループの取得（まずは quick_test を使って動作確認）  :contentReference[oaicite:11]{index=11}
    cfg = load_config()
    symbols_file = cfg["data"]["symbols_file"]
    group = "quick_test"
    syms = load_symbols(group, symbols_file)
    if not syms:
        logger.warning("symbols group '{}' is empty in {}", group, symbols_file)
        syms = ["AAPL", "TSLA", "AMD", "NVDA"]

    # 3) Polygonキーの有無で分岐（未設定でも“警告＋最小出力で継続”する方針）  :contentReference[oaicite:12]{index=12}
    polygon_key = os.getenv("POLYGON_API_KEY", "").strip()
    out_dir = Path("data") / "eod"
    if not polygon_key:
        logger.warning("POLYGON_API_KEY is empty. Using stub EOD dataset to produce ranked watchlists.")
        # 1) 雛形データを作成（キー無しでも“基本8割”のロジックを試せる）  :contentReference[oaicite:11]{index=11}
        df = build_df_stub(syms)
        # 2) ハードフィルタ → スコア → A/B上位抽出（eod_screen.py を利用）  :contentReference[oaicite:12]{index=12}
        df = apply_hard_filters(df, cfg)
        df = compute_scores_basic(df, cfg)
        p_parq, p_csv = save_eod_features(df, out_dir)  # 何をする関数？：EOD特徴量のスナップショットを保存。
        logger.info("eod snapshot saved: {} , {}", p_parq, p_csv)

        topA, topB = rank_watchlists(df, top_n=20)
        # 3) 順位付きウォッチリストを書き出し（Runbook準拠の場所へ）  :contentReference[oaicite:13]{index=13}
        a, b = write_watchlists_ranked(topA, topB, out_dir)
        logger.info("ranked watchlists written (stub dataset): {} , {}", a, b)
        return 0


    # Polygonキー有り：EODを取得→ハードフィルタ→スコア→上位抽出→JSON（失敗時は雛形にフォールバック）
    source_label = "polygon"  # 何をする行？：最終ログに表示する“データソース”。fallback時は 'stub' に切り替える。

    try:
        # 何をする行？：Polygonの“取り口”は使う時だけ読み込む（未実装でも起動を止めないための遅延インポート）。  :contentReference[oaicite:3]{index=3}
        from rh_pdc_daytrade.providers.polygon_rest import fetch_eod_dataset  # 何をする関数？：Polygon RESTでEOD特徴量を作る  :contentReference[oaicite:4]{index=4}
    except Exception as e:
        logger.error("polygon provider import failed: {} ; fallback to stub dataset", e)
        df = build_df_stub(syms)  # 何をする行？：最小の雛形EODを使って“止めずに”続行  :contentReference[oaicite:5]{index=5}
        source_label = "stub"  # 何をする行？：例外時のフォールバックも 'stub' と明示する。

    else:
        try:
            df = fetch_eod_dataset(syms, api_key=polygon_key)
            if df.empty:
                logger.warning("polygon returned empty dataset; falling back to stub.")
                source_label = "stub"  # 何をする行？：実際はスタブで続行したことを最終ログに反映する。

                df = build_df_stub(syms)
        except Exception as e:
            logger.error("polygon failed: {} ; fallback to stub dataset", e)
            df = build_df_stub(syms)
            source_label = "stub"  # 何をする行？：例外時のフォールバックも 'stub' と明示する。



    df = apply_hard_filters(df, cfg)                 # 何をする関数？：価格/出来高/ATR%/トレンド/フロートで合否を付ける
    df = compute_scores_basic(df, cfg)               # 何をする関数？：“基本8割”の線形和で A/B スコアを出す
    p_parq, p_csv = save_eod_features(df, out_dir)  # 何をする関数？：EOD特徴量のスナップショットを保存。
    logger.info("eod snapshot saved: {} , {}", p_parq, p_csv)

    topA, topB = rank_watchlists(df, top_n=20)       # 何をする関数？：A/B の上位N銘柄を選ぶ
    a, b = write_watchlists_ranked(topA, topB, out_dir)  # 何をする関数？：Runbook準拠のA/B watchlistを書き出す
    logger.info("ranked watchlists written ({} dataset): {} , {}", source_label, a, b)  # 何をする行？：実データソース名を正しく出す。
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
