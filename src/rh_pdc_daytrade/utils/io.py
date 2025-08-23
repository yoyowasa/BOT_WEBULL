# DataFrame を Parquet/CSV に保存するためのユーティリティです。
# ねらい：保存形式を Parquet に統一（高速・省容量）、人が見る用に CSV も用意します。 :contentReference[oaicite:1]{index=1}

from __future__ import annotations
from pathlib import Path  # パス操作（保存先フォルダの作成に使う）
import pandas as pd       # DataFrame の to_parquet / to_csv を使う

def _ensure_parent(path: Path) -> Path:
    """
    何をする関数？：
      - 書き出し先ファイルの親フォルダが無ければ作ります（安全に失敗しないための基本）。
    使い方：
      _ensure_parent(Path("data/eod/file.parquet"))
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _choose_parquet_engine() -> str:
    """
    何をする関数？：
      - 利用可能な Parquet エンジン名を返します（pyarrow → fastparquet の順に優先）。
      - どちらも無ければ ValueError を投げます（依存追加後に再実行）。 :contentReference[oaicite:2]{index=2}
    使い方：
      engine = _choose_parquet_engine()
    """
    try:
        import pyarrow  # 関数内だけで使うためここでインポート
        return "pyarrow"
    except Exception:
        pass
    try:
        import fastparquet  # 関数内だけで使うためここでインポート
        return "fastparquet"
    except Exception as e:
        raise ValueError("No Parquet engine found: install pyarrow or fastparquet") from e

def write_parquet(df: pd.DataFrame, path: str | Path, compression: str = "snappy") -> Path:
    """
    何をする関数？：
      - DataFrame を Parquet で保存します（既定圧縮=snappy）。
      - ランブックの推奨どおり、標準の保存形式として利用します。 :contentReference[oaicite:3]{index=3}
    使い方：
      write_parquet(df, 'data/eod/xxx.parquet')
    戻り値：
      書き出したファイルの Path
    """
    p = _ensure_parent(Path(path))
    engine = _choose_parquet_engine()
    df.to_parquet(p, engine=engine, compression=compression, index=False)
    return p

def write_csv(df: pd.DataFrame, path: str | Path) -> Path:
    """
    何をする関数？：
      - DataFrame を CSV（UTF-8）で保存します（人が中身を開いて確認する用途）。
    使い方：
      write_csv(df, 'data/eod/xxx.csv')
    戻り値：
      書き出したファイルの Path
    """
    p = _ensure_parent(Path(path))
    df.to_csv(p, index=False, encoding="utf-8")
    return p
