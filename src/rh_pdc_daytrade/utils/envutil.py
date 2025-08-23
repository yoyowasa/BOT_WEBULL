# これは「.env を見つけて読めたら環境変数に流し込む」ためのユーティリティです。
# すべてのスクリプトの冒頭で一度だけ呼ぶ想定です（資料の運用前提）。  :contentReference[oaicite:3]{index=3}

from pathlib import Path  # パス操作（.envの場所を探す）
import os                 # 環境変数へ反映するために使う
from dotenv import load_dotenv  # .envを読み込む公式関数（pyprojectで追加済み）

def load_dotenv_if_exists(env_path: str | os.PathLike[str] | None = None) -> None:
    """
    何をする関数？：
      - プロジェクト直下（E:\BOT_WEBULL）の .env を「見つけたら」読み込みます。
      - 既に OS 側に入っている環境変数は尊重し、上書きしません（override=False）。
      - 見つからない時はエラーにせず、そのまま何もしません（安全に先へ進めます）。
    使い方：
      from rh_pdc_daytrade.utils.envutil import load_dotenv_if_exists
      load_dotenv_if_exists()
    """
    # 1) .envの場所を決める：引数が無ければ、プロジェクトルートの .env を探します。
    #    このファイルは .../src/rh_pdc_daytrade/utils/envutil.py にあるので、3つ上がプロジェクト直下です。
    root = Path(__file__).resolve().parents[3] if env_path is None else Path(env_path).resolve()
    env_file = (root / ".env") if env_path is None else root

    # 2) .env が存在する時だけ読み込みます（存在しなければ何もしない設計）。
    if env_file.exists():
        load_dotenv(dotenv_path=env_file, override=False)  # 既存の環境変数は上書きしない
    # 3) 戻り値はありません。呼ぶ側は os.getenv("RUN_MODE") のように取り出します。
