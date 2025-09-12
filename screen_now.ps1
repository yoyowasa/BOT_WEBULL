<# 
  screen_now.ps1
  目的：手動でスクリーニングしたいときに、Nightly → 指標作成 → シグナル生成 を
        1回の実行で確実に通すワンコマンド。
  使い方の例：
    cd E:\BOT_WEBULL
    .\screen_now.ps1 -Setup A -TopN 20 -Group quick_test       # Aセットで自動スクリーニング
    .\screen_now.ps1 -Setup B -TopN 4  -Group fixed_watchlist  # Bセットで固定銘柄テスト
#>

param(
  # どのセットアップでシグナルを出すか（A or B）。環境変数 ACTIVE_SETUP に反映。
  [ValidateSet('A','B')] [string]$Setup = 'A',

  # ランキングで上位何件に絞るか（WATCHLIST_TOP_N に反映）。未指定なら既定20。
  [int]$TopN = 20,

  # symbols のグループ名（WATCHLIST_GROUP に反映）。未指定なら quick_test。
  [string]$Group = 'quick_test',

  # 当日のNDJSONが無くても最新にフォールバックして指標計算を通すか（ALLOW_BARS_FALLBACK）。
  [switch]$NoFallback,  # 何をする行？：末尾にカンマを付けて次の引数へ正しく続ける

  [string]$Symbols = '',  # 何をする引数？：手でファイルを用意せず、その場のティッカーをカンマ区切りで渡すための引数

  [switch]$CollectBars,  # 何をする行？：WSで1分バーを“この実行で”収集するか（既定は収集する）

  [int]$WsSeconds = 120        # 何をする行？：WSの収集時間（秒）。短すぎるとバー不足なので最低90秒を推奨

)

# 1) ここで環境変数をセットして、Python側の設定解決（env→config→既定）を制御します。
$env:ACTIVE_SETUP        = $Setup
$env:WATCHLIST_TOP_N     = $TopN.ToString()
$env:WATCHLIST_GROUP     = $Group
$env:ALLOW_BARS_FALLBACK = if ($NoFallback) { '0' } else { '1' }
$env:PYTHONPATH = if ($env:PYTHONPATH) { "$root;$env:PYTHONPATH" } else { "$root" }  # 何をする行？：プロジェクト直下（E:\BOT_WEBULL）をPYTHONPATHに追加→rh_pdc_daytrade配下のモジュールを確実にimport可能にする
$env:STREAM_DIR = 'E:\data\stream'  # 何をする行？：1分バーの保存先をレガシー実績パスに固定し、compute_indicators の探索先と一致させる

$env:WATCHLIST_FILE = 'E:\BOT_WEBULL\configs\manual_watchlist.txt'  # 手動ウォッチリストを最優先で適用（symbols.ymlのquick_testより優先）

# 2) 実行ディレクトリをプロジェクト直下に合わせます（相対パス解決のため）
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

Write-Host "screen_now: start | setup=$($env:ACTIVE_SETUP) group=$($env:WATCHLIST_GROUP) top_n=$($env:WATCHLIST_TOP_N) fallback=$($env:ALLOW_BARS_FALLBACK)"

# 何をするブロック？：主要依存の存在チェック→足りなければ poetry install で自己回復（pandas/orjson/pyarrow/numpy）
poetry run python -c "import pandas,orjson,pyarrow,numpy" 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Host "screen_now: missing deps -> running 'poetry install' ..."
  poetry install --no-interaction
  if ($LASTEXITCODE -ne 0) { Write-Error "poetry install failed"; exit 1 }
}

# 3) Nightly（ウォッチリストの準備：Polygon失敗ならstubへ自動フォールバック）
poetry run python scripts/nightly_screen.py
# 何をするブロック？：-Symbols が渡されたら、その場のティッカーで watchlist_A/B.json を上書き（手動ファイル不要）
if ($Symbols -and $Symbols.Trim().Length -gt 0) {
  # 文字列 → 配列（カンマ/空白で分割）→ 大文字化 → 重複排除 → 妥当性チェック（英字・数字・.-のみ）
  $syms = $Symbols -split '[,\s]+' | Where-Object { $_ -match '^[A-Za-z0-9\.\-]+$' } | ForEach-Object { $_.ToUpper() } | Select-Object -Unique
  if (-not $syms -or $syms.Count -eq 0) {
    Write-Error "screen_now: -Symbols に有効なティッカーがありません。例: -Symbols 'AAPL,MSFT,NVDA'"
    exit 1
  }

  # 出力先（EOD_DIRがあれば優先、無ければ data\eod）
  $eodDir = if ($env:EOD_DIR -and $env:EOD_DIR.Trim().Length -gt 0) { $env:EOD_DIR } else { 'data\eod' }
  if (-not (Test-Path $eodDir)) { New-Item -ItemType Directory $eodDir | Out-Null }

  # {"symbols":[...]} 形式でA/Bを同内容で上書き（Nightlyの結果よりもこの場の指示を優先）
  $json = @{ symbols = $syms } | ConvertTo-Json -Depth 3 -Compress
  Set-Content -Path (Join-Path $eodDir 'watchlist_A.json') -Value $json -Encoding utf8
  Set-Content -Path (Join-Path $eodDir 'watchlist_B.json') -Value $json -Encoding utf8
  Write-Host ("screen_now: watchlist_A/B.json overridden from -Symbols (" + $syms.Count + " symbols)")
  if ($CollectBars) {
    # 何をする行？：古いロックが残っているとWS接続をスキップするため、先に削除して確実に接続させる
    $lock = (Join-Path $env:STREAM_DIR '.alpaca_ws.lock')  # 何をする行？：固定した保存先と同じ場所のロックを確実に削除する
    if (Test-Path $lock) { Remove-Item $lock -Force -ErrorAction SilentlyContinue }

    # 何をする行？：WSの実行秒数とFEEDを設定（既定iex／30銘柄上限）
    $env:WS_RUN_SECONDS = [Math]::Max($WsSeconds, 90).ToString()
    if (-not $env:ALPACA_FEED) { $env:ALPACA_FEED = 'iex' }

    Write-Host "screen_now: collecting 1m bars via ws_run ($($env:WS_RUN_SECONDS)s)"
    poetry run python scripts/ws_run.py
    if ($LASTEXITCODE -ne 0) { Write-Warning "ws_run exited with code $LASTEXITCODE (continuing)" }
  }

}


# 4) 指標計算（当日NDJSONが無くても、既定では最新にフォールバック）
poetry run python scripts/compute_indicators.py
if ($LASTEXITCODE -ne 0) { Write-Error "compute_indicators failed (exit=$LASTEXITCODE)"; exit 1 }

# 5) シグナル生成（ACTIVE_SETUP と watchlist_{A|B}.json で銘柄を絞る）
poetry run python scripts/run_signals.py
if ($LASTEXITCODE -ne 0) { Write-Error "run_signals failed (exit=$LASTEXITCODE)"; exit 1 }

# 6) 直近のシグナル出力ファイルを案内（手動エントリー判断の入口）
Write-Host "screen_now: done."
Get-ChildItem -Path "data\signals\*.json" -ErrorAction SilentlyContinue |
  Sort-Object LastWriteTime -Descending | Select-Object -First 5 |
  ForEach-Object { Write-Host ("signals file: " + $_.FullName) }
