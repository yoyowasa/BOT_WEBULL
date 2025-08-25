# scripts/run_all.ps1
# 何をするスクリプト？：
#   ランブックの運用フローを「1本のボタン」で回します（WS→指標→シグナル→発注→取消/クローズ→KPI）。
#   起動のたびに PYTHONPATH=./src を先頭に追加し、ImportError を防ぎます。  :contentReference[oaicite:1]{index=1}
#   各スクリプトは自前で .env 読込＆時刻ガードを持つので、市場時間外は“何もしない”で安全終了します。  :contentReference[oaicite:2]{index=2}

param(
  # 何をする引数？：どのフェーズを実行するか選びます（既定=session：WS→指標→シグナル→発注）。
  [ValidateSet('nightly','session','cancel','close','kpi','all')]
  [string]$Phase = 'session',
  # 何をする引数？：WS の実行秒数（テスト用）。session/all で使います。
  [int]$WsSeconds = 10,
  # 何をする引数？：紙トレ（既定）/ライブ切替。Switch は既定値を持たせず、内部変数で既定ONにします。  :contentReference[oaicite:3]{index=3}
  [switch]$Paper
)

$ErrorActionPreference = 'Stop'

function Set-RepoEnv {
  <#
    何をする関数？：
      - リポジトリ直下に移動して、PYTHONPATH の先頭に ./src を追加します。
      - VSCode/Pylance と実行時の両方で「rh_pdc_daytrade を見つけられない」問題を防ぎます。  :contentReference[oaicite:4]{index=4}
  #>
  $root = Split-Path $PSScriptRoot -Parent
  Set-Location $root
  $src = Join-Path $root 'src'
  if ($env:PYTHONPATH) { $env:PYTHONPATH = "$src;$env:PYTHONPATH" } else { $env:PYTHONPATH = $src }
}

function Set-RunMode {
  <#
    何をする関数？：
      - RUN_MODE を paper/live に設定します（既定は paper）。ランブックの動作モードに対応。  :contentReference[oaicite:5]{index=5}
  #>
  $UsePaper = $true
  if ($PSBoundParameters.ContainsKey('Paper')) { $UsePaper = $Paper.IsPresent }
  $env:RUN_MODE = ($UsePaper ? 'paper' : 'live')
}

function Invoke-Py([string]$relPath) {
  <#
    何をする関数？：
      - Poetry の仮想環境で Python スクリプトを実行します。失敗時は直ちに止めます。
  #>
  Write-Host ">> $relPath" -ForegroundColor Cyan
  & poetry run python $relPath
  if ($LASTEXITCODE -ne 0) { throw "failed: $relPath" }
}

function Do-Nightly {
  <#
    何をする関数？：
      - 夜間EODスクリーニングを実行します（Polygon→ハードフィルタ→スコア→watchlist出力）。  :contentReference[oaicite:6]{index=6}
  #>
  Invoke-Py 'scripts/nightly_screen.py'
}

function Do-Session {
  <#
    何をする関数？：
      - 寄り後の勝負時間フロー（WS→指標→シグナル→発注）をまとめて実行します。  :contentReference[oaicite:7]{index=7}
  #>
  $env:WS_RUN_SECONDS = "$WsSeconds"  # 短時間だけWS接続するテストにも使えます
  Invoke-Py 'scripts/ws_run.py'
  Invoke-Py 'scripts/compute_indicators.py'
  Invoke-Py 'scripts/run_signals.py'
  Invoke-Py 'scripts/place_orders.py'
}

function Do-Cancel {
  <#
    何をする関数？：
      - 10:30 ET 未約定の一括取消（紙トレは sent→cancelled へ移動）。時刻前は何もしません。  :contentReference[oaicite:8]{index=8}
  #>
  Invoke-Py 'scripts/cancel_unfilled.py'
}

function Do-Close {
  <#
    何をする関数？：
      - クローズ前の強制クローズ（紙トレは残ファイル整理）。時刻前は何もしません。  :contentReference[oaicite:9]{index=9}
  #>
  Invoke-Py 'scripts/close_positions.py'
}

function Do-Kpi {
  <#
    何をする関数？：
      - 紙の約定ログ executions.csv を集計して、当日KPIを kpi_daily.csv に upsert します。  :contentReference[oaicite:10]{index=10}
  #>
  Invoke-Py 'scripts/daily_kpi.py'
}

### RUN_ALL ENTRY ###
Set-RepoEnv
Set-RunMode
Write-Host ("run_all: Phase={0}  WS_RUN_SECONDS={1}  RUN_MODE={2}" -f $Phase,$WsSeconds,$env:RUN_MODE) -ForegroundColor Green

switch ($Phase) {
  'nightly' { Do-Nightly }
  'session' { Do-Session }
  'cancel'  { Do-Cancel }
  'close'   { Do-Close }
  'kpi'     { Do-Kpi }
  'all'     {
    # ランブックの1日フロー：市場時間に依存する処理は各スクリプト側の時刻ガードに任せます。  :contentReference[oaicite:11]{index=11}
    Do-Nightly
    Do-Session
    Do-Cancel
    Do-Close
    Do-Kpi
  }
  default   { Do-Session }
}
