<# 何をするスクリプト？
   - BOT_WEBULL の各工程を Phase 引数で実行する“共通エントリ”
   - 使い方（例）:
       powershell.exe -File E:\BOT_WEBULL\run_all.ps1 -Phase nightly
       powershell.exe -File E:\BOT_WEBULL\run_all.ps1 -Phase session
       powershell.exe -File E:\BOT_WEBULL\run_all.ps1 -Phase indicators
       powershell.exe -File E:\BOT_WEBULL\run_all.ps1 -Phase signals
       powershell.exe -File E:\BOT_WEBULL\run_all.ps1 -Phase orders
       powershell.exe -File E:\BOT_WEBULL\run_all.ps1 -Phase cancel
       powershell.exe -File E:\BOT_WEBULL\run_all.ps1 -Phase close
       powershell.exe -File E:\BOT_WEBULL\run_all.ps1 -Phase kpi
   - タスクスケジューラでは Action に上記を設定し、Start in は必ず E:\BOT_WEBULL にする
#>

param(
  [Parameter(Mandatory = $true)]
  [ValidateSet('nightly','session','indicators','signals','orders','cancel','close','kpi')]
  [string]$Phase
)

# 何をする行？：実行ディレクトリをリポジトリ直下に固定（Start in がずれても安心）
$ErrorActionPreference = 'Stop'
if ($PSScriptRoot) { Set-Location -LiteralPath $PSScriptRoot } else { Set-Location -LiteralPath 'E:\BOT_WEBULL' }

# 何をする関数？：.venv があればそれを優先し、無ければ poetry 経由で Python を実行
function Invoke-Py {
  param([Parameter(Mandatory)][string]$RelPath)
  $venvPy = Join-Path $PWD '.venv\Scripts\python.exe'
  $script = Join-Path $PWD $RelPath
  if (Test-Path $venvPy) {
    & $venvPy $script
  } else {
    & poetry run python $script
  }
  if ($LASTEXITCODE -ne 0) { throw "Python exited with code $LASTEXITCODE for $RelPath" }
}

# 何をする関数？：Phase ごとに実行するスクリプトを1つ選んで呼ぶ
function Invoke-Phase {
  param([string]$Name)
  switch ($Name) {
    'nightly'     { Invoke-Py 'scripts\nightly_screen.py' ; break }
    'session'     {
      # 何をする行？：短すぎ防止の下限（90秒）を未設定時だけ補う
      if (-not $env:WS_RUN_SECONDS) { $env:WS_RUN_SECONDS = '90' }
      Invoke-Py 'scripts\ws_run.py' ; break
    }
    'indicators'  { Invoke-Py 'scripts\compute_indicators.py' ; break }
    'signals'     { Invoke-Py 'scripts\run_signals.py' ; break }
    'orders'      { Invoke-Py 'scripts\place_orders.py' ; break }
    'cancel'      { Invoke-Py 'scripts\cancel_unfilled.py' ; break }
    'close'       { Invoke-Py 'scripts\close_positions.py' ; break }
    'kpi'         { Invoke-Py 'scripts\daily_kpi.py' ; break }
    default       { throw "Unknown phase: $Name" }
  }
}

# 何をする行？：開始ログ（人間が見ても分かりやすく）
Write-Host ("[run_all] start Phase={0}  PWD={1}  at {2}" -f $Phase, $PWD, (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))

# 何をする行？：本体の実行
Invoke-Phase -Name $Phase

# 何をする行？：終了ログ
Write-Host ("[run_all] done  Phase={0}  exit=0  at {1}" -f $Phase, (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))
exit 0
