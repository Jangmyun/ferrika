# Installs the ferrika CLI from a GitHub release (Windows, x64 and arm64
# through x64 emulation). Works in Windows PowerShell 5.1 and PowerShell 7.
#
#   irm https://github.com/Jangmyun/ferrika/releases/latest/download/install.ps1 | iex
#
# Environment:
#   FERRIKA_VERSION       release tag to install, for example v0.1.0 (default: latest)
#   FERRIKA_INSTALL_DIR   target folder (default: %LOCALAPPDATA%\Programs\ferrika\bin)
#   FERRIKA_NO_MODIFY_PATH=1   leave the user PATH alone
#
# The archive is checked against its published sha256 before anything is
# installed. The folder is added to the user PATH; no administrator rights.

# Everything runs inside a function, so a download cut off half way through
# `irm | iex` runs nothing and nothing leaks into the caller's session.
function Install-Ferrika {
    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'
    # Windows PowerShell 5.1 may default to TLS 1.0, which GitHub refuses.
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

    $repo = 'Jangmyun/ferrika'
    $target = 'x86_64-pc-windows-msvc'

    $version = $env:FERRIKA_VERSION
    if (-not $version) {
        $release = Invoke-RestMethod -UseBasicParsing -Uri "https://api.github.com/repos/$repo/releases/latest"
        $version = $release.tag_name
    }
    if (-not $version.StartsWith('v')) { $version = "v$version" }

    $dir = $env:FERRIKA_INSTALL_DIR
    if (-not $dir) { $dir = Join-Path $env:LOCALAPPDATA 'Programs\ferrika\bin' }

    $archive = "ferrika-$version-$target.zip"
    $url = "https://github.com/$repo/releases/download/$version/$archive"
    $tmp = Join-Path ([IO.Path]::GetTempPath()) ("ferrika-install-" + [Guid]::NewGuid())
    New-Item -ItemType Directory -Path $tmp | Out-Null
    try {
        Write-Host "ferrika-install: downloading ferrika $version for $target"
        $zip = Join-Path $tmp $archive
        Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $zip
        Invoke-WebRequest -UseBasicParsing -Uri "$url.sha256" -OutFile "$zip.sha256"

        $want = ((Get-Content -Raw "$zip.sha256").Trim() -split '\s+')[0].ToLowerInvariant()
        $got = (Get-FileHash -Algorithm SHA256 -Path $zip).Hash.ToLowerInvariant()
        if ($got -ne $want) { throw "checksum mismatch for $archive (expected $want, got $got)" }

        Expand-Archive -Path $zip -DestinationPath (Join-Path $tmp 'x')
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
        $exe = Join-Path $dir 'ferrika.exe'
        # A running ferrika.exe cannot be overwritten, but it can be renamed.
        if (Test-Path $exe) {
            $old = "$exe.old"
            Remove-Item -Force $old -ErrorAction SilentlyContinue
            Move-Item -Force $exe $old
        }
        try {
            Copy-Item (Join-Path $tmp 'x\ferrika.exe') $exe
        }
        catch {
            # Put the previous version back rather than leave none.
            if (Test-Path "$exe.old") { Move-Item -Force "$exe.old" $exe }
            throw
        }
        Remove-Item -Force "$exe.old" -ErrorAction SilentlyContinue
    }
    finally {
        Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
    }

    Write-Host "ferrika-install: installed $exe"
    & $exe --version

    if ($env:FERRIKA_NO_MODIFY_PATH -eq '1') { return }
    if (Add-UserPath $dir) {
        $env:Path = "$env:Path;$dir"
        Write-Host "ferrika-install: added $dir to your user PATH; open a new terminal to use 'ferrika'"
    }
}

# Appends $dir to the user PATH unless it is there already; $true when it
# was added. The value is read and written as stored (REG_EXPAND_SZ):
# [Environment]::SetEnvironmentVariable would write back the expanded text
# as REG_SZ, turning entries such as %USERPROFILE%\AppData\Local\Microsoft\
# WindowsApps into fixed paths. $subKey is for tests.
function Add-UserPath([string]$dir, [string]$subKey = 'Environment') {
    $key = [Microsoft.Win32.Registry]::CurrentUser.CreateSubKey($subKey)
    try {
        $raw = [string]$key.GetValue('Path', '', [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
        $parts = @($raw -split ';' | Where-Object { $_ })
        $want = $dir.TrimEnd('\')
        foreach ($part in $parts) {
            if ([Environment]::ExpandEnvironmentVariables($part).TrimEnd('\') -eq $want) { return $false }
        }
        $key.SetValue('Path', (($parts + $dir) -join ';'), [Microsoft.Win32.RegistryValueKind]::ExpandString)
    }
    finally {
        $key.Close()
    }
    if ($subKey -eq 'Environment') {
        # A registry write does not tell Explorer the environment changed;
        # setting a user variable through .NET does (WM_SETTINGCHANGE), so
        # set and remove a throwaway one. New terminals then see the PATH.
        [Environment]::SetEnvironmentVariable('FERRIKA_INSTALL_PATH_CHANGED', '1', 'User')
        [Environment]::SetEnvironmentVariable('FERRIKA_INSTALL_PATH_CHANGED', $null, 'User')
    }
    return $true
}

Install-Ferrika
