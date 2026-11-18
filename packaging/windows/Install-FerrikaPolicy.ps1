<#
.SYNOPSIS
    Installs the signed Ferrika company policy machine-wide (Intune / GPO / SCCM).

.DESCRIPTION
    - Creates Ferrika\ in the ProgramData known folder (the folder Ferrika reads;
      not the %ProgramData% environment variable, which any user can change).
    - Copies company.policy, policy.pub and, when given, policy.keys into it.
    - Locks the folder down: owner BUILTIN\Administrators, inheritance off,
      Administrators and SYSTEM full control, Users read and execute only.
      A folder that already exists (for example one a user created first, and
      therefore owns) gets its owner and ACL reset, and so does everything in it.
    - Registers the Application event log source "Ferrika" used by the audit
      anchor (policy "audit_anchor": "os_log").
    - With -SpoolDir, creates Ferrika\spool for rules.audit_forward.spool_dir:
      users may add files there and change their own, but not other users' files.

    Run elevated (Intune: "System" context; GPO: computer startup script).
    -WhatIf shows every step without changing anything and does not need
    administrator rights. Exit code 0 on success, 1 on failure.

.EXAMPLE
    .\Install-FerrikaPolicy.ps1 -SourceDir .\policy -WhatIf

.EXAMPLE
    .\Install-FerrikaPolicy.ps1 -SourceDir \\fileserver\ferrika\policy -SpoolDir
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    # Folder containing company.policy, policy.pub and optionally policy.keys.
    [Parameter(Mandatory = $true)]
    [string]$SourceDir,

    # Only lock the folder down and register the event source; copy nothing.
    [switch]$AclOnly,

    # Also create the audit forwarding spool folder (Ferrika\spool).
    [switch]$SpoolDir,

    # Skip registering the "Ferrika" event log source.
    [switch]$NoEventSource,

    # Install into this folder instead of <ProgramData>\Ferrika (packaging tests only;
    # Ferrika release builds read only the ProgramData folder).
    [string]$TargetDir
)

Set-StrictMode -Version 2.0
$ErrorActionPreference = 'Stop'

$PolicyFile = 'company.policy'
$PublicKeyFile = 'policy.pub'
$KeyListFile = 'policy.keys'
$EventSource = 'Ferrika'

$Administrators = New-Object System.Security.Principal.SecurityIdentifier 'S-1-5-32-544'
$System = New-Object System.Security.Principal.SecurityIdentifier 'S-1-5-18'
$Users = New-Object System.Security.Principal.SecurityIdentifier 'S-1-5-32-545'
$CreatorOwner = New-Object System.Security.Principal.SecurityIdentifier 'S-1-3-0'

function Test-Elevated {
    $principal = New-Object System.Security.Principal.WindowsPrincipal(
        [System.Security.Principal.WindowsIdentity]::GetCurrent())
    return $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Get-PolicyDir {
    if ($TargetDir) { return $TargetDir }
    # SHGetFolderPath(CSIDL_COMMON_APPDATA): the same known folder Ferrika resolves.
    $programData = [Environment]::GetFolderPath([Environment+SpecialFolder]::CommonApplicationData)
    if (-not $programData) { $programData = 'C:\ProgramData' }
    return (Join-Path $programData 'Ferrika')
}

function Assert-NotReparsePoint([string]$Path) {
    # A user could pre-create the folder (or a file in it) as a junction or
    # symlink to a place they control; refuse rather than follow it.
    $item = Get-Item -LiteralPath $Path -Force
    if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
        throw "$Path is a junction or symbolic link. Remove it and run again."
    }
}

function New-RuleSet([bool]$IsDirectory) {
    if ($IsDirectory) {
        $security = New-Object System.Security.AccessControl.DirectorySecurity
        $inherit = [System.Security.AccessControl.InheritanceFlags]'ContainerInherit, ObjectInherit'
    } else {
        $security = New-Object System.Security.AccessControl.FileSecurity
        $inherit = [System.Security.AccessControl.InheritanceFlags]::None
    }
    $none = [System.Security.AccessControl.PropagationFlags]::None
    $allow = [System.Security.AccessControl.AccessControlType]::Allow
    $full = [System.Security.AccessControl.FileSystemRights]::FullControl
    $read = [System.Security.AccessControl.FileSystemRights]::ReadAndExecute
    $security.SetOwner($Administrators)
    # Protected: nothing inherited from ProgramData (which lets Users create files).
    $security.SetAccessRuleProtection($true, $false)
    foreach ($rule in @(
            (New-Object System.Security.AccessControl.FileSystemAccessRule($Administrators, $full, $inherit, $none, $allow)),
            (New-Object System.Security.AccessControl.FileSystemAccessRule($System, $full, $inherit, $none, $allow)),
            (New-Object System.Security.AccessControl.FileSystemAccessRule($Users, $read, $inherit, $none, $allow)))) {
        $security.AddAccessRule($rule)
    }
    return $security
}

function Set-LockedAcl([string]$Path, [bool]$IsDirectory) {
    if (-not $PSCmdlet.ShouldProcess($Path, 'Set owner Administrators; ACL Administrators/SYSTEM full, Users read; no inheritance')) {
        return
    }
    $security = New-RuleSet $IsDirectory
    if ($IsDirectory) {
        [System.IO.Directory]::SetAccessControl($Path, $security)
    } else {
        [System.IO.File]::SetAccessControl($Path, $security)
    }
}

function Set-SpoolAcl([string]$Path) {
    if (-not $PSCmdlet.ShouldProcess($Path, 'Set spool ACL: Users may create files, CREATOR OWNER modifies own files')) {
        return
    }
    $security = New-RuleSet $true
    $allow = [System.Security.AccessControl.AccessControlType]::Allow
    $none = [System.Security.AccessControl.PropagationFlags]::None
    # Users: add files to this folder only (no delete, no subfolder ACL changes).
    $security.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule(
                $Users,
                [System.Security.AccessControl.FileSystemRights]'CreateFiles, AppendData, ReadAndExecute',
                [System.Security.AccessControl.InheritanceFlags]::None, $none, $allow)))
    # The account that created a spool file may keep writing to it.
    $security.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule(
                $CreatorOwner,
                [System.Security.AccessControl.FileSystemRights]::Modify,
                [System.Security.AccessControl.InheritanceFlags]::ObjectInherit,
                [System.Security.AccessControl.PropagationFlags]::InheritOnly, $allow)))
    [System.IO.Directory]::SetAccessControl($Path, $security)
}

function Reset-Owner([string]$Path) {
    # takeown uses SeTakeOwnershipPrivilege, so it works even when a user who
    # owns the folder has removed the administrators' access to it.
    if ($PSCmdlet.ShouldProcess($Path, 'Take ownership for BUILTIN\Administrators (recursive)')) {
        $output = & takeown.exe /F $Path /A /R /D Y 2>&1
        if ($LASTEXITCODE -ne 0) { throw "takeown failed for ${Path}: $output" }
    }
}

function Register-EventSource {
    $exists = $false
    try {
        $exists = [System.Diagnostics.EventLog]::SourceExists($EventSource)
    } catch {
        # Reading the Security log's sources needs elevation; assume missing.
        $exists = $false
    }
    if ($exists) {
        Write-Verbose "Event log source '$EventSource' already registered."
        return
    }
    if ($PSCmdlet.ShouldProcess("Application log", "Register event source '$EventSource'")) {
        New-EventLog -LogName Application -Source $EventSource
    }
}

try {
    $whatIf = [bool]$WhatIfPreference
    if (-not $whatIf -and -not (Test-Elevated)) {
        throw 'Run this script elevated (administrator or SYSTEM), or with -WhatIf to preview.'
    }

    $sourceFiles = @()
    if (-not $AclOnly) {
        foreach ($name in @($PolicyFile, $PublicKeyFile)) {
            $path = Join-Path $SourceDir $name
            if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
                # Ferrika treats a lone key or a lone policy as invalid
                # (strictest mode), so never install one without the other.
                throw "Missing $path. Both $PolicyFile and $PublicKeyFile are required."
            }
            $sourceFiles += $path
        }
        $keys = Join-Path $SourceDir $KeyListFile
        if (Test-Path -LiteralPath $keys -PathType Leaf) { $sourceFiles += $keys }
    }

    $dir = Get-PolicyDir
    Write-Verbose "Policy folder: $dir"

    if (Test-Path -LiteralPath $dir) {
        if (-not (Test-Path -LiteralPath $dir -PathType Container)) {
            throw "$dir exists and is not a folder."
        }
        Assert-NotReparsePoint $dir
        Reset-Owner $dir
    } elseif ($PSCmdlet.ShouldProcess($dir, 'Create folder')) {
        New-Item -ItemType Directory -Path $dir | Out-Null
    }

    # Lock the folder before copying, so the files never sit in a writable folder.
    Set-LockedAcl $dir $true

    $spool = Join-Path $dir 'spool'
    if (Test-Path -LiteralPath $dir) {
        foreach ($item in Get-ChildItem -LiteralPath $dir -Force -Recurse) {
            if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
                throw "$($item.FullName) is a junction or symbolic link. Remove it and run again."
            }
            # The spool folder keeps its own ACL (below); users' spool files stay theirs.
            if ($item.FullName -eq $spool -or $item.FullName.StartsWith($spool + [System.IO.Path]::DirectorySeparatorChar)) { continue }
            Set-LockedAcl $item.FullName $item.PSIsContainer
        }
    }

    foreach ($file in $sourceFiles) {
        $dest = Join-Path $dir (Split-Path $file -Leaf)
        if ($PSCmdlet.ShouldProcess($dest, "Copy from $file")) {
            Copy-Item -LiteralPath $file -Destination $dest -Force
        }
        Set-LockedAcl $dest $false
    }

    # An old policy.keys left behind would keep trusting a retired key.
    if (-not $AclOnly -and -not ($sourceFiles | Where-Object { (Split-Path $_ -Leaf) -eq $KeyListFile })) {
        $stale = Join-Path $dir $KeyListFile
        if ((Test-Path -LiteralPath $stale) -and $PSCmdlet.ShouldProcess($stale, 'Remove (not in the source folder)')) {
            Remove-Item -LiteralPath $stale -Force
        }
    }

    if ($SpoolDir) {
        if (-not (Test-Path -LiteralPath $spool)) {
            if ($PSCmdlet.ShouldProcess($spool, 'Create folder')) {
                New-Item -ItemType Directory -Path $spool | Out-Null
            }
        } else {
            Assert-NotReparsePoint $spool
        }
        Set-SpoolAcl $spool
    }

    if (-not $NoEventSource) { Register-EventSource }

    if ($whatIf) {
        Write-Output "WhatIf: nothing was changed in $dir"
    } else {
        Write-Output "Ferrika policy installed in $dir"
    }
    exit 0
} catch {
    Write-Error -ErrorAction Continue $_
    exit 1
}
