# Deploying the company policy

Scripts that install a signed company policy ([docs/POLICY.md](../docs/POLICY.md))
on developer machines. They install the policy only, not the Ferrika
binary. Roll out the binary first: an older binary that gets a policy with
fields it does not know runs in strictest mode.

| File | For |
|---|---|
| [`windows/Install-FerrikaPolicy.ps1`](windows/Install-FerrikaPolicy.ps1) | Intune (Win32 app or platform script), GPO computer startup script, SCCM |
| [`windows/FerrikaPolicy.wxs`](windows/FerrikaPolicy.wxs) | A WiX v4/v5 fragment for an MSI |
| [`linux/install-ferrika-policy.sh`](linux/install-ferrika-policy.sh) | Linux and macOS (`/etc/ferrika`), for Ansible, Puppet, a package post-install script |

All of them take a source folder that holds `company.policy` and
`policy.pub` (from `ferrika policy keygen` / `policy sign`) and, during a key
rotation, `policy.keys`. The private key `policy-signing.pk8` never goes
into it.

## Why the folder permissions matter

Ferrika reads the policy from a machine-wide folder that normal users must
not be able to write: `Ferrika\` in the ProgramData known folder on Windows,
`/etc/ferrika` elsewhere. ProgramData itself lets any user create a
subfolder, and the creator owns it. A user who creates
`C:\ProgramData\Ferrika` before the policy is deployed could later delete
or replace the policy files (Ferrika then runs without a policy). The
scripts therefore do not trust an existing folder: they take ownership for
Administrators and replace its ACL, and that of everything in it.

| | Windows | Linux / macOS |
|---|---|---|
| Folder | owner Administrators; inheritance off; Administrators and SYSTEM full control; Users read and execute | `root:root` (`root:wheel` on macOS) `0755` |
| Files | same, explicit on each file | `root:root` `0644` |
| Existing folder or files | owner taken (`takeown /A`), ACL replaced | owner and mode reset |
| Junction / symlink at the policy path | refused | refused |
| `policy.keys` not in the source folder | removed from the target (a retired key stops being trusted) | removed |

## Windows: PowerShell (Intune, GPO)

```powershell
# Preview: changes nothing, needs no administrator rights
.\Install-FerrikaPolicy.ps1 -SourceDir .\policy -WhatIf

# Install (elevated, or as SYSTEM)
.\Install-FerrikaPolicy.ps1 -SourceDir .\policy

# Also create Ferrika\spool for rules.audit_forward.spool_dir
.\Install-FerrikaPolicy.ps1 -SourceDir .\policy -SpoolDir

# Only fix the folder ACL and the event source (after an MSI install)
.\Install-FerrikaPolicy.ps1 -SourceDir . -AclOnly
```

Exit code 0 on success, 1 on failure (the reason goes to stderr), so Intune
and SCCM report it. `-Verbose` prints each step.

- **Intune:** package the script and the policy folder as a Win32 app
  (`IntuneWinAppUtil`), install command
  `powershell.exe -NoProfile -ExecutionPolicy Bypass -File Install-FerrikaPolicy.ps1 -SourceDir .`,
  install behaviour *System*. Detection rule: file
  `%ProgramData%\Ferrika\company.policy` exists (a new policy needs a new
  detection value, for example its modified date or a version in the app).
  A platform script works too, but it cannot carry the policy files: put
  them on a share readable by computer accounts and pass `-SourceDir \\server\share`.
- **GPO:** *Computer Configuration → Policies → Windows Settings → Scripts →
  Startup*, PowerShell script with parameter `-SourceDir \\server\share\ferrika-policy`.
  Startup scripts run as SYSTEM; grant *Domain Computers* read on the share.
- The script uses the ProgramData known folder
  (`[Environment]::GetFolderPath('CommonApplicationData')`), the same folder
  Ferrika resolves, not the `%ProgramData%` variable.

The `spool` folder (`-SpoolDir`) keeps the policy folder's rights plus:
Users may create files in it, and the account that created a file may write
to it (`CREATOR OWNER` modify). Users can read, but not change or delete,
other accounts' spool files.

### Event log source

`audit_anchor: os_log` writes to the Application log with source
`Ferrika` (event ID 1000). Writing works without a registered source, but
Event Viewer then shows "The description for Event ID 1000 from source
Ferrika cannot be found". The script registers it
(`New-EventLog -LogName Application -Source Ferrika`; skip with
`-NoEventSource`). Remove it with `Remove-EventLog -Source Ferrika`.

## Windows: MSI (WiX)

[`FerrikaPolicy.wxs`](windows/FerrikaPolicy.wxs) installs the same files
with the same rights (`PermissionEx` SDDL, Windows Installer 5.0+) and the
event source registry key. Reference the `FerrikaPolicy` component group
from a per-machine package:

```text
wix build Product.wxs FerrikaPolicy.wxs -d PolicySourceDir=policy [-d PolicyKeys=1]
```

The SDDL also sets the owner of the folder (Administrators), but Windows
Installer leaves files it did not install alone. If users may have created
the folder before, also run `Install-FerrikaPolicy.ps1 -AclOnly` (for
example as a deferred custom action or a one-off Intune script). Uninstalling
the MSI removes the policy, so Ferrika then runs without one: keep the
package installed, or deploy policy updates as a major upgrade.

## Linux / macOS

```sh
./install-ferrika-policy.sh --dry-run ./policy   # show the steps only
sudo ./install-ferrika-policy.sh ./policy
```

Files are written to a temporary name and renamed, so a running Ferrika
never reads a half-written policy (Ferrika reads it once at start-up).

### Audit anchor on Linux: journald / syslog

`audit_anchor: os_log` writes to syslog with ident `ferrika`, facility
`auth`. Ferrika does not read it back (`audit verify --anchor` only says
so); compare by hand:

```sh
journalctl -t ferrika --since today          # systemd-journald
grep 'ferrika\[' /var/log/auth.log           # rsyslog, Debian/Ubuntu
grep 'ferrika\[' /var/log/secure             # rsyslog, RHEL/Fedora
log show --predicate 'process == "ferrika"'  # macOS unified log
```

- Many distributions keep the journal in memory only (`/run/log/journal`).
  For the anchor to survive a reboot, set `Storage=persistent` in
  `/etc/systemd/journald.conf` (or create `/var/log/journal`).
- Normal users cannot read `auth` messages on most systems (`adm` /
  `systemd-journal` group), which is what you want: the anchor is for the
  security team.
- To ship anchors off the machine, forward the `auth` facility from rsyslog
  (`auth.* @@siem.example.com:6514`) or use `rules.audit_forward`.

## Checking an installation

```text
ferrika policy show
```

shows the active policy, its issuer and the trusted keys. On Windows,
`icacls "%ProgramData%\Ferrika"` should list exactly `BUILTIN\Administrators:(OI)(CI)(F)`,
`NT AUTHORITY\SYSTEM:(OI)(CI)(F)` and `BUILTIN\Users:(OI)(CI)(RX)`, and
`(Get-Acl "$env:ProgramData\Ferrika").Owner` should be `BUILTIN\Administrators`.
On Linux, `ls -ld /etc/ferrika /etc/ferrika/*`.

## Executable names

The CLI is built as `ferrika` (`ferrika.exe`). The separately distributed
commercial desktop app uses the names `Ferrika.exe` / `Ferrika.app`. The two
names must stay different crates' outputs: on a case-insensitive file system
(Windows, macOS) `ferrika.exe` and `Ferrika.exe` are the same file.
