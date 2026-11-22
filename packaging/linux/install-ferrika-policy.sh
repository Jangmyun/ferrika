#!/bin/sh
# Installs the signed Ferrika company policy into /etc/ferrika (Linux, macOS).
#
#   sudo ./install-ferrika-policy.sh <source-dir>        # install
#   ./install-ferrika-policy.sh --dry-run <source-dir>   # show the steps only
#
# <source-dir> holds company.policy and policy.pub, optionally policy.keys.
# Result: /etc/ferrika root:root 0755, files root:root 0644. An existing
# /etc/ferrika (or file in it) gets its owner and mode reset; a symlink is
# refused. A policy.keys missing from <source-dir> is removed from /etc/ferrika
# so a retired key stops being trusted. The result is verified: the directory
# and everything in it must be owned by root and not writable by group or
# others (anyone who could write there could force strictest mode with a
# broken policy.keys), or the script fails. FERRIKA_POLICY_TARGET overrides the
# target directory for packaging tests (release builds read only /etc/ferrika).
set -eu

TARGET="${FERRIKA_POLICY_TARGET:-/etc/ferrika}"
# macOS has no "root" group; wheel is gid 0 there.
GROUP=root
if [ "$(uname -s)" = Darwin ]; then GROUP=wheel; fi

DRY_RUN=0
if [ "${1:-}" = "--dry-run" ]; then
    DRY_RUN=1
    shift
fi
if [ $# -ne 1 ]; then
    echo "usage: $0 [--dry-run] <source-dir>" >&2
    exit 2
fi
SRC=$1

run() {
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "would run: $*"
    else
        "$@"
    fi
}

for f in company.policy policy.pub; do
    if [ ! -f "$SRC/$f" ]; then
        # A lone key or a lone policy puts Ferrika into strictest mode.
        echo "missing $SRC/$f: company.policy and policy.pub are both required" >&2
        exit 1
    fi
done

if [ "$DRY_RUN" -eq 0 ] && [ "$(id -u)" -ne 0 ]; then
    echo "run as root (or with --dry-run)" >&2
    exit 1
fi

if [ -L "$TARGET" ]; then
    echo "$TARGET is a symbolic link; remove it and run again" >&2
    exit 1
fi
if [ -e "$TARGET" ] && [ ! -d "$TARGET" ]; then
    echo "$TARGET exists and is not a directory" >&2
    exit 1
fi

run mkdir -p "$TARGET"
run chown "root:$GROUP" "$TARGET"
run chmod 0755 "$TARGET"

# Write each file to a temporary name and rename it, so Ferrika never reads
# a half-written policy.
install_file() {
    run cp "$SRC/$1" "$TARGET/.$1.tmp"
    run chown "root:$GROUP" "$TARGET/.$1.tmp"
    run chmod 0644 "$TARGET/.$1.tmp"
    run mv -f "$TARGET/.$1.tmp" "$TARGET/$1"
}

install_file company.policy
install_file policy.pub
if [ -f "$SRC/policy.keys" ]; then
    install_file policy.keys
elif [ -e "$TARGET/policy.keys" ]; then
    run rm -f "$TARGET/policy.keys"
fi

# Anything else already in the directory: same owner, no write for others.
if [ -d "$TARGET" ]; then
    find "$TARGET" -mindepth 1 -type l -exec sh -c 'echo "symbolic link in policy directory: $1" >&2' _ {} \;
    run find "$TARGET" -mindepth 1 ! -type l -exec chown "root:$GROUP" {} +
    run find "$TARGET" -mindepth 1 ! -type l -exec chmod go-w {} +
fi

# "<uid> <octal mode>" of a path (GNU stat, then BSD/macOS stat).
owner_mode() {
    stat -c '%u %a' "$1" 2>/dev/null || stat -f '%u %Lp' "$1"
}

# Fails when the directory or anything in it is not owned by root or may be
# written by group or others.
verify_locked() {
    bad=0
    # One path per line, so names with spaces stay whole.
    old_ifs=$IFS
    IFS='
'
    for path in "$TARGET" $(find "$TARGET" -mindepth 1 ! -type l); do
        om=$(owner_mode "$path")
        uid=${om%% *}
        mode=${om##* }
        if [ "$uid" != 0 ]; then
            echo "not owned by root: $path (uid $uid)" >&2
            bad=1
        fi
        if [ $(( 0$mode & 022 )) -ne 0 ]; then
            echo "writable by group or others: $path (mode $mode)" >&2
            bad=1
        fi
    done
    IFS=$old_ifs
    if [ "$bad" -ne 0 ]; then
        echo "the policy directory is still writable by non-root accounts; fix the above and run again" >&2
        exit 1
    fi
}

if [ "$DRY_RUN" -eq 0 ]; then
    verify_locked
fi

if [ "$DRY_RUN" -eq 1 ]; then
    echo "dry run: nothing was changed in $TARGET"
else
    echo "Ferrika policy installed in $TARGET"
fi
