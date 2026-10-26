# Installation and operation

WeKeeper 0.5.0 is a local Linux WeChat archive. The distribution and import name
remain `chatlog-keeper` and `chatlog_keeper`; the CLI is `chatlog-keeper` and the
MCP server identifier is `wechat-archive`.

## Verified environment

The real-client baseline was verified on Ubuntu 26.04 x86_64, Python 3.11.15 and
official Linux WeChat 4.1.13.26. A passing synthetic test suite on another Linux
version does not establish real-client compatibility. Client health and native
voice conversion recognize a reviewed executable build and fail closed when it
changes. Pure Wayland or a different build may report `unknown` or unsupported.

Use an existing official WeChat installation and your own local account data.
The repository does not redistribute the client. Reading needs cached,
account-bound database keys; initial acquisition additionally needs GCC and GDB
with Python support. Native conversion also needs the system Python's GI/AT-SPI
bindings and a working graphical D-Bus/X11 session. These optional desktop
components are separate from the Python virtual environment.

On Ubuntu, the relevant system packages can be installed explicitly:

```bash
sudo apt-get install gcc gdb python3-venv python3-gi gir1.2-atspi-2.0 libglib2.0-bin
```

Use a Python 3.11+ interpreter for the project environment; `python3.11` below
denotes the verified baseline. Native helpers use `/usr/bin/python3` for the
system GI bindings. Installing these packages does not enable native conversion.

## Install from source

```bash
git clone https://github.com/RethinkCraft/WeKeeper.git
cd WeKeeper
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[mcp]'
export CHATLOG_KEEPER_DATA_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/wekeeper"
chatlog-keeper probe
```

Keep this state directory outside the checkout and use the same absolute path
for key initialization, collection and MCP. If unset, the implementation uses
`$XDG_DATA_HOME/chatlog-keeper`, or `~/.local/share/chatlog-keeper` when XDG is unset.
The state contains plaintext chat data and media as well as secrets. Created
private directories use mode 0700 and files use 0600.

`probe` reads client/account discovery and cached-key verification status. It
does not acquire keys, start or log in to WeChat. With multiple accounts, select
one account directory ID explicitly using `--account`; `--data-root` accepts the
directory containing those account directories, normally `xwechat_files`.

## Explicit initialization

First log in to the official client yourself and let local databases appear.
For the reviewed database-key capture flow, normally exit WeChat rather than
only closing its window, then explicitly run:

```bash
chatlog-keeper extract-key --account ACCOUNT_DIRECTORY_ID --timeout 180
chatlog-keeper probe
```

`extract-key` launches the official client under the capture flow and verifies
the candidate against the database HMAC before saving it. It is an explicit
setup action, not part of collection or MCP. Handle any required login or phone
confirmation yourself. Never publish its private state or key material.

If using cached V2 images, explicitly initialize image material after opening
an image in the client yourself so its full cache is present:

```bash
chatlog-keeper image-key --account ACCOUNT_DIRECTORY_ID
```

No background collector or ordinary MCP query opens images to create a cache.
Only locally present data can be archived; unavailable history or media remains
unavailable. Recheck compatibility after a client update.

## Commands

Run `chatlog-keeper COMMAND --help` for each command's exact flags. All eight
commands accept optional `--data-root` and `--account` selectors.

| Command | Behavior and command-specific options |
|---|---|
| `probe` | Read-only client/account and cached-key status |
| `extract-key` | Explicit client launch and verified database-key capture; `--timeout` 10–600 seconds, default 180 |
| `groups` | Read local group IDs and names using cached keys |
| `messages` | Read one selected group: required `--group`, `--minutes` 1–1440 (default 60), `--limit` 1–1000 (default 100) |
| `image-key` | Explicit image-material initialization for the reviewed Linux build |
| `image` | Decode one local `.dat` path; required `--out` destination |
| `collect` | Archive all discovered groups; `--state-dir`, `--interval` 1–3600 seconds (default 30), `--page-size` 1–1000 (default 500), optional `--once` |
| `mcp` | Serve the archive over stdio; optional `--state-dir` |

The direct `messages` command reads a bounded page from the source. Use MCP for
archive pagination, search, context and statistics. Duplicate group names require
the native group ID. Example values below are placeholders, not real identities:

```bash
chatlog-keeper groups --account ACCOUNT_DIRECTORY_ID
chatlog-keeper messages --account ACCOUNT_DIRECTORY_ID --group GROUP_ID --minutes 60 --limit 100
chatlog-keeper collect --state-dir "$CHATLOG_KEEPER_DATA_DIR" --account ACCOUNT_DIRECTORY_ID --interval 30 --page-size 500
```

The collector prioritizes new rows, then resumes bounded backfill and reconciliation.
A single `--once` cycle may not finish history. Do not run a second collector
against a live one; an exclusive lock rejects concurrent collection instances.
Closing Codex does not stop a separately running collector. Exiting WeChat stops
new remote synchronization but does not erase the archive.

## User systemd service

Use [the service template](../deploy/wechat-archive-collector.service.example).
Replace all three placeholders before installation:

| Placeholder | Replacement |
|---|---|
| `@PROJECT_ROOT@` | Absolute path of the WeKeeper checkout |
| `@PYTHON@` | Absolute path of its virtual environment's Python |
| `@STATE_DIR@` | The same absolute private state path used above |

Edit a copy in a text editor, preserving the unit's quoting and escaping any
literal `%` in a replacement as `%%`. An unexpanded placeholder is not a usable
service. Inspect the resulting paths, then install that rendered file:

```bash
install -Dm600 /absolute/path/to/rendered.service "$HOME/.config/systemd/user/wechat-archive-collector.service"
systemctl --user daemon-reload
systemctl --user enable --now wechat-archive-collector.service
systemctl --user status wechat-archive-collector.service
journalctl --user -u wechat-archive-collector.service -n 50 --no-pager
```

Run the foreground collector or the service, not both. The service is tied to the
user session by default. Its desktop alerts need that user's graphical session
and notification settings; a running service alone does not prove WeChat is online.

## MCP connection

The package must be installed with the `mcp` extra. Use the absolute interpreter
and state paths in [the MCP template](../examples/mcp-config.toml). The equivalent
Codex registration is:

```bash
codex mcp add wechat-archive --env CHATLOG_KEEPER_DATA_DIR=/absolute/private/state --env CHATLOG_KEEPER_RUNTIME_VERSION=0.5.0 -- /absolute/WeKeeper/.venv/bin/python -m chatlog_keeper.mcp_server --state-dir /absolute/private/state
```

Initialize the archive with collection before querying. Reload the MCP connection
after deployment changes, then check `get_status.version`, schema and the fifteen
tools from the actual conversation that will use them. An independent stdio test
does not prove that an existing conversation has reloaded. See [MCP usage](mcp.md).

## Optional native voice conversion

Raw SILK archiving and existing transcript queries work without UI automation.
New conversion is disabled on unverified installations. Registering the tool or
enabling accessibility does not certify it: an operator must first validate exact
targeting, a real native result, association and offline reading for that build.
There is no automatic capability-enablement command in this release.

The reviewed scope is `same_day_unique_visible_history`: a target must be a
top-level voice from today, uniquely visible after group/member filtering. A
separate read-only child must prove that the current authenticated `VoiceInfo`
cache still contains the unique group/server-ID/time record with the archived
audio hash. Archive-only audio is insufficient to authorize a client operation.

The optional preparation helper is self-contained:

```bash
python scripts/restart_wechat_accessibility.py --state-dir "$CHATLOG_KEEPER_DATA_DIR" --prepare
```

Preparation is the default. It records the reviewed client and original
accessibility settings in `STATE_DIR/setup/accessibility.json`; it does not restart
the client. Arrange
any restart explicitly after reviewing the preparation result:

```bash
python scripts/restart_wechat_accessibility.py --state-dir "$CHATLOG_KEEPER_DATA_DIR" --restart
```

The helper must stop if preparation no longer matches. The explicit restart mode
applies the required recorded accessibility changes and Qt startup environment.
It never force-kills,
logs in, confirms a phone prompt or sends a message. To roll back, use the saved
original-settings record and the documented rollback mode after arranging that
restart:

```bash
python scripts/restart_wechat_accessibility.py --state-dir "$CHATLOG_KEEPER_DATA_DIR" --restart --without-accessibility
```

Conversion jobs run one at a time outside the collector loop with a 90-second
deadline and parent-exit protection. Ambiguity, missing cache, login loss, an
unsupported UI/build or timeout stops the operation. It does not download audio,
type in the composer, use OCR, or invoke a local/third-party ASR service.

## Upgrade, backup and recovery

Version 0.5.0 uses schema 4 and parser 4. Opening a writable archive from schema
1, 2 or 3 first creates a private consistent backup under `archive/backups/`.
Historical reparsing proceeds in batches of at most 500 per collector cycle,
while retaining message references, first-seen timestamps and source checkpoints.
Restart the collector and reload MCP after an upgrade; old processes cannot read
the new schema. Inspect `get_status.semantics` for pending work or source gaps.

For a full consistent backup, stop the collector, copy the entire private state
directory including media and decoding material to a protected destination, then
start the collector again. Do not restore only the SQLite file while omitting its
media. Existing archived content remains readable when the source disappears;
new source/key failures are reported and never trigger automatic login or capture.
