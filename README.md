# hermes-kchat

Infomaniak **kChat** platform adapter for [Hermes Agent](https://github.com/NousResearch/hermes-agent).

## Requirements

Requires hermes-agent>=0.22.5

## Install (directory plugin — the supported path)

`hermes plugins enable <name>` only recognises **directory** plugins
(`~/.hermes/plugins/<name>/`) and bundled plugins — it does **not** recognise pip
*entry-point* plugins. So install kChat as a directory plugin (this is how the
built-in adapters ship). No `pip install` into the hermes
environment is needed — directory plugins load by file path, and `aiohttp` is
already a hermes dependency.

```bash
scripts/install.sh            # symlinks src/hermes_kchat -> ~/.hermes/plugins/kchat
hermes plugins enable kchat   # now recognised; takes effect next session
```

Or by hand:

```bash
ln -s "$PWD/src/hermes_kchat" ~/.hermes/plugins/kchat
hermes plugins enable kchat
```

> The symlink keeps the plugin live-editable from this repo. Use
> `scripts/install.sh --copy` to copy the files instead, or set `HERMES_HOME` to
> target a non-default hermes home.
>
> A `pip install` + `hermes_agent.plugins` entry point is also declared in
> `pyproject.toml` and the loader *discovers* it, but the `enable` CLI won't
> recognise it (see above) — prefer the directory install.

## Configure

```bash
export KCHAT_URL="https://your-org.kchat.infomaniak.com"
export KCHAT_TOKEN="your-bot-token"
# optional:
export KCHAT_WEBSOCKET_URL="websocket.kchat.infomaniak.com"   # default
export KCHAT_PROXY="http://proxy:8080"    # REST + websocket; SOCKS needs aiohttp_socks
export KCHAT_ALLOWED_USERS="userid1,userid2"
export KCHAT_HOME_CHANNEL="channelid"
export KCHAT_REPLY_MODE="thread"          # nest replies (default: off)
export KCHAT_REQUIRE_MENTION="true"       # require @bot in channels
```

Create the bot token on the kChat **Integrations** page. Some kChat actions
require administrator rights on the bot account, grant them if posting/upload
fails with a permissions error.

## Develop / test

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
# pusher.py tests run with only aiohttp:
pytest tests/test_pusher.py -v
# adapter/standalone tests need the hermes-agent host (they importorskip otherwise).
# vendor/ holds a gitignored checkout — fetch and pin it yourself:
git clone https://github.com/NousResearch/hermes-agent vendor/hermes-agent
git -C vendor/hermes-agent checkout v0.22.5
pip install -e vendor/hermes-agent   # re-run after changing the checkout: refreshes the editable finder
pytest -v
```
