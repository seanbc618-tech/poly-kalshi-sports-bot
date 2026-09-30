#!/bin/bash
# 一键部署「监控模式」到 Debian/Ubuntu 服务器（只看套利机会，不下单）
# 用法: ./deploy_monitor.sh root@1.2.3.4
# 要求: 本地有 node/npm、rsync、ssh；服务器用 root 或免密 sudo 登录
# 重复执行即更新：重新编译并重启，不覆盖服务器上已有的 config.toml
set -e

HOST="$1"
if [ -z "$HOST" ]; then
    echo "用法: $0 user@host"
    exit 1
fi
cd "$(dirname "$0")"

echo "== 1/3 本地构建前端 =="
(cd web && { [ -d node_modules ] || npm install; } && npm run build)
rm -rf rust-backend/static
cp -r web/dist rust-backend/static

echo "== 2/3 上传代码 =="
rsync -az --delete --exclude target --exclude 'config.toml*' --exclude logs --exclude '*.db*' \
    rust-backend/ "$HOST:polytaoli-src/"
# 首次部署时把本地 config.toml 带上去
if [ -f rust-backend/config.toml ] && ! ssh "$HOST" "test -f /opt/polytaoli/config.toml"; then
    scp rust-backend/config.toml "$HOST:polytaoli-src/config.toml"
fi

echo "== 3/3 服务器编译并启动 =="
ssh "$HOST" 'bash -s' <<'REMOTE'
set -e
SUDO=""
[ "$(id -u)" -ne 0 ] && SUDO=sudo

if ! command -v cc >/dev/null || ! command -v make >/dev/null || ! command -v perl >/dev/null; then
    $SUDO apt-get update
    $SUDO apt-get install -y build-essential pkg-config perl curl
fi
if ! command -v cargo >/dev/null && [ ! -x "$HOME/.cargo/bin/cargo" ]; then
    curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
source "$HOME/.cargo/env"

cd ~/polytaoli-src
cargo build --release

$SUDO mkdir -p /opt/polytaoli
$SUDO install -m 755 target/release/polytaoli /opt/polytaoli/polytaoli

NEED_EDIT=0
if [ ! -f /opt/polytaoli/config.toml ]; then
    if [ -f config.toml ]; then
        $SUDO cp config.toml /opt/polytaoli/config.toml
        rm -f config.toml
    else
        $SUDO cp config.example.toml /opt/polytaoli/config.toml
        NEED_EDIT=1
    fi
fi
# 没写 monitor_only 的配置，默认按监控模式跑
if ! grep -q '^monitor_only' /opt/polytaoli/config.toml; then
    $SUDO sed -i '1i monitor_only = true' /opt/polytaoli/config.toml
fi

$SUDO tee /etc/systemd/system/polytaoli.service >/dev/null <<'UNIT'
[Unit]
Description=Polytaoli monitor
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/opt/polytaoli
ExecStart=/opt/polytaoli/polytaoli
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT
$SUDO systemctl daemon-reload
$SUDO systemctl enable polytaoli >/dev/null

echo ""
if [ "$NEED_EDIT" = 1 ]; then
    echo "⚠️  已生成 /opt/polytaoli/config.toml，请填入 Kalshi API key 并修改登录密码，然后执行:"
    echo "    systemctl restart polytaoli"
else
    $SUDO systemctl restart polytaoli
    echo "✅ 已启动。面板: http://<服务器IP>:8000"
fi
echo "查看日志: journalctl -u polytaoli -f"
REMOTE
