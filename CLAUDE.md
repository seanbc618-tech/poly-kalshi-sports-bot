# poly-kalshi-sports-bot 交接说明

Kalshi ↔ Polymarket 的 NBA 跨平台套利扫描器。个人项目，保持简单。

## 结构
- `rust-backend/`：主程序（Axum，端口 8000），扫描、匹配、算利润、自动下单，前端静态文件打包进二进制
  - `src/clients/kalshi.rs`：只拉 `KXNBAGAME` 系列；`src/clients/polymarket.rs`：Gamma API + 下单转发给 Python 服务
  - `src/core/matcher.rs` + `nba_teams.rs`：两边比赛配对；`calculator.rs`：利润计算
  - `src/api/mod.rs`：后台任务（扫描、自动下单队列/执行器）和路由
- `web/`：React 前端，`npm run build` 后复制到 `rust-backend/static`（编译时必须存在）
- `poly-order-service/`：Python（py-clob-client），Polymarket 下单用，端口 8001
- `deploy_monitor.sh`：一键部署监控模式到服务器

## 监控模式（当前在用）
- 配置顶层 `monitor_only = true`：不启动自动下单任务，`ArbitrageService` 的三个下单方法直接报错
- 监控仍需 Kalshi API key（盘口 WS 要签名）；Polymarket 不需要密钥，也不用跑 Python 服务

## 部署 / 运维
```bash
./deploy_monitor.sh root@服务器IP     # 首次部署和以后更新都是这一条
```
- 本地需要 node、rsync、ssh；服务器 Debian/Ubuntu，root 或免密 sudo，建议 ≥2GB 内存（要编译 Rust）
- 服务器上：程序和配置在 `/opt/polytaoli/`（`config.toml`、`arbitrage_history.db`、`logs/`），源码在 `~/polytaoli-src`
- systemd 服务名 `polytaoli`：`systemctl restart polytaoli`、`journalctl -u polytaoli -f`
- 面板 `http://服务器IP:8000`，记得改掉默认 admin/admin123
- 更新时不会覆盖服务器上已有的 `config.toml`

## 本地开发
```bash
cd web && npm install && npm run build && rm -rf ../rust-backend/static && cp -r dist ../rust-backend/static
cd ../rust-backend && cp config.example.toml config.toml   # 填 Kalshi key
cargo run --release
```

## 当前状态 / 下一步
- [x] 监控模式 + 一键部署脚本（编译通过；假密钥启动能走到请求 Kalshi 这一步）
- [ ] 真实 Kalshi key + 真服务器上跑通，确认能出套利机会
- [ ] 跑一段时间，看 NBA 价差出现频率和大小，再决定要不要开自动下单
- [ ] 可选：扩展 NFL/MLB/NHL（加 series ticker、队名表、匹配规则）
