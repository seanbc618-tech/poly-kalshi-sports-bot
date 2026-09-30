//! API Layer
//!
//! HTTP routes and WebSocket server for the frontend.

pub mod routes;
pub mod websocket;
pub mod static_files;

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use axum::{
    routing::{get, post, delete, put},
    Router,
};
use chrono::Utc;
use tokio::sync::{mpsc, RwLock};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{info, warn, error};

use crate::config::Config;
use crate::models::PriceUpdate;
use crate::services::{ArbitrageService, PerformanceMetrics, TelegramClient};

/// Market scan interval in seconds (5 minutes)
const MARKET_SCAN_INTERVAL_SECS: u64 = 300;

/// Application state shared across handlers
pub struct AppState {
    pub service: RwLock<ArbitrageService>,
    pub config: Config,
    pub telegram_client: Arc<TelegramClient>,
}

/// Create the Axum application
pub async fn create_app(config: Config) -> Result<Router> {
    // Initialize the arbitrage service
    let mut service = ArbitrageService::new(&config).await?;
    service.initialize().await?;

    // Get metrics reference before moving service
    let metrics = service.metrics.clone();

    // Create price update channel
    let (price_tx, mut price_rx) = mpsc::channel::<PriceUpdate>(10000);

    // Start WebSocket connections
    service.start_websocket_connections(price_tx).await?;

    // Start periodic scanning
    service.run_periodic_scan(config.settings.refresh_interval).await;

    // Initialize Telegram client
    let telegram_client = Arc::new(TelegramClient::new(config.telegram.clone()));
    if telegram_client.is_enabled() {
        info!("✅ Telegram 通知已启用");
    } else {
        info!("ℹ️ Telegram 通知未启用");
    }

    // Create shared state
    let state = Arc::new(AppState {
        service: RwLock::new(service),
        config: config.clone(),
        telegram_client,
    });

    // Spawn price update handler
    let state_clone = state.clone();
    tokio::spawn(async move {
        while let Some(update) = price_rx.recv().await {
            let service = state_clone.service.read().await;
            service.ws_manager.on_price_update(update);
        }
    });

    // Spawn metrics reporter and API ping tester (every 10 seconds)
    let state_for_metrics = state.clone();
    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        
        loop {
            interval.tick().await;
            
            // Perform API ping tests
            ping_apis(&state_for_metrics, &metrics_clone).await;
            
            // Reset metrics for next period (metrics are sent via WebSocket in websocket.rs)
            metrics_clone.reset();
        }
    });

    // Spawn periodic market scanner (every 5 minutes)
    let state_for_scanner = state.clone();
    tokio::spawn(async move {
        info!("🔍 市场扫描任务启动，间隔 {} 秒 ({} 分钟)", 
            MARKET_SCAN_INTERVAL_SECS, 
            MARKET_SCAN_INTERVAL_SECS / 60
        );
        
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(MARKET_SCAN_INTERVAL_SECS));
        let mut scan_count = 0u64;
        
        // Wait for initial WebSocket connections to establish
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        
        loop {
            interval.tick().await;
            scan_count += 1;
            
            info!("🔄 开始第 {} 次定期市场扫描...", scan_count);
            
            // Scan for new markets
            let scan_result = {
                let mut service = state_for_scanner.service.write().await;
                service.scan_for_new_markets().await
            };
            
            match scan_result {
                Ok((new_markets, sub_info)) => {
                    if new_markets.is_empty() {
                        info!("✅ 第 {} 次扫描完成，没有发现新市场", scan_count);
                    } else {
                        info!("🆕 发现 {} 个新匹配市场，开始热订阅...", new_markets.len());
                        
                        // Update WebSocketManager with new markets
                        {
                            let service = state_for_scanner.service.read().await;
                            let added = service.ws_manager.add_matched_markets(
                                new_markets.clone(),
                                sub_info.market_lookup.clone(),
                            );
                            info!("📊 已添加 {} 个市场到 WebSocket 管理器", added);
                        }
                        
                        // Hot subscribe to new markets
                        let kalshi_success = {
                            let service = state_for_scanner.service.read().await;
                            if !sub_info.kalshi_tickers.is_empty() {
                                match service.kalshi_client.subscribe_markets(sub_info.kalshi_tickers.clone()).await {
                                    Ok(success) => success,
                                    Err(e) => {
                                        error!("❌ Kalshi 热订阅失败: {}", e);
                                        false
                                    }
                                }
                            } else {
                                true
                            }
                        };
                        
                        let poly_success = {
                            let service = state_for_scanner.service.read().await;
                            if !sub_info.polymarket_token_ids.is_empty() {
                                match service.polymarket_client.subscribe_tokens(sub_info.polymarket_token_ids.clone()).await {
                                    Ok(success) => success,
                                    Err(e) => {
                                        error!("❌ Polymarket 热订阅失败: {}", e);
                                        false
                                    }
                                }
                            } else {
                                true
                            }
                        };
                        
                        if kalshi_success && poly_success {
                            let service = state_for_scanner.service.read().await;
                            let total_markets = service.ws_manager.get_matched_markets_for_frontend().len();
                            info!("✅ 热订阅成功，当前共 {} 个配对市场", total_markets);
                            
                            // Broadcast scan stats to frontend via WebSocket
                            let scan_stats = crate::models::ScanStats {
                                scan_count,
                                new_markets_found: new_markets.len(),
                                total_matched_markets: total_markets,
                                timestamp: Utc::now(),
                            };
                            service.ws_manager.broadcast_scan_stats(scan_stats);
                            info!("📡 已广播扫描统计到前端");
                        } else {
                            warn!("⚠️ 热订阅部分失败");
                        }
                    }
                }
                Err(e) => {
                    error!("❌ 市场扫描失败: {}", e);
                }
            }
        }
    });

    if config.monitor_only {
        info!("👀 监控模式：自动下单已禁用，不会下任何单");
    } else {
        // Spawn auto-trade queue checker (every 200ms for faster response)
        let state_for_queue = state.clone();
        let metrics_for_queue = metrics.clone();
        tokio::spawn(async move {
            info!("📋 自动下单队列检查任务启动，间隔 200ms");
        
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(200));
        
            loop {
                interval.tick().await;
            
                // Check and add opportunities to queue
                check_and_queue_auto_trade(&state_for_queue, &metrics_for_queue).await;
            }
        });

        // Spawn auto-trade executor (processes queue with 1s interval)
        let state_for_executor = state.clone();
        let metrics_for_executor = metrics.clone();
        tokio::spawn(async move {
            info!("🚀 自动下单执行器启动，机会间隔 1 秒");
        
            loop {
                let service = state_for_executor.service.read().await;
            
                // Check if already executing
                if service.ws_manager.is_auto_trading.load(std::sync::atomic::Ordering::Relaxed) {
                    drop(service);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }
            
                // Get next opportunity from queue
                let key = {
                    let mut queue = service.ws_manager.auto_trade_queue.write();
                    queue.pop_front()
                };
            
                if let Some(key) = key {
                    // Mark as executing
                    service.ws_manager.is_auto_trading.store(true, std::sync::atomic::Ordering::Relaxed);
                
                    info!("🎯 [自动下单执行器] 开始处理: {}", key);
                
                    // Execute the opportunity
                    execute_single_auto_trade(&service, &state_for_executor, &metrics_for_executor, &key).await;
                
                    // Mark as done
                    service.ws_manager.is_auto_trading.store(false, std::sync::atomic::Ordering::Relaxed);
                
                    drop(service);
                
                    info!("⏱️  [自动下单执行器] 等待 1 秒后处理下一个机会");
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                } else {
                    // Queue empty, wait a bit
                    drop(service);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        });
    }

    // Spawn ended market cleanup task (every 60 seconds)
    let state_for_cleanup = state.clone();
    tokio::spawn(async move {
        info!("🧹 已结束比赛清理任务启动，间隔 60 秒");
        
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        
        // Wait for initial data to be populated
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        
        loop {
            interval.tick().await;
            
            // Check and clean up ended markets
            cleanup_ended_markets(&state_for_cleanup).await;
        }
    });

    // Build router
    let app = Router::new()
        // Health check
        .route("/api/health", get(routes::health_check))
        // Authentication
        .route("/api/auth/login", post(routes::login))
        // Stats and data
        .route("/api/stats", get(routes::get_stats))
        .route("/api/data-coverage", get(routes::get_data_coverage))
        .route("/api/opportunities", get(routes::get_opportunities))
        .route("/api/matched-markets", get(routes::get_matched_markets))
        .route("/api/arbitrage-history", get(routes::get_arbitrage_history))
        // Account info
        .route("/api/balance/kalshi", get(routes::get_kalshi_balance))
        .route("/api/balance/polymarket", get(routes::get_polymarket_balance))
        .route("/api/account-balance", get(routes::get_account_balance))
        // Orders
        .route("/api/order/kalshi", post(routes::place_kalshi_order))
        .route("/api/order/polymarket", post(routes::place_polymarket_order))
        .route("/api/arbitrage/execute", post(routes::execute_arbitrage))
        // Order management
        .route("/api/orders/kalshi", get(routes::get_kalshi_orders))
        .route("/api/orders/polymarket", get(routes::get_polymarket_orders))
        .route("/api/orders/kalshi/:order_id", delete(routes::cancel_kalshi_order))
        .route("/api/orders/polymarket/:order_id", delete(routes::cancel_polymarket_order))
        // Position management
        .route("/api/positions/kalshi", get(routes::get_kalshi_positions))
        .route("/api/positions/polymarket", get(routes::get_polymarket_positions))
        // Tracking
        .route("/api/tracking", get(routes::get_tracking))
        // History search
        .route("/api/history/search", get(routes::search_history))
        .route("/api/history/statistics", get(routes::get_history_statistics))
        // Orderbook depth
        .route("/api/orderbook/depth", get(routes::get_orderbook_depth))
        // Auto-trade
        .route("/api/auto-trade/status", get(routes::get_auto_trade_status))
        .route("/api/auto-trade/enable", post(routes::enable_auto_trade))
        .route("/api/auto-trade/disable", post(routes::disable_auto_trade))
        .route("/api/auto-trade/reset", post(routes::reset_auto_trade))
        .route("/api/auto-trade/settings", put(routes::update_auto_trade_settings))
        .route("/api/auto-trade/history", get(routes::get_auto_trade_history))
        .route("/api/auto-trade/excluded", get(routes::get_excluded_markets))
        .route("/api/auto-trade/exclude", post(routes::exclude_market))
        .route("/api/auto-trade/unexclude", post(routes::unexclude_market))
        .route("/api/auto-trade/queue", get(routes::get_auto_trade_queue))
        // App settings (hot-updatable)
        .route("/api/settings", get(routes::get_app_settings))
        .route("/api/settings", put(routes::update_app_settings))
        // WebSocket
        .route("/ws", get(websocket::ws_handler))
        // Add state
        .with_state(state)
        // Add middleware
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .layer(TraceLayer::new_for_http())
        // Static files - must be last!
        .fallback(static_files::static_handler);

    info!("✅ API 路由配置完成（包含前端静态文件）");

    Ok(app)
}

/// Ping both APIs to measure latency and cache balances
async fn ping_apis(state: &Arc<AppState>, metrics: &Arc<PerformanceMetrics>) {
    // Test Kalshi API latency and cache balance
    let kalshi_start = Instant::now();
    let service = state.service.read().await;
    
    match service.kalshi_client.get_balance().await {
        Ok(balance) => {
            let latency_ms = kalshi_start.elapsed().as_millis() as u64;
            metrics.set_kalshi_latency(latency_ms);
            metrics.set_kalshi_balance(balance);
        }
        Err(e) => {
            warn!("Kalshi API ping 失败: {}", e);
        }
    }
    
    // Test Polymarket API latency and cache balance
    let poly_start = Instant::now();
    match service.polymarket_client.get_balance().await {
        Ok(balance) => {
            let latency_ms = poly_start.elapsed().as_millis() as u64;
            metrics.set_polymarket_latency(latency_ms);
            metrics.set_polymarket_balance(balance);
        }
        Err(e) => {
            warn!("Polymarket API ping 失败: {}", e);
        }
    }
}

/// Calculate the number of contracts to trade based on depth and settings
/// 
/// Returns None if depth is insufficient (below min_contracts)
fn calculate_contracts_to_trade(
    kalshi_depth: i32,
    poly_depth_contracts: i32,
    flexible_mode: bool,
    min_contracts: i32,
    max_contracts: i32,
) -> Option<i32> {
    let min_depth = kalshi_depth.min(poly_depth_contracts);
    
    // Check minimum depth requirement
    if min_depth < min_contracts {
        return None;
    }
    
    // Fixed mode: always use min_contracts
    if !flexible_mode {
        return Some(min_contracts);
    }
    
    // Flexible mode logic:
    // - Depth 10-20: use min_contracts (10)
    // - Depth >= 20: use half of the smaller depth
    let contracts = if min_depth < 20 {
        min_contracts
    } else {
        min_depth / 2
    };
    
    // Apply max_contracts limit
    Some(contracts.min(max_contracts))
}

/// Check and add eligible opportunities to auto-trade queue
async fn check_and_queue_auto_trade(state: &Arc<AppState>, _metrics: &Arc<PerformanceMetrics>) {
    let service = state.service.read().await;
    
    // Get auto-trade state
    let auto_state = service.ws_manager.get_auto_trade_state();
    
    // Early return if not enabled or already at limit
    if !auto_state.enabled {
        return;
    }
    
    if auto_state.trade_count >= auto_state.max_trade_count {
        return;
    }
    
    // Get active tracking records
    let tracking_records = service.ws_manager.get_active_tracking_for_auto_trade();
    
    // Add eligible opportunities to queue
    for (key, _record, duration_ms) in tracking_records {
        // Check eligibility
        let (eligible, _reason) = service.ws_manager.check_auto_trade_eligibility(&key, duration_ms);
        
        if !eligible {
            continue;
        }
        
        // Add to queue if not already queued
        let mut queue = service.ws_manager.auto_trade_queue.write();
        if !queue.contains(&key) {
            queue.push_back(key.clone());
            info!("📋 [自动下单队列] 添加机会: {}", key);
        }
    }
}

/// Execute auto-trade for a single opportunity (extracted from original for loop)
async fn execute_single_auto_trade(
    service: &ArbitrageService,
    state: &Arc<AppState>,
    metrics: &Arc<PerformanceMetrics>,
    key: &str,
) {
    // Revalidate opportunity (may have expired)
    let (record, duration_ms) = match service.ws_manager.get_tracking_record(key) {
        Some((r, d)) => (r, d),
        None => {
            info!("⚠️  [自动下单] 机会已不存在: {}", key);
            return;
        }
    };
    
    // Recheck eligibility
    let auto_state = service.ws_manager.get_auto_trade_state();
    let (eligible, reason) = service.ws_manager.check_auto_trade_eligibility(key, duration_ms);
    
    if !eligible {
        info!("⚠️  [自动下单] 机会不再符合条件: {} - {}", key, reason);
        return;
    }
    
    // Get current opportunity data
    let opportunity = match service.ws_manager.get_opportunity_by_key(key) {
        Some(opp) => opp,
        None => {
            info!("⚠️  [自动下单] 机会数据不存在: {}", key);
            return;
        }
    };
        
        info!("🤖 [自动下单] 检测到符合条件的套利机会:");
        info!("   事件: {} - {}", record.event_name, record.team_name);
        info!("   持续时间: {}ms, 利润率: {:.2}%", duration_ms, opportunity.profit_margin);
        info!("   原因: {}", reason);
        info!("   模式: {} (最小={}, 最大={})", 
            if auto_state.flexible_mode { "灵活" } else { "固定" },
            auto_state.min_contracts, auto_state.max_contracts);
        
        // Get poly token first for depth validation
        let poly_token = match service.ws_manager.get_poly_token_for_side(
            &record.event_name,
            &record.team_name,
            &opportunity.polymarket_side,
        ) {
            Some(token) => token,
            None => {
                let skip_reason = format!("无法获取 Polymarket token ({}侧)", opportunity.polymarket_side);
                error!("❌ {}: {} - {}", skip_reason, record.event_name, record.team_name);
                if service.ws_manager.should_record_skip(&key, &skip_reason) {
                    let storage = service.ws_manager.get_storage();
                    let _ = storage.save_skipped_auto_trade_record(
                        &record.event_name, &record.team_name,
                        &record.kalshi_market_id, &record.polymarket_market_id,
                        &opportunity.kalshi_side, &opportunity.polymarket_side,
                        auto_state.min_contracts, opportunity.kalshi_price, opportunity.polymarket_price,
                        opportunity.profit_margin, duration_ms, &skip_reason,
                    );
                }
                return;
            }
        };
        
        // Get depth information for contract calculation
        let kalshi_depth = service.ws_manager.get_kalshi_ask_depth(
            &record.kalshi_market_id, 
            &opportunity.kalshi_side
        );
        
        // Get poly depth in contracts (depth_usd / price = contracts)
        let (poly_depth_usd, _poly_size) = service.ws_manager.get_poly_ask_depth_and_size(&poly_token);
        let poly_depth_contracts = if opportunity.polymarket_price > 0.0 {
            (poly_depth_usd / opportunity.polymarket_price).floor() as i32
        } else {
            0
        };
        
        info!("   深度: Kalshi={} 合约, Poly={} 合约 (${:.2})", 
            kalshi_depth, poly_depth_contracts, poly_depth_usd);
        
        // Calculate contracts to trade using flexible/fixed logic
        let contracts_to_trade = match calculate_contracts_to_trade(
            kalshi_depth,
            poly_depth_contracts,
            auto_state.flexible_mode,
            auto_state.min_contracts,
            auto_state.max_contracts,
        ) {
            Some(c) => c,
            None => {
                let skip_reason = format!(
                    "深度不足: Kalshi={}, Poly={} (需要最少 {} 合约)",
                    kalshi_depth, poly_depth_contracts, auto_state.min_contracts
                );
                info!("   ⚠️ {}", skip_reason);
                if service.ws_manager.should_record_skip(&key, &skip_reason) {
                    let storage = service.ws_manager.get_storage();
                    let _ = storage.save_skipped_auto_trade_record(
                        &record.event_name, &record.team_name,
                        &record.kalshi_market_id, &record.polymarket_market_id,
                        &opportunity.kalshi_side, &opportunity.polymarket_side,
                        auto_state.min_contracts, opportunity.kalshi_price, opportunity.polymarket_price,
                        opportunity.profit_margin, duration_ms, &skip_reason,
                    );
                }
                return;
            }
        };
        
        info!("   计算合约数: {} 份", contracts_to_trade);
        
        // Calculate amounts based on contracts
        let kalshi_contracts = contracts_to_trade;
        let kalshi_bet = contracts_to_trade as f64 * opportunity.kalshi_price;
        
        // Calculate Kalshi trading fee: fee = 0.07 × C × P × (1-P), rounded up to cent
        let kalshi_fee_raw = 0.07 * kalshi_contracts as f64 * opportunity.kalshi_price * (1.0 - opportunity.kalshi_price);
        let kalshi_fee = (kalshi_fee_raw * 100.0).ceil() / 100.0;
        
        // Polymarket: same number of "contracts" (equivalent amount), no trading fee
        let poly_amount = contracts_to_trade as f64 * opportunity.polymarket_price;
        
        // Calculate total investment including fees
        let total_bet = kalshi_bet + kalshi_fee + poly_amount;
        
        // Verify total doesn't exceed max_amount
        if total_bet > auto_state.max_amount {
            let skip_reason = format!("总投入 ${:.2} 超过限额 ${:.2}", total_bet, auto_state.max_amount);
            info!("   ⚠️ {}", skip_reason);
            if service.ws_manager.should_record_skip(&key, &skip_reason) {
                let storage = service.ws_manager.get_storage();
                let _ = storage.save_skipped_auto_trade_record(
                    &record.event_name, &record.team_name,
                    &record.kalshi_market_id, &record.polymarket_market_id,
                    &opportunity.kalshi_side, &opportunity.polymarket_side,
                    contracts_to_trade, opportunity.kalshi_price, opportunity.polymarket_price,
                    opportunity.profit_margin, duration_ms, &skip_reason,
                );
            }
            return;
        }
        
        // === Balance check from local cache (no API call needed) ===
        let (kalshi_balance, poly_balance) = metrics.get_cached_balances();
        
        let kalshi_required = kalshi_bet + kalshi_fee;
        let poly_required = poly_amount;
        
        match (kalshi_balance, poly_balance) {
            (Some(k_bal), Some(p_bal)) => {
                if k_bal < kalshi_required {
                    let skip_reason = format!(
                        "Kalshi 余额不足: 需要 ${:.2}, 可用 ${:.2}",
                        kalshi_required, k_bal
                    );
                    info!("   ⚠️ {}", skip_reason);
                    if service.ws_manager.should_record_skip(&key, &skip_reason) {
                        let storage = service.ws_manager.get_storage();
                        let _ = storage.save_skipped_auto_trade_record(
                            &record.event_name, &record.team_name,
                            &record.kalshi_market_id, &record.polymarket_market_id,
                            &opportunity.kalshi_side, &opportunity.polymarket_side,
                            contracts_to_trade, opportunity.kalshi_price, opportunity.polymarket_price,
                            opportunity.profit_margin, duration_ms, &skip_reason,
                        );
                    }
                    return;
                }
                if p_bal < poly_required {
                    let skip_reason = format!(
                        "Polymarket 余额不足: 需要 ${:.2}, 可用 ${:.2}",
                        poly_required, p_bal
                    );
                    info!("   ⚠️ {}", skip_reason);
                    if service.ws_manager.should_record_skip(&key, &skip_reason) {
                        let storage = service.ws_manager.get_storage();
                        let _ = storage.save_skipped_auto_trade_record(
                            &record.event_name, &record.team_name,
                            &record.kalshi_market_id, &record.polymarket_market_id,
                            &opportunity.kalshi_side, &opportunity.polymarket_side,
                            contracts_to_trade, opportunity.kalshi_price, opportunity.polymarket_price,
                            opportunity.profit_margin, duration_ms, &skip_reason,
                        );
                    }
                    return;
                }
                info!("   ✅ 余额检查通过: Kalshi ${:.2}/{:.2}, Poly ${:.2}/{:.2}",
                    kalshi_required, k_bal, poly_required, p_bal);
            }
            _ => {
                info!("   ⚠️ 余额缓存未就绪，跳过余额检查（等待下次ping_apis更新）");
            }
        }
        
        // === Pre-order depth and price validation from local orderbook cache ===
        let (depth_valid, validated_k_depth, validated_p_depth, current_k_price, current_p_price, validation_reason) = 
            service.ws_manager.validate_auto_trade_depth(
                &record.kalshi_market_id,
                &opportunity.kalshi_side,
                &poly_token,
                contracts_to_trade,
            );
        
        if !depth_valid {
            info!("   ⚠️ [深度/价格检查] 跳过下单: {}", validation_reason);
            if service.ws_manager.should_record_skip(&key, &validation_reason) {
                let storage = service.ws_manager.get_storage();
                let _ = storage.save_skipped_auto_trade_record(
                    &record.event_name, &record.team_name,
                    &record.kalshi_market_id, &record.polymarket_market_id,
                    &opportunity.kalshi_side, &opportunity.polymarket_side,
                    contracts_to_trade, opportunity.kalshi_price, opportunity.polymarket_price,
                    opportunity.profit_margin, duration_ms, &validation_reason,
                );
            }
            return;
        }
        
        info!("   ✅ [深度/价格检查] {}", validation_reason);
        info!("      Kalshi 深度: {} 合约, Poly 深度: ${:.2}", validated_k_depth, validated_p_depth);
        info!("      当前价格: K={:.4}, P={:.4}, 价格和={:.4}", 
            current_k_price, current_p_price, current_k_price + current_p_price);
        
        // Recalculate amounts using current prices from local orderbook
        let kalshi_price_cents = (current_k_price * 100.0).round() as i32;
        let kalshi_bet = contracts_to_trade as f64 * current_k_price;
        let kalshi_fee_raw = 0.07 * kalshi_contracts as f64 * current_k_price * (1.0 - current_k_price);
        let kalshi_fee = (kalshi_fee_raw * 100.0).ceil() / 100.0;
        let poly_amount = contracts_to_trade as f64 * current_p_price;
        let total_bet = kalshi_bet + kalshi_fee + poly_amount;
        
        // Check if either side price exceeds 0.90 (90 cents) - avoid late-stage markets
        if current_k_price > 0.90 || current_p_price > 0.90 {
            let skip_reason = format!(
                "价格过高风险 - Kalshi: {:.4}, Polymarket: {:.4} (任一方超过0.90)",
                current_k_price, current_p_price
            );
            info!("   ⚠️ {}", skip_reason);
            if service.ws_manager.should_record_skip(&key, &skip_reason) {
                let storage = service.ws_manager.get_storage();
                let _ = storage.save_skipped_auto_trade_record(
                    &record.event_name, &record.team_name,
                    &record.kalshi_market_id, &record.polymarket_market_id,
                    &opportunity.kalshi_side, &opportunity.polymarket_side,
                    contracts_to_trade, current_k_price, current_p_price,
                    opportunity.profit_margin, duration_ms, &skip_reason,
                );
            }
            return;
        }
        
        // Check if either side price is below 0.10 (10 cents) - avoid low-probability markets
        if current_k_price < 0.10 || current_p_price < 0.10 {
            let skip_reason = format!(
                "价格过低风险 - Kalshi: {:.4}, Polymarket: {:.4} (任一方低于0.10)",
                current_k_price, current_p_price
            );
            info!("   ⚠️ {}", skip_reason);
            if service.ws_manager.should_record_skip(&key, &skip_reason) {
                let storage = service.ws_manager.get_storage();
                let _ = storage.save_skipped_auto_trade_record(
                    &record.event_name, &record.team_name,
                    &record.kalshi_market_id, &record.polymarket_market_id,
                    &opportunity.kalshi_side, &opportunity.polymarket_side,
                    contracts_to_trade, current_k_price, current_p_price,
                    opportunity.profit_margin, duration_ms, &skip_reason,
                );
            }
            return;
        }
        
        // Re-verify total doesn't exceed max_amount with updated prices
        if total_bet > auto_state.max_amount {
            let skip_reason = format!("更新价格后总投入 ${:.2} 超过限额 ${:.2}", total_bet, auto_state.max_amount);
            info!("   ⚠️ {}", skip_reason);
            if service.ws_manager.should_record_skip(&key, &skip_reason) {
                let storage = service.ws_manager.get_storage();
                let _ = storage.save_skipped_auto_trade_record(
                    &record.event_name, &record.team_name,
                    &record.kalshi_market_id, &record.polymarket_market_id,
                    &opportunity.kalshi_side, &opportunity.polymarket_side,
                    contracts_to_trade, current_k_price, current_p_price,
                    opportunity.profit_margin, duration_ms, &skip_reason,
                );
            }
            return;
        }
        
        info!("   📊 订单计算 ({} {} 合约, 使用最新价格):", 
            if auto_state.flexible_mode { "灵活" } else { "固定" }, contracts_to_trade);
        info!("      Kalshi: {} 合约 @ {:.2}¢ = ${:.2} + 手续费 ${:.2} ({}侧)", 
            kalshi_contracts, kalshi_price_cents, kalshi_bet, kalshi_fee, opportunity.kalshi_side);
        info!("      Polymarket: {} 合约等价 = ${:.4} ({}侧)", contracts_to_trade, poly_amount, opportunity.polymarket_side);
        info!("      总投入: ${:.2} (含手续费) / ${:.2}", total_bet, auto_state.max_amount);
        
        // === DETAILED PRE-ORDER LOGGING ===
        info!("════════════════════════════════════════════════════════════");
        info!("📝 [自动下单-详细上下文]");
        info!("   市场信息:");
        info!("      事件名: {}", record.event_name);
        info!("      队伍名: {}", record.team_name);
        info!("      市场Key: {}", key);
        info!("      游戏日期: {:?}", record.game_date);
        info!("   Kalshi 订单:");
        info!("      market_id: {}", record.kalshi_market_id);
        info!("      side: {} (买{}侧)", opportunity.kalshi_side, opportunity.kalshi_side);
        info!("      contracts: {}", kalshi_contracts);
        info!("      price_cents: {}", kalshi_price_cents);
        info!("      当前价格: {:.4}", current_k_price);
        info!("   Polymarket 订单:");
        info!("      token_id: {}", poly_token);
        info!("      token_id (前20字符): {}...", &poly_token[..20.min(poly_token.len())]);
        info!("      side: buy (买{}侧)", opportunity.polymarket_side);
        info!("      amount: {:.4}", poly_amount);
        info!("      当前价格: {:.4}", current_p_price);
        info!("   原始机会数据:");
        info!("      polymarket_market_id: {}", record.polymarket_market_id);
        info!("      profit_margin: {:.4}%", opportunity.profit_margin);
        info!("      duration_ms: {}", duration_ms);
        
        // Write to debug log for post-mortem analysis
        {
            use std::io::Write;
            let debug_log = serde_json::json!({
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "location": "api/mod.rs:auto_trade_execution",
                "message": "自动下单执行前上下文",
                "data": {
                    "event_name": &record.event_name,
                    "team_name": &record.team_name,
                    "market_key": &key,
                    "game_date": format!("{:?}", record.game_date),
                    "kalshi": {
                        "market_id": &record.kalshi_market_id,
                        "side": &opportunity.kalshi_side,
                        "contracts": kalshi_contracts,
                        "price_cents": kalshi_price_cents,
                        "current_price": current_k_price,
                    },
                    "polymarket": {
                        "token_id": &poly_token,
                        "market_id": &record.polymarket_market_id,
                        "side": &opportunity.polymarket_side,
                        "amount": poly_amount,
                        "current_price": current_p_price,
                    },
                    "opportunity": {
                        "profit_margin": opportunity.profit_margin,
                        "duration_ms": duration_ms,
                    },
                    "amounts": {
                        "kalshi_bet": kalshi_bet,
                        "kalshi_fee": kalshi_fee,
                        "poly_amount": poly_amount,
                        "total_bet": total_bet,
                    }
                }
            });
            let path = crate::utils::get_debug_log_path();
            if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
                let _ = writeln!(f, "{}", debug_log.to_string());
            }
        }
        info!("════════════════════════════════════════════════════════════");
        
        // Mark as auto-traded BEFORE executing (to prevent duplicate orders)
        service.ws_manager.mark_as_auto_traded(&key);
        
        // End tracking for this opportunity (mark as completed in arbitrage_tracking table)
        // This ensures the tracking record has end_time set and can be queried later
        service.storage.track_end(&key);
        
        // Record execution start time for measuring order latency
        let exec_start = Instant::now();
        
        // Execute orders SEQUENTIALLY: Polymarket first, then Kalshi
        // This ensures Poly order is confirmed before placing Kalshi order
        info!("🚀 [串行执行订单] 先 Polymarket，成功后再 Kalshi...");
        info!("   Poly: {} tokens, 预计USDC={:.4}", contracts_to_trade, poly_amount);
        info!("   Kalshi: {} 合约 @ {}¢", kalshi_contracts, kalshi_price_cents);
        
        // Step 1: Place Polymarket order first
        let poly_start = Instant::now();
        let poly_result = service.polymarket_client.place_market_order_by_tokens(
            &poly_token,
            "buy",
            contracts_to_trade as f64,
        ).await;
        let poly_latency_ms = poly_start.elapsed().as_millis() as i64;
        
        // Check if Poly order succeeded
        let poly_success = poly_result.is_ok();
        let (poly_order_id, poly_error) = match &poly_result {
            Ok(res) => {
                let order_id = res.get("order_id").and_then(|v| v.as_str()).map(|s| s.to_string());
                let status = res.get("status").and_then(|v| v.as_str()).unwrap_or("unknown");
                
                // Check if order actually matched
                if status.to_uppercase() == "MATCHED" {
                    info!("✅ Poly订单成功成交: order_id={:?}, status={}", order_id, status);
                    (order_id, None)
                } else {
                    warn!("⚠️ Poly订单未成交: status={}, 取消Kalshi下单", status);
                    (order_id, Some(format!("订单状态为 {}, 未立即成交", status)))
                }
            },
            Err(e) => {
                warn!("❌ Poly订单失败: {}, 取消Kalshi下单", e);
                (None, Some(e.to_string()))
            }
        };
        
        // Step 2: Only place Kalshi order if Poly succeeded AND matched
        let (kalshi_result, kalshi_latency_ms) = if poly_success && poly_error.is_none() {
            info!("🎯 Poly订单已成交，继续下Kalshi订单...");
            let kalshi_start = Instant::now();
            let result = service.kalshi_client.place_order(
                &record.kalshi_market_id,
                "buy",
                &opportunity.kalshi_side,
                kalshi_contracts,
                kalshi_price_cents,
            ).await;
            let duration = kalshi_start.elapsed().as_millis() as i64;
            (result, duration)
        } else {
            warn!("🚫 Poly订单未成功，跳过Kalshi下单");
            (Err(anyhow::anyhow!("Poly订单未成功，已取消")), 0)
        };
        
        // Calculate total execution time
        let exec_duration_ms = exec_start.elapsed().as_millis() as i64;
        
        // Calculate total time from opportunity detection to order completion
        let total_duration_ms = Utc::now().signed_duration_since(record.start_time).num_milliseconds();
        
        // Extract results for logging and recording
        let kalshi_success = kalshi_result.is_ok();
        // poly_success already determined above based on MATCHED status
        let poly_success = poly_result.is_ok() && poly_error.is_none();
        
        // Extract Kalshi order ID and error
        let (kalshi_order_id, kalshi_error) = match &kalshi_result {
            Ok(res) => (res.get("order_id").and_then(|v| v.as_str()).map(|s| s.to_string()), None),
            Err(e) => (None, Some(e.to_string())),
        };
        
        // poly_order_id and poly_error already extracted above
        
        // Save trade record to database (using current validated prices)
        // Calculate actual profit margin based on current prices
        let actual_profit_margin = if total_bet > 0.0 {
            let expected_profit = contracts_to_trade as f64 - total_bet;
            (expected_profit / total_bet) * 100.0
        } else {
            0.0
        };
        
        let storage = service.ws_manager.get_storage();
        if let Err(e) = storage.save_auto_trade_record(
            &record.event_name,
            &record.team_name,
            &record.kalshi_market_id,
            &record.polymarket_market_id,
            &opportunity.kalshi_side,
            &opportunity.polymarket_side,
            kalshi_contracts,
            current_k_price,  // Use current validated price
            kalshi_fee,
            poly_amount,
            current_p_price,  // Use current validated price
            total_bet,
            actual_profit_margin,  // Use recalculated profit margin
            duration_ms,  // Opportunity duration before order (ms)
            total_duration_ms,  // Total time from detection to order completion (ms)
            kalshi_success,
            poly_success,
            kalshi_order_id.as_deref(),
            poly_order_id.as_deref(),
            kalshi_error.as_deref(),
            poly_error.as_deref(),
            kalshi_latency_ms,  // Kalshi API latency
            poly_latency_ms,    // Polymarket API latency
        ) {
            error!("保存自动下单记录失败: {}", e);
        }
        
        // Send Telegram notification (non-blocking, errors logged only)
        {
            let telegram = state.telegram_client.clone();
            let event_name = record.event_name.clone();
            let team_name = record.team_name.clone();
            let profit = actual_profit_margin;
            let k_err = kalshi_error.clone();
            let p_err = poly_error.clone();
            let expected_profit = contracts_to_trade as f64 - total_bet;
            
            tokio::spawn(async move {
                telegram.send_auto_trade_notification(
                    &event_name,
                    &team_name,
                    profit,
                    kalshi_success,
                    poly_success,
                    k_err.as_deref(),
                    p_err.as_deref(),
                    total_bet,
                    expected_profit,
                ).await;
            });
        }
        
        // Log results
        if kalshi_success && poly_success {
            // Increment trade count on success
            if let Ok(new_count) = service.ws_manager.increment_trade_count() {
                info!("✅ [自动下单] 成功! 已执行 {}/{} 次", new_count, auto_state.max_trade_count);
                info!("   ⏱️ 总耗时: {}ms (从发现机会到下单完成)", total_duration_ms);
                info!("   ⏱️ API延迟: Kalshi={}ms, Poly={}ms (并发执行总耗时: {}ms)", 
                    kalshi_latency_ms, poly_latency_ms, exec_duration_ms);
            }
            info!("   Kalshi: {:?}", kalshi_result.unwrap());
            info!("   Polymarket: {:?}", poly_result.unwrap());
        } else {
            // Still increment count even on partial failure to track attempts
            if let Ok(new_count) = service.ws_manager.increment_trade_count() {
                info!("⚠️ [自动下单] 部分成功，已记录 {}/{} 次", new_count, auto_state.max_trade_count);
                info!("   ⏱️ API延迟: Kalshi={}ms, Poly={}ms", kalshi_latency_ms, poly_latency_ms);
            }
            error!("❌ [自动下单] 部分失败:");
            if let Some(err) = &kalshi_error {
                error!("   Kalshi 失败 ({}ms): {}", kalshi_latency_ms, err);
            }
            if let Some(err) = &poly_error {
                error!("   Polymarket 失败 ({}ms): {}", poly_latency_ms, err);
            }
        }
}

/// Clean up ended markets by unsubscribing from WebSocket feeds
/// 
/// This function:
/// 1. Checks all markets for extreme prices (Kalshi 99/2, Poly 100/0)
/// 2. If extreme prices persist for 20+ minutes, marks market as ended
/// 3. Unsubscribes from WebSocket feeds for ended markets
/// 4. Removes ended markets from internal caches
async fn cleanup_ended_markets(state: &Arc<AppState>) {
    let service = state.service.read().await;
    
    // Get detection counts for logging
    let detecting_count = service.ws_manager.get_ending_detection_count();
    let ended_count = service.ws_manager.get_confirmed_ended_count();
    
    if detecting_count > 0 {
        info!(
            "🔍 [清理] 正在监控 {} 个潜在已结束市场，已确认 {} 个",
            detecting_count, ended_count
        );
    }
    
    // Remove ended markets and get subscription IDs to unsubscribe
    let (kalshi_to_unsub, poly_to_unsub) = service.ws_manager.remove_ended_markets();
    
    if kalshi_to_unsub.is_empty() && poly_to_unsub.is_empty() {
        return;
    }
    
    // Unsubscribe from Kalshi markets
    if !kalshi_to_unsub.is_empty() {
        match service.kalshi_client.unsubscribe_markets(kalshi_to_unsub.clone()).await {
            Ok(success) => {
                if success {
                    info!("✅ [清理] Kalshi 取消订阅成功: {} 个市场", kalshi_to_unsub.len());
                } else {
                    warn!("⚠️ [清理] Kalshi 取消订阅部分失败");
                }
            }
            Err(e) => {
                error!("❌ [清理] Kalshi 取消订阅失败: {}", e);
            }
        }
    }
    
    // Unsubscribe from Polymarket tokens
    if !poly_to_unsub.is_empty() {
        match service.polymarket_client.unsubscribe_tokens(poly_to_unsub.clone()).await {
            Ok(success) => {
                if success {
                    info!("✅ [清理] Polymarket 取消订阅成功: {} 个 token", poly_to_unsub.len());
                } else {
                    warn!("⚠️ [清理] Polymarket 取消订阅部分失败");
                }
            }
            Err(e) => {
                error!("❌ [清理] Polymarket 取消订阅失败: {}", e);
            }
        }
    }
    
    // Log summary
    let remaining_markets = service.ws_manager.get_matched_markets_for_frontend().len();
    info!(
        "✅ [清理] 完成，剩余 {} 个活跃市场",
        remaining_markets
    );
}
