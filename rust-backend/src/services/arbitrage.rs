//! Arbitrage Service
//!
//! Orchestrates market data fetching, matching, and arbitrage scanning.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::clients::{KalshiClient, PolymarketClient};
use crate::config::Config;
use crate::core::{ArbitrageCalculator, EventMatcher, SubscriptionInfo};
use crate::models::{ArbitrageOpportunity, MatchedEvent, MatchedMarket, PriceUpdate, SystemStats};
use crate::services::{ArbitrageStorage, WebSocketManager, PerformanceMetrics, Operation};

/// Arbitrage service
pub struct ArbitrageService {
    pub kalshi_client: KalshiClient,
    pub polymarket_client: PolymarketClient,
    pub matcher: EventMatcher,
    pub calculator: ArbitrageCalculator,
    pub ws_manager: Arc<WebSocketManager>,
    pub storage: Arc<ArbitrageStorage>,
    pub matched_events: Vec<MatchedEvent>,
    pub matched_markets: Vec<MatchedMarket>,
    /// Performance metrics
    pub metrics: Arc<PerformanceMetrics>,
    /// 只监控不下单
    pub monitor_only: bool,
}

impl ArbitrageService {
    /// Create a new arbitrage service
    pub async fn new(config: &Config) -> Result<Self> {
        // Initialize storage
        let storage = Arc::new(ArbitrageStorage::new("arbitrage_history.db")?);

        // Initialize performance metrics
        let metrics = Arc::new(PerformanceMetrics::new());

        // Initialize clients
        let kalshi_client = KalshiClient::new(config.kalshi.clone())?;
        let mut polymarket_client = PolymarketClient::new(config.polymarket.clone());

        // Initialize Polymarket CLOB for order placement
        if let Err(e) = polymarket_client.init_clob().await {
            info!("Polymarket CLOB 初始化跳过: {}", e);
        }

        // Create matcher and calculator
        let matcher = EventMatcher::new(24);
        let calculator = ArbitrageCalculator::new(
            config.settings.min_profit_margin,
            config.settings.default_bet_amount,
        );

        // Create WebSocket manager with metrics
        let mut ws_manager = WebSocketManager::new(
            config.settings.min_profit_margin,
            config.settings.default_bet_amount,
            config.settings.tracking_threshold,
            storage.clone(),
            metrics.clone(),
        );

        // Set clients for orderbook depth queries
        ws_manager.set_clients(kalshi_client.clone(), polymarket_client.clone());
        
        // Load excluded markets from database
        ws_manager.load_excluded_markets();

        let ws_manager = Arc::new(ws_manager);

        Ok(Self {
            kalshi_client,
            polymarket_client,
            matcher,
            calculator,
            ws_manager,
            storage,
            matched_events: Vec::new(),
            matched_markets: Vec::new(),
            metrics,
            monitor_only: config.monitor_only,
        })
    }

    /// Initialize the service by fetching and matching markets
    pub async fn initialize(&mut self) -> Result<()> {
        info!("🔍 正在从两个平台获取市场数据...");

        // Fetch data from both platforms
        let (kalshi_events, kalshi_markets) = self
            .kalshi_client
            .get_nba_events_and_markets()
            .await?;

        let (polymarket_events, polymarket_markets) = self
            .polymarket_client
            .get_nba_events_and_markets()
            .await?;

        info!(
            "📊 已加载: Kalshi {} 个事件/{} 个市场, Polymarket {} 个事件/{} 个市场",
            kalshi_events.len(),
            kalshi_markets.len(),
            polymarket_events.len(),
            polymarket_markets.len()
        );

        // Match events and markets (with timing)
        let match_start = Instant::now();
        let (matched_events, matched_markets) = self.matcher.match_events_and_markets(
            &kalshi_events,
            &kalshi_markets,
            &polymarket_events,
            &polymarket_markets,
        );
        self.metrics.record(Operation::MarketMatch, match_start.elapsed());

        self.matched_events = matched_events;
        self.matched_markets = matched_markets.clone();

        // Configure WebSocket manager
        self.ws_manager.set_matched_markets(matched_markets);

        info!(
            "✅ 初始化完成: {} 个匹配的市场",
            self.matched_markets.len()
        );

        Ok(())
    }

    /// Start WebSocket connections for real-time updates
    pub async fn start_websocket_connections(&self, price_tx: mpsc::Sender<PriceUpdate>) -> Result<()> {
        let (kalshi_tickers, poly_tokens) = self.ws_manager.get_subscription_ids();

        info!(
            "📡 启动 WebSocket 连接: {} 个 Kalshi 市场, {} 个 Polymarket 代币",
            kalshi_tickers.len(),
            poly_tokens.len()
        );

        let kalshi_client = self.kalshi_client.clone();
        let polymarket_client = self.polymarket_client.clone();

        let price_tx_kalshi = price_tx.clone();
        let price_tx_poly = price_tx.clone();

        // Spawn Kalshi WebSocket
        let kalshi_tickers_clone = kalshi_tickers.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = kalshi_client
                    .connect_websocket(kalshi_tickers_clone.clone(), price_tx_kalshi.clone())
                    .await
                {
                    error!("Kalshi WebSocket 错误: {}. 5秒后重连...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        });

        // Spawn Polymarket WebSocket
        let poly_tokens_clone = poly_tokens.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = polymarket_client
                    .connect_websocket(poly_tokens_clone.clone(), price_tx_poly.clone())
                    .await
                {
                    error!("Polymarket WebSocket 错误: {}. 5秒后重连...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        });

        Ok(())
    }

    /// Run periodic market scanning
    pub async fn run_periodic_scan(&self, interval_secs: u64) {
        let ws_manager = self.ws_manager.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

            loop {
                interval.tick().await;
                let opportunities = ws_manager.calculate_all();

                if !opportunities.is_empty() {
                    info!(
                        "📊 定期扫描: 发现 {} 个套利机会, 最佳: {:.2}%",
                        opportunities.len(),
                        opportunities.first().map(|o| o.profit_margin).unwrap_or(0.0)
                    );
                }
            }
        });
    }

    /// Get current opportunities
    pub fn get_opportunities(&self) -> Vec<ArbitrageOpportunity> {
        self.ws_manager.get_opportunities()
    }

    /// Get system statistics
    pub fn get_stats(&self) -> SystemStats {
        let mut stats = self.ws_manager.get_stats();
        stats.matched_events = self.matched_events.len();
        stats.matched_markets = self.matched_markets.len();
        stats
    }

    /// Get matched markets
    pub fn get_matched_markets(&self) -> &[MatchedMarket] {
        &self.matched_markets
    }

    /// Place an order on Kalshi
    pub async fn place_kalshi_order(
        &self,
        ticker: &str,
        side: &str,
        outcome: &str,
        count: i32,
        price: i32,
    ) -> Result<serde_json::Value> {
        if self.monitor_only {
            anyhow::bail!("监控模式，已禁止下单");
        }
        self.kalshi_client
            .place_order(ticker, side, outcome, count, price)
            .await
    }

    /// Place an order on Polymarket (legacy - uses USDC amount)
    pub async fn place_polymarket_order(
        &self,
        token_id: &str,
        side: &str,
        amount: f64,
    ) -> Result<serde_json::Value> {
        if self.monitor_only {
            anyhow::bail!("监控模式，已禁止下单");
        }
        self.polymarket_client
            .place_market_order(token_id, side, amount)
            .await
    }

    /// Place an order on Polymarket by tokens quantity (RECOMMENDED)
    /// Uses 5% slippage and traverses order book from best price
    pub async fn place_polymarket_order_by_tokens(
        &self,
        token_id: &str,
        side: &str,
        tokens: f64,
    ) -> Result<serde_json::Value> {
        if self.monitor_only {
            anyhow::bail!("监控模式，已禁止下单");
        }
        self.polymarket_client
            .place_market_order_by_tokens(token_id, side, tokens)
            .await
    }

    /// Get arbitrage history
    pub fn get_arbitrage_history(&self, limit: usize) -> Result<Vec<crate::models::ArbitrageTrackingRecord>> {
        self.storage.get_history(limit)
    }

    /// Scan for new markets and return incremental subscription info
    ///
    /// This method fetches fresh market data from both platforms, runs matching,
    /// and returns only the NEW matched markets that weren't in the previous set.
    ///
    /// Returns: (new_matched_markets, new_subscription_info)
    pub async fn scan_for_new_markets(&mut self) -> Result<(Vec<MatchedMarket>, SubscriptionInfo)> {
        info!("============================================================");
        info!("🔄 开始扫描新市场...");
        info!("============================================================");

        // 1. Save old matched market IDs
        let old_matched_ids: HashSet<String> = self
            .matched_markets
            .iter()
            .map(|mm| format!("{}_{}", mm.kalshi_market.market_id, mm.team_name))
            .collect();

        let old_count = self.matched_markets.len();
        info!("   扫描前状态: {} 个已匹配市场", old_count);

        // 2. Fetch fresh market data
        let (kalshi_events, kalshi_markets) = match self
            .kalshi_client
            .get_nba_events_and_markets()
            .await
        {
            Ok(data) => data,
            Err(e) => {
                error!("❌ 获取 Kalshi 市场数据失败: {}", e);
                return Ok((Vec::new(), SubscriptionInfo::empty()));
            }
        };

        let (polymarket_events, polymarket_markets) = match self
            .polymarket_client
            .get_nba_events_and_markets()
            .await
        {
            Ok(data) => data,
            Err(e) => {
                error!("❌ 获取 Polymarket 市场数据失败: {}", e);
                return Ok((Vec::new(), SubscriptionInfo::empty()));
            }
        };

        info!(
            "   扫描后状态: Kalshi {} 事件/{} 市场, Polymarket {} 事件/{} 市场",
            kalshi_events.len(),
            kalshi_markets.len(),
            polymarket_events.len(),
            polymarket_markets.len()
        );

        // 3. Re-run matching
        let match_start = Instant::now();
        let (matched_events, matched_markets) = self.matcher.match_events_and_markets(
            &kalshi_events,
            &kalshi_markets,
            &polymarket_events,
            &polymarket_markets,
        );
        self.metrics.record(Operation::MarketMatch, match_start.elapsed());

        // 4. Find new matched markets
        let mut new_matched_markets = Vec::new();
        for mm in &matched_markets {
            let key = format!("{}_{}", mm.kalshi_market.market_id, mm.team_name);
            if !old_matched_ids.contains(&key) {
                new_matched_markets.push(mm.clone());
            }
        }

        // 5. Update internal state
        self.matched_events = matched_events;
        self.matched_markets = matched_markets;

        if new_matched_markets.is_empty() {
            info!("   ✅ 没有发现新的匹配市场");
            info!("============================================================");
            return Ok((Vec::new(), SubscriptionInfo::empty()));
        }

        info!("   🆕 发现 {} 个新匹配市场:", new_matched_markets.len());
        for (i, mm) in new_matched_markets.iter().enumerate().take(5) {
            info!("      {}. {} ({})", i + 1, mm.event_name, mm.team_name);
        }
        if new_matched_markets.len() > 5 {
            info!("      ... 还有 {} 个新市场", new_matched_markets.len() - 5);
        }

        // 6. Generate subscription info for new markets only
        let new_sub_info = self.matcher.get_subscription_info(&new_matched_markets);

        info!(
            "   📡 新订阅需求: Kalshi {} 个市场, Polymarket {} 个 token",
            new_sub_info.kalshi_tickers.len(),
            new_sub_info.polymarket_token_ids.len()
        );
        info!("============================================================");

        Ok((new_matched_markets, new_sub_info))
    }

    /// Search history records with filters
    pub fn search_history(
        &self,
        min_profit: Option<f64>,
        max_profit: Option<f64>,
        min_duration: Option<f64>,
        max_duration: Option<f64>,
        event_name: Option<String>,
        team_name: Option<String>,
        sort_by: Option<String>,
        sort_order: Option<String>,
        limit: Option<usize>,
        offset: Option<usize>,
        include_history: Option<bool>,
    ) -> Result<serde_json::Value> {
        self.storage.search_records(
            min_profit,
            max_profit,
            min_duration,
            max_duration,
            event_name,
            team_name,
            sort_by,
            sort_order,
            limit,
            offset,
            include_history,
        )
    }

    /// Get history statistics
    pub fn get_history_statistics(&self) -> Result<serde_json::Value> {
        self.storage.get_statistics()
    }
}

