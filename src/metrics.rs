use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

const RING_BUFFER_SIZE: usize = 60;
const MAX_RECENT_QUERIES: usize = 30;

/// Atomic metrics tracker for server-wide throughput and performance statistics.
#[derive(Debug)]
pub struct ServerMetrics {
    start_time: Instant,

    total_requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    active_requests: AtomicUsize,

    total_bytes_csv: AtomicU64,
    total_bytes_parquet: AtomicU64,
    total_rows_streamed: AtomicU64,
    total_duration_us: AtomicU64,

    // Lock-protected structures for rolling time series, clients, and recent queries
    history: RwLock<RollingHistory>,
    clients: RwLock<HashMap<IpAddr, ClientStats>>,
    recent_queries: RwLock<VecDeque<RecentQueryLog>>,
}

#[derive(Debug)]
struct RollingHistory {
    buckets: [SecondBucket; RING_BUFFER_SIZE],
    last_second: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct SecondBucket {
    timestamp_sec: u64,
    requests: u32,
    bytes: u64,
    rows: u64,
    duration_us: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientStats {
    pub ip: String,
    pub requests: u64,
    pub bytes: u64,
    pub rows: u64,
    pub total_duration_us: u64,
    pub avg_latency_ms: f64,
    pub last_seen_ms_ago: u64,
    #[serde(skip)]
    pub last_seen: Instant,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecentQueryLog {
    pub timestamp: String,
    pub client_ip: String,
    pub format: String,
    pub duration_ms: f64,
    pub bytes: u64,
    pub status: u16,
    pub sql: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub uptime_seconds: u64,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub active_requests: usize,
    pub total_bytes: u64,
    pub total_bytes_csv: u64,
    pub total_bytes_parquet: u64,
    pub total_rows_streamed: u64,
    pub avg_latency_ms: f64,

    pub current_rps: f64,
    pub current_bytes_per_sec: f64,
    pub current_rows_per_sec: f64,

    pub throughput_history_60s: Vec<HistoryPoint>,
    pub clients: Vec<ClientStats>,
    pub recent_queries: Vec<RecentQueryLog>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistoryPoint {
    pub timestamp_sec: u64,
    pub requests: u32,
    pub bytes: u64,
    pub rows: u64,
    pub avg_latency_ms: f64,
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Data describing a completed query request event.
#[derive(Debug, Clone)]
pub struct RequestEvent<'a> {
    pub client_ip: IpAddr,
    pub format_indicator: &'a str,
    pub duration: std::time::Duration,
    pub status: u16,
    pub bytes_sent: u64,
    pub rows_streamed: u64,
    pub sql_query: &'a str,
}

impl ServerMetrics {
    /// Creates a new `ServerMetrics` instance.
    pub fn new() -> Self {
        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            start_time: Instant::now(),
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            active_requests: AtomicUsize::new(0),
            total_bytes_csv: AtomicU64::new(0),
            total_bytes_parquet: AtomicU64::new(0),
            total_rows_streamed: AtomicU64::new(0),
            total_duration_us: AtomicU64::new(0),
            history: RwLock::new(RollingHistory {
                buckets: [SecondBucket::default(); RING_BUFFER_SIZE],
                last_second: now_sec,
            }),
            clients: RwLock::new(HashMap::new()),
            recent_queries: RwLock::new(VecDeque::with_capacity(MAX_RECENT_QUERIES)),
        }
    }

    /// Increments active requests count.
    pub fn request_started(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.active_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Records the completion of a query request.
    pub fn record_request(&self, event: RequestEvent<'_>) {
        self.active_requests.fetch_sub(1, Ordering::Relaxed);

        if event.status < 400 {
            self.successful_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_requests.fetch_add(1, Ordering::Relaxed);
        }

        if event.format_indicator.eq_ignore_ascii_case("P") {
            self.total_bytes_parquet
                .fetch_add(event.bytes_sent, Ordering::Relaxed);
        } else {
            self.total_bytes_csv
                .fetch_add(event.bytes_sent, Ordering::Relaxed);
        }

        self.total_rows_streamed
            .fetch_add(event.rows_streamed, Ordering::Relaxed);

        let duration_us = event.duration.as_micros() as u64;
        self.total_duration_us
            .fetch_add(duration_us, Ordering::Relaxed);

        let now_system = SystemTime::now();
        let current_sec = now_system
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Update rolling history buckets
        if let Ok(mut hist) = self.history.write() {
            let idx = (current_sec as usize) % RING_BUFFER_SIZE;
            let bucket = &mut hist.buckets[idx];
            if bucket.timestamp_sec != current_sec {
                // New second slot
                *bucket = SecondBucket {
                    timestamp_sec: current_sec,
                    requests: 1,
                    bytes: event.bytes_sent,
                    rows: event.rows_streamed,
                    duration_us,
                };
            } else {
                bucket.requests += 1;
                bucket.bytes += event.bytes_sent;
                bucket.rows += event.rows_streamed;
                bucket.duration_us += duration_us;
            }
            hist.last_second = current_sec;
        }

        // Update client host statistics
        if let Ok(mut clients) = self.clients.write() {
            let stats = clients
                .entry(event.client_ip)
                .or_insert_with(|| ClientStats {
                    ip: event.client_ip.to_string(),
                    requests: 0,
                    bytes: 0,
                    rows: 0,
                    total_duration_us: 0,
                    avg_latency_ms: 0.0,
                    last_seen_ms_ago: 0,
                    last_seen: Instant::now(),
                });

            stats.requests += 1;
            stats.bytes += event.bytes_sent;
            stats.rows += event.rows_streamed;
            stats.total_duration_us += duration_us;
            stats.avg_latency_ms =
                (stats.total_duration_us as f64) / (stats.requests as f64) / 1000.0;
            stats.last_seen = Instant::now();
        }

        // Update recent queries log
        if let Ok(mut queries) = self.recent_queries.write() {
            if queries.len() >= MAX_RECENT_QUERIES {
                queries.pop_back();
            }

            let timestamp_str = format_time_hms(now_system);
            queries.push_front(RecentQueryLog {
                timestamp: timestamp_str,
                client_ip: event.client_ip.to_string(),
                format: event.format_indicator.to_uppercase(),
                duration_ms: (duration_us as f64) / 1000.0,
                bytes: event.bytes_sent,
                status: event.status,
                sql: event.sql_query.to_string(),
            });
        }
    }

    /// Generates a point-in-time metrics snapshot for the dashboard / JSON endpoint.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let uptime_seconds = self.start_time.elapsed().as_secs();
        let total_requests = self.total_requests.load(Ordering::Relaxed);
        let successful_requests = self.successful_requests.load(Ordering::Relaxed);
        let failed_requests = self.failed_requests.load(Ordering::Relaxed);
        let active_requests = self.active_requests.load(Ordering::Relaxed);
        let total_bytes_csv = self.total_bytes_csv.load(Ordering::Relaxed);
        let total_bytes_parquet = self.total_bytes_parquet.load(Ordering::Relaxed);
        let total_bytes = total_bytes_csv + total_bytes_parquet;
        let total_rows_streamed = self.total_rows_streamed.load(Ordering::Relaxed);
        let total_duration_us = self.total_duration_us.load(Ordering::Relaxed);

        let avg_latency_ms = if total_requests > 0 {
            (total_duration_us as f64) / (total_requests as f64) / 1000.0
        } else {
            0.0
        };

        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut history_points = Vec::with_capacity(RING_BUFFER_SIZE);
        let mut current_rps = 0.0;
        let mut current_bytes_per_sec = 0.0;
        let mut current_rows_per_sec = 0.0;

        if let Ok(hist) = self.history.read() {
            // Build chronological 60-second slice
            for offset in (0..RING_BUFFER_SIZE).rev() {
                let target_sec = now_sec.saturating_sub(offset as u64);
                let idx = (target_sec as usize) % RING_BUFFER_SIZE;
                let bucket = hist.buckets[idx];

                if bucket.timestamp_sec == target_sec {
                    let avg_lat = if bucket.requests > 0 {
                        (bucket.duration_us as f64) / (bucket.requests as f64) / 1000.0
                    } else {
                        0.0
                    };
                    history_points.push(HistoryPoint {
                        timestamp_sec: target_sec,
                        requests: bucket.requests,
                        bytes: bucket.bytes,
                        rows: bucket.rows,
                        avg_latency_ms: avg_lat,
                    });
                } else {
                    history_points.push(HistoryPoint {
                        timestamp_sec: target_sec,
                        requests: 0,
                        bytes: 0,
                        rows: 0,
                        avg_latency_ms: 0.0,
                    });
                }
            }

            // Calculate rolling short-term rate over the last 3-5 active seconds
            let recent_window = &history_points[history_points.len().saturating_sub(5)..];
            if !recent_window.is_empty() {
                let sum_reqs: u32 = recent_window.iter().map(|p| p.requests).sum();
                let sum_bytes: u64 = recent_window.iter().map(|p| p.bytes).sum();
                let sum_rows: u64 = recent_window.iter().map(|p| p.rows).sum();
                let count = recent_window.len() as f64;
                current_rps = (sum_reqs as f64) / count;
                current_bytes_per_sec = (sum_bytes as f64) / count;
                current_rows_per_sec = (sum_rows as f64) / count;
            }
        }

        let mut clients_vec = Vec::new();
        if let Ok(clients) = self.clients.read() {
            for stats in clients.values() {
                let mut s = stats.clone();
                s.last_seen_ms_ago = s.last_seen.elapsed().as_millis() as u64;
                clients_vec.push(s);
            }
        }
        clients_vec.sort_by_key(|b| std::cmp::Reverse(b.requests));

        let recent_queries = if let Ok(queries) = self.recent_queries.read() {
            queries.iter().cloned().collect()
        } else {
            Vec::new()
        };

        MetricsSnapshot {
            uptime_seconds,
            total_requests,
            successful_requests,
            failed_requests,
            active_requests,
            total_bytes,
            total_bytes_csv,
            total_bytes_parquet,
            total_rows_streamed,
            avg_latency_ms,
            current_rps,
            current_bytes_per_sec,
            current_rows_per_sec,
            throughput_history_60s: history_points,
            clients: clients_vec,
            recent_queries,
        }
    }
}

fn format_time_hms(st: SystemTime) -> String {
    let dur = st.duration_since(UNIX_EPOCH).unwrap_or_default();
    let total_secs = dur.as_secs();
    let secs = total_secs % 60;
    let mins = (total_secs / 60) % 60;
    let hours = (total_secs / 3600) % 24;
    format!("{hours:02}:{mins:02}:{secs:02}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_metrics_lifecycle() {
        let metrics = Arc::new(ServerMetrics::new());
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        metrics.request_started();
        assert_eq!(metrics.active_requests.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.total_requests.load(Ordering::Relaxed), 1);

        metrics.record_request(RequestEvent {
            client_ip: ip,
            format_indicator: "P",
            duration: Duration::from_millis(25),
            status: 200,
            bytes_sent: 1024,
            rows_streamed: 500,
            sql_query: "SELECT * FROM users",
        });

        assert_eq!(metrics.active_requests.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.successful_requests.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.total_bytes_parquet.load(Ordering::Relaxed), 1024);
        assert_eq!(metrics.total_rows_streamed.load(Ordering::Relaxed), 500);

        let snap = metrics.snapshot();
        assert_eq!(snap.total_requests, 1);
        assert_eq!(snap.successful_requests, 1);
        assert_eq!(snap.total_bytes_parquet, 1024);
        assert_eq!(snap.clients.len(), 1);
        assert_eq!(snap.clients[0].ip, "127.0.0.1");
        assert_eq!(snap.recent_queries.len(), 1);
        assert_eq!(snap.recent_queries[0].format, "P");
    }
}
