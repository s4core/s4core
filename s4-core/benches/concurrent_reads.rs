//! Benchmark: concurrent reads for BitcaskStorageEngine
//!
//! Compares two modes:
//! - "optimized" — current code (lock-free reads via redb + pread)
//! - "with_lock" — artificial Mutex around get_object simulating the old behavior
//!
//! Run: cargo bench -p s4-core

use s4_core::{BitcaskStorageEngine, StorageEngine};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

const NUM_OBJECTS: usize = 1_000;
const OBJECT_SIZE: usize = 10 * 1024; // 10 KB
const READS_PER_TASK: usize = 1_000;
const CONCURRENCY_LEVELS: &[usize] = &[1, 2, 4, 8];

fn pseudo_random(seed: u64) -> u64 {
    let mut x = seed;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

async fn setup_engine(dir: &std::path::Path) -> Arc<BitcaskStorageEngine> {
    let volumes_dir = dir.join("volumes");
    let metadata_path = dir.join("metadata.redb");
    std::fs::create_dir_all(&volumes_dir).unwrap();

    let engine = BitcaskStorageEngine::new(
        &volumes_dir,
        &metadata_path,
        1024 * 1024 * 1024, // 1 GB max volume
        4096,                // 4 KB inline threshold
        false,               // no fsync for bench speed
    )
    .await
    .expect("failed to create engine");

    let engine = Arc::new(engine);

    let data = vec![0xABu8; OBJECT_SIZE];
    let metadata = HashMap::new();
    for i in 0..NUM_OBJECTS {
        engine
            .put_object("bench-bucket", &format!("obj-{:05}", i), &data, "application/octet-stream", &metadata)
            .await
            .expect("put_object failed");
    }

    engine
}

struct BenchResult {
    mode: &'static str,
    concurrency: usize,
    total_ops: usize,
    wall_time_ms: f64,
    ops_per_sec: f64,
    avg_latency_us: f64,
}

async fn bench_optimized(engine: &Arc<BitcaskStorageEngine>, concurrency: usize) -> BenchResult {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);

    for task_id in 0..concurrency {
        let eng = Arc::clone(engine);
        handles.push(tokio::spawn(async move {
            let mut seed = (task_id as u64 + 1) * 7919;
            for _ in 0..READS_PER_TASK {
                seed = pseudo_random(seed);
                let idx = (seed % NUM_OBJECTS as u64) as usize;
                let key = format!("obj-{:05}", idx);
                eng.get_object("bench-bucket", &key)
                    .await
                    .expect("get_object failed");
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = concurrency * READS_PER_TASK;
    let wall_ms = elapsed.as_secs_f64() * 1000.0;
    let ops_sec = total_ops as f64 / elapsed.as_secs_f64();
    let avg_lat = (elapsed.as_micros() as f64) / total_ops as f64;

    BenchResult {
        mode: "optimized",
        concurrency,
        total_ops,
        wall_time_ms: wall_ms,
        ops_per_sec: ops_sec,
        avg_latency_us: avg_lat,
    }
}

async fn bench_with_lock(engine: &Arc<BitcaskStorageEngine>, concurrency: usize) -> BenchResult {
    let lock = Arc::new(Mutex::new(()));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);

    for task_id in 0..concurrency {
        let eng = Arc::clone(engine);
        let lk = Arc::clone(&lock);
        handles.push(tokio::spawn(async move {
            let mut seed = (task_id as u64 + 1) * 7919;
            for _ in 0..READS_PER_TASK {
                seed = pseudo_random(seed);
                let idx = (seed % NUM_OBJECTS as u64) as usize;
                let key = format!("obj-{:05}", idx);
                let _guard = lk.lock().await;
                eng.get_object("bench-bucket", &key)
                    .await
                    .expect("get_object failed");
                drop(_guard);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total_ops = concurrency * READS_PER_TASK;
    let wall_ms = elapsed.as_secs_f64() * 1000.0;
    let ops_sec = total_ops as f64 / elapsed.as_secs_f64();
    let avg_lat = (elapsed.as_micros() as f64) / total_ops as f64;

    BenchResult {
        mode: "with_lock",
        concurrency,
        total_ops,
        wall_time_ms: wall_ms,
        ops_per_sec: ops_sec,
        avg_latency_us: avg_lat,
    }
}

fn print_results(results: &[BenchResult]) {
    println!();
    println!("╔══════════════╦═════════════╦════════════╦══════════════╦══════════════╦═══════════════╗");
    println!("║ Mode         ║ Concurrency ║ Total Ops  ║ Wall Time ms ║    Ops/sec   ║ Avg Lat (µs)  ║");
    println!("╠══════════════╬═════════════╬════════════╬══════════════╬══════════════╬═══════════════╣");
    for r in results {
        println!(
            "║ {:<12} ║ {:>11} ║ {:>10} ║ {:>12.1} ║ {:>12.0} ║ {:>13.1} ║",
            r.mode, r.concurrency, r.total_ops, r.wall_time_ms, r.ops_per_sec, r.avg_latency_us,
        );
    }
    println!("╚══════════════╩═════════════╩════════════╩══════════════╩══════════════╩═══════════════╝");
    println!();

    println!("── Scaling Summary ──");
    let opt_results: Vec<&BenchResult> = results.iter().filter(|r| r.mode == "optimized").collect();
    let lock_results: Vec<&BenchResult> = results.iter().filter(|r| r.mode == "with_lock").collect();

    if let (Some(opt_base), Some(lock_base)) = (opt_results.first(), lock_results.first()) {
        println!();
        println!("{:<12} {:>12} {:>12}", "Concurrency", "Optimized ×", "With Lock ×");
        println!("{}", "-".repeat(38));
        for (o, l) in opt_results.iter().zip(lock_results.iter()) {
            let opt_scale = o.ops_per_sec / opt_base.ops_per_sec;
            let lock_scale = l.ops_per_sec / lock_base.ops_per_sec;
            println!(
                "{:<12} {:>11.2}× {:>11.2}×",
                o.concurrency, opt_scale, lock_scale,
            );
        }
    }

    if let (Some(opt_max), Some(lock_max)) = (
        results.iter().filter(|r| r.mode == "optimized").last(),
        results.iter().filter(|r| r.mode == "with_lock").last(),
    ) {
        let speedup = opt_max.ops_per_sec / lock_max.ops_per_sec;
        println!();
        println!(
            "Speedup at {} threads: {:.1}× (optimized vs with_lock)",
            opt_max.concurrency, speedup
        );
    }
    println!();
}

#[tokio::main]
async fn main() {
    let tmp = tempfile::TempDir::new().expect("failed to create tempdir");
    println!("Setting up engine with {} objects × {} bytes...", NUM_OBJECTS, OBJECT_SIZE);
    let engine = setup_engine(tmp.path()).await;
    println!("Setup complete. Starting benchmarks...\n");

    // warmup
    for i in 0..50 {
        let key = format!("obj-{:05}", i % NUM_OBJECTS);
        let _ = engine.get_object("bench-bucket", &key).await;
    }

    let mut results = Vec::new();

    for &c in CONCURRENCY_LEVELS {
        let r = bench_optimized(&engine, c).await;
        println!("  optimized  concurrency={:>2}  ops/sec={:>10.0}  avg_lat={:.1}µs", c, r.ops_per_sec, r.avg_latency_us);
        results.push(r);

        let r = bench_with_lock(&engine, c).await;
        println!("  with_lock  concurrency={:>2}  ops/sec={:>10.0}  avg_lat={:.1}µs", c, r.ops_per_sec, r.avg_latency_us);
        results.push(r);
    }

    print_results(&results);

}
