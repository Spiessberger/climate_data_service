use climate_data_service::Service;
use rusqlite::{Connection, params};
use serde_json::Value;
use serialport::{SerialPort, TTYPort};
use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tempfile::TempDir;

struct Demo {
    _master: TTYPort,
    address: SocketAddr,
    database_path: std::path::PathBuf,
    _service: Service,
    _directory: Arc<TempDir>,
}

impl Demo {
    fn start(web_root: Option<std::path::PathBuf>) -> Self {
        let directory = Arc::new(tempfile::tempdir().unwrap());
        let database_path = directory.path().join("climate.sqlite3");
        let (master, slave) = TTYPort::pair().unwrap();
        let serial_path = slave.name().unwrap();
        drop(slave);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let service = if let Some(web_root) = web_root {
            Service::start_with_web_root(
                &serial_path,
                listener,
                &database_path,
                web_root,
                now_ms,
                |_| {},
            )
        } else {
            Service::start(&serial_path, listener, &database_path, now_ms, |_| {})
        }
        .unwrap();
        Self {
            _master: master,
            address,
            database_path,
            _service: service,
            _directory: directory,
        }
    }

    fn request(&self, method: &str, path: &str) -> (u16, String, String) {
        let mut client = TcpStream::connect(self.address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write!(
            client,
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).unwrap();
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        (
            response[9..12].parse().unwrap(),
            headers.to_owned(),
            body.to_owned(),
        )
    }

    fn json(&self, path: &str) -> (u16, Value) {
        let (status, _, body) = self.request("GET", path);
        (status, serde_json::from_str(&body).unwrap())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

type WeatherValues = (i64, Option<f64>, Option<i64>, Option<f64>, Option<f64>, f64);

fn insert_weather(connection: &Connection, time: i64, values: WeatherValues) {
    let (station, temperature, humidity, wind, gust, rain) = values;
    connection
        .execute(
            "INSERT INTO weather_readings (
                received_at_unix_ms, v, type, boot_id, seq, station_id,
                temperature_celsius, relative_humidity_percent,
                wind_direction_degrees, wind_speed_mps, gust_speed_mps,
                rain_mm, uv_microwatts_per_cm2, uv_index, light_lux,
                battery_low, rssi_dbm, lqi
             ) VALUES (?1, 1, 'weather', '6a9d3c1f80b24e67a511d92cb837046e', 1,
                       ?2, ?3, ?4, NULL, ?5, ?6, ?7, NULL, NULL, NULL, 0, -80.0, 20)",
            params![time, station, temperature, humidity, wind, gust, rain],
        )
        .unwrap();
}

#[test]
fn dashboard_uses_persisted_rows_and_reports_rain_coverage() {
    let demo = Demo::start(None);
    let connection = Connection::open(&demo.database_path).unwrap();
    let now = now_ms();
    let day = 24 * 60 * 60 * 1_000;
    insert_weather(
        &connection,
        now - day - 60_000,
        (1, Some(8.0), Some(70), Some(1.0), Some(2.0), 10.0),
    );
    insert_weather(
        &connection,
        now - day + 60_000,
        (1, Some(9.0), Some(69), Some(1.5), Some(2.5), 10.5),
    );
    insert_weather(
        &connection,
        now - day + 360_000,
        (1, Some(10.0), Some(68), Some(2.0), Some(3.0), 10.5),
    );
    insert_weather(
        &connection,
        now - day + 720_000,
        (1, Some(11.0), Some(67), Some(2.5), Some(3.5), 11.0),
    );
    insert_weather(&connection, now - 60_000, (2, None, None, None, None, 1.0));
    insert_weather(&connection, now - 30_000, (2, None, None, None, None, 1.2));
    connection
        .execute(
            "INSERT INTO indoor_readings (
            received_at_unix_ms, v, type, boot_id, seq,
            temperature_celsius, relative_humidity_percent
         ) VALUES (?1, 1, 'indoor', '6a9d3c1f80b24e67a511d92cb837046e', 7, 22.5, 45.0)",
            [now - 10_000],
        )
        .unwrap();

    let (status, body) = demo.json("/api/v1/dashboard");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["station_timezone"], "Europe/Vienna");
    assert_eq!(body["stale_after_ms"], 300_000);
    assert_eq!(body["latest_stored"]["weather"]["station_id"], 2);
    assert_eq!(body["latest_stored"]["indoor"]["seq"], 7);
    assert_eq!(
        body["history"]["weather"]["first_received_at_unix_ms"],
        now - day - 60_000
    );
    assert_eq!(body["rain_last_24_hours"]["coverage"], "partial");
    assert_eq!(body["rain_last_24_hours"]["excluded_transitions"], 1);
    assert!((body["rain_last_24_hours"]["total_mm"].as_f64().unwrap() - 1.2).abs() < 1e-9);
    assert!(
        body["rain_last_24_hours"]["largest_gap_ms"]
            .as_i64()
            .unwrap()
            > 300_000
    );
    assert!(matches!(
        body["night_temperature"]["state"].as_str(),
        Some("ongoing" | "completed")
    ));
}

#[test]
fn summary_normalizes_vienna_dates_and_keeps_nulls_separate_from_zero() {
    let demo = Demo::start(None);
    let connection = Connection::open(&demo.database_path).unwrap();
    // 2026-09-23 local midnight is 2026-09-22 22:00 UTC (CEST).
    let from = 1_790_114_400_000_i64;
    let to = from + 24 * 60 * 60 * 1_000;
    insert_weather(
        &connection,
        from - 60_000,
        (1, Some(5.0), Some(40), Some(0.0), Some(1.0), 10.0),
    );
    insert_weather(
        &connection,
        from,
        (1, None, Some(50), Some(1.0), Some(3.0), 10.2),
    );
    insert_weather(
        &connection,
        from + 12 * 60 * 60 * 1_000,
        (1, Some(20.0), None, Some(3.0), None, 10.2),
    );
    insert_weather(
        &connection,
        from + 13 * 60 * 60 * 1_000,
        (2, None, None, None, None, 0.0),
    );

    let (status, body) = demo.json(
        "/api/v1/history/weather/summary?from_date=2026-09-23&through_date=2026-09-23&max_points=4",
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["range"]["from_unix_ms"], from);
    assert_eq!(body["range"]["to_unix_ms"], to);
    assert_eq!(body["range"]["mode"], "dates");
    assert_eq!(body["range"]["timezone"], "Europe/Vienna");
    assert_eq!(body["sample_count"], 3);
    assert_eq!(body["buckets"].as_array().unwrap().len(), 4);
    assert_eq!(body["statistics"]["temperature_celsius"]["min"], 20.0);
    assert_eq!(body["statistics"]["temperature_celsius"]["average"], 20.0);
    assert_eq!(body["statistics"]["wind_speed_mps"]["average"], 2.0);
    assert_eq!(body["statistics"]["wind_speed_mps"]["max"], 3.0);
    assert_eq!(body["statistics"]["gust_speed_mps"]["max"], 3.0);
    assert_eq!(body["statistics"]["rain"]["coverage"], "partial");
    assert_eq!(body["statistics"]["rain"]["excluded_transitions"], 1);
    assert!((body["statistics"]["rain"]["total_mm"].as_f64().unwrap() - 0.2).abs() < 1e-9);
    assert_eq!(body["buckets"][1]["sample_count"], 0);
    assert_eq!(
        body["buckets"][1]["temperature_celsius"]["average"],
        Value::Null
    );
    assert_eq!(body["buckets"][1]["rain"]["total_mm"], Value::Null);
    assert_eq!(body["buckets"][1]["rain"]["coverage"], "unavailable");

    for path in [
        "/api/v1/history/weather/summary?from_date=2026-09-24&through_date=2026-09-23",
        "/api/v1/history/weather/summary?from_date=2026-09-23&through_date=2026-09-23&max_points=0",
        "/api/v1/history/weather/summary?from_date=2026-09-23&through_date=2026-09-23&max_points=601",
        "/api/v1/history/weather/summary?from_date=2025-01-01&through_date=2026-09-23",
    ] {
        assert_eq!(demo.json(path).0, 422, "{path}");
    }

    let (status, spring) = demo.json(
        "/api/v1/history/weather/summary?from_date=2026-03-29&through_date=2026-03-29&max_points=1",
    );
    assert_eq!(status, 200);
    assert_eq!(
        spring["range"]["to_unix_ms"].as_i64().unwrap()
            - spring["range"]["from_unix_ms"].as_i64().unwrap(),
        23 * 60 * 60 * 1_000
    );
}

#[test]
fn instant_summary_preserves_exact_half_open_bounds_and_short_bucket_count() {
    let demo = Demo::start(None);
    let connection = Connection::open(&demo.database_path).unwrap();
    let from = 1_790_114_400_000_i64;
    insert_weather(
        &connection,
        from - 1,
        (1, Some(5.0), None, None, None, 10.0),
    );
    insert_weather(&connection, from, (1, Some(10.0), None, None, None, 10.5));
    insert_weather(
        &connection,
        from + 1,
        (1, Some(20.0), None, None, None, 10.5),
    );
    insert_weather(
        &connection,
        from + 2,
        (1, Some(99.0), None, None, None, 11.0),
    );

    let (status, body) = demo.json(&format!(
        "/api/v1/history/weather/summary?from_unix_ms={from}&to_unix_ms={}&max_points=600",
        from + 2
    ));
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["range"]["mode"], "instants");
    assert_eq!(body["range"]["from_unix_ms"], from);
    assert_eq!(body["range"]["to_unix_ms"], from + 2);
    assert_eq!(body["range"]["from_date"], "2026-09-23");
    assert_eq!(body["range"]["through_date"], "2026-09-23");
    assert_eq!(body["sample_count"], 2);
    assert_eq!(body["statistics"]["temperature_celsius"]["min"], 10.0);
    assert_eq!(body["statistics"]["temperature_celsius"]["max"], 20.0);
    assert_eq!(body["statistics"]["rain"]["total_mm"], 0.5);
    assert_eq!(body["statistics"]["rain"]["coverage"], "complete");
    assert_eq!(body["buckets"].as_array().unwrap().len(), 2);
    assert_eq!(body["buckets"][0]["from_unix_ms"], from);
    assert_eq!(body["buckets"][0]["to_unix_ms"], from + 1);
    assert_eq!(body["buckets"][0]["sample_count"], 1);
    assert_eq!(body["buckets"][1]["from_unix_ms"], from + 1);
    assert_eq!(body["buckets"][1]["to_unix_ms"], from + 2);
    assert_eq!(body["buckets"][1]["sample_count"], 1);

    let (status, one_ms) = demo.json(&format!(
        "/api/v1/history/weather/summary?from_unix_ms={}&to_unix_ms={}&max_points=600",
        from + 1,
        from + 2
    ));
    assert_eq!(status, 200, "{one_ms}");
    assert_eq!(one_ms["sample_count"], 1);
    assert_eq!(one_ms["buckets"].as_array().unwrap().len(), 1);
    assert_eq!(one_ms["buckets"][0]["from_unix_ms"], from + 1);
    assert_eq!(one_ms["buckets"][0]["to_unix_ms"], from + 2);
}

#[test]
fn instant_summary_validates_mode_bounds_and_vienna_date_metadata() {
    let demo = Demo::start(None);
    let (_, date_range) = demo.json(
        "/api/v1/history/weather/summary?from_date=2025-10-25&through_date=2026-10-25&max_points=1",
    );
    let from = date_range["range"]["from_unix_ms"].as_i64().unwrap();
    let to = date_range["range"]["to_unix_ms"].as_i64().unwrap();
    assert_eq!(to - from, (366 * 24 + 1) * 60 * 60 * 1_000);
    let (status, instant_range) = demo.json(&format!(
        "/api/v1/history/weather/summary?from_unix_ms={from}&to_unix_ms={to}&max_points=1"
    ));
    assert_eq!(status, 200, "{instant_range}");
    assert_eq!(instant_range["range"]["mode"], "instants");
    assert_eq!(instant_range["range"]["from_date"], "2025-10-25");
    assert_eq!(instant_range["range"]["through_date"], "2026-10-25");

    let midnight_from = 1_790_114_400_000_i64;
    let midnight_to = midnight_from + 24 * 60 * 60 * 1_000;
    let (status, midnight) = demo.json(&format!(
        "/api/v1/history/weather/summary?from_unix_ms={midnight_from}&to_unix_ms={midnight_to}&max_points=1"
    ));
    assert_eq!(status, 200, "{midnight}");
    assert_eq!(midnight["range"]["through_date"], "2026-09-23");

    for path in [
        "/api/v1/history/weather/summary?from_unix_ms=0",
        "/api/v1/history/weather/summary?to_unix_ms=1",
        "/api/v1/history/weather/summary?from_unix_ms=1&to_unix_ms=1",
        "/api/v1/history/weather/summary?from_unix_ms=2&to_unix_ms=1",
        "/api/v1/history/weather/summary?from_date=2026-09-23&through_date=2026-09-23&from_unix_ms=0&to_unix_ms=1",
        "/api/v1/history/weather/summary?from_unix_ms=9223372036854775806&to_unix_ms=9223372036854775807",
        "/api/v1/history/weather/summary?from_unix_ms=0&to_unix_ms=31626000001",
    ] {
        assert_eq!(demo.json(path).0, 422, "{path}");
    }
}

#[test]
fn nondivisible_bucket_boundaries_put_exact_edge_samples_in_the_later_bucket() {
    let demo = Demo::start(None);
    let connection = Connection::open(&demo.database_path).unwrap();
    let from = 1_790_114_400_000_i64;
    let duration = 24 * 60 * 60 * 1_000_i64;
    let boundary = duration / 359;
    insert_weather(
        &connection,
        from + boundary - 1,
        (1, Some(1.0), None, None, None, 5.0),
    );
    insert_weather(
        &connection,
        from + boundary,
        (1, Some(2.0), None, None, None, 5.0),
    );
    let (status, body) = demo.json(
        "/api/v1/history/weather/summary?from_date=2026-09-23&through_date=2026-09-23&max_points=359",
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["buckets"][0]["to_unix_ms"], from + boundary);
    assert_eq!(body["buckets"][0]["sample_count"], 1);
    assert_eq!(body["buckets"][1]["from_unix_ms"], from + boundary);
    assert_eq!(body["buckets"][1]["sample_count"], 1);
    assert_eq!(body["statistics"]["rain"]["total_mm"], 0.0);
}

#[test]
fn web_root_serves_spa_and_assets_without_capturing_api_routes() {
    let directory = tempfile::tempdir().unwrap();
    fs::create_dir(directory.path().join("assets")).unwrap();
    fs::write(directory.path().join("index.html"), "<h1>weather</h1>").unwrap();
    fs::write(
        directory.path().join("assets/app-abc.js"),
        "export default 1;",
    )
    .unwrap();
    let demo = Demo::start(Some(directory.path().to_owned()));

    let (status, headers, body) = demo.request("GET", "/history-view");
    assert_eq!(status, 200);
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: text/html; charset=utf-8")
    );
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("cache-control: no-store")
    );
    assert_eq!(body, "<h1>weather</h1>");

    let (status, headers, body) = demo.request("GET", "/assets/app-abc.js");
    assert_eq!(status, 200);
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("cache-control: public, max-age=31536000, immutable")
    );
    assert_eq!(body, "export default 1;");

    let (status, headers, body) = demo.request("HEAD", "/");
    assert_eq!(status, 200);
    assert!(headers.to_ascii_lowercase().contains("content-length: 16"));
    assert!(body.is_empty());
    assert_eq!(demo.request("GET", "/assets/missing.js").0, 404);
    assert_eq!(demo.request("GET", "/%2e%2e/secret").0, 404);
    let (status, headers, _) = demo.request("GET", "/api/v1/missing");
    assert_eq!(status, 404);
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: application/json")
    );
}

#[test]
fn calendar_rain_preserves_totals_and_coverage_across_groupings() {
    let demo = Demo::start(None);
    let connection = Connection::open(&demo.database_path).unwrap();
    let from = chrono::DateTime::parse_from_rfc3339("2026-01-05T00:00:00+01:00")
        .unwrap()
        .timestamp_millis();
    for (offset, station, rain) in [
        (-60_000, 1, 10.0),
        (60_000, 1, 10.5),
        (3_660_000, 1, 10.5),
        (86_460_000, 1, 1.0),
        (86_520_000, 1, 1.5),
        (86_580_000, 2, 20.0),
        (86_640_000, 2, 21.0),
    ] {
        insert_weather(
            &connection,
            from + offset,
            (station, None, None, None, None, rain),
        );
    }
    for grouping in ["auto", "hour", "day", "week", "month"] {
        let (status, body) = demo.json(&format!("/api/v1/history/weather/summary?from_date=2026-01-05&through_date=2026-01-07&rain_grouping={grouping}"));
        assert_eq!(status, 200, "{body}");
        let buckets = body["rain_buckets"].as_array().unwrap();
        let total: f64 = buckets
            .iter()
            .filter_map(|b| b["rain"]["total_mm"].as_f64())
            .sum();
        assert_eq!(
            total,
            body["statistics"]["rain"]["total_mm"].as_f64().unwrap()
        );
        assert_eq!(total, 2.0);
        assert_eq!(
            buckets
                .iter()
                .filter_map(|b| b["rain"]["excluded_transitions"].as_u64())
                .sum::<u64>(),
            2
        );
        assert_eq!(body["buckets"].as_array().unwrap().len(), 360);
        if grouping == "hour" {
            assert_eq!(buckets[1]["rain"]["total_mm"], 0.0);
            assert!(buckets[2]["rain"]["total_mm"].is_null());
            assert_eq!(buckets[2]["rain"]["coverage"], "unavailable");
        }
    }
    let (status, _) = demo.json("/api/v1/history/weather/summary?from_date=2026-01-05&through_date=2026-01-07&rain_grouping=year");
    assert_eq!(status, 422);
    let (status, body) = demo.json("/api/v1/history/weather/summary?from_date=2026-01-01&through_date=2026-12-31&rain_grouping=hour");
    assert_eq!(status, 200);
    assert_eq!(body["rain_grouping"], "month");
    assert_eq!(
        body["available_rain_groupings"],
        serde_json::json!(["day", "week", "month"])
    );
}

#[test]
fn current_rain_period_is_so_far_and_future_periods_are_unavailable() {
    let demo = Demo::start(None);
    let now = now_ms();
    let (status, body) = demo.json(&format!(
        "/api/v1/history/weather/summary?from_unix_ms={}&to_unix_ms={}&rain_grouping=hour",
        now - 3_600_000,
        now + 7_200_000
    ));
    assert_eq!(status, 200);
    let buckets = body["rain_buckets"].as_array().unwrap();
    let ongoing = buckets.iter().find(|b| b["ongoing"] == true).unwrap();
    assert!(ongoing["observed_through_unix_ms"].as_i64().unwrap() >= now);
    assert!(
        ongoing["observed_through_unix_ms"].as_i64().unwrap()
            < ongoing["to_unix_ms"].as_i64().unwrap()
    );
    let future = buckets.iter().find(|b| b["future"] == true).unwrap();
    assert!(future["rain"]["total_mm"].is_null());
}
