//! A private PostgreSQL database for each test.
//!
//! `#[sqlx::test]` does not suit these tests, for two reasons:
//!
//! - All of a binary's test pools share a budget of 20 connections, and a pool only
//!   gives its share back once its last clone is dropped. Dropping the test database
//!   needs a connection from that same budget. A test that leaves a server running
//!   keeps a clone alive until after that point, so finished tests can end up waiting
//!   on each other until the 30-second acquire timeout.
//! - Database names come from the test's path, so two runs at once on one server
//!   drop each other's databases.
//!
//! Here every database has a random name and every pool has its own small cap, so the
//! only shared limit is the server's own: test threads x (`POOL_SIZE` + 1) connections.

use std::str::FromStr;

use ledger_store::MIGRATOR;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

const POOL_SIZE: u32 = 5;

/// A migrated database that is dropped when this value goes out of scope, even if the
/// test panicked. Declare it first in a test, so that it is dropped last.
pub struct TestDb {
    name: String,
    server: PgConnectOptions,
    url: String,
    pool: PgPool,
}

impl TestDb {
    /// Creates the database on the server named by `DATABASE_URL`.
    pub async fn new() -> Self {
        let server_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let server = PgConnectOptions::from_str(&server_url).expect("invalid DATABASE_URL");
        let name = format!("ledger_test_{}", Uuid::new_v4().simple());

        let mut admin = PgConnection::connect_with(&server)
            .await
            .expect("could not connect to the test server; is it running? try `make db-up`");
        admin
            .execute(AssertSqlSafe(format!("CREATE DATABASE \"{name}\"")))
            .await
            .expect("could not create the test database");
        admin.close().await.expect("could not close the connection");

        let pool = PgPoolOptions::new()
            .max_connections(POOL_SIZE)
            .connect_with(server.clone().database(&name))
            .await
            .expect("could not connect to the test database");
        MIGRATOR.run(&pool).await.expect("migrations failed");

        let (base, _) = server_url.rsplit_once('/').expect("invalid DATABASE_URL");
        let url = format!("{base}/{name}");
        Self {
            name,
            server,
            url,
            pool,
        }
    }

    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }

    /// For anything that connects by itself, such as a child process.
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let (server, name) = (self.server.clone(), self.name.clone());
        // On its own thread and runtime, since a test's runtime cannot be blocked on from
        // inside. WITH (FORCE) because servers the test started may still be connected.
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("could not build a runtime");
            runtime.block_on(async {
                let mut admin = PgConnection::connect_with(&server).await?;
                admin
                    .execute(AssertSqlSafe(format!(
                        "DROP DATABASE \"{name}\" WITH (FORCE)"
                    )))
                    .await?;
                admin.close().await
            })
        })
        .join();
        match dropped {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("could not drop test database {}: {e}", self.name),
            Err(_) => eprintln!("could not drop test database {}", self.name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn exists(pool: &PgPool, name: &str) -> bool {
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn databases_are_private_migrated_and_dropped_afterwards() {
        let observer = TestDb::new().await;
        let (first, second) = (TestDb::new().await, TestDb::new().await);
        assert_ne!(first.name, second.name);

        let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
            .fetch_one(&first.pool())
            .await
            .unwrap();
        assert_eq!(events, 0);

        let name = first.name.clone();
        assert!(exists(&observer.pool(), &name).await);
        drop(first);
        assert!(!exists(&observer.pool(), &name).await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_database_is_dropped_even_with_a_connection_still_open() {
        let observer = TestDb::new().await;
        let db = TestDb::new().await;
        let name = db.name.clone();
        let _left_open = db.pool().acquire().await.unwrap();

        drop(db);

        assert!(!exists(&observer.pool(), &name).await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_database_is_dropped_when_a_test_panics() {
        let observer = TestDb::new().await;
        let name = tokio::spawn(async {
            let db = TestDb::new().await;
            std::panic::panic_any(db.name.clone());
        })
        .await
        .unwrap_err()
        .into_panic();
        let name = name.downcast_ref::<String>().unwrap();

        assert!(!exists(&observer.pool(), name).await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_url_reaches_the_same_database() {
        let db = TestDb::new().await;
        let mut direct = PgConnection::connect(db.url()).await.unwrap();
        let current: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&mut direct)
            .await
            .unwrap();
        assert_eq!(current, db.name);
    }
}
