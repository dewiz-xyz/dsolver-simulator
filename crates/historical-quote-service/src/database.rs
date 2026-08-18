use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use aws_sdk_rds::auth_token::{AuthTokenGenerator, Config as AuthTokenConfig};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::{ConnectOptions, Connection, PgConnection};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ReplicaDatabaseConfig;

type DatabaseFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, DatabaseError>> + Send + 'a>>;

pub trait IamTokenProvider: Send + Sync + 'static {
    fn generate(&self) -> DatabaseFuture<'_, SecretToken>;
}

pub trait PhysicalConnectionFactory: Send + Sync + 'static {
    type Connection: Send + 'static;

    fn open(&self, token: SecretToken) -> DatabaseFuture<'_, Self::Connection>;

    fn ping<'a>(&'a self, connection: &'a mut Self::Connection) -> DatabaseFuture<'a, ()>;
}

pub struct SecretToken(String);

impl SecretToken {
    fn expose(&self) -> &str {
        &self.0
    }
}

pub struct RdsIamTokenProvider {
    generator: AuthTokenGenerator,
    sdk_config: aws_config::SdkConfig,
}

impl RdsIamTokenProvider {
    /// Creates the signer for the exact replica endpoint and observer user.
    ///
    /// # Errors
    ///
    /// Returns an error when the signing configuration is invalid.
    pub fn new(
        config: &ReplicaDatabaseConfig,
        sdk_config: aws_config::SdkConfig,
    ) -> Result<Self, DatabaseError> {
        let signer_config = AuthTokenConfig::builder()
            .hostname(config.host.clone())
            .port(u64::from(config.port))
            .username(config.username.clone())
            .build()
            .map_err(|_| DatabaseError::Configuration)?;
        Ok(Self {
            generator: AuthTokenGenerator::new(signer_config),
            sdk_config,
        })
    }
}

impl IamTokenProvider for RdsIamTokenProvider {
    fn generate(&self) -> DatabaseFuture<'_, SecretToken> {
        Box::pin(async move {
            let token = self
                .generator
                .auth_token(&self.sdk_config)
                .await
                .map_err(|_| DatabaseError::Credential)?;
            Ok(SecretToken(token.as_str().to_owned()))
        })
    }
}

pub struct PostgresConnectionFactory {
    base_options: PgConnectOptions,
}

impl PostgresConnectionFactory {
    #[must_use]
    pub fn new(config: &ReplicaDatabaseConfig) -> Self {
        let base_options = PgConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .username(&config.username)
            .database(&config.database)
            .ssl_mode(PgSslMode::VerifyFull)
            .disable_statement_logging();
        Self { base_options }
    }
}

impl PhysicalConnectionFactory for PostgresConnectionFactory {
    type Connection = PgConnection;

    fn open(&self, token: SecretToken) -> DatabaseFuture<'_, Self::Connection> {
        Box::pin(async move {
            let options = self.base_options.clone().password(token.expose());
            PgConnection::connect_with(&options)
                .await
                .map_err(|_| DatabaseError::Connect)
        })
    }

    fn ping<'a>(&'a self, connection: &'a mut Self::Connection) -> DatabaseFuture<'a, ()> {
        Box::pin(async move { connection.ping().await.map_err(|_| DatabaseError::Health) })
    }
}

pub struct IamConnectionPool<T, F>
where
    T: IamTokenProvider,
    F: PhysicalConnectionFactory,
{
    inner: Arc<PoolInner<T, F>>,
}

impl<T, F> Clone for IamConnectionPool<T, F>
where
    T: IamTokenProvider,
    F: PhysicalConnectionFactory,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

struct PoolInner<T, F>
where
    T: IamTokenProvider,
    F: PhysicalConnectionFactory,
{
    token_provider: T,
    connection_factory: F,
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<IdleConnection<F::Connection>>>,
    max_lifetime: Duration,
}

struct IdleConnection<C> {
    connection: C,
    opened_at: Instant,
}

pub struct ConnectionLease<T, F>
where
    T: IamTokenProvider,
    F: PhysicalConnectionFactory,
{
    connection: Option<F::Connection>,
    opened_at: Instant,
    permit: Option<OwnedSemaphorePermit>,
    pool: Weak<PoolInner<T, F>>,
}

impl<T, F> IamConnectionPool<T, F>
where
    T: IamTokenProvider,
    F: PhysicalConnectionFactory,
{
    /// Creates a small pool that signs only when it opens a physical connection.
    ///
    /// # Errors
    ///
    /// Returns an error when the connection count or lifetime is zero.
    pub fn new(
        token_provider: T,
        connection_factory: F,
        max_connections: usize,
        max_lifetime: Duration,
    ) -> Result<Self, DatabaseError> {
        if max_connections == 0 || max_lifetime.is_zero() {
            return Err(DatabaseError::Configuration);
        }
        Ok(Self {
            inner: Arc::new(PoolInner {
                token_provider,
                connection_factory,
                permits: Arc::new(Semaphore::new(max_connections)),
                idle: Mutex::new(Vec::new()),
                max_lifetime,
            }),
        })
    }

    /// Acquires a healthy connection or opens one with a fresh IAM token.
    ///
    /// # Errors
    ///
    /// Returns an error when credential generation, connection establishment,
    /// or pool coordination fails.
    pub async fn acquire(&self) -> Result<ConnectionLease<T, F>, DatabaseError> {
        let permit = Arc::clone(&self.inner.permits)
            .acquire_owned()
            .await
            .map_err(|_| DatabaseError::Closed)?;
        loop {
            if let Some(mut idle) = self.take_idle()? {
                if idle.opened_at.elapsed() >= self.inner.max_lifetime {
                    continue;
                }
                if self
                    .inner
                    .connection_factory
                    .ping(&mut idle.connection)
                    .await
                    .is_ok()
                {
                    tracing::trace!(
                        credential_label = "state_history_observer",
                        "reusing healthy replica connection"
                    );
                    return Ok(ConnectionLease {
                        connection: Some(idle.connection),
                        opened_at: idle.opened_at,
                        permit: Some(permit),
                        pool: Arc::downgrade(&self.inner),
                    });
                }
                continue;
            }

            tracing::debug!(
                credential_label = "state_history_observer",
                "opening replica connection with a fresh IAM credential"
            );
            let token = self.inner.token_provider.generate().await?;
            let connection = self.inner.connection_factory.open(token).await?;
            return Ok(ConnectionLease {
                connection: Some(connection),
                opened_at: Instant::now(),
                permit: Some(permit),
                pool: Arc::downgrade(&self.inner),
            });
        }
    }

    fn take_idle(&self) -> Result<Option<IdleConnection<F::Connection>>, DatabaseError> {
        self.inner
            .idle
            .lock()
            .map_err(|_| DatabaseError::Closed)
            .map(|mut idle| idle.pop())
    }
}

impl<T, F> Deref for ConnectionLease<T, F>
where
    T: IamTokenProvider,
    F: PhysicalConnectionFactory,
{
    type Target = F::Connection;

    #[expect(
        clippy::expect_used,
        reason = "a live lease always owns its connection"
    )]
    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().expect("connection lease is empty")
    }
}

impl<T, F> DerefMut for ConnectionLease<T, F>
where
    T: IamTokenProvider,
    F: PhysicalConnectionFactory,
{
    #[expect(
        clippy::expect_used,
        reason = "a live lease always owns its connection"
    )]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().expect("connection lease is empty")
    }
}

impl<T, F> Drop for ConnectionLease<T, F>
where
    T: IamTokenProvider,
    F: PhysicalConnectionFactory,
{
    fn drop(&mut self) {
        let (Some(connection), Some(permit), Some(pool)) = (
            self.connection.take(),
            self.permit.take(),
            self.pool.upgrade(),
        ) else {
            return;
        };
        if self.opened_at.elapsed() >= pool.max_lifetime {
            return;
        }
        if let Ok(mut idle) = pool.idle.lock() {
            idle.push(IdleConnection {
                connection,
                opened_at: self.opened_at,
            });
        };
        drop(permit);
    }
}

impl<T, F> state_history::ReadConnectionProvider for IamConnectionPool<T, F>
where
    T: IamTokenProvider,
    F: PhysicalConnectionFactory<Connection = PgConnection>,
{
    type Connection<'a>
        = ConnectionLease<T, F>
    where
        Self: 'a;

    async fn acquire(&self) -> Result<Self::Connection<'_>, sqlx::Error> {
        IamConnectionPool::acquire(self)
            .await
            .map_err(|error| sqlx::Error::Configuration(Box::new(error)))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("database configuration is invalid")]
    Configuration,
    #[error("database login credential could not be generated")]
    Credential,
    #[error("database connection could not be opened")]
    Connect,
    #[error("database connection health check failed")]
    Health,
    #[error("database connection pool is closed")]
    Closed,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    #[derive(Clone)]
    struct FakeTokenProvider {
        calls: Arc<AtomicUsize>,
    }

    impl IamTokenProvider for FakeTokenProvider {
        fn generate(&self) -> DatabaseFuture<'_, SecretToken> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(SecretToken("signed-token".to_owned())) })
        }
    }

    #[derive(Clone)]
    struct FakeConnectionFactory {
        opens: Arc<AtomicUsize>,
        pings: Arc<AtomicUsize>,
    }

    struct FakeConnection {
        healthy: Arc<AtomicBool>,
    }

    impl PhysicalConnectionFactory for FakeConnectionFactory {
        type Connection = FakeConnection;

        fn open(&self, _token: SecretToken) -> DatabaseFuture<'_, Self::Connection> {
            self.opens.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {
                Ok(FakeConnection {
                    healthy: Arc::new(AtomicBool::new(true)),
                })
            })
        }

        fn ping<'a>(&'a self, connection: &'a mut Self::Connection) -> DatabaseFuture<'a, ()> {
            self.pings.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move {
                if connection.healthy.load(Ordering::Relaxed) {
                    Ok(())
                } else {
                    Err(DatabaseError::Health)
                }
            })
        }
    }

    #[tokio::test]
    #[expect(clippy::unwrap_used, reason = "the fake pool must acquire connections")]
    async fn signs_each_physical_open_and_reuses_a_healthy_connection() {
        let token_calls = Arc::new(AtomicUsize::new(0));
        let opens = Arc::new(AtomicUsize::new(0));
        let pings = Arc::new(AtomicUsize::new(0));
        let pool = IamConnectionPool::new(
            FakeTokenProvider {
                calls: Arc::clone(&token_calls),
            },
            FakeConnectionFactory {
                opens: Arc::clone(&opens),
                pings: Arc::clone(&pings),
            },
            2,
            Duration::from_secs(60),
        )
        .unwrap();

        drop(pool.acquire().await.unwrap());
        drop(pool.acquire().await.unwrap());
        assert_eq!(token_calls.load(Ordering::Relaxed), 1);
        assert_eq!(opens.load(Ordering::Relaxed), 1);
        assert_eq!(pings.load(Ordering::Relaxed), 1);

        let first = pool.acquire().await.unwrap();
        let second = pool.acquire().await.unwrap();
        assert_eq!(token_calls.load(Ordering::Relaxed), 2);
        assert_eq!(opens.load(Ordering::Relaxed), 2);
        drop((first, second));
    }

    #[tokio::test]
    #[expect(clippy::unwrap_used, reason = "the fake pool must acquire connections")]
    async fn failed_health_check_opens_with_a_new_token() {
        let token_calls = Arc::new(AtomicUsize::new(0));
        let pool = IamConnectionPool::new(
            FakeTokenProvider {
                calls: Arc::clone(&token_calls),
            },
            FakeConnectionFactory {
                opens: Arc::new(AtomicUsize::new(0)),
                pings: Arc::new(AtomicUsize::new(0)),
            },
            1,
            Duration::from_secs(60),
        )
        .unwrap();
        let connection = pool.acquire().await.unwrap();
        connection.healthy.store(false, Ordering::Relaxed);
        drop(connection);

        drop(pool.acquire().await.unwrap());
        assert_eq!(token_calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    #[expect(clippy::unwrap_used, reason = "the fake pool must acquire connections")]
    async fn waiting_acquire_reuses_a_connection_after_the_lease_returns() {
        let token_calls = Arc::new(AtomicUsize::new(0));
        let pool = IamConnectionPool::new(
            FakeTokenProvider {
                calls: Arc::clone(&token_calls),
            },
            FakeConnectionFactory {
                opens: Arc::new(AtomicUsize::new(0)),
                pings: Arc::new(AtomicUsize::new(0)),
            },
            1,
            Duration::from_secs(60),
        )
        .unwrap();
        let first = pool.acquire().await.unwrap();
        let waiting_pool = pool.clone();
        let waiter = tokio::spawn(async move { waiting_pool.acquire().await.unwrap() });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(first);
        drop(waiter.await.unwrap());
        assert_eq!(token_calls.load(Ordering::Relaxed), 1);
    }
}
