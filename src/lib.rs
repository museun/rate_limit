//! # rate_limit
//! This is a simple token-butcket style rate limiter
//!
//! It provides neat features like an estimate of the next block
//!
//! and possible inverse of the bucket (filling vs draining)
//!
//! this provides both thread-safe and non-thread-safe versions

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// SyncLimiter is a simple token bucket-style rate limiter
///
/// When its tokens are exhausted, it will block the current thread until its refilled
///
/// The limiter is cheaply-clonable
/// ```
/// # fn main() {
/// # use std::time::Duration;
/// # use rate_limit::*;
/// // limits to 3 `.take()` per 1 second
/// let limiter = SyncLimiter::full(3, Duration::from_secs(1));
/// for _ in 0..10 {
///     // every 3 calls within 1 second will cause the next .take() to block
///     // so this will take ~3 seconds to run (10 / 3 = ~3)
///     limiter.take();
/// }
///
/// // initially empty, it will block for 1 second then block for every 3 calls per 1 second
/// let limiter = SyncLimiter::empty(3, Duration::from_secs(1));
/// for _ in 0..10 {
///     limiter.take();
/// }
/// # }
/// ```
#[derive(Clone)]
pub struct SyncLimiter {
    cap: u64,
    bucket: SyncBucket,
}

impl SyncLimiter {
    /// Create a new, thread-safe limiter
    ///
    /// `cap` is the number of total tokens available
    ///
    /// `initial` is how many are initially available
    ///
    /// `period` is how long it'll take to refill all of the tokens
    pub fn new(cap: u64, initial: u64, period: Duration) -> Self {
        Self {
            cap,
            bucket: sync(cap, initial, period),
        }
    }

    /// Create a thread-safe limiter thats pre-filled
    ///
    /// `cap` is the number of total tokens available
    ///
    /// `period` is how long it'll take to refill all of the tokens
    #[inline]
    pub fn full(cap: u64, period: Duration) -> Self {
        Self {
            cap,
            bucket: sync(cap, cap, period),
        }
    }

    /// Create an empty thread-safe limiter
    ///
    /// `cap` is the number of total tokens available
    ///
    /// `period` is how long it'll take to refill all of the tokens
    #[inline]
    pub fn empty(cap: u64, period: Duration) -> Self {
        Self {
            cap,
            bucket: sync(cap, 0, period),
        }
    }

    /// Tries to consume `tokens`
    ///
    /// If it will consume more than available then an Error is returned.
    /// Otherwise it returns how many tokens are left
    ///
    /// This error is the `Duration` of the next available time
    pub fn consume(&self, tokens: u64) -> Result<u64, Duration> {
        let now = Instant::now();

        let mut bucket = self.bucket.0.lock().unwrap();
        if let Some(n) = bucket.refill(now) {
            bucket.tokens = std::cmp::min(bucket.tokens + n, self.cap);
        };

        if tokens <= bucket.tokens {
            bucket.tokens -= tokens;
            bucket.backoff = 0;
            return Ok(bucket.tokens);
        }

        let prev = bucket.tokens;
        Err(bucket.estimate(tokens - prev, now))
    }

    /// Consumes `tokens` blocking if its trying to consume more than available
    ///
    /// Returns how many tokens are available
    pub fn throttle(&self, tokens: u64) -> u64 {
        loop {
            match self.consume(tokens) {
                Ok(rem) => return rem,
                Err(time) => std::thread::sleep(time),
            }
        }
    }

    /// Take a token, blocking if unavailable
    ///
    /// Returns how many tokens are available
    #[inline]
    pub fn take(&self) -> u64 {
        self.throttle(1)
    }
}

/// UnsyncLimiter is a simple token bucket-style rate limiter (cheaper to clone, but not thread safe)
///
/// When its tokens are exhausted, it will block the current thread until its refilled
///
/// The limiter is cheaply-clonable
/// ```
/// # fn main() {
/// # use std::time::Duration;
/// # use rate_limit::*;
/// // limits to 3 `.take()` per 1 second
/// let limiter = UnsyncLimiter::full(3, Duration::from_secs(1));
/// for _ in 0..10 {
///     // every 3 calls within 1 second will cause the next .take() to block
///     // so this will take ~3 seconds to run (10 / 3 = ~3)
///     limiter.take();
/// }
///
/// // initially empty, it will block for 1 second then block for every 3 calls per 1 second
/// let limiter = UnsyncLimiter::empty(3, Duration::from_secs(1));
/// for _ in 0..10 {
///     limiter.take();
/// }
/// # }
/// ```
#[derive(Clone)]
pub struct UnsyncLimiter {
    cap: u64,
    bucket: UnsyncBucket,
}

impl UnsyncLimiter {
    /// Create a new, thread-safe limiter
    ///
    /// `cap` is the number of total tokens available
    ///
    /// `initial` is how many are initially available
    ///
    /// `period` is how long it'll take to refill all of the tokens
    pub fn new(cap: u64, initial: u64, period: Duration) -> Self {
        Self {
            cap,
            bucket: unsync(cap, initial, period),
        }
    }

    /// Create a thread-safe limiter thats pre-filled
    ///
    /// `cap` is the number of total tokens available
    ///
    /// `period` is how long it'll take to refill all of the tokens
    #[inline]
    pub fn full(cap: u64, period: Duration) -> Self {
        Self {
            cap,
            bucket: unsync(cap, cap, period),
        }
    }

    /// Create an empty thread-safe limiter
    ///
    /// `cap` is the number of total tokens available
    ///
    /// `period` is how long it'll take to refill all of the tokens
    #[inline]
    pub fn empty(cap: u64, period: Duration) -> Self {
        Self {
            cap,
            bucket: unsync(cap, 0, period),
        }
    }

    /// Tries to consume `tokens`
    ///
    /// If it will consume more than available then an Error is returned.
    /// Otherwise it returns how many tokens are left
    ///
    /// This error is the `Duration` of the next available time
    pub fn consume(&self, tokens: u64) -> Result<u64, Duration> {
        let now = Instant::now();

        let mut bucket = self.bucket.0.borrow_mut();
        if let Some(n) = bucket.refill(now) {
            bucket.tokens = std::cmp::min(bucket.tokens + n, self.cap);
        };

        if tokens <= bucket.tokens {
            bucket.tokens -= tokens;
            bucket.backoff = 0;
            return Ok(bucket.tokens);
        }

        let prev = bucket.tokens;
        Err(bucket.estimate(tokens - prev, now))
    }

    /// Consumes `tokens` blocking if its trying to consume more than available
    ///
    /// Returns how many tokens are available
    pub fn throttle(&self, tokens: u64) -> u64 {
        loop {
            match self.consume(tokens) {
                Ok(rem) => return rem,
                Err(time) => std::thread::sleep(time),
            }
        }
    }

    /// Take a token, blocking if unavailable
    ///
    /// Returns how many tokens are available
    #[inline]
    pub fn take(&self) -> u64 {
        self.throttle(1)
    }
}

#[derive(Clone)]
struct SyncBucket(Arc<Mutex<Inner>>);

#[derive(Clone)]
struct UnsyncBucket(Rc<RefCell<Inner>>);

fn sync(cap: u64, initial: u64, period: Duration) -> SyncBucket {
    SyncBucket(Arc::new(Mutex::new(Inner::new(cap, initial, period))))
}

fn unsync(cap: u64, initial: u64, period: Duration) -> UnsyncBucket {
    UnsyncBucket(Rc::new(RefCell::new(Inner::new(cap, initial, period))))
}

#[derive(Clone)]
struct Inner {
    tokens: u64,
    backoff: u32,

    next: Instant,
    last: Instant,

    quantum: u64,
    period: Duration,
}

impl Inner {
    fn new(tokens: u64, initial: u64, period: Duration) -> Self {
        let now = Instant::now();
        Self {
            tokens: initial,
            backoff: 0,

            next: now + period,
            last: now,

            quantum: tokens,
            period,
        }
    }

    fn refill(&mut self, now: Instant) -> Option<u64> {
        if now < self.next {
            return None;
        }
        let last = now.duration_since(self.last);
        let periods = last.as_nanos().checked_div(self.period.as_nanos())? as u64;
        self.last += self.period * (periods as u32);
        self.next = self.last + self.period;
        Some(periods * self.quantum)
    }

    fn estimate(&mut self, tokens: u64, now: Instant) -> Duration {
        let until = self.next.duration_since(now);
        let periods = (tokens.checked_add(self.quantum).unwrap() - 1) / self.quantum;
        until + self.period * (periods as u32 - 1)
    }
}
