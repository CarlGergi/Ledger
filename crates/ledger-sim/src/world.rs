//! Everything a run shares: its one source of randomness, its trace, and the record of
//! what really happened outside the log, which the invariants compare the log against.

use std::sync::{Arc, Mutex, MutexGuard};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tokio::time::Instant;

/// A side effect a tool really had, as the outside world would see it.
#[derive(Debug, Clone)]
pub struct Effect {
    pub tool: String,
    pub intent: String,
    /// The idempotency key it was sent with, if any.
    pub key: Option<String>,
    /// When it happened, on the run's one ordering of everything.
    pub tick: u64,
}

/// What an agent was told about one attempt at one intent.
#[derive(Debug, Clone)]
pub struct Seen {
    pub intent: String,
    pub answer: Result<serde_json::Value, i32>,
}

/// An append the store accepted, for ordering against effects.
#[derive(Debug, Clone)]
pub struct Appended {
    pub session: String,
    pub seq: i64,
    pub tick: u64,
}

#[derive(Debug)]
pub struct State {
    rng: ChaCha8Rng,
    /// How much harsher than the base rates this run's faults are.
    intensity: f64,
    tick: u64,
    next_id: u64,
    pub trace: Vec<String>,
    pub effects: Vec<Effect>,
    pub seen: Vec<Seen>,
    pub appended: Vec<Appended>,
}

#[derive(Debug, Clone)]
pub struct World {
    state: Arc<Mutex<State>>,
    started: Instant,
}

impl World {
    pub fn new(seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        // From half to four times the base rates, so that seeds explore gentle and rough
        // runs alike.
        let intensity = rng.random_range(0.5..4.0);
        Self {
            state: Arc::new(Mutex::new(State {
                rng,
                intensity,
                tick: 0,
                next_id: 0,
                trace: Vec::new(),
                effects: Vec::new(),
                seen: Vec::new(),
                appended: Vec::new(),
            })),
            started: Instant::now(),
        }
    }

    pub fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("world lock poisoned")
    }

    /// True with probability `p`, a fault's base rate, scaled by this run's intensity.
    pub fn chance(&self, p: f64) -> bool {
        let mut state = self.state();
        let p = (p * state.intensity).min(0.9);
        state.rng.random_bool(p)
    }

    pub fn intensity(&self) -> f64 {
        self.state().intensity
    }

    /// A number in `low..high`.
    pub fn pick(&self, low: u64, high: u64) -> u64 {
        self.state().rng.random_range(low..high)
    }

    /// The next point on the run's single ordering of events.
    pub fn tick(&self) -> u64 {
        let mut state = self.state();
        state.tick += 1;
        state.tick
    }

    /// A JSON-RPC id no agent in this run has used.
    pub fn next_id(&self) -> u64 {
        let mut state = self.state();
        state.next_id += 1;
        state.next_id
    }

    /// Adds a line to the trace, stamped with simulated time.
    pub fn note(&self, line: impl AsRef<str>) {
        let millis = (Instant::now() - self.started).as_millis();
        let mut state = self.state();
        state
            .trace
            .push(format!("{millis:>7}ms  {}", line.as_ref()));
    }
}
