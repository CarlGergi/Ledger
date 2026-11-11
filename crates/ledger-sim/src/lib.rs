//! Deterministic simulation of Ledger: three gateway copies, a shared store, a tool
//! server and several agents, all in one thread, with every fault chosen by one seeded
//! random number generator and every timer firing in simulated time. A seed therefore
//! replays exactly, and a failing one can be stepped through.
//!
//! The gateway is the real one, driven through [`Gateway::call`]. The store is the
//! in-memory store, which `ledger-store`'s conformance suite holds to the PostgreSQL
//! store's behaviour. HTTP and the MCP transport are not simulated; the integration tests
//! cover them.

mod check;
mod faults;
mod world;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{TimeZone, Utc};
use ledger_gateway::{Arrival, EffectPolicy, Gateway, Upstream, error_codes};
use ledger_store::{EventStore, MemoryStore};
use rmcp::model::CallToolRequestParams;
use serde_json::{Map, Value, json};

use crate::faults::{CopyStore, CopyUpstream, Life, OVERRIDES, TOOLS};
use crate::world::{Seen, World};

pub use crate::check::Stats;

const COPIES: usize = 3;
const CALL_TIMEOUT: Duration = Duration::from_secs(3);
const LEASE: Duration = Duration::from_secs(2);
/// How long an agent waits for an answer before trying again.
const AGENT_PATIENCE: Duration = Duration::from_secs(6);
const ATTEMPTS: usize = 4;
/// Long enough for every call still running at the end to finish, and every lease to lapse.
const SETTLE: Duration = Duration::from_secs(30);

const DROP_REQUEST: f64 = 0.03;
const DUPLICATE_REQUEST: f64 = 0.03;
const DROP_ANSWER: f64 = 0.03;

/// The outcome of one seed.
#[derive(Debug)]
pub struct Report {
    pub seed: u64,
    pub trace: Vec<String>,
    /// Every invariant the run broke. Empty means it passed.
    pub violations: Vec<String>,
    pub stats: Stats,
}

impl Report {
    /// A digest of the trace, for checking that a seed replays exactly.
    pub fn fingerprint(&self) -> u64 {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.trace.hash(&mut hasher);
        hasher.finish()
    }
}

/// Runs one seed to completion and checks every invariant.
pub fn run(seed: u64) -> Report {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("could not build a runtime");
    runtime.block_on(simulate(seed))
}

/// The gateway copies, each replaceable by a new incarnation after it dies.
struct Cluster {
    store: Arc<MemoryStore>,
    world: World,
    slots: Mutex<Vec<(Arc<Life>, Gateway)>>,
    generations: Mutex<Vec<u32>>,
}

impl Cluster {
    fn incarnate(&self, index: usize, faulty: bool) -> (Arc<Life>, Gateway) {
        let generation = {
            let mut generations = self.generations.lock().expect("lock poisoned");
            generations[index] += 1;
            generations[index]
        };
        let name = if faulty {
            format!("copy{index}.{generation}")
        } else {
            "sweeper".to_owned()
        };
        let life = Life::new(name.clone());
        let store: Arc<dyn EventStore> = Arc::new(CopyStore {
            inner: self.store.clone(),
            life: life.clone(),
            world: self.world.clone(),
            faulty,
        });
        let upstream: Arc<dyn Upstream> = Arc::new(CopyUpstream {
            life: life.clone(),
            world: self.world.clone(),
            faulty,
        });
        let policy = EffectPolicy::parse(OVERRIDES).expect("valid overrides");
        let gateway = Gateway::with_policy(store, vec![upstream], CALL_TIMEOUT, policy)
            .with_lease_ttl(LEASE)
            .with_owner(name);
        (life, gateway)
    }

    /// Any copy, alive or not, as a load balancer that does not know would pick.
    fn pick(&self) -> (Arc<Life>, Gateway) {
        let index = self.world.pick(0, COPIES as u64) as usize;
        self.slots.lock().expect("lock poisoned")[index].clone()
    }
}

async fn simulate(seed: u64) -> Report {
    let world = World::new(seed);
    let epoch = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let store = Arc::new(MemoryStore::with_epoch(epoch));
    let cluster = Arc::new(Cluster {
        store: store.clone(),
        world: world.clone(),
        slots: Mutex::new(Vec::new()),
        generations: Mutex::new(vec![0; COPIES + 1]),
    });
    let first: Vec<_> = (0..COPIES).map(|i| cluster.incarnate(i, true)).collect();
    *cluster.slots.lock().expect("lock poisoned") = first;

    let agents: Vec<Agent> = (0..world.pick(2, 6))
        .map(|n| Agent::new(&world, n))
        .collect();
    world.note(format!(
        "faults at {:.2} times their base rates",
        world.intensity()
    ));
    for agent in &agents {
        world.note(format!(
            "{} is {:?}, {} intents",
            agent.session,
            agent.kind,
            agent.intents.len()
        ));
    }

    let done = Arc::new(AtomicBool::new(false));
    let chaos = tokio::spawn(chaos(cluster.clone(), done.clone()));
    let running: Vec<_> = agents
        .iter()
        .cloned()
        .map(|agent| tokio::spawn(agent.run(world.clone(), cluster.clone())))
        .collect();
    for agent in running {
        agent.await.expect("agent panicked");
    }
    done.store(true, Ordering::SeqCst);
    chaos.await.expect("chaos panicked");

    // Wake every paused copy, let everything still running finish, and let leases lapse.
    for (life, _) in cluster.slots.lock().expect("lock poisoned").iter() {
        life.wake();
    }
    tokio::time::sleep(SETTLE).await;
    world.note("settled");

    // A healthy copy takes every session over, which settles anything left open.
    let (_, sweeper) = cluster.incarnate(COPIES, false);
    for agent in &agents {
        let arrival = arrival(
            &agent.session,
            "sweeper",
            json!("sweep"),
            None,
            "read",
            "sweep",
        );
        let answer = sweeper.call(arrival).await;
        world.note(format!(
            "sweeper took {}: {}",
            agent.session,
            describe(&answer)
        ));
    }

    let (violations, stats) = check::check(&world, &store, &agents).await;
    let trace = std::mem::take(&mut world.state().trace);
    Report {
        seed,
        trace,
        violations,
        stats,
    }
}

/// Kills and pauses copies at random until the agents are done.
async fn chaos(cluster: Arc<Cluster>, done: Arc<AtomicBool>) {
    let world = cluster.world.clone();
    while !done.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(world.pick(200, 3000))).await;
        let index = world.pick(0, COPIES as u64) as usize;
        let (life, _) = cluster.slots.lock().expect("lock poisoned")[index].clone();
        if life.is_dead() {
            continue;
        }
        match world.pick(0, 4) {
            0 => {
                world.note(format!("chaos: kill {}", life.name));
                life.kill();
                let downtime = Duration::from_millis(world.pick(500, 5000));
                let cluster = cluster.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(downtime).await;
                    let next = cluster.incarnate(index, true);
                    cluster.world.note(format!("chaos: start {}", next.0.name));
                    cluster.slots.lock().expect("lock poisoned")[index] = next;
                });
            }
            1 => {
                // Often longer than a lease, which makes a zombie of it.
                let pause = Duration::from_millis(world.pick(500, 6000));
                world.note(format!(
                    "chaos: pause {} for {}ms",
                    life.name,
                    pause.as_millis()
                ));
                life.pause_for(pause);
            }
            _ => {}
        }
    }
}

/// How an agent recognises its own retries, if at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Numbers its steps, and reconnects to retry, with a new transport session and ids.
    Stepped,
    /// Resends with the same JSON-RPC id over the same transport session.
    SameId,
    /// Resends with a new id and no step, which ledger cannot tell from a new call.
    NewId,
}

#[derive(Debug, Clone)]
pub struct Agent {
    pub session: String,
    pub kind: Kind,
    /// Tool and intent, in order.
    pub intents: Vec<(String, String)>,
}

impl Agent {
    fn new(world: &World, n: u64) -> Self {
        let kind = match world.pick(0, 3) {
            0 => Kind::Stepped,
            1 => Kind::SameId,
            _ => Kind::NewId,
        };
        let session = format!("s{n}");
        let intents = (0..world.pick(2, 9))
            .map(|i| {
                let tool = TOOLS[world.pick(0, TOOLS.len() as u64) as usize].0;
                (tool.to_owned(), format!("{session}.{i}"))
            })
            .collect();
        Self {
            session,
            kind,
            intents,
        }
    }

    async fn run(self, world: World, cluster: Arc<Cluster>) {
        for (tool, intent) in &self.intents {
            let mut id = json!(world.next_id());
            let mut transport = format!("{}-t{}", self.session, world.next_id());
            let step = (self.kind == Kind::Stepped).then(|| intent.clone());
            for attempt in 1..=ATTEMPTS {
                match self.kind {
                    // Reconnects after a failure, so a new transport session and new ids:
                    // only its step says which call it repeats.
                    Kind::Stepped if attempt > 1 => {
                        transport = format!("{}-t{}", self.session, world.next_id());
                        id = json!(world.next_id());
                    }
                    Kind::NewId if attempt > 1 => id = json!(world.next_id()),
                    _ => {}
                }
                let deliver = || {
                    arrival(
                        &self.session,
                        &transport,
                        id.clone(),
                        step.clone(),
                        tool,
                        intent,
                    )
                };
                tokio::time::sleep(Duration::from_millis(world.pick(0, 200))).await;
                let tag = format!("{} attempt {attempt}", intent);

                if world.chance(DROP_REQUEST) {
                    world.note(format!("network: {tag} lost on the way"));
                    tokio::time::sleep(AGENT_PATIENCE).await;
                    continue;
                }
                if world.chance(DUPLICATE_REQUEST) {
                    let (life, copy) = cluster.pick();
                    world.note(format!("network: {tag} duplicated to {}", life.name));
                    let duplicate = deliver();
                    tokio::spawn(async move { copy.call(duplicate).await });
                }
                let (life, copy) = cluster.pick();
                world.note(format!("{tag} to {}", life.name));
                let answer = tokio::time::timeout(AGENT_PATIENCE, copy.call(deliver())).await;
                let Ok(answer) = answer else {
                    world.note(format!("{tag}: no answer in time"));
                    continue;
                };
                if world.chance(DROP_ANSWER) {
                    world.note(format!("network: answer to {tag} lost"));
                    continue;
                }
                world.note(format!("{tag}: {}", describe(&answer)));
                let seen = match &answer {
                    Ok(result) => {
                        let result = rmcp::model::ServerResult::from(result.clone());
                        Ok(serde_json::to_value(result).expect("serialisable"))
                    }
                    Err(error) => Err(error.code.0),
                };
                world.state().seen.push(Seen {
                    intent: intent.clone(),
                    answer: seen,
                });
                let try_again = match &answer {
                    Ok(_) => false,
                    Err(error) => {
                        let flagged = error.data.as_ref().is_some_and(|d| d["in_doubt"] == true);
                        [
                            error_codes::NOT_RECORDED,
                            error_codes::OUTCOME_NOT_RECORDED,
                            error_codes::OUTCOME_UNKNOWN,
                        ]
                        .contains(&error.code)
                            && !flagged
                    }
                };
                if !try_again {
                    break;
                }
            }
        }
    }
}

fn arrival(
    session: &str,
    transport: &str,
    id: Value,
    step: Option<String>,
    tool: &str,
    intent: &str,
) -> Arrival {
    let mut arguments = Map::new();
    arguments.insert("intent".to_owned(), json!(intent));
    Arrival {
        session: session.to_owned(),
        rpc_id: id,
        meta: json!({}),
        transport: Some(transport.to_owned()),
        step,
        params: CallToolRequestParams::new(format!("svc__{tool}")).with_arguments(arguments),
    }
}

fn describe(answer: &Result<rmcp::model::CallToolResponse, rmcp::ErrorData>) -> String {
    match answer {
        Ok(_) => "ok".to_owned(),
        Err(error) => format!("error {}", error.code.0),
    }
}
