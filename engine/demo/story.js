export const PAYMENT_KEY = "order:charge";

export const GITHUB = "https://github.com/vinaybadgujar102/mini-temporal";

export const DISCLAIMER =
  "Illustrated story of how this repo's Postgres, outbox, Kafka, and workers behave. Proof stays in the engine tests.";

export const NODES = [
  "client",
  "postgres",
  "outbox",
  "kafka",
  "workerA",
  "workerB",
  "charge",
  "reserve",
  "email",
];

const TASKS = ["charge", "reserve", "email"];

const pending = { charge: "PENDING", reserve: "PENDING", email: "PENDING" };
const rootsReady = { charge: "READY", reserve: "READY", email: "PENDING" };

function beat(partial) {
  return {
    hot: [],
    tasks: pending,
    workflow: null,
    lease: null,
    tokens: {},
    paymentKey: null,
    attempt: null,
    terminal: false,
    href: null,
    ...partial,
  };
}

export const CHAPTERS = [
  {
    id: "happy",
    title: "Happy path",
    beats: [
      beat({
        id: "start",
        cta: "Start",
        caption:
          "Process Order charges the card and reserves stock in parallel, then sends email.",
        focus: "client",
        hot: ["client"],
        focusTitle: "Client",
        focusBody: "One workflow. Charge and Reserve start together. Email waits for both.",
      }),
      beat({
        id: "commit",
        cta: "Dispatch",
        caption:
          "Postgres commits the workflow, the tasks, and the outbox rows in one transaction.",
        focus: "postgres",
        hot: ["postgres", "charge", "reserve"],
        tasks: rootsReady,
        workflow: "RUNNING",
        focusTitle: "PostgreSQL",
        focusBody: "Email stays pending and has no outbox row yet.",
      }),
      beat({
        id: "dispatch",
        cta: "Claim tasks",
        caption: "The relay publishes those unpublished outbox rows to Kafka.",
        focus: "outbox",
        hot: ["outbox", "kafka"],
        tasks: rootsReady,
        workflow: "RUNNING",
        focusTitle: "Outbox relay",
        focusBody: "Kafka is this same hop. The commit already happened.",
      }),
      beat({
        id: "claim",
        cta: "Finish both",
        caption:
          "Worker A claims Charge and Worker B claims Reserve, each with fencing token 1.",
        focus: "workerA",
        hot: ["workerA", "workerB", "charge", "reserve"],
        tasks: { charge: "RUNNING", reserve: "RUNNING", email: "PENDING" },
        workflow: "RUNNING",
        tokens: { A: 1, B: 1 },
        focusTitle: "Claim",
        focusBody: "READY becomes RUNNING, and the attempt stores a fencing token.",
      }),
      beat({
        id: "both-done",
        cta: "Send email",
        caption: "Charge and Reserve finish, so Email becomes ready and its outbox row is committed.",
        focus: "email",
        hot: ["email", "charge", "reserve", "outbox"],
        tasks: { charge: "COMPLETED", reserve: "COMPLETED", email: "READY" },
        workflow: "RUNNING",
        focusTitle: "Email",
        focusBody: "The second completion writes that outbox row. Kafka has not been told yet.",
      }),
      beat({
        id: "email",
        cta: "Next chapter",
        caption: "The relay delivers Email, Worker A finishes it, and the workflow is marked COMPLETED.",
        focus: "postgres",
        hot: ["postgres", "email", "outbox", "kafka", "workerA"],
        tasks: { charge: "COMPLETED", reserve: "COMPLETED", email: "COMPLETED" },
        workflow: "COMPLETED",
        tokens: { A: 1 },
        terminal: true,
        focusTitle: "Completed",
        focusBody: "Finishing the last task and marking the workflow COMPLETED are one commit.",
      }),
    ],
  },
  {
    id: "crash",
    title: "Crash and lease",
    beats: [
      beat({
        id: "start",
        cta: "Start",
        caption: "This new run stops Worker A after it claims Charge.",
        focus: "client",
        hot: ["client"],
        focusTitle: "New run",
        focusBody: "Same Process Order graph. Reserve stays in view, and the lesson follows Charge.",
      }),
      beat({
        id: "commit",
        cta: "Dispatch",
        caption: "Postgres commits Charge as ready, with an outbox row, before anyone claims it.",
        focus: "postgres",
        hot: ["postgres", "charge"],
        tasks: rootsReady,
        workflow: "RUNNING",
        focusTitle: "PostgreSQL",
        focusBody: "The outbox row is in this commit. Kafka is not.",
      }),
      beat({
        id: "dispatch",
        cta: "Claim Charge",
        caption: "The relay publishes the outbox row to Kafka.",
        focus: "outbox",
        hot: ["outbox", "kafka"],
        tasks: rootsReady,
        workflow: "RUNNING",
        focusTitle: "Outbox relay",
        focusBody: "One hop from unpublished outbox to Kafka.",
      }),
      beat({
        id: "claim",
        cta: "Crash A",
        caption: "Worker A claims Charge with fencing token 1, and heartbeats keep the lease alive.",
        focus: "workerA",
        hot: ["workerA", "charge"],
        tasks: { charge: "RUNNING", reserve: "READY", email: "PENDING" },
        workflow: "RUNNING",
        lease: "live",
        tokens: { A: 1 },
        focusTitle: "Lease",
        focusBody: "Heartbeats extend the lease. Charge stays RUNNING while they arrive.",
      }),
      beat({
        id: "crash",
        cta: "Recover",
        caption: "Worker A crashes, heartbeats stop, and Charge stays RUNNING while the lease runs out.",
        focus: "workerA",
        hot: ["workerA", "charge"],
        tasks: { charge: "RUNNING", reserve: "READY", email: "PENDING" },
        workflow: "RUNNING",
        lease: "shrink",
        tokens: { A: 1 },
        focusTitle: "Crash",
        focusBody: "Charge stays RUNNING. The bar stands in for that wait, and Recover is the next click.",
      }),
      beat({
        id: "recover",
        cta: "Let B claim",
        caption: "Recovery marks the attempt TIMED_OUT, sets Charge back to READY, and writes a new outbox row.",
        focus: "postgres",
        hot: ["postgres", "charge", "outbox"],
        tasks: rootsReady,
        workflow: "RUNNING",
        lease: "dead",
        attempt: "TIMED_OUT",
        focusTitle: "Recover",
        focusBody: "Token 1 is the dead attempt. The next claim gets a new token.",
      }),
      beat({
        id: "reclaim",
        cta: "Next chapter",
        caption: "The relay publishes that new outbox row, and Worker B claims Charge with fencing token 2.",
        focus: "workerB",
        hot: ["workerB", "charge", "outbox", "kafka"],
        tasks: { charge: "RUNNING", reserve: "READY", email: "PENDING" },
        workflow: "RUNNING",
        tokens: { B: 2 },
        terminal: true,
        focusTitle: "New claim",
        focusBody: "Dispatch and the new claim are one click. Token 1 cannot finish this attempt.",
      }),
    ],
  },
  {
    id: "fence",
    title: "Fence",
    beats: [
      beat({
        id: "ready",
        cta: "Both try",
        caption: "Charge is READY after recovery, and the next claim will be fencing token 2.",
        focus: "charge",
        hot: ["charge", "outbox"],
        tasks: rootsReady,
        workflow: "RUNNING",
        attempt: "TIMED_OUT",
        tokens: { A: "token 1 timed out" },
        focusTitle: "After recovery",
        focusBody: "This chapter starts after the lease died. It does not replay the crash.",
      }),
      beat({
        id: "race",
        cta: "Dead A finishes",
        caption: "Worker A wins fencing token 2, and Worker B skips because Charge is no longer READY.",
        focus: "workerA",
        hot: ["workerA", "workerB", "charge"],
        tasks: { charge: "RUNNING", reserve: "READY", email: "PENDING" },
        workflow: "RUNNING",
        tokens: { A: 2, B: "skip" },
        focusTitle: "One winner",
        focusBody: "Two consumers try. One claim updates READY. The other skips.",
      }),
      beat({
        id: "stale",
        cta: "Live A finishes",
        caption: "The crashed worker tries to complete with token 1, and Postgres refuses it.",
        focus: "workerA",
        hot: ["workerA", "postgres", "charge"],
        tasks: { charge: "RUNNING", reserve: "READY", email: "PENDING" },
        workflow: "RUNNING",
        tokens: { A: "token 2 live, token 1 refused", B: "skip" },
        stale: "A",
        focusTitle: "Fencing token",
        focusBody: "Token 1's attempt is already TIMED_OUT, so that complete matches nothing.",
      }),
      beat({
        id: "finish",
        cta: "Next chapter",
        caption: "Worker A, holding token 2, completes Charge.",
        focus: "workerA",
        hot: ["workerA", "charge"],
        tasks: { charge: "COMPLETED", reserve: "READY", email: "PENDING" },
        workflow: "RUNNING",
        tokens: { A: 2 },
        terminal: true,
        focusTitle: "Live owner",
        focusBody: "The stale complete did not land. The live owner did.",
      }),
    ],
  },
  {
    id: "saga",
    title: "Saga",
    beats: [
      beat({
        id: "start",
        cta: "Start",
        caption: "This new Process Order gives Charge one payment key, so a repeat charge does nothing.",
        focus: "client",
        hot: ["client"],
        focusTitle: "Payment key",
        focusBody: `The illustrated key is ${PAYMENT_KEY}.`,
      }),
      beat({
        id: "commit",
        cta: "Dispatch",
        caption: `Postgres stores the workflow and the charge key ${PAYMENT_KEY} in the same commit.`,
        focus: "postgres",
        hot: ["postgres", "charge", "reserve"],
        tasks: rootsReady,
        workflow: "RUNNING",
        paymentKey: PAYMENT_KEY,
        focusTitle: "PostgreSQL",
        focusBody: "The key is the workflow plus the charge task. A second charge with that key does nothing.",
      }),
      beat({
        id: "dispatch",
        cta: "Run charge and reserve",
        caption: "The relay publishes Charge and Reserve to Kafka.",
        focus: "outbox",
        hot: ["outbox", "kafka"],
        tasks: rootsReady,
        workflow: "RUNNING",
        paymentKey: PAYMENT_KEY,
        focusTitle: "Outbox relay",
        focusBody: "Both root tasks go out in this hop.",
      }),
      beat({
        id: "both-done",
        cta: "Fail email",
        caption: "Charge and Reserve are complete, and the same charge key would not charge twice.",
        focus: "charge",
        hot: ["charge", "reserve"],
        tasks: { charge: "COMPLETED", reserve: "COMPLETED", email: "READY" },
        workflow: "RUNNING",
        paymentKey: PAYMENT_KEY,
        focusTitle: "Idempotent charge",
        focusBody: `${PAYMENT_KEY} already succeeded. Email is now ready.`,
      }),
      beat({
        id: "email-fail",
        cta: "Refund charge",
        caption: "Email fails permanently, nothing else is still open, and the workflow turns COMPENSATING.",
        focus: "postgres",
        hot: ["postgres", "email", "charge"],
        tasks: { charge: "COMPLETED", reserve: "COMPLETED", email: "FAILED" },
        workflow: "COMPENSATING",
        paymentKey: PAYMENT_KEY,
        focusTitle: "COMPENSATING",
        focusBody: "The fail, the cancel, and the refund intent commit together. Only the charge has an undo.",
      }),
      beat({
        id: "refund",
        cta: "End workflow",
        caption: `Refund uses the same payment key ${PAYMENT_KEY}, and inventory has no undo.`,
        focus: "charge",
        hot: ["charge"],
        tasks: { charge: "COMPLETED", reserve: "COMPLETED", email: "FAILED" },
        workflow: "COMPENSATING",
        paymentKey: PAYMENT_KEY,
        sameKey: true,
        focusTitle: "Refund",
        focusBody: "Refund looks up that same charge key. Reserve has no compensator.",
      }),
      beat({
        id: "failed",
        cta: "Source on GitHub",
        caption: "The refund finished, so the workflow ends FAILED.",
        focus: "postgres",
        hot: ["postgres", "charge", "reserve", "email"],
        tasks: { charge: "COMPLETED", reserve: "COMPLETED", email: "FAILED" },
        workflow: "FAILED",
        paymentKey: PAYMENT_KEY,
        sameKey: true,
        terminal: true,
        href: GITHUB,
        focusTitle: "FAILED",
        focusBody: "The charge was undone. The inventory reserve was not.",
      }),
    ],
  },
];

export function jump(chapter) {
  if (!Number.isInteger(chapter) || chapter < 0 || chapter >= CHAPTERS.length) {
    throw new Error("bad chapter");
  }
  return { chapter, beat: 0 };
}

export function initial(chapter = 0) {
  return jump(chapter);
}

export function beatAt(state) {
  return CHAPTERS[state.chapter].beats[state.beat];
}

export function advance(state) {
  const current = beatAt(state);
  if (current.terminal) {
    if (state.chapter >= CHAPTERS.length - 1) return state;
    return { chapter: state.chapter + 1, beat: 0 };
  }
  return { chapter: state.chapter, beat: state.beat + 1 };
}

export function keptNodes(current) {
  const keep = new Set([current.focus, ...current.hot]);
  if (TASKS.some((id) => keep.has(id))) keep.add("postgres");
  return keep;
}

export function present(state) {
  const chapter = CHAPTERS[state.chapter];
  const current = beatAt(state);
  const keep = keptNodes(current);
  return {
    chapterId: chapter.id,
    beat: current,
    dim: NODES.filter((id) => !keep.has(id)),
  };
}
