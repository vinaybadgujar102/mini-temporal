import { advance, beatAt, CHAPTERS, jump, keptNodes } from "./story.js";

const cta = document.querySelector("#cta");
const ctaLink = document.querySelector("#cta-link");
const caption = document.querySelector("#caption");
const focusTitle = document.querySelector("#focus-title");
const focusBody = document.querySelector("#focus-body");
const pay = document.querySelector("#pay");
const attempt = document.querySelector("#attempt");
const heartbeat = document.querySelector("#heartbeat");
const lease = document.querySelector("[data-lease]");
const chapterButtons = [...document.querySelectorAll("[data-chapter]")];
const nodes = [...document.querySelectorAll("[data-node]")];
const wires = [...document.querySelectorAll("[data-ends]")];

let state = jump(0);

function tokenText(tokens, who) {
  const value = tokens?.[who];
  if (value == null) return "";
  if (value === "skip") return "skipped";
  if (typeof value === "number") return `token ${value}`;
  return value;
}

function render(animate) {
  document.body.classList.toggle("skip-motion", !animate);
  const current = beatAt(state);
  const keep = keptNodes(current);

  caption.textContent = current.caption;
  focusTitle.textContent = current.focusTitle;
  focusBody.textContent = current.focusBody;

  const external = Boolean(current.href);
  cta.hidden = external;
  ctaLink.hidden = !external;
  if (external) {
    ctaLink.textContent = current.cta;
    ctaLink.href = current.href;
  } else {
    cta.textContent = current.cta;
  }

  for (const button of chapterButtons) {
    const on = Number(button.dataset.chapter) === state.chapter;
    button.setAttribute("aria-pressed", on ? "true" : "false");
  }

  for (const el of nodes) {
    const id = el.dataset.node;
    const nested = el.classList.contains("mini");
    const quiet = !keep.has(id) && !(nested && !keep.has("postgres"));
    el.classList.toggle("is-focus", id === current.focus);
    el.classList.toggle("is-hot", id !== current.focus && keep.has(id));
    el.classList.toggle("is-dim", quiet);
  }

  for (const wire of wires) {
    const ends = wire.dataset.ends.split(",");
    wire.classList.toggle("is-hot", ends.some((id) => keep.has(id)));
  }

  for (const pill of document.querySelectorAll("[data-pill]")) {
    const key = pill.dataset.pill;
    const label = key === "workflow" ? current.workflow ?? "waiting" : current.tasks[key];
    pill.textContent = label;
    pill.dataset.status = label;
  }

  if (current.paymentKey) {
    pay.hidden = false;
    pay.textContent = `payment key ${current.paymentKey}`;
  } else {
    pay.hidden = true;
    pay.textContent = "";
  }

  if (current.attempt) {
    attempt.hidden = false;
    attempt.textContent = `attempt ${current.attempt}`;
  } else {
    attempt.hidden = true;
    attempt.textContent = "";
  }

  for (const el of document.querySelectorAll("[data-token]")) {
    el.textContent = tokenText(current.tokens, el.dataset.token);
  }

  if (current.lease) {
    lease.hidden = false;
    lease.dataset.mode = current.lease;
    heartbeat.hidden = false;
    heartbeat.textContent = current.lease === "live" ? "heartbeats on" : "heartbeats stopped";
  } else {
    lease.hidden = true;
    lease.dataset.mode = "";
    heartbeat.hidden = true;
    heartbeat.textContent = "";
  }

}

cta.addEventListener("click", () => {
  state = advance(state);
  render(true);
});

for (const button of chapterButtons) {
  button.addEventListener("click", () => {
    state = jump(Number(button.dataset.chapter));
    render(false);
  });
}

render(false);

export { CHAPTERS };
