import { expect, test } from "bun:test";
import {
  advance,
  beatAt,
  CHAPTERS,
  DISCLAIMER,
  GITHUB,
  initial,
  jump,
  keptNodes,
  NODES,
  PAYMENT_KEY,
  present,
} from "./story.js";

test("four chapters and every beat names a real node", () => {
  expect(CHAPTERS.map((chapter) => chapter.id)).toEqual(["happy", "crash", "fence", "saga"]);
  for (const chapter of CHAPTERS) {
    expect(chapter.beats.length).toBeGreaterThan(1);
    for (const beat of chapter.beats) {
      expect(NODES).toContain(beat.focus);
      expect(beat.caption.endsWith(".")).toBe(true);
      expect(beat.caption.split(".").filter(Boolean)).toHaveLength(1);
      expect(beat.cta.length).toBeGreaterThan(0);
      for (const id of beat.hot) expect(NODES).toContain(id);
    }
  }
});

test("happy path dispatch is one hop, including the email send", () => {
  const beats = CHAPTERS[0].beats;
  expect(beats.map((beat) => beat.id)).toEqual([
    "start",
    "commit",
    "dispatch",
    "claim",
    "both-done",
    "email",
  ]);
  expect(beats.find((beat) => beat.id === "both-done")?.tasks.email).toBe("READY");
  const kafkaBeats = beats.filter((beat) => beat.hot.includes("kafka"));
  expect(kafkaBeats.map((beat) => beat.id)).toEqual(["dispatch", "email"]);
  for (const beat of kafkaBeats) expect(beat.hot).toContain("outbox");
  expect(beats.find((beat) => beat.id === "claim")?.tokens).toEqual({ A: 1, B: 1 });
  expect(beats.at(-1)?.workflow).toBe("COMPLETED");
  expect(beats.at(-1)?.cta).toBe("Next chapter");
});

test("crash recovers only after the lease shrinks, then B claims token 2", () => {
  const beats = CHAPTERS[1].beats;
  const claim = beats.find((beat) => beat.id === "claim");
  const crash = beats.find((beat) => beat.id === "crash");
  const recover = beats.find((beat) => beat.id === "recover");
  const reclaim = beats.find((beat) => beat.id === "reclaim");
  expect(claim?.lease).toBe("live");
  expect(claim?.tasks.charge).toBe("RUNNING");
  expect(crash?.lease).toBe("shrink");
  expect(crash?.tasks.charge).toBe("RUNNING");
  expect(crash?.cta).toBe("Recover");
  expect(recover?.attempt).toBe("TIMED_OUT");
  expect(recover?.tasks.charge).toBe("READY");
  expect(recover?.hot).toContain("outbox");
  expect(recover?.hot).not.toContain("kafka");
  expect(reclaim?.hot).toEqual(expect.arrayContaining(["outbox", "kafka"]));
  expect(reclaim?.tokens.B).toBe(2);
});

test("fence: A wins token 2, B skips, stale complete is a separate beat", () => {
  const beats = CHAPTERS[2].beats;
  expect(beats.map((beat) => beat.id)).toEqual(["ready", "race", "stale", "finish"]);
  expect(beats[1].tokens).toEqual({ A: 2, B: "skip" });
  expect(beats[2].stale).toBe("A");
  expect(beats[2].tasks.charge).toBe("RUNNING");
  expect(beats[3].tasks.charge).toBe("COMPLETED");
  expect(beats[3].tokens.A).toBe(2);
});

test("saga refunds the same charge key and ends FAILED", () => {
  const beats = CHAPTERS[3].beats;
  const both = beats.find((beat) => beat.id === "both-done");
  const refund = beats.find((beat) => beat.id === "refund");
  const failed = beats.at(-1);
  expect(both?.paymentKey).toBe(PAYMENT_KEY);
  expect(both?.caption).toContain("would not charge twice");
  expect(refund?.sameKey).toBe(true);
  expect(refund?.paymentKey).toBe(both?.paymentKey);
  expect(refund?.caption).toContain("inventory has no undo");
  expect(beats.find((beat) => beat.id === "email-fail")?.workflow).toBe("COMPENSATING");
  expect(beats.find((beat) => beat.id === "cancel")).toBeUndefined();
  expect(failed?.workflow).toBe("FAILED");
  expect(failed?.href).toBe(GITHUB);
  expect(failed?.cta).not.toBe("Next chapter");
});

test("rail jump clears lease and peach from the previous chapter", () => {
  const crashIndex = CHAPTERS[1].beats.findIndex((beat) => beat.id === "crash");
  const mid = { chapter: 1, beat: crashIndex };
  expect(beatAt(mid).lease).toBe("shrink");
  expect(beatAt(mid).focus).toBe("workerA");
  const jumped = jump(2);
  expect(jumped).toEqual({ chapter: 2, beat: 0 });
  expect(beatAt(jumped).lease).toBeNull();
  expect(present(jumped).dim).not.toContain(beatAt(jumped).focus);
  expect(present(mid).dim).toContain("workerB");
});

test("next chapter advances, and the last chapter stays put", () => {
  let state = { chapter: 0, beat: CHAPTERS[0].beats.length - 1 };
  expect(beatAt(state).terminal).toBe(true);
  state = advance(state);
  expect(state).toEqual(initial(1));
  const end = { chapter: 3, beat: CHAPTERS[3].beats.length - 1 };
  expect(advance(end)).toBe(end);
});

test("copy does not claim a live in-browser engine", async () => {
  const html = await Bun.file(new URL("./index.html", import.meta.url)).text();
  const app = await Bun.file(new URL("./app.js", import.meta.url)).text();
  const css = await Bun.file(new URL("./styles.css", import.meta.url)).text();
  const blob = `${JSON.stringify(CHAPTERS)} ${DISCLAIMER} ${html} ${app}`;
  expect(blob.toLowerCase()).not.toContain("same concurrency model");
  expect(blob.toLowerCase()).not.toContain("live engine");
  expect(blob).not.toContain("MiniTemporalEngine");
  expect(css).not.toContain("transition: all");
  expect(css).not.toMatch(/scale\(0\)/);
  expect(keptNodes(CHAPTERS[0].beats[2]).has("outbox")).toBe(true);
});
