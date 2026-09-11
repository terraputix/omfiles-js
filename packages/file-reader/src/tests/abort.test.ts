import { describe, beforeAll, afterEach, it, expect, beforeEach, vi } from "vitest";
import { initWasm, WasmModule } from "../lib/wasm";
import { OmFileReader } from "../lib/OmFileReader";
import path from "path";
import { OmDataType, Range } from "../lib/types";
import { FileBackendNode } from "../lib/backends/FileBackendNode";
import { OmFileReaderBackend } from "../lib/backends/OmFileReaderBackend";
import { runLimited } from "../lib/utils";

/** Let every queued microtask run before asserting on scheduling. */
const flushMicrotasks = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

// ---------------------------------------------------------------------------
// A backend wrapper that counts getBytes calls and aborts a controller after
// a configurable number of calls.  This lets us test mid-read cancellation.
// ---------------------------------------------------------------------------
class AbortAfterNBackend implements OmFileReaderBackend {
  private readonly inner: OmFileReaderBackend;
  private readonly controller: AbortController;
  private readonly abortAfter: number;
  public getBytesCallCount = 0;

  constructor(inner: OmFileReaderBackend, controller: AbortController, abortAfter: number) {
    this.inner = inner;
    this.controller = controller;
    this.abortAfter = abortAfter;
  }

  async getBytes(offset: number, size: number, _signal?: AbortSignal): Promise<Uint8Array> {
    this.getBytesCallCount++;
    if (this.getBytesCallCount >= this.abortAfter) {
      this.controller.abort();
    }
    return this.inner.getBytes(offset, size);
  }

  count(signal?: AbortSignal): Promise<number> {
    return this.inner.count(signal);
  }

  close(): Promise<void> {
    return this.inner.close();
  }
}

// ---------------------------------------------------------------------------
// A backend wrapper that adds a 1-tick async delay to every getBytes call so
// the abort signal has a chance to be checked between iterations.
// ---------------------------------------------------------------------------
class AsyncDelayBackend implements OmFileReaderBackend {
  private readonly inner: OmFileReaderBackend;
  public getBytesCallCount = 0;

  constructor(inner: OmFileReaderBackend) {
    this.inner = inner;
  }

  async getBytes(offset: number, size: number, _signal?: AbortSignal): Promise<Uint8Array> {
    this.getBytesCallCount++;
    // Yield to the event loop so abort checks in loops can fire.
    await new Promise((r) => setTimeout(r, 0));
    return this.inner.getBytes(offset, size);
  }

  count(signal?: AbortSignal): Promise<number> {
    return this.inner.count(signal);
  }

  close(): Promise<void> {
    return this.inner.close();
  }
}

// ---------------------------------------------------------------------------
// runLimited with AbortSignal
// ---------------------------------------------------------------------------
describe("runLimited with AbortSignal", () => {
  it("should complete normally without a signal", async () => {
    const results = await runLimited([() => Promise.resolve(1), () => Promise.resolve(2), () => Promise.resolve(3)], 2);
    expect(results).toEqual([1, 2, 3]);
  });

  it("should complete normally with a non-aborted signal", async () => {
    const controller = new AbortController();
    const results = await runLimited([() => Promise.resolve("a"), () => Promise.resolve("b")], 2, controller.signal);
    expect(results).toEqual(["a", "b"]);
  });

  it("should throw immediately when signal is already aborted", async () => {
    const controller = new AbortController();
    controller.abort();

    const task = vi.fn(() => Promise.resolve(42));
    await expect(runLimited([task, task, task], 2, controller.signal)).rejects.toThrow();
    // No tasks should have been executed
    expect(task).not.toHaveBeenCalled();
  });

  it("should stop dispatching further tasks when the signal is aborted mid-run", async () => {
    const controller = new AbortController();
    let executed = 0;

    const makeTask = (abortWhenRun: boolean) => () => {
      executed++;
      if (abortWhenRun) {
        controller.abort();
      }
      return Promise.resolve(executed);
    };

    // 4 tasks, limit 2. The first one aborts as it runs.
    const tasks = [makeTask(true), makeTask(false), makeTask(false), makeTask(false)];

    await expect(runLimited(tasks, 2, controller.signal)).rejects.toThrow();
    // The signal is checked before every task, not once per batch, so nothing
    // is dispatched after the abort
    expect(executed).toBe(1);
  });

  it("should keep every slot busy instead of waiting for a batch to finish", async () => {
    const started: number[] = [];
    const resolvers: ((value: number) => void)[] = [];
    const tasks = [0, 1, 2, 3].map(
      (index) => () =>
        new Promise<number>((resolve) => {
          started.push(index);
          resolvers[index] = resolve;
        })
    );

    const run = runLimited(tasks, 2);

    await flushMicrotasks();
    expect(started).toEqual([0, 1]);

    // Finishing one task must start the next one, rather than waiting for its
    // neighbour to finish the batch first
    resolvers[0](0);
    await flushMicrotasks();
    expect(started).toEqual([0, 1, 2]);

    resolvers[1](1);
    resolvers[2](2);
    await flushMicrotasks();
    resolvers[3](3);

    await expect(run).resolves.toEqual([0, 1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// OmFileReader read operations with AbortSignal
// ---------------------------------------------------------------------------
describe("OmFileReader abort signal support", () => {
  let wasm: WasmModule;
  let innerBackend: FileBackendNode;

  const fullRange: Range[] = [
    { start: 0, end: 5 },
    { start: 0, end: 5 },
  ];

  beforeAll(async () => {
    wasm = await initWasm();
  });

  beforeEach(() => {
    const testFilePath = path.join(__dirname, "../../test-data/read_test.om");
    innerBackend = new FileBackendNode(testFilePath);
  });

  afterEach(async () => {
    await innerBackend.close();
  });

  // -- Pre-aborted signal --------------------------------------------------

  it("read() should throw immediately with an already-aborted signal", async () => {
    const reader = new OmFileReader(innerBackend, wasm);
    await reader.initialize();

    const controller = new AbortController();
    controller.abort();

    await expect(
      reader.read({
        type: OmDataType.FloatArray,
        ranges: fullRange,
        signal: controller.signal,
      })
    ).rejects.toThrow("This operation was aborted");

    reader.dispose();
  });

  it("readInto() should throw immediately with an already-aborted signal", async () => {
    const reader = new OmFileReader(innerBackend, wasm);
    await reader.initialize();

    const controller = new AbortController();
    controller.abort();

    const output = new Float32Array(25);
    await expect(
      reader.readInto({
        type: OmDataType.FloatArray,
        output,
        ranges: fullRange,
        signal: controller.signal,
      })
    ).rejects.toThrow("This operation was aborted");

    reader.dispose();
  });

  it("readPrefetch() should throw immediately with an already-aborted signal", async () => {
    const reader = new OmFileReader(innerBackend, wasm);
    await reader.initialize();

    const controller = new AbortController();
    controller.abort();

    await expect(
      reader.readPrefetch({
        ranges: fullRange,
        signal: controller.signal,
      })
    ).rejects.toThrow("This operation was aborted");

    reader.dispose();
  });

  // -- Abort during read ---------------------------------------------------

  it("read() should abort mid-operation when signal fires during I/O", async () => {
    const controller = new AbortController();
    // Abort after the 2nd getBytes call. The full 5x5 read with 2x2 chunks
    // needs multiple getBytes calls, so aborting after 2 should interrupt it.
    const abortBackend = new AbortAfterNBackend(innerBackend, controller, 2);

    const reader = new OmFileReader(abortBackend, wasm);
    await reader.initialize();

    await expect(
      reader.read({
        type: OmDataType.FloatArray,
        ranges: fullRange,
        signal: controller.signal,
      })
    ).rejects.toThrow("This operation was aborted");

    // The backend should have been called, but the read should not have completed
    expect(abortBackend.getBytesCallCount).toBeGreaterThanOrEqual(2);

    reader.dispose();
  });

  // -- Normal read still works with a signal that never fires ---------------

  it("read() should complete normally when signal is never aborted", async () => {
    const controller = new AbortController();
    const delayBackend = new AsyncDelayBackend(innerBackend);

    const reader = new OmFileReader(delayBackend, wasm);
    await reader.initialize();

    const result = await reader.read({
      type: OmDataType.FloatArray,
      ranges: [
        { start: 0, end: 2 },
        { start: 0, end: 2 },
      ],
      signal: controller.signal,
    });

    expect(result).toBeInstanceOf(Float32Array);
    expect(result).toStrictEqual(new Float32Array([0, 1, 5, 6]));
    expect(delayBackend.getBytesCallCount).toBe(4);

    reader.dispose();
  });

  it("read() should produce the same result with or without a signal", async () => {
    const ranges: Range[] = [
      { start: 1, end: 4 },
      { start: 0, end: 3 },
    ];

    // Without signal
    const reader1 = new OmFileReader(innerBackend, wasm);
    await reader1.initialize();
    const withoutSignal = await reader1.read({ type: OmDataType.FloatArray, ranges });
    reader1.dispose();

    // With non-aborted signal
    const controller = new AbortController();
    const reader2 = new OmFileReader(innerBackend, wasm);
    await reader2.initialize();
    const withSignal = await reader2.read({
      type: OmDataType.FloatArray,
      ranges,
      signal: controller.signal,
    });
    reader2.dispose();

    expect(withSignal).toStrictEqual(withoutSignal);
  });
});
