import { describe, expect, it } from "vitest";
import { BlockCacheBackend } from "../lib/backends/BlockCacheBackend";
import { OmFileReaderBackend } from "../lib/backends/OmFileReaderBackend";
import { LruBlockCache } from "../lib/BlockCache";

/** Let every queued microtask run before asserting. */
const flushMicrotasks = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

/**
 * Backend whose reads stay pending until the test resolves them, and reject
 * the way an aborted fetch does.
 */
class FakeBackend implements OmFileReaderBackend {
  readonly calls: { offset: number; size: number; resolve: (data: Uint8Array) => void }[] = [];

  constructor(private readonly fileSize: number) {}

  count(): Promise<number> {
    return Promise.resolve(this.fileSize);
  }

  getBytes(offset: number, size: number, signal?: AbortSignal): Promise<Uint8Array> {
    return new Promise<Uint8Array>((resolve, reject) => {
      this.calls.push({ offset, size, resolve });
      signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
    });
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}

const makeBackend = () => {
  const backend = new FakeBackend(64);
  const cache = new LruBlockCache(16, 8);
  return { backend, cached: BlockCacheBackend.withBigIntKeys(backend, cache, 0n) };
};

describe("BlockCacheBackend – shared block fetches", () => {
  it("deduplicates concurrent reads of the same block", async () => {
    const { backend, cached } = makeBackend();

    const first = cached.getBytes(0, 8);
    const second = cached.getBytes(0, 8);
    await flushMicrotasks();

    expect(backend.calls).toHaveLength(1);

    backend.calls[0].resolve(new Uint8Array(16).fill(7));
    await expect(first).resolves.toEqual(new Uint8Array(8).fill(7));
    await expect(second).resolves.toEqual(new Uint8Array(8).fill(7));
  });

  it("re-fetches for a reader whose shared block another reader cancelled", async () => {
    const { backend, cached } = makeBackend();

    const abandoned = new AbortController();
    const wanted = new AbortController();

    // The first reader's signal is the one the shared fetch runs under
    const abandonedRead = cached.getBytes(0, 8, abandoned.signal);
    const abandonedRejected = expect(abandonedRead).rejects.toThrow();
    const wantedRead = cached.getBytes(0, 8, wanted.signal);
    await flushMicrotasks();
    expect(backend.calls).toHaveLength(1);

    abandoned.abort();
    await abandonedRejected;
    await flushMicrotasks();

    // Exactly one more read: for the reader that is still interested. The
    // reader that aborted must not retry its own cancellation.
    expect(backend.calls).toHaveLength(2);

    backend.calls[1].resolve(new Uint8Array(16).fill(3));
    await expect(wantedRead).resolves.toEqual(new Uint8Array(8).fill(3));
  });

  it("gives up when the caller's own signal is aborted", async () => {
    const { backend, cached } = makeBackend();

    const controller = new AbortController();
    const read = cached.getBytes(0, 8, controller.signal);
    const rejected = expect(read).rejects.toThrow();
    await flushMicrotasks();

    controller.abort();
    await rejected;
    await flushMicrotasks();

    expect(backend.calls).toHaveLength(1);
  });
});
