export type KeyKind = "string" | "bigint";

/**
 * Interface for a block-level cache.
 * Implementations can be in-memory, persistent, or leverage browser APIs.
 */
export interface BlockCache<K = bigint> {
  keyKind: KeyKind;

  /** Returns the block size used by the cache. */
  blockSize(): number;

  /**
   * Retrieves a block from the cache or fetches it using the provided function.
   *
   * Implementations that deduplicate concurrent fetches must drop the shared
   * entry before the callers awaiting it resume: a fetch runs under the signal
   * of whoever asked first, and a caller that is still interested has to be
   * able to re-issue one that was cancelled on it.
   */
  get(key: K, fetchFn: () => Promise<Uint8Array>, fileSize?: number): Promise<Uint8Array>;

  /** Retrieves the total size of the cached file corresponding to key, if cached */
  size(key: K): Promise<number | undefined>;

  /** Optionally starts fetching a block into the cache without blocking. */
  prefetch(key: K, fetchFn: () => Promise<Uint8Array>, fileSize?: number): Promise<void>;

  /** Clears the cache contents. */
  clear(): void | Promise<void>;
}

export class LruBlockCache implements BlockCache {
  readonly keyKind = "bigint";
  private readonly _blockSize: number;
  private readonly maxBlocks: number;
  private readonly cache = new Map<bigint, Uint8Array>();
  private readonly inflight = new Map<bigint, Promise<Uint8Array>>();

  constructor(blockSize: number = 64 * 1024, maxBlocks = 256) {
    this._blockSize = blockSize;
    this.maxBlocks = maxBlocks;
  }

  blockSize(): number {
    return this._blockSize;
  }

  size(_key: bigint): Promise<number | undefined> {
    return Promise.resolve(undefined);
  }

  async get(key: bigint, fetchFn: () => Promise<Uint8Array>): Promise<Uint8Array> {
    // Check cache
    const cached = this.cache.get(key);
    if (cached) {
      // Move to end (LRU refresh) - Map preserves insertion order
      this.cache.delete(key);
      this.cache.set(key, cached);
      return cached;
    }

    // Deduplicate inflight requests
    let pending = this.inflight.get(key);
    if (!pending) {
      pending = fetchFn();
      this.inflight.set(key, pending);

      // Attached before anything else, so the entry is gone by the time the
      // callers waiting on it resume: one reacting to a cancelled fetch must
      // be able to start a fresh one instead of joining the dead entry again.
      const forget = () => {
        this.inflight.delete(key);
      };
      void pending.then(forget, forget);

      void pending
        .then((data) => {
          // Evict if needed
          if (this.cache.size >= this.maxBlocks) {
            const oldest = this.cache.keys().next().value;
            if (oldest !== undefined) this.cache.delete(oldest);
          }
          this.cache.set(key, data);
        })
        .catch(() => {
          // The caller awaiting the returned promise reports the failure; this
          // chain only fills the cache. Without the catch, every cancelled
          // block fetch would surface as an unhandled rejection.
        });
    }
    return pending;
  }

  async prefetch(key: bigint, fetchFn: () => Promise<Uint8Array>): Promise<void> {
    if (!this.cache.has(key) && !this.inflight.has(key)) {
      await this.get(key, fetchFn).catch(() => {
        // ignore errors during prefetch
      });
    }
  }

  clear(): void {
    this.cache.clear();
    this.inflight.clear();
  }
}
