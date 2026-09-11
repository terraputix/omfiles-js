/**
 * Throws the signal's abort reason if the signal has been aborted.
 * Uses the standard DOMException with name "AbortError" as fallback.
 */
export function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.throwIfAborted) {
    signal.throwIfAborted();
  } else if (signal?.aborted) {
    throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
  }
}

/**
 * True for the rejection an aborted fetch produces, whether it arrives as a
 * DOMException or as a plain Error carrying the same name.
 */
export function isAbortError(error: unknown): boolean {
  return typeof error === "object" && error !== null && (error as { name?: unknown }).name === "AbortError";
}

/**
 * FNV-1a 64-bit hash implementation
 */
export function fnv1aHash64(str: string): bigint {
  const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
  const FNV_PRIME = 0x100000001b3n;

  let hash = FNV_OFFSET_BASIS;
  const bytes = new TextEncoder().encode(str);

  for (const byte of bytes) {
    hash ^= BigInt(byte);
    hash = (hash * FNV_PRIME) & 0xffffffffffffffffn;
  }

  return hash;
}

/**
 * Fetch with exponential backoff retry on server-side errors (HTTP 5xx) and per-attempt timeout.
 */
export async function fetchRetry(
  input: RequestInfo,
  init?: RequestInit,
  timeoutMs = 5000,
  retries = 3,
  signal?: AbortSignal
): Promise<Response> {
  let lastError: Error;

  function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_, reject) => setTimeout(() => reject(new Error(`Timeout after ${ms}ms`)), ms)),
    ]);
  }

  for (let attempt = 0; attempt < retries; attempt++) {
    throwIfAborted(signal);
    try {
      const mergedInit: RequestInit = { ...init };
      if (signal) {
        mergedInit.signal = signal;
      }
      const response = await withTimeout(fetch(input, mergedInit), timeoutMs);
      if (response.status >= 500 && response.status < 600) {
        throw new Error(`Server error: ${response.status}`);
      }
      return response;
    } catch (error) {
      // If the signal was aborted, re-throw immediately without retrying
      throwIfAborted(signal);
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt < retries - 1) {
        const delay = Math.min(500 * Math.pow(2, attempt), 5000);
        //console.debug(`Attempt ${attempt + 1} failed, retrying in ${delay}ms: ${lastError.message}`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  throw lastError!;
}

/**
 * Runs `tasks` with at most `limit` of them in flight, in order.
 *
 * `limit` workers pull from a shared queue rather than running fixed batches:
 * the signal is checked before every task, so an abort stops dispatching at
 * once instead of at the next batch boundary (which, with the default limit,
 * meant up to ten more requests were sent for a read nobody waits for). It
 * also keeps every slot busy when one task is slower than its neighbours.
 */
export async function runLimited<T>(tasks: (() => Promise<T>)[], limit: number, signal?: AbortSignal): Promise<T[]> {
  const results: T[] = new Array(tasks.length) as T[];
  let next = 0;
  let stopped = false;

  const worker = async (): Promise<void> => {
    while (!stopped) {
      throwIfAborted(signal);
      const index = next++;
      if (index >= tasks.length) return;
      results[index] = await tasks[index]();
    }
  };

  const workers: Promise<void>[] = [];
  for (let i = 0; i < Math.min(limit, tasks.length); i++) {
    workers.push(worker());
  }

  try {
    await Promise.all(workers);
  } finally {
    // Every worker has to come to a stop before this returns, or a failed run
    // would leave tasks dispatching behind it
    stopped = true;
    await Promise.allSettled(workers);
  }

  return results;
}
