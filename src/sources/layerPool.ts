import crypto from "node:crypto";
import { RN } from "../core/rn";
import type { Depot } from "../depot/depot";
import type { SourceLayer } from "./sourceLayer";
import { WalManager, type WalMode } from "../wal/wal";
import { BackMan } from "./backMan";
import { LayerLockStore, type LockMode, type LockStore } from "./lockStore";
import {
  buildPoolMetaConfig,
  comparePoolMetaConfig,
  findExistingPoolMeta,
  formatPoolMetaMismatch,
  writePoolMetaConfig,
  type PoolMetaBackups,
  type PoolMetaConfig,
  type PoolMetaMode,
} from "./poolMeta";

type LayerPoolOptions = {
  walMode?: WalMode;
  lockMode?: LockMode;
  metaMode?: PoolMetaMode;
  ident?: string;
  backups?: {
    depotIdent: string;
    manager: BackMan;
  };
};

/** Default lock acquisition timeout in milliseconds. */
const DEFAULT_LOCK_TIMEOUT_MS = 30_000;
const DEFAULT_LOCK_TTL_MS = 60_000;
const DEFAULT_LOCK_RETRY_MS = 50;
const MIN_LOCK_REFRESH_MS = 1_000;

function stableUuidFromSeed(seed: string): string {
  const hash = crypto.createHash("sha256").update(seed).digest();
  const bytes = Buffer.from(hash);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x40;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = bytes.toString("hex").slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

/** Error thrown when lock acquisition times out. */
export class LockTimeoutError extends Error {
  constructor(rn: RN<any>, timeoutMs: number) {
    super(
      `Lock acquisition timed out after ${timeoutMs}ms for RN "${rn.value()}"`,
    );
    this.name = "LockTimeoutError";
  }
}

/**
 * In-process mutex manager for RN-keyed resources.
 * Prevents concurrent operations on the same item or depot file within a single process.
 */
type SharedLockConfig = {
  store: LockStore;
  ownerId: string;
  ttlMs: number;
  retryMs: number;
  refreshMs: number;
  ready?: Promise<void>;
};

/**
 * RN-keyed lock manager used by korm operations.
 * Next: call `acquire(...)` or `acquireMultiple(...)` to guard critical sections.
 */
export class KormLocker {
  private _locks = new Map<string, Promise<void>>();
  private _resolvers = new Map<string, () => void>();
  private _shared?: SharedLockConfig;

  constructor(shared?: SharedLockConfig) {
    if (shared) {
      this._shared = shared;
    }
  }

  private async _ensureSharedReady(): Promise<void> {
    if (!this._shared) return;
    if (!this._shared.ready) {
      this._shared.ready = this._shared.store.ensure();
    }
    await this._shared.ready;
  }

  private _sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async _acquireShared(
    rn: RN<any>,
    timeoutMs: number,
  ): Promise<() => void> {
    if (!this._shared) {
      return () => {};
    }
    await this._ensureSharedReady();
    const start = Date.now();
    const { store, ownerId, ttlMs, retryMs, refreshMs } = this._shared;
    const lockId = rn.value();
    while (true) {
      const acquired = await store.tryAcquire(lockId, ownerId, ttlMs);
      if (acquired) {
        let stopped = false;
        const interval = setInterval(() => {
          if (stopped) return;
          void store.refresh(lockId, ownerId, ttlMs).catch(() => {});
        }, refreshMs);
        return () => {
          stopped = true;
          clearInterval(interval);
          void store.release(lockId, ownerId).catch(() => {});
        };
      }
      const elapsed = Date.now() - start;
      if (elapsed >= timeoutMs) {
        throw new LockTimeoutError(rn, timeoutMs);
      }
      await this._sleep(retryMs);
    }
  }

  /**
   * Acquire an exclusive lock on an RN.
   * Waits if the RN is already locked, up to the specified timeout.
   * @param rn The resource name to lock.
   * @param timeoutMs Maximum time to wait for the lock (default: 30 seconds).
   * @returns A release function - call it when done.
   * @throws LockTimeoutError if the lock cannot be acquired within the timeout.
   */
  async acquire(
    rn: RN<any>,
    timeoutMs: number = DEFAULT_LOCK_TIMEOUT_MS,
  ): Promise<() => void> {
    const rnString = rn.value();
    const startTime = Date.now();

    // Wait for existing local lock to release
    while (this._locks.has(rnString)) {
      const elapsed = Date.now() - startTime;
      if (elapsed >= timeoutMs) {
        throw new LockTimeoutError(rn, timeoutMs);
      }
      const remainingTime = timeoutMs - elapsed;
      const currentLock = this._locks.get(rnString)!;

      // Race between the lock releasing and the timeout
      const timeoutPromise = new Promise<"timeout">((resolve) => {
        setTimeout(() => resolve("timeout"), remainingTime);
      });
      const result = await Promise.race([
        currentLock.then(() => "released" as const),
        timeoutPromise,
      ]);
      if (result === "timeout" && this._locks.has(rnString)) {
        throw new LockTimeoutError(rn, timeoutMs);
      }
    }

    // Create local lock
    let releaseLocal!: () => void;
    const promise = new Promise<void>((resolve) => {
      releaseLocal = resolve;
    });
    this._locks.set(rnString, promise);
    this._resolvers.set(rnString, releaseLocal);

    const elapsedLocal = Date.now() - startTime;
    const remaining = Math.max(0, timeoutMs - elapsedLocal);

    if (!this._shared) {
      return () => {
        this._locks.delete(rnString);
        this._resolvers.delete(rnString);
        releaseLocal();
      };
    }

    try {
      const releaseShared = await this._acquireShared(rn, remaining);
      return () => {
        releaseShared();
        this._locks.delete(rnString);
        this._resolvers.delete(rnString);
        releaseLocal();
      };
    } catch (error) {
      this._locks.delete(rnString);
      this._resolvers.delete(rnString);
      releaseLocal();
      throw error;
    }
  }

  /**
   * Try to acquire an exclusive lock on an RN without waiting.
   * Note: when shared locks are enabled, this only checks local locks.
   * @param rn The resource name to lock.
   * @returns A release function if the lock was acquired, or `undefined` if already locked.
   */
  tryAcquire(rn: RN<any>): (() => void) | undefined {
    if (this._shared) {
      return undefined;
    }
    const rnString = rn.value();
    if (this._locks.has(rnString)) {
      return undefined;
    }

    let releaseLocal!: () => void;
    const promise = new Promise<void>((resolve) => {
      releaseLocal = resolve;
    });
    this._locks.set(rnString, promise);
    this._resolvers.set(rnString, releaseLocal);

    return () => {
      this._locks.delete(rnString);
      this._resolvers.delete(rnString);
      releaseLocal();
    };
  }

  /** Check if an RN is currently locked. */
  isLocked(rn: RN<any>): boolean {
    return this._locks.has(rn.value());
  }

  /**
   * Acquire locks on multiple RNs in sorted order to prevent deadlocks.
   * @param rns The resource names to lock.
   * @param timeoutMs Maximum time to wait for all locks (default: 30 seconds).
   * @returns A single release function that releases all locks.
   * @throws LockTimeoutError if any lock cannot be acquired within the timeout.
   */
  async acquireMultiple(
    rns: RN<any>[],
    timeoutMs: number = DEFAULT_LOCK_TIMEOUT_MS,
  ): Promise<() => void> {
    // Sort by RN string to ensure consistent lock ordering and prevent deadlocks
    const sorted = [...rns].sort((a, b) => a.value().localeCompare(b.value()));
    const releases: (() => void)[] = [];

    try {
      for (const rn of sorted) {
        const release = await this.acquire(rn, timeoutMs);
        releases.push(release);
      }
    } catch (error) {
      // Release any locks we already acquired
      for (const release of releases) {
        release();
      }
      throw error;
    }

    return () => {
      // Release in reverse order (LIFO)
      for (let i = releases.length - 1; i >= 0; i--) {
        releases[i]!();
      }
    };
  }
}

/**
 * Aggregates layers and depots for korm operations.
 * Created via `korm.pool().setLayers(...).open()` (builder) or `korm.pool(...)` (legacy).
 */
export class LayerPool {
  private _ident: string;
  private _layers: Map<string, SourceLayer>;
  private _depots: Map<string, Depot>;
  private _walManager?: WalManager;
  private _walReady: Promise<void>;
  private _walError?: Error;
  private _metaReady: Promise<void>;
  private _metaError?: Error;
  private _metaConfig?: PoolMetaConfig;
  private _metaLayerIdent?: string;
  private _backupsConfig?: PoolMetaBackups;
  private _backupsReady: Promise<void>;
  private _backupsError?: Error;
  private _locker: KormLocker;
  private backMan?: BackMan;

  /**
   * Construct a pool from layer and depot maps.
   * Prefer using `korm.pool().setLayers(...).open()` or `korm.pool(...)` instead of calling this directly.
   */
  constructor(
    layers: Map<string, SourceLayer>,
    depots: Map<string, Depot> = new Map(),
    options?: LayerPoolOptions,
  ) {
    this._layers = layers;
    this._depots = depots;
    this._walReady = Promise.resolve();
    this._metaReady = Promise.resolve();
    this._backupsReady = Promise.resolve();
    this._metaLayerIdent = options?.metaMode?.layerIdent;

    if (options?.ident) {
      this._ident = options.ident;
    } else {
      this._ident = crypto.randomUUID().substring(0, 8);
    }

    if (options?.lockMode) {
      this._locker = this._initLocks(options.lockMode);
    } else {
      this._locker = new KormLocker();
    }

    if (options?.backups) {
      this._initBackups(options.backups.depotIdent, options.backups.manager);
    }

    this._initPoolMeta(options);

    if (this.backMan) {
      this._startBackups();
    }

    if (options?.walMode) {
      this._initWal(options.walMode);
    }
  }

  private _initLocks(lockMode: LockMode): KormLocker {
    const layer = this._layers.get(lockMode.layerIdent);
    if (!layer) {
      const available = Array.from(this._layers.keys()).join(", ");
      throw new Error(
        `Lock layer "${lockMode.layerIdent}" is not present in the pool. Available layers: ${available}`,
      );
    }
    const store = new LayerLockStore(layer);
    const ownerId =
      lockMode.ownerId ??
      `${this._ident}-${crypto.randomUUID().slice(0, 8)}-${process.pid ?? "pid"}`;
    const ttlMs = lockMode.ttlMs ?? DEFAULT_LOCK_TTL_MS;
    const retryMs = lockMode.retryMs ?? DEFAULT_LOCK_RETRY_MS;
    const refreshMs =
      lockMode.refreshMs ??
      Math.max(MIN_LOCK_REFRESH_MS, Math.floor(ttlMs / 2));
    const shared: SharedLockConfig = {
      store,
      ownerId,
      ttlMs,
      retryMs,
      refreshMs,
    };
    return new KormLocker(shared);
  }

  private _initPoolMeta(options?: LayerPoolOptions): void {
    this._metaReady = (async () => {
      const existing = await findExistingPoolMeta(this._layers);
      const needsConfig = existing.length > 0 || Boolean(options?.metaMode);
      if (!needsConfig) {
        return;
      }

      const config = await buildPoolMetaConfig({
        layers: this._layers,
        depots: this._depots,
        walMode: options?.walMode,
        lockMode: options?.lockMode,
        metaMode: options?.metaMode,
        backups: this._backupsConfig,
      });
      this._metaConfig = config;

      if (existing.length === 0) {
        if (options?.metaMode) {
          const target = this._layers.get(options.metaMode.layerIdent);
          if (!target) {
            const available = Array.from(this._layers.keys()).join(", ");
            throw new Error(
              `Pool meta layer "${options.metaMode.layerIdent}" is not present in the pool. Available layers: ${available}`,
            );
          }
          await writePoolMetaConfig(target, config);
        }
        return;
      }

      const discrepancies = new Set<string>();
      for (const entry of existing) {
        comparePoolMetaConfig(config, entry.config).forEach((item) =>
          discrepancies.add(item),
        );
      }
      if (existing.length > 1) {
        discrepancies.add(
          `pool meta present in multiple layers: ${existing.map((entry) => entry.ident).join(", ")}`,
        );
      }
      if (discrepancies.size > 0) {
        throw formatPoolMetaMismatch({
          discrepancies: Array.from(discrepancies),
          layerIdents: existing.map((entry) => entry.ident),
        });
      }

      if (options?.metaMode) {
        const target = this._layers.get(options.metaMode.layerIdent);
        if (!target) {
          const available = Array.from(this._layers.keys()).join(", ");
          throw new Error(
            `Pool meta layer "${options.metaMode.layerIdent}" is not present in the pool. Available layers: ${available}`,
          );
        }
        await writePoolMetaConfig(target, config);
      }
    })();

    this._metaReady.catch((error) => {
      this._metaError = error as Error;
    });
  }

  private _initWal(walMode: WalMode): void {
    if (this._walManager) {
      throw new Error("WAL is already configured for this pool.");
    }
    this._walManager = new WalManager(
      {
        getLayer: (identifier) => this.getLayer(identifier),
        getLayers: () => this.getLayers(),
        getDepot: (identifier) => this.getDepot(identifier),
        getDepots: () => this.getDepots(),
        findLayerForRn: (rn) => this.findLayerForRn(rn),
        findDepotForRn: (rn) => this.findDepotForRn(rn),
        poolRef: this,
      },
      walMode,
    );
    const lockSeed = `${this._walManager.namespace}:${this._walManager.poolId}`;
    const walLockId = stableUuidFromSeed(lockSeed);
    const walLockRn = RN.create("korm", "wal", walLockId).unwrap();

    this._walReady = (async () => {
      let release: (() => void) | undefined;
      try {
        await this._metaReady;
        release = await this._locker.acquire(walLockRn);
        await this._walManager!.recoverPending();
      } catch (error) {
        this._walError = error as Error;
      } finally {
        if (release) {
          release();
        }
      }
    })();
  }

  private _initBackups(depotIdent: string, manager: BackMan): void {
    if (this.backMan) {
      throw new Error("Backups are already configured for this pool.");
    }
    if (!this._metaLayerIdent) {
      throw new Error(
        "Backups require pool metadata. Call withMeta(...) before backups.",
      );
    }
    if (!this._depots.has(depotIdent)) {
      const available = Array.from(this._depots.keys()).join(", ");
      throw new Error(
        `Tried to point backups at depot that doesn't exist.\nTried to use: ${depotIdent}\nPool ident: ${this._ident}\nAvailable depots: ${available}`,
      );
    }
    this.backMan = manager
      .conveyDepot(this._depots.get(depotIdent), depotIdent)
      .conveyLayers(this._layers);
    const config = this.backMan.getPoolConfig();
    this._backupsConfig = {
      depotIdent,
      intervals: config.intervals,
      retention: config.retention,
    };
  }

  private _startBackups(): void {
    if (!this.backMan) return;
    const metaIdent = this._metaLayerIdent;
    if (!metaIdent) {
      throw new Error(
        "Backups require pool metadata. Call withMeta(...) before backups.",
      );
    }
    const metaLayer = this._layers.get(metaIdent);
    if (!metaLayer) {
      const available = Array.from(this._layers.keys()).join(", ");
      throw new Error(
        `Pool meta layer "${metaIdent}" is not present in the pool. Available layers: ${available}`,
      );
    }
    const store = new LayerLockStore(metaLayer);
    const ownerId = `${this._ident}-backups-${crypto.randomUUID().slice(0, 8)}-${process.pid ?? "pid"}`;
    const ttlMs = DEFAULT_LOCK_TTL_MS;
    const retryMs = DEFAULT_LOCK_RETRY_MS;
    const refreshMs = Math.max(MIN_LOCK_REFRESH_MS, Math.floor(ttlMs / 2));
    const shared: SharedLockConfig = {
      store,
      ownerId,
      ttlMs,
      retryMs,
      refreshMs,
    };
    const locker = new KormLocker(shared);

    this._backupsReady = (async () => {
      await this._metaReady;
      await this.backMan!.start(metaLayer, locker);
    })();

    this._backupsReady.catch((error) => {
      this._backupsError = error as Error;
    });
  }

  /**
   * Attach depots after the pool is created.
   * Use this when building pools progressively.
   */
  attachDepots(depots: Map<string, Depot>): void {
    if (depots.size === 0) {
      throw new Error("At least one depot is required.");
    }
    if (this._walManager) {
      throw new Error("Cannot add depots after WAL is configured.");
    }
    if (this.backMan) {
      throw new Error("Cannot add depots after backups are configured.");
    }
    for (const [ident, depot] of depots) {
      this._depots.set(ident, depot);
    }
  }

  /**
   * Configure WAL after depots are attached.
   * Use this when building pools progressively.
   */
  configureWal(walMode: WalMode): void {
    if (this._depots.size === 0) {
      throw new Error("Cannot enable WAL without at least one depot.");
    }
    this._initWal(walMode);
  }

  /**
   * Configure backups after depots are attached.
   * Use this when building pools progressively.
   */
  configureBackups(depotIdent: string, manager: BackMan): void {
    if (this._depots.size === 0) {
      throw new Error("Cannot enable backups without at least one depot.");
    }
    this._initBackups(depotIdent, manager);
    this._startBackups();
  }

  /**
   * Access the RN lock manager for manual critical sections.
   * Next: call `locker.acquire(...)` or `locker.tryAcquire(...)`.
   */
  get locker(): KormLocker {
    return this._locker;
  }

  /** Get a layer by identifier, if present. */
  getLayer(identifier: string): SourceLayer | undefined {
    return this._layers.get(identifier);
  }

  /** Get the map of all layers. */
  getLayers(): Map<string, SourceLayer> {
    return this._layers;
  }

  /** Get a depot by identifier, if present. */
  getDepot(identifier: string): Depot | undefined {
    return this._depots.get(identifier);
  }

  /** Get the map of all depots. */
  getDepots(): Map<string, Depot> {
    return this._depots;
  }

  /** WAL manager if WAL is enabled. */
  get wal(): WalManager | undefined {
    return this._walManager;
  }

  /** Wait for pool metadata checks to finish or throw on mismatch. */
  async ensureMetaReady(): Promise<void> {
    await this._metaReady;
    if (this._metaError) {
      throw this._metaError;
    }
  }

  /** Wait for WAL recovery to finish or throw if recovery failed. */
  async ensureWalReady(): Promise<void> {
    await this.ensureMetaReady();
    await this._walReady;
    if (this._walError) {
      throw this._walError;
    }
  }

  /** Close all layers and depots. */
  async close(): Promise<void> {
    await this.ensureWalReady();
    if (this.backMan) {
      await this.backMan.close();
    }
    const closes = Array.from(this._layers.values()).map((layer) => {
      if (typeof layer.close === "function") {
        return Promise.resolve(layer.close());
      }
      return Promise.resolve();
    });
    const depotCloses = Array.from(this._depots.values()).map((depot) => {
      const maybeClose = depot as Depot & {
        close?: () => Promise<void> | void;
      };
      if (typeof maybeClose.close === "function") {
        return Promise.resolve(maybeClose.close());
      }
      return Promise.resolve();
    });
    await Promise.all([...closes, ...depotCloses]);
  }

  /**
   * Resolve the correct layer for an item RN.
   * Uses the `from` mod when present or falls back to the only layer in the pool.
   */
  findLayerForRn(rn: RN): SourceLayer {
    if (rn.isDepot()) {
      throw new Error(
        `Cannot use depot RN "${rn.value()}" with layer operations.`,
      );
    }
    // Decide which source layer to use
    const fromMod = rn.mods?.get("from");
    if (fromMod) {
      const layer = this.getLayer(fromMod);
      if (layer) {
        return layer;
      } else {
        throw new Error(
          "Query targets source layer not present in layer pool: " + fromMod,
        );
      }
    } else {
      if (this.getLayers().size === 1) {
        const layer = this.getLayers().values().next().value!;
        return layer;
      }
      throw new Error(
        "Query does not target a source layer while more than one layer is available in pool. Please use the `from` mod to target a layer.",
      );
    }
  }

  /**
   * Resolve the correct depot for a depot RN.
   * Uses the `depot` mod when present or falls back to the only depot in the pool.
   */
  findDepotForRn(rn: RN): Depot {
    if (!rn.isDepot()) {
      throw new Error(`RN "${rn.value()}" does not refer to a depot.`);
    }
    const ident = rn.depotIdent();
    if (ident) {
      const depot = this.getDepot(ident);
      if (depot) return depot;
      throw new Error(`Depot "${ident}" not present in pool.`);
    }
    if (this.getDepots().size === 1) {
      return this.getDepots().values().next().value!;
    }
    throw new Error(
      `Depot RN "${rn.value()}" does not specify a depot identifier.`,
    );
  }
}
