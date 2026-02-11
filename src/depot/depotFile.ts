import util from "node:util";
import type { JSONable } from "../korm";
import type { RN } from "../core/rn";
import type { LayerPool } from "../sources/layerPool";

type SliceableBlob = {
  slice(begin?: number, end?: number, contentType?: string): Blob;
};

function isReadableStream(value: unknown): value is ReadableStream<Uint8Array> {
  return (
    !!value &&
    typeof value === "object" &&
    "getReader" in value &&
    typeof (value as ReadableStream).getReader === "function"
  );
}

/**
 * File-like payload accepted by depots, including readable byte streams.
 * Next: pass to `korm.file(...)` or `DepotFile.update(...)` to upload.
 */
export type DepotBlob =
  | Bun.BunFile
  | Bun.S3File
  | Blob
  | ReadableStream<Uint8Array>;
/** Lifecycle state for depot files. */
export type DepotFileState = "floating" | "committed" | "uncommitted";

/**
 * Base class for depot file wrappers.
 * Implements common file helpers and RN serialization.
 * Next: use `DepotFile`, `FloatingDepotFile`, or `UncommittedDepotFile`.
 */
export abstract class DepotFileBase {
  readonly __DEPOT_FILE__: true = true;
  /** RN identifying this depot file. */
  readonly rn: RN<JSONable>;
  /** Underlying file payload. */
  readonly file: DepotBlob;
  /** Lifecycle state of the file wrapper. */
  readonly state: DepotFileState;

  protected constructor(
    rn: RN<JSONable>,
    file: DepotBlob,
    state: DepotFileState,
  ) {
    this.rn = rn;
    this.file = file;
    this.state = state;
  }

  /**
   * Serialize to the RN string when stored inside item data.
   * Next: resolve the RN via `korm.resolve(...)` or `depot.getFile(...)`.
   */
  toJSON(): JSONable {
    return this.rn.value();
  }

  /**
   * Read file content as ArrayBuffer.
   * Next: parse binary payloads or pass to storage APIs.
   */
  async arrayBuffer(): Promise<ArrayBuffer> {
    if (isReadableStream(this.file)) {
      return await new Response(this.file).arrayBuffer();
    }
    return await this.file.arrayBuffer();
  }

  /**
   * Read file content as text.
   * Next: parse or display the returned string.
   */
  async text(): Promise<string> {
    if (isReadableStream(this.file)) {
      return await new Response(this.file).text();
    }
    return await this.file.text();
  }

  /**
   * Stream file content.
   * Next: pipe the stream to a response or writer.
   */
  stream(): ReadableStream<Uint8Array> {
    if (isReadableStream(this.file)) {
      return this.file;
    }
    return this.file.stream();
  }

  /**
   * Slice the underlying Blob if supported.
   * Next: call `.text()` or `.arrayBuffer()` on the returned Blob.
   */
  slice(begin?: number, end?: number, contentType?: string): Blob {
    const sliceable = this.file as SliceableBlob | undefined;
    if (sliceable && typeof sliceable.slice === "function") {
      return sliceable.slice(begin, end, contentType);
    }
    throw new Error("DepotFile.slice() is not supported for this file type.");
  }

  [util.inspect.custom](
    depth: number,
    options: util.InspectOptionsStylized,
  ): string {
    const fileLabel = isReadableStream(this.file)
      ? "[stream]"
      : ((this.file as { name?: string }).name ?? "[blob]");
    const preview = {
      rn: this.rn.value(),
      state: this.state,
      file: fileLabel,
    };
    return `DepotFile ${util.inspect(preview, options)}`;
  }
}

/**
 * A depot file that has not been uploaded yet.
 * Create via `korm.file(...)` or by instantiating directly.
 */
export class FloatingDepotFile extends DepotFileBase {
  constructor(rn: RN<JSONable>, file: DepotBlob) {
    super(rn, file, "floating");
  }

  /**
   * Upload the file to the depot identified by the RN.
   * Next: use the returned DepotFile to read or update the stored file.
   */
  async create(pool: LayerPool): Promise<DepotFile> {
    if (!this.rn.isDepot() || this.rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${this.rn.value()}".`);
    }
    const depot = pool.findDepotForRn(this.rn);
    await depot.createFile(this);
    return await depot.getFile(this.rn);
  }
}

type DepotFileEditResult = DepotBlob | DepotFileBase;

/**
 * A committed depot file that exists in storage.
 */
export class DepotFile extends DepotFileBase {
  constructor(rn: RN<JSONable>, file: DepotBlob) {
    super(rn, file, "committed");
  }

  /**
   * Edit the file and return an UncommittedDepotFile.
   * Next: call `.commit(pool)` to persist the update.
   */
  async update(
    edit: (
      file: DepotFile,
    ) => Promise<DepotFileEditResult> | DepotFileEditResult,
  ): Promise<UncommittedDepotFile> {
    const edited = await edit(this);
    if (isDepotFile(edited)) {
      if (edited.rn.value() !== this.rn.value()) {
        throw new Error(
          `Edited depot file RN "${edited.rn.value()}" does not match "${this.rn.value()}".`,
        );
      }
      return new UncommittedDepotFile(this.rn, edited.file);
    }
    return new UncommittedDepotFile(this.rn, edited);
  }

  /**
   * Delete the file from its depot.
   * If WAL depot ops are enabled, the delete is recorded for recovery.
   * Next: create a new file with `korm.file(...)` or re-fetch using `depot.getFile(...)`.
   */
  async delete(pool: LayerPool): Promise<boolean> {
    if (!this.rn.isDepot() || this.rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${this.rn.value()}".`);
    }
    const depot = pool.findDepotForRn(this.rn);
    const wal = pool.wal;
    let walHandle:
      | Awaited<ReturnType<NonNullable<typeof wal>["stage"]>>
      | undefined;
    try {
      if (wal && wal.depotOpsEnabled) {
        walHandle = await wal.stage([], [{ type: "delete", rn: this.rn }]);
        await wal.applyDepotOps(walHandle);
        await wal.markDone(walHandle);
        const deleteOp = walHandle.record.depotOps?.find(
          (op) => op.type === "delete" && op.rn === this.rn.value(),
        );
        if (!deleteOp) {
          return false;
        }
        return deleteOp.beforePayloadRn !== null;
      }
      return await depot.deleteFile(this.rn);
    } catch (error) {
      if (wal && walHandle) {
        try {
          await wal.undoDepotOps(walHandle);
        } catch {}
        await wal.abort(walHandle);
      }
      throw error;
    }
  }
}

/**
 * A depot file with pending changes.
 * Call `.commit(pool)` to upload.
 */
export class UncommittedDepotFile extends DepotFileBase {
  constructor(rn: RN<JSONable>, file: DepotBlob) {
    super(rn, file, "uncommitted");
  }

  /**
   * Persist the updated file to the depot.
   * Returns a committed DepotFile.
   * Next: call `.update(...)` or `.delete(pool)` on the returned file.
   */
  async commit(pool: LayerPool): Promise<DepotFile> {
    if (!this.rn.isDepot() || this.rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${this.rn.value()}".`);
    }
    const depot = pool.findDepotForRn(this.rn);
    await depot.createFile(this);
    return await depot.getFile(this.rn);
  }
}

/**
 * Any depot file wrapper (floating, committed, or uncommitted).
 * Next: pass to `persist` operations or resolve to a committed file.
 */
export type DepotFileLike = DepotFileBase;

/**
 * Type guard for depot file wrappers.
 * Next: use the narrowed type with depot helpers or item persistence.
 */
export function isDepotFile(value: unknown): value is DepotFileBase {
  return Boolean(
    value &&
    typeof value === "object" &&
    (value as { __DEPOT_FILE__?: boolean }).__DEPOT_FILE__ === true &&
    "rn" in (value as Record<string, unknown>),
  );
}
