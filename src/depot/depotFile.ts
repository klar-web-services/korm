import util from "node:util";
import type { JSONable } from "../korm";
import type { RN } from "../core/rn";
import type { LayerPool } from "../sources/layerPool";

type SliceableBlob = {
    slice(begin?: number, end?: number, contentType?: string): Blob;
};

/** File-like payload accepted by depots. */
export type DepotBlob = Bun.BunFile | Bun.S3File | Blob;
/** Lifecycle state for depot files. */
export type DepotFileState = "floating" | "committed" | "uncommitted";

/**
 * Base class for depot file wrappers.
 * Implements common file helpers and RN serialization.
 */
export abstract class DepotFileBase {
    readonly __DEPOT_FILE__: true = true;
    /** RN identifying this depot file. */
    readonly rn: RN<JSONable>;
    /** Underlying file payload. */
    readonly file: DepotBlob;
    /** Lifecycle state of the file wrapper. */
    readonly state: DepotFileState;

    protected constructor(rn: RN<JSONable>, file: DepotBlob, state: DepotFileState) {
        this.rn = rn;
        this.file = file;
        this.state = state;
    }

    /** Serialize to the RN string when stored inside item data. */
    toJSON(): JSONable {
        return this.rn.value();
    }

    /** Read file content as ArrayBuffer. */
    async arrayBuffer(): Promise<ArrayBuffer> {
        return await this.file.arrayBuffer();
    }

    /** Read file content as text. */
    async text(): Promise<string> {
        return await this.file.text();
    }

    /** Stream file content. */
    stream(): ReadableStream<Uint8Array<ArrayBuffer>> {
        return this.file.stream();
    }

    /** Slice the underlying Blob if supported. */
    slice(begin?: number, end?: number, contentType?: string): Blob {
        const sliceable = this.file as SliceableBlob | undefined;
        if (sliceable && typeof sliceable.slice === "function") {
            return sliceable.slice(begin, end, contentType);
        }
        throw new Error("DepotFile.slice() is not supported for this file type.");
    }

    [util.inspect.custom](depth: number, options: util.InspectOptionsStylized): string {
        const preview = {
            rn: this.rn.value(),
            state: this.state,
            file: (this.file as { name?: string }).name ?? "[blob]",
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
     * Next: use the returned DepotFile to update or read it.
     */
    async create(pool: LayerPool): Promise<DepotFile> {
        if (!this.rn.isDepot() || this.rn.pointsTo() !== "depotFile") {
            throw new Error(`Expected depot file RN but got "${this.rn.value()}".`);
        }
        const depot = pool.findDepotForRn(this.rn);
        await depot.createFile(this);
        return new DepotFile(this.rn, this.file);
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
    async update(edit: (file: DepotFile) => Promise<DepotFileEditResult> | DepotFileEditResult): Promise<UncommittedDepotFile> {
        const edited = await edit(this);
        if (isDepotFile(edited)) {
            if (edited.rn.value() !== this.rn.value()) {
                throw new Error(`Edited depot file RN "${edited.rn.value()}" does not match "${this.rn.value()}".`);
            }
            return new UncommittedDepotFile(this.rn, edited.file);
        }
        return new UncommittedDepotFile(this.rn, edited);
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
     */
    async commit(pool: LayerPool): Promise<DepotFile> {
        if (!this.rn.isDepot() || this.rn.pointsTo() !== "depotFile") {
            throw new Error(`Expected depot file RN but got "${this.rn.value()}".`);
        }
        const depot = pool.findDepotForRn(this.rn);
        await depot.createFile(this);
        return new DepotFile(this.rn, this.file);
    }
}

/** Any depot file wrapper (floating, committed, or uncommitted). */
export type DepotFileLike = DepotFileBase;

/** Type guard for depot file wrappers. */
export function isDepotFile(value: unknown): value is DepotFileBase {
    return Boolean(
        value &&
        typeof value === "object" &&
        (value as { __DEPOT_FILE__?: boolean }).__DEPOT_FILE__ === true &&
        "rn" in (value as Record<string, unknown>)
    );
}
