import crypto from "node:crypto";
import type { JSONable } from "../korm";
import type { Depot } from "../depot/depot";
import { FloatingDepotFile, type DepotFileBase } from "../depot/depotFile";
import { RN } from "../core/rn";
import type { SourceLayer } from "../sources/sourceLayer";

const WAL_VERSION = 3;
const WAL_ROOT = "__korm_wal__";

/** WAL retention mode for completed records. */
export type WalRetention = "keep" | "delete";

/**
 * WAL configuration for a pool.
 * Pass this to `korm.pool().setLayers(...).setDepots(...).withWal(...).open()`
 * (or as `{ walMode }` in the legacy `korm.pool(...)` signature).
 */
export type WalMode = {
    /** Depot identifier in the pool used to store WAL records. */
    depotIdent: string;
    /** Optional namespace; defaults to the pool id. */
    walNamespace?: string;
    /** Retention policy for completed records. */
    retention?: WalRetention;
    /** Whether to record depot file writes in WAL. */
    depotOps?: "off" | "record";
};

/** A WAL operation entry for insert/update. */
export type WalOp = {
    /** Operation type. */
    type: "insert" | "update";
    /** RN string for the target item. */
    rn: string;
    /** Payload written for this operation. */
    data: JSONable;
    /** Optional before-image for undo. */
    before?: JSONable | null;
    /** When true, allow destructive schema changes. */
    destructive?: boolean;
};

/** WAL operation describing a depot file write. */
export type WalDepotOp = {
    /** Operation type (currently only puts). */
    type: "put";
    /** RN string for the depot file. */
    rn: string;
    /** RN string for the payload stored in the WAL depot. */
    payloadRn: string;
    /** Optional RN for the previous payload, if any. */
    beforePayloadRn?: string | null;
};

/** WAL record persisted in the WAL depot. */
export type WalRecord = {
    /** WAL schema version. */
    version: number;
    /** Unique record id. */
    id: string;
    /** Pool id this record belongs to. */
    poolId: string;
    /** WAL namespace. */
    namespace: string;
    /** ISO timestamp when created. */
    createdAt: string;
    /** Current record state. */
    state: "prepared" | "done";
    /** Item operations recorded for this record. */
    ops: WalOp[];
    /** Optional depot file operations recorded for this record. */
    depotOps?: WalDepotOp[];
    /** ISO timestamp when marked done. */
    completedAt?: string;
};

/** Handle returned from `WalManager.stage(...)` for follow-up actions. */
export type WalHandle = {
    /** WAL record payload. */
    record: WalRecord;
    /** RN pointing to the pending record file. */
    pendingRn: RN;
    /** RN pointing to the done record file. */
    doneRn: RN;
};

type WalPoolAccess = {
    getLayer: (identifier: string) => SourceLayer | undefined;
    getLayers: () => Map<string, SourceLayer>;
    getDepot: (identifier: string) => Depot | undefined;
    getDepots: () => Map<string, Depot>;
    findLayerForRn: (rn: RN) => SourceLayer;
    findDepotForRn: (rn: RN) => Depot;
    poolRef: unknown;
};

function normalizeSegment(value: string, label: string): string {
    const trimmed = value.trim();
    if (!trimmed) {
        throw new Error(`WAL ${label} cannot be empty.`);
    }
    if (trimmed === "." || trimmed === "..") {
        throw new Error(`WAL ${label} cannot be "." or "..".`);
    }
    if (trimmed.includes(":") || trimmed.includes("[") || trimmed.includes("]") || trimmed.includes("*")) {
        throw new Error(`WAL ${label} cannot include ":", brackets, or "*".`);
    }
    if (trimmed.includes("/") || trimmed.includes("\\") || trimmed.includes("\u0000")) {
        throw new Error(`WAL ${label} cannot include path separators.`);
    }
    return trimmed;
}

function computePoolId(layers: Map<string, SourceLayer>, depots: Map<string, Depot>): string {
    const layerKeys = Array.from(layers.keys()).sort();
    const depotKeys = Array.from(depots.keys()).sort();
    const seed = JSON.stringify({ layers: layerKeys, depots: depotKeys });
    return crypto.createHash("sha256").update(seed).digest("hex").slice(0, 16);
}

function isDuplicateError(error: unknown): boolean {
    const text = String(error).toLowerCase();
    return text.includes("already exists")
        || text.includes("duplicate")
        || text.includes("unique constraint");
}

function isNotFoundError(error: unknown): boolean {
    const text = String(error).toLowerCase();
    return text.includes("enoent")
        || text.includes("not found")
        || text.includes("no such")
        || text.includes("nosuchkey")
        || text.includes("404");
}

/**
 * WAL manager responsible for staging, applying, and recovering WAL records.
 * Used internally by LayerPool when WAL is enabled.
 */
export class WalManager {
    private _access: WalPoolAccess;
    private _depot: Depot;
    private _depotIdent: string;
    private _retention: WalRetention;
    private _namespace: string;
    private _poolId: string;
    private _depotOpsMode: "off" | "record";

    /**
     * Create a WAL manager for a pool.
     * This is typically constructed by LayerPool when WAL is enabled.
     */
    constructor(access: WalPoolAccess, options: WalMode) {
        this._access = access;
        this._depotIdent = normalizeSegment(options.depotIdent, "depotIdent");
        const depot = access.getDepot(this._depotIdent);
        if (!depot) {
            throw new Error(`WAL depot "${this._depotIdent}" is not present in the pool.`);
        }
        this._depot = depot;
        this._poolId = computePoolId(access.getLayers(), access.getDepots());
        const namespace = options.walNamespace ? normalizeSegment(options.walNamespace, "walNamespace") : this._poolId;
        this._namespace = namespace;
        this._retention = options.retention ?? "keep";
        this._depotOpsMode = options.depotOps ?? "off";
    }

    /** Retention mode for completed WAL records. */
    get retention(): WalRetention {
        return this._retention;
    }

    /** WAL namespace used for record grouping. */
    get namespace(): string {
        return this._namespace;
    }

    /** Depot identifier used to store WAL records. */
    get depotIdent(): string {
        return this._depotIdent;
    }

    /** Pool id derived from layer and depot identifiers. */
    get poolId(): string {
        return this._poolId;
    }

    /** True when depot file operations are recorded in WAL. */
    get depotOpsEnabled(): boolean {
        return this._depotOpsMode === "record";
    }

    private _segments(state: "pending" | "done"): string[] {
        return [WAL_ROOT, this._namespace, this._poolId, state];
    }

    private _fileRn(state: "pending" | "done", id: string): RN {
        const segments = [...this._segments(state), `${id}.json`];
        const rnStr = `[rn][depot::${this._depotIdent}]:${segments.join(":")}`;
        return RN.create(rnStr).unwrap();
    }

    private _prefixRn(state: "pending" | "done"): RN {
        const segments = [...this._segments(state), "*"];
        const rnStr = `[rn][depot::${this._depotIdent}]:${segments.join(":")}`;
        return RN.create(rnStr).unwrap();
    }

    private _payloadRn(id: string, index: number, kind: "after" | "before"): RN {
        const segments = [WAL_ROOT, this._namespace, this._poolId, "payloads", id, String(index), `${kind}.bin`];
        const rnStr = `[rn][depot::${this._depotIdent}]:${segments.join(":")}`;
        return RN.create(rnStr).unwrap();
    }

    private async _writePayload(rn: RN, file: DepotFileBase): Promise<void> {
        const payload = new FloatingDepotFile(rn, file.file);
        await this._depot.createFile(payload);
    }

    private async _deletePayload(rn: RN): Promise<void> {
        await this._depot.deleteFile(rn);
    }

    private async _deletePayloads(record: WalRecord): Promise<void> {
        if (!record.depotOps || record.depotOps.length === 0) return;
        const deletes: Promise<void>[] = [];
        for (const op of record.depotOps) {
            const payloadRes = RN.create(op.payloadRn);
            if (payloadRes.isOk()) {
                deletes.push(this._deletePayload(payloadRes.unwrap()));
            }
            if (op.beforePayloadRn) {
                const beforeRes = RN.create(op.beforePayloadRn);
                if (beforeRes.isOk()) {
                    deletes.push(this._deletePayload(beforeRes.unwrap()));
                }
            }
        }
        if (deletes.length > 0) {
            await Promise.all(deletes);
        }
    }

    private async _writeRecord(rn: RN, record: WalRecord): Promise<void> {
        const body = JSON.stringify(record);
        const file = new FloatingDepotFile(rn, new Blob([body], { type: "application/json" }));
        await this._depot.createFile(file);
    }

    private async _attachBeforeImages(ops: WalOp[]): Promise<WalOp[]> {
        return Promise.all(ops.map(async (op) => {
            if (op.before !== undefined) return op;
            const rnRes = RN.create(op.rn);
            if (rnRes.isErr()) {
                throw rnRes.error;
            }
            const rn = rnRes.unwrap();
            if (rn.isDepot()) {
                throw new Error(`WAL op RN "${rn.value()}" points to a depot.`);
            }
            const layer = this._access.findLayerForRn(rn);
            const before = await layer.readItemRaw(rn);
            return { ...op, before: before ?? null };
        }));
    }

    private async _deleteRecord(rn: RN): Promise<void> {
        await this._depot.deleteFile(rn);
    }

    private async _stageDepotOps(depotOps: DepotFileBase[], recordId: string): Promise<WalDepotOp[]> {
        const staged: WalDepotOp[] = [];
        const createdPayloads: RN[] = [];
        try {
            for (let index = 0; index < depotOps.length; index++) {
                const file = depotOps[index]!;
                if (file.state === "committed") {
                    continue;
                }
                const rn = file.rn;
                if (!rn.isDepot() || rn.pointsTo() !== "depotFile") {
                    throw new Error(`WAL depot op RN "${rn.value()}" is not a depot file.`);
                }
                const payloadRn = this._payloadRn(recordId, index, "after");
                await this._writePayload(payloadRn, file);
                createdPayloads.push(payloadRn);
                let beforePayloadRn: string | null | undefined;
                try {
                    const depot = this._access.findDepotForRn(rn);
                    const existing = await depot.getFile(rn);
                    const beforeRn = this._payloadRn(recordId, index, "before");
                    await this._writePayload(beforeRn, existing);
                    createdPayloads.push(beforeRn);
                    beforePayloadRn = beforeRn.value();
                } catch (error) {
                    if (isNotFoundError(error)) {
                        beforePayloadRn = null;
                    } else {
                        throw error;
                    }
                }
                staged.push({
                    type: "put",
                    rn: rn.value(),
                    payloadRn: payloadRn.value(),
                    beforePayloadRn,
                });
            }
            return staged;
        } catch (error) {
            if (createdPayloads.length > 0) {
                await Promise.all(createdPayloads.map((created) => this._deletePayload(created)));
            }
            throw error;
        }
    }

    /**
     * Stage WAL operations by writing a pending record.
     * Returns a handle used for markDone/abort and depot ops.
     */
    async stage(ops: WalOp[], depotOps?: DepotFileBase[]): Promise<WalHandle> {
        const id = crypto.randomUUID();
        const opsWithBefore = await this._attachBeforeImages(ops);
        let stagedDepotOps: WalDepotOp[] | undefined;
        if (depotOps && depotOps.length > 0) {
            if (!this.depotOpsEnabled) {
                throw new Error("WAL depot ops are disabled. Set walMode.depotOps to \"record\".");
            }
            stagedDepotOps = await this._stageDepotOps(depotOps, id);
        }
        const record: WalRecord = {
            version: WAL_VERSION,
            id,
            poolId: this._poolId,
            namespace: this._namespace,
            createdAt: new Date().toISOString(),
            state: "prepared",
            ops: opsWithBefore,
            depotOps: stagedDepotOps && stagedDepotOps.length > 0 ? stagedDepotOps : undefined,
        };
        const pendingRn = this._fileRn("pending", id);
        const doneRn = this._fileRn("done", id);
        try {
            await this._writeRecord(pendingRn, record);
        } catch (error) {
            if (record.depotOps && record.depotOps.length > 0) {
                await this._deletePayloads(record);
            }
            throw error;
        }
        return { record, pendingRn, doneRn };
    }

    /** Mark a staged WAL record as done and apply retention policy. */
    async markDone(handle: WalHandle): Promise<void> {
        if (this._retention === "keep") {
            const record: WalRecord = {
                ...handle.record,
                state: "done",
                completedAt: new Date().toISOString(),
            };
            await this._writeRecord(handle.doneRn, record);
            await this._deleteRecord(handle.pendingRn);
            return;
        }
        await this._deleteRecord(handle.pendingRn);
        await this._deletePayloads(handle.record);
    }

    /** Abort a staged WAL record and clean up pending entries. */
    async abort(handle: WalHandle): Promise<void> {
        await this._deleteRecord(handle.pendingRn);
        await this._deletePayloads(handle.record);
    }

    /** Apply staged depot file operations for a WAL record. */
    async applyDepotOps(handle: WalHandle): Promise<void> {
        await this._applyDepotOps(handle.record);
    }

    /** Undo staged depot file operations for a WAL record. */
    async undoDepotOps(handle: WalHandle): Promise<void> {
        await this._undoDepotOps(handle.record);
    }

    private async _applyDepotOps(record: WalRecord): Promise<void> {
        if (!record.depotOps || record.depotOps.length === 0) return;
        for (const op of record.depotOps) {
            const rnRes = RN.create(op.rn);
            if (rnRes.isErr()) {
                throw rnRes.error;
            }
            const rn = rnRes.unwrap();
            if (!rn.isDepot() || rn.pointsTo() !== "depotFile") {
                throw new Error(`WAL depot op RN "${rn.value()}" is not a depot file.`);
            }
            const payloadRes = RN.create(op.payloadRn);
            if (payloadRes.isErr()) {
                throw payloadRes.error;
            }
            const payloadRn = payloadRes.unwrap();
            const payload = await this._depot.getFile(payloadRn);
            const depot = this._access.findDepotForRn(rn);
            await depot.createFile(new FloatingDepotFile(rn, payload.file));
        }
    }

    private async _undoDepotOps(record: WalRecord): Promise<void> {
        if (!record.depotOps || record.depotOps.length === 0) return;
        const ops = [...record.depotOps].reverse();
        for (const op of ops) {
            if (op.beforePayloadRn === undefined) {
                continue;
            }
            const rnRes = RN.create(op.rn);
            if (rnRes.isErr()) {
                throw rnRes.error;
            }
            const rn = rnRes.unwrap();
            if (!rn.isDepot() || rn.pointsTo() !== "depotFile") {
                throw new Error(`WAL depot op RN "${rn.value()}" is not a depot file.`);
            }
            const depot = this._access.findDepotForRn(rn);
            if (op.beforePayloadRn === null) {
                try {
                    await depot.deleteFile(rn);
                } catch (error) {
                    if (!isNotFoundError(error)) {
                        throw error;
                    }
                }
                continue;
            }
            const payloadRes = RN.create(op.beforePayloadRn);
            if (payloadRes.isErr()) {
                throw payloadRes.error;
            }
            const payloadRn = payloadRes.unwrap();
            const payload = await this._depot.getFile(payloadRn);
            await depot.createFile(new FloatingDepotFile(rn, payload.file));
        }
    }

    private async _applyRecord(record: WalRecord): Promise<void> {
        for (const op of record.ops) {
            const rnRes = RN.create(op.rn);
            if (rnRes.isErr()) {
                throw rnRes.error;
            }
            const rn = rnRes.unwrap();
            if (rn.isDepot()) {
                throw new Error(`WAL op RN "${rn.value()}" points to a depot.`);
            }
            const layer = this._access.findLayerForRn(rn);
            const payload = {
                rn,
                data: op.data,
                pool: this._access.poolRef as any,
            };
            if (op.type === "insert") {
                const result = await layer.insertItem(payload as any, { destructive: op.destructive });
                if (!result.success && !isDuplicateError(result.error)) {
                    throw result.error ?? new Error(`Failed WAL insert for RN "${rn.value()}"`);
                }
                continue;
            }
            const result = await layer.updateItem(payload as any, { destructive: op.destructive });
            if (!result.success) {
                throw result.error ?? new Error(`Failed WAL update for RN "${rn.value()}"`);
            }
        }
    }

    private async _undoRecord(record: WalRecord): Promise<void> {
        const ops = [...record.ops].reverse();
        for (const op of ops) {
            if (op.before === undefined) {
                continue;
            }
            const rnRes = RN.create(op.rn);
            if (rnRes.isErr()) {
                throw rnRes.error;
            }
            const rn = rnRes.unwrap();
            if (rn.isDepot()) {
                throw new Error(`WAL op RN "${rn.value()}" points to a depot.`);
            }
            const layer = this._access.findLayerForRn(rn);
            if (op.before === null) {
                const result = await layer.deleteItem(rn, { destructive: op.destructive });
                if (!result.success) {
                    throw result.error ?? new Error(`Failed WAL delete for RN "${rn.value()}"`);
                }
                continue;
            }
            const payload = {
                rn,
                data: op.before,
                pool: this._access.poolRef as any,
            };
            const result = await layer.updateItem(payload as any, { destructive: op.destructive });
            if (!result.success) {
                throw result.error ?? new Error(`Failed WAL undo update for RN "${rn.value()}"`);
            }
        }
    }

    /**
     * Recover any pending WAL records: undo then retry them.
     * Called on pool startup when WAL is enabled.
     */
    async recoverPending(): Promise<void> {
        const pending = this._prefixRn("pending");
        const files = await this._depot.listFiles(pending);
        for (const file of files) {
            const text = await file.text();
            let record: WalRecord;
            try {
                record = JSON.parse(text) as WalRecord;
            } catch (error) {
                throw new Error(`Failed to parse WAL record "${file.rn.value()}": ${error}`);
            }
            if (record.state === "done") {
                continue;
            }
            if (record.version >= 2) {
                await this._undoRecord(record);
                await this._undoDepotOps(record);
            }
            await this._applyDepotOps(record);
            await this._applyRecord(record);
            const handle: WalHandle = {
                record,
                pendingRn: file.rn,
                doneRn: this._fileRn("done", record.id),
            };
            await this.markDone(handle);
        }
    }
}
