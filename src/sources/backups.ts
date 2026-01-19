import crypto from "node:crypto";
import { RN } from "../core/rn";
import type { Depot } from "../depot/depot";

export type BackupContext = {
    depot: Depot;
    depotIdent: string;
    layerIdent: string;
    now?: Date;
};

export type BackupTableDump = {
    name: string;
    rows: Record<string, unknown>[];
};

export type BackupPayload = {
    version: 1;
    createdAt: string;
    layerIdent: string;
    layerType: "sqlite" | "pg" | "mysql";
    tables: BackupTableDump[];
};

export type BackupRestoreMode = "replace" | "merge";

export type BackupRestoreOptions = {
    mode?: BackupRestoreMode;
};

const BACKUP_ROOT = "__korm_backups__";

function hashSegment(value: string): string {
    return crypto.createHash("sha256").update(value).digest("hex").slice(0, 12);
}

function normalizeSegment(value: string, label: string): string {
    const trimmed = value.trim();
    if (!trimmed || trimmed === "." || trimmed === "..") {
        return `${label}-${hashSegment(value)}`;
    }
    if (trimmed.includes(":") || trimmed.includes("[") || trimmed.includes("]") || trimmed.includes("*")) {
        return `${label}-${hashSegment(trimmed)}`;
    }
    if (trimmed.includes("/") || trimmed.includes("\\") || trimmed.includes("\u0000")) {
        return `${label}-${hashSegment(trimmed)}`;
    }
    return trimmed;
}

function formatTimestamp(now: Date): string {
    const iso = now.toISOString().replace(/\.\d{3}Z$/, "Z");
    return iso.replace(/[-:]/g, "");
}

export function buildBackupRn(depotIdent: string, layerIdent: string, now: Date, extension: string): RN {
    const safeLayer = normalizeSegment(layerIdent, "layer");
    const stamp = formatTimestamp(now);
    const id = crypto.randomUUID();
    const filename = `backup-${id}.${extension}`;
    const rnStr = `[rn][depot::${depotIdent}]:${BACKUP_ROOT}:${safeLayer}:${stamp}:${filename}`;
    return RN.create(rnStr).unwrap();
}

export function buildBackupPrefixRn(depotIdent: string, layerIdent: string): RN {
    const safeLayer = normalizeSegment(layerIdent, "layer");
    const rnStr = `[rn][depot::${depotIdent}]:${BACKUP_ROOT}:${safeLayer}:*`;
    return RN.create(rnStr).unwrap();
}

function parseTimestampSegment(segment: string): number | undefined {
    const match = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(segment);
    if (!match) return undefined;
    const [, year, month, day, hour, minute, second] = match;
    const ts = Date.UTC(
        Number(year),
        Number(month) - 1,
        Number(day),
        Number(hour),
        Number(minute),
        Number(second)
    );
    return Number.isNaN(ts) ? undefined : ts;
}

export function parseBackupTimestamp(rn: RN): number | undefined {
    if (!rn.isDepot()) return undefined;
    const parts = rn.depotParts();
    if (!parts || parts.length < 4) return undefined;
    if (parts[0] !== BACKUP_ROOT) return undefined;
    return parseTimestampSegment(parts[2] ?? "");
}

export function buildBackupPayload(layerType: BackupPayload["layerType"], layerIdent: string, tables: BackupTableDump[], now: Date): BackupPayload {
    return {
        version: 1,
        createdAt: now.toISOString(),
        layerIdent,
        layerType,
        tables,
    };
}

export function serializeBackupPayload(payload: BackupPayload): Blob {
    const json = JSON.stringify(payload, (_, value) => {
        if (typeof value === "bigint") {
            return value.toString();
        }
        return value;
    });
    return new Blob([json], { type: "application/json" });
}
