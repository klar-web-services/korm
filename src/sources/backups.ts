import crypto from "node:crypto";
import { RN } from "../core/rn";
import type { Depot } from "../depot/depot";

export const BACKUP_FORMAT_VERSION = 2;
export const BACKUP_EXTENSION = "ndjson";

export type BackupContext = {
  depot: Depot;
  depotIdent: string;
  layerIdent: string;
  now?: Date;
};

export type BackupHeader = {
  version: typeof BACKUP_FORMAT_VERSION;
  createdAt: string;
  layerIdent: string;
  layerType: "sqlite" | "pg" | "mysql";
};

export type BackupHeaderEvent = BackupHeader & { t: "header" };
export type BackupTableEvent = { t: "table"; name: string };
export type BackupRowEvent = {
  t: "row";
  table: string;
  row: Record<string, unknown>;
};
export type BackupEndTableEvent = { t: "endTable"; name: string };
export type BackupEndEvent = { t: "end" };

export type BackupEvent =
  | BackupHeaderEvent
  | BackupTableEvent
  | BackupRowEvent
  | BackupEndTableEvent
  | BackupEndEvent;

export type BackupRestoreMode = "replace" | "merge";

export type BackupRestoreOptions = {
  mode?: BackupRestoreMode;
};

export type BackupRestorePayload = {
  header: BackupHeader;
  reader: BackupEventReader;
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
  if (
    trimmed.includes(":") ||
    trimmed.includes("[") ||
    trimmed.includes("]") ||
    trimmed.includes("*")
  ) {
    return `${label}-${hashSegment(trimmed)}`;
  }
  if (
    trimmed.includes("/") ||
    trimmed.includes("\\") ||
    trimmed.includes("\u0000")
  ) {
    return `${label}-${hashSegment(trimmed)}`;
  }
  return trimmed;
}

function formatTimestamp(now: Date): string {
  const iso = now.toISOString().replace(/\.\d{3}Z$/, "Z");
  return iso.replace(/[-:]/g, "");
}

export function buildBackupRn(
  depotIdent: string,
  layerIdent: string,
  now: Date,
  extension: string,
): RN {
  const safeLayer = normalizeSegment(layerIdent, "layer");
  const stamp = formatTimestamp(now);
  const id = crypto.randomUUID();
  const filename = `backup-${id}.${extension}`;
  const rnStr = `[rn][depot::${depotIdent}]:${BACKUP_ROOT}:${safeLayer}:${stamp}:${filename}`;
  return RN.create(rnStr).unwrap();
}

export function buildBackupPrefixRn(
  depotIdent: string,
  layerIdent: string,
): RN {
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
    Number(second),
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

export function buildBackupHeader(
  layerType: BackupHeader["layerType"],
  layerIdent: string,
  now: Date,
): BackupHeader {
  return {
    version: BACKUP_FORMAT_VERSION,
    createdAt: now.toISOString(),
    layerIdent,
    layerType,
  };
}

export function buildBackupHeaderEvent(
  layerType: BackupHeader["layerType"],
  layerIdent: string,
  now: Date,
): BackupHeaderEvent {
  return { t: "header", ...buildBackupHeader(layerType, layerIdent, now) };
}

export function encodeBackupEvent(event: BackupEvent): string {
  const json = JSON.stringify(event, (_, value) => {
    if (typeof value === "bigint") return value.toString();
    return value;
  });
  return `${json}\n`;
}

export function streamBackupEvents(
  events: AsyncIterable<BackupEvent>,
): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  const iterator = events[Symbol.asyncIterator]();
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      const { value, done } = await iterator.next();
      if (done) {
        controller.close();
        return;
      }
      controller.enqueue(encoder.encode(encodeBackupEvent(value)));
    },
    async cancel() {
      if (typeof iterator.return === "function") {
        await iterator.return();
      }
    },
  });
}

export function parseBackupEvent(line: string): BackupEvent {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch (error) {
    throw new Error("Backup stream contains invalid JSON.");
  }
  if (!parsed || typeof parsed !== "object") {
    throw new Error("Backup stream contains malformed events.");
  }
  const event = parsed as Record<string, unknown>;
  const tag = event.t;
  if (tag === "header") {
    const version = event.version;
    const layerIdent = event.layerIdent;
    const layerType = event.layerType;
    const createdAt = event.createdAt;
    if (version !== BACKUP_FORMAT_VERSION) {
      throw new Error(
        `Unsupported backup payload version "${version ?? "unknown"}".`,
      );
    }
    if (typeof layerIdent !== "string" || typeof layerType !== "string") {
      throw new Error("Backup header is missing layer identifiers.");
    }
    if (typeof createdAt !== "string") {
      throw new Error("Backup header is missing a creation timestamp.");
    }
    if (layerType !== "sqlite" && layerType !== "pg" && layerType !== "mysql") {
      throw new Error(`Unsupported backup layer type "${layerType}".`);
    }
    return event as BackupHeaderEvent;
  }
  if (tag === "table") {
    if (typeof event.name !== "string") {
      throw new Error("Backup table event is missing a name.");
    }
    return event as BackupTableEvent;
  }
  if (tag === "row") {
    if (typeof event.table !== "string") {
      throw new Error("Backup row event is missing a table name.");
    }
    if (!event.row || typeof event.row !== "object") {
      throw new Error("Backup row event is missing row data.");
    }
    return event as BackupRowEvent;
  }
  if (tag === "endTable") {
    if (typeof event.name !== "string") {
      throw new Error("Backup endTable event is missing a name.");
    }
    return event as BackupEndTableEvent;
  }
  if (tag === "end") {
    return event as BackupEndEvent;
  }
  throw new Error(`Backup stream contains unknown event "${String(tag)}".`);
}

export class BackupEventReader {
  private _reader: {
    read: () => Promise<{ value?: Uint8Array; done: boolean }>;
  };
  private _decoder: TextDecoder;
  private _buffer: string;
  private _done: boolean;

  constructor(stream: ReadableStream<Uint8Array>) {
    this._reader = stream.getReader();
    this._decoder = new TextDecoder();
    this._buffer = "";
    this._done = false;
  }

  async next(): Promise<BackupEvent | null> {
    if (this._done) return null;

    while (true) {
      const newlineIndex = this._buffer.indexOf("\n");
      if (newlineIndex >= 0) {
        const line = this._buffer.slice(0, newlineIndex).trim();
        this._buffer = this._buffer.slice(newlineIndex + 1);
        if (!line) {
          continue;
        }
        return parseBackupEvent(line);
      }

      const { value, done } = await this._reader.read();
      if (done) {
        this._done = true;
        this._buffer += this._decoder.decode();
        const line = this._buffer.trim();
        this._buffer = "";
        if (!line) return null;
        return parseBackupEvent(line);
      }
      this._buffer += this._decoder.decode(value, { stream: true });
    }
  }
}
