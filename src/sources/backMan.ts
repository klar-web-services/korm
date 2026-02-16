import crypto from "node:crypto";
import { RN } from "../core/rn";
import type { JSONable } from "../korm";
import {
  BackupEventReader,
  buildBackupPrefixRn,
  parseBackupTimestamp,
  type BackupContext,
  type BackupRestorePayload,
  type BackupRestoreOptions,
} from "./backups";
import type { Depot } from "../depot/depot";
import type { SourceLayer } from "./sourceLayer";

/**
 * Time units supported by backup interval schedules.
 * Use with `korm.interval.every(...)`.
 */
export type IntervalUnit =
  | "minute"
  | "hour"
  | "day"
  | "week"
  | "year"
  | "decade"
  | "century"
  | "millennium";
/** Weekday names for interval schedules (Monday-first). */
export type WeekdayName =
  | "monday"
  | "tuesday"
  | "wednesday"
  | "thursday"
  | "friday"
  | "saturday"
  | "sunday";
/** Weekday indexes for interval schedules (0=Monday .. 6=Sunday). */
export type WeekdayIndex = 0 | 1 | 2 | 3 | 4 | 5 | 6;
/** Weekday selector for interval schedules. */
export type Weekday = WeekdayName | WeekdayIndex;

type MultiYearUnit = "decade" | "century" | "millennium";
type YearUnit = "year" | MultiYearUnit;

/**
 * Fully-specified interval definition.
 * Obtain via `korm.interval.every(...)` or pass directly to `backups.addInterval(...)`.
 */
export type IntervalSpec = Readonly<{
  unit: IntervalUnit;
  yearInUnit?: number;
  month?: number;
  day?: number;
  weekday?: Weekday;
  hour?: number;
  minute?: number;
  second?: number;
  startsNow?: boolean;
}>;

/** Alias for IntervalSpec (exposed for API compatibility). */
export type IntervalPartial = IntervalSpec;
/**
 * Interval builder factory used by `korm.interval`.
 * Next: call `every(...)` to start a schedule.
 */
export type IntervalFactory = {
  every<U extends IntervalUnit>(unit: U): IntervalBuilder<U, "start">;
};
/** Backup retention policy for a layer schedule. */
export type BackupRetention =
  | { mode: "all" }
  | { mode: "none" }
  | { mode: "count"; count: number }
  | { mode: "days"; days: number };

type IntervalStage =
  | "start"
  | "yearSelected"
  | "dateSelected"
  | "timeSelected"
  | "secondSelected";

type OmitNever<T> = {
  [K in keyof T as T[K] extends never ? never : K]: T[K];
};

type InYearMethod<
  U extends IntervalUnit,
  S extends IntervalStage,
> = U extends MultiYearUnit
  ? S extends "start"
    ? (year: number) => IntervalBuilder<U, "yearSelected">
    : never
  : never;

type OnDateMethod<
  U extends IntervalUnit,
  S extends IntervalStage,
> = U extends "year"
  ? S extends "start"
    ? (month: number, day: number) => IntervalBuilder<U, "dateSelected">
    : never
  : U extends MultiYearUnit
    ? S extends "yearSelected"
      ? (month: number, day: number) => IntervalBuilder<U, "dateSelected">
      : never
    : never;

type OnWeekdayMethod<
  U extends IntervalUnit,
  S extends IntervalStage,
> = U extends "week"
  ? S extends "start"
    ? (weekday: Weekday) => IntervalBuilder<U, "dateSelected">
    : never
  : never;

type AtMethod<
  U extends IntervalUnit,
  S extends IntervalStage,
> = U extends "hour"
  ? S extends "start"
    ? (minute: number) => IntervalBuilder<U, "timeSelected">
    : never
  : U extends "day"
    ? S extends "start"
      ? (hour: number, minute: number) => IntervalBuilder<U, "timeSelected">
      : never
    : U extends "week"
      ? S extends "dateSelected"
        ? (hour: number, minute: number) => IntervalBuilder<U, "timeSelected">
        : never
      : U extends YearUnit
        ? S extends "dateSelected"
          ? (hour: number, minute: number) => IntervalBuilder<U, "timeSelected">
          : never
        : never;

type OnSecondMethod<
  U extends IntervalUnit,
  S extends IntervalStage,
> = U extends "minute"
  ? S extends "start"
    ? (second: number) => IntervalBuilder<U, "secondSelected">
    : never
  : U extends "hour" | "day" | "week" | YearUnit
    ? S extends "timeSelected"
      ? (second: number) => IntervalBuilder<U, "secondSelected">
      : never
    : never;

/**
 * Fluent interval builder returned by `korm.interval.every(...)`.
 * Chain the available methods, then pass the resulting spec to `backups.addInterval(...)`.
 */
export type IntervalBuilder<
  U extends IntervalUnit,
  S extends IntervalStage,
> = IntervalSpec &
  OmitNever<{
    /**
     * Run immediately, then follow the interval schedule.
     * Next: continue chaining precision steps, or stop.
     */
    runNow: () => IntervalBuilder<U, S>;
    /**
     * Choose the year within a multi-year unit (0-based).
     * Next: call `onDate(...)` or stop.
     */
    inYear: InYearMethod<U, S>;
    /**
     * Choose a specific calendar date (1-based month/day).
     * Next: call `at(...)` or stop.
     */
    onDate: OnDateMethod<U, S>;
    /**
     * Choose the weekday within a week (name or 0=Monday..6=Sunday).
     * Next: call `at(...)` or stop.
     */
    on: OnWeekdayMethod<U, S>;
    /**
     * Choose the time within the unit (minute for hours, hour/minute for days+).
     * Next: call `onSecond(...)` or stop.
     */
    at: AtMethod<U, S>;
    /**
     * Choose the second within the minute.
     * Next: stop.
     */
    onSecond: OnSecondMethod<U, S>;
  }>;

type IntervalSpecData = {
  unit: IntervalUnit;
  yearInUnit?: number;
  month?: number;
  day?: number;
  weekday?: Weekday;
  hour?: number;
  minute?: number;
  second?: number;
  startsNow?: boolean;
};

const DAYS_IN_MONTH = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] as const;
const WEEKDAY_NAMES: WeekdayName[] = [
  "monday",
  "tuesday",
  "wednesday",
  "thursday",
  "friday",
  "saturday",
  "sunday",
];
const MAX_YEAR_IN_UNIT: Record<MultiYearUnit, number> = {
  decade: 9,
  century: 99,
  millennium: 999,
};
const BACKUPS_TABLE = "__korm_backups__";
const BACKUP_CLAIM_TIMEOUT_MS = 0;
const BACKUP_CLAIM_SWEEP_MS = 30_000;
const MAX_TIMER_DELAY_MS = 2_147_483_647;

function isMultiYearUnit(unit: IntervalUnit): unit is MultiYearUnit {
  return unit === "decade" || unit === "century" || unit === "millennium";
}

function assertIntegerInRange(
  value: number,
  min: number,
  max: number,
  label: string,
): void {
  if (!Number.isFinite(value) || !Number.isInteger(value)) {
    throw new Error(`Expected ${label} to be an integer.`);
  }
  if (value < min || value > max) {
    throw new Error(`Expected ${label} to be between ${min} and ${max}.`);
  }
}

function assertWeekday(weekday: Weekday): void {
  if (typeof weekday === "number") {
    assertIntegerInRange(weekday, 0, 6, "weekday");
    return;
  }
  if (!WEEKDAY_NAMES.includes(weekday)) {
    throw new Error(
      `Expected weekday to be one of ${WEEKDAY_NAMES.join(", ")} or 0-6 (0=Monday).`,
    );
  }
}

function assertMonthDay(month: number, day: number): void {
  assertIntegerInRange(month, 1, 12, "month");
  const maxDay = DAYS_IN_MONTH[month - 1] ?? 31;
  assertIntegerInRange(day, 1, maxDay, "day");
}

function assertNonNegativeInteger(value: number, label: string): void {
  if (!Number.isFinite(value) || !Number.isInteger(value)) {
    throw new Error(`Expected ${label} to be an integer.`);
  }
  if (value < 0) {
    throw new Error(`Expected ${label} to be 0 or greater.`);
  }
}

class IntervalBuilderImpl {
  private _spec: IntervalSpecData;

  constructor(unit: IntervalUnit) {
    this._spec = { unit };
  }

  get unit(): IntervalUnit {
    return this._spec.unit;
  }

  get yearInUnit(): number | undefined {
    return this._spec.yearInUnit;
  }

  get month(): number | undefined {
    return this._spec.month;
  }

  get day(): number | undefined {
    return this._spec.day;
  }

  get weekday(): Weekday | undefined {
    return this._spec.weekday;
  }

  get hour(): number | undefined {
    return this._spec.hour;
  }

  get minute(): number | undefined {
    return this._spec.minute;
  }

  get second(): number | undefined {
    return this._spec.second;
  }

  get startsNow(): boolean | undefined {
    return this._spec.startsNow;
  }

  runNow(): this {
    this._spec.startsNow = true;
    return this;
  }

  inYear(year: number): this {
    if (!isMultiYearUnit(this._spec.unit)) {
      throw new Error(
        `inYear(...) is only valid for decade, century, or millennium units.`,
      );
    }
    const max = MAX_YEAR_IN_UNIT[this._spec.unit];
    assertIntegerInRange(year, 0, max, `yearInUnit (${this._spec.unit})`);
    this._spec.yearInUnit = year;
    return this;
  }

  onDate(month: number, day: number): this {
    if (this._spec.unit !== "year" && !isMultiYearUnit(this._spec.unit)) {
      throw new Error(
        `onDate(...) is only valid for year, decade, century, or millennium units.`,
      );
    }
    if (
      isMultiYearUnit(this._spec.unit) &&
      this._spec.yearInUnit === undefined
    ) {
      throw new Error(
        `onDate(...) requires inYear(...) for unit "${this._spec.unit}".`,
      );
    }
    assertMonthDay(month, day);
    this._spec.month = month;
    this._spec.day = day;
    return this;
  }

  on(weekday: Weekday): this {
    if (this._spec.unit !== "week") {
      throw new Error(`on(...) is only valid for unit "week".`);
    }
    assertWeekday(weekday);
    this._spec.weekday = weekday;
    return this;
  }

  at(hourOrMinute: number, minute?: number): this {
    if (minute === undefined) {
      if (this._spec.unit !== "hour") {
        throw new Error(`at(minute) is only valid for unit "hour".`);
      }
      assertIntegerInRange(hourOrMinute, 0, 59, "minute");
      this._spec.minute = hourOrMinute;
      return this;
    }
    if (this._spec.unit === "hour") {
      throw new Error(
        `at(hour, minute) is not valid for unit "hour". Use at(minute).`,
      );
    }
    if (this._spec.unit === "minute") {
      throw new Error(`at(...) is not valid for unit "minute".`);
    }
    if (this._spec.unit === "week" && this._spec.weekday === undefined) {
      throw new Error(`at(...) requires on(...) for unit "week".`);
    }
    if (this._spec.unit === "year") {
      if (this._spec.month === undefined || this._spec.day === undefined) {
        throw new Error(`at(...) requires onDate(...) for unit "year".`);
      }
    }
    if (isMultiYearUnit(this._spec.unit)) {
      if (this._spec.yearInUnit === undefined) {
        throw new Error(
          `at(...) requires inYear(...) for unit "${this._spec.unit}".`,
        );
      }
      if (this._spec.month === undefined || this._spec.day === undefined) {
        throw new Error(
          `at(...) requires onDate(...) for unit "${this._spec.unit}".`,
        );
      }
    }
    assertIntegerInRange(hourOrMinute, 0, 23, "hour");
    assertIntegerInRange(minute, 0, 59, "minute");
    this._spec.hour = hourOrMinute;
    this._spec.minute = minute;
    return this;
  }

  onSecond(second: number): this {
    assertIntegerInRange(second, 0, 59, "second");
    if (this._spec.unit === "minute") {
      this._spec.second = second;
      return this;
    }
    if (this._spec.unit === "hour") {
      if (this._spec.minute === undefined) {
        throw new Error(`onSecond(...) requires at(minute) for unit "hour".`);
      }
      this._spec.second = second;
      return this;
    }
    if (this._spec.unit === "day") {
      if (this._spec.hour === undefined || this._spec.minute === undefined) {
        throw new Error(
          `onSecond(...) requires at(hour, minute) for unit "day".`,
        );
      }
      this._spec.second = second;
      return this;
    }
    if (this._spec.unit === "week") {
      if (this._spec.weekday === undefined) {
        throw new Error(`onSecond(...) requires on(...) for unit "week".`);
      }
      if (this._spec.hour === undefined || this._spec.minute === undefined) {
        throw new Error(
          `onSecond(...) requires at(hour, minute) for unit "week".`,
        );
      }
      this._spec.second = second;
      return this;
    }
    if (this._spec.unit === "year") {
      if (this._spec.month === undefined || this._spec.day === undefined) {
        throw new Error(`onSecond(...) requires onDate(...) for unit "year".`);
      }
      if (this._spec.hour === undefined || this._spec.minute === undefined) {
        throw new Error(
          `onSecond(...) requires at(hour, minute) for unit "year".`,
        );
      }
      this._spec.second = second;
      return this;
    }
    if (isMultiYearUnit(this._spec.unit)) {
      if (this._spec.yearInUnit === undefined) {
        throw new Error(
          `onSecond(...) requires inYear(...) for unit "${this._spec.unit}".`,
        );
      }
      if (this._spec.month === undefined || this._spec.day === undefined) {
        throw new Error(
          `onSecond(...) requires onDate(...) for unit "${this._spec.unit}".`,
        );
      }
      if (this._spec.hour === undefined || this._spec.minute === undefined) {
        throw new Error(
          `onSecond(...) requires at(hour, minute) for unit "${this._spec.unit}".`,
        );
      }
      this._spec.second = second;
      return this;
    }
    this._spec.second = second;
    return this;
  }
}

type NormalizedIntervalSpec = {
  unit: IntervalUnit;
  yearInUnit?: number;
  month?: number;
  day?: number;
  weekday?: Weekday;
  hour?: number;
  minute?: number;
  second?: number;
  startsNow?: boolean;
};

type BackupScheduleSeed = {
  scheduleId: string;
  layerIdent: string;
  interval: NormalizedIntervalSpec;
  intervalKey: string;
};

type BackupScheduleEntry = BackupScheduleSeed & {
  nextRunAt: number;
  lastRunAt: number | null;
};

type BackupScheduleRow = {
  scheduleId: string;
  nextRunAt: number;
  lastRunAt: number | null;
};

type BackupScheduleInsert = {
  scheduleId: string;
  layerIdent: string;
  intervalJson: string;
  nextRunAt: number;
  lastRunAt: number | null;
};

type BackupTimerState = BackupScheduleEntry & {
  timer?: ReturnType<typeof setTimeout>;
  releaseLock: () => void;
  running: boolean;
};

type BackupsLocker = {
  acquire: (rn: RN<JSONable>, timeoutMs?: number) => Promise<() => void>;
};

type SqliteDb = {
  run: (sql: string, params?: unknown[]) => void;
  prepare: (sql: string) => {
    all: (params?: unknown[]) => Array<Record<string, unknown>>;
  };
};

type PgDb = {
  unsafe: (sql: string, params?: unknown[]) => Promise<unknown>;
};

type MysqlPool = {
  query: (sql: string, params?: unknown[]) => Promise<unknown>;
};

const MULTI_YEAR_UNIT_SIZE: Record<MultiYearUnit, number> = {
  decade: 10,
  century: 100,
  millennium: 1000,
};

function normalizeIntervalSpec(spec: IntervalSpec): NormalizedIntervalSpec {
  const normalized: NormalizedIntervalSpec = { unit: spec.unit };
  if (spec.yearInUnit !== undefined) normalized.yearInUnit = spec.yearInUnit;
  if (spec.month !== undefined) normalized.month = spec.month;
  if (spec.day !== undefined) normalized.day = spec.day;
  if (spec.weekday !== undefined) normalized.weekday = spec.weekday;
  if (spec.hour !== undefined) normalized.hour = spec.hour;
  if (spec.minute !== undefined) normalized.minute = spec.minute;
  if (spec.second !== undefined) normalized.second = spec.second;
  if (spec.startsNow) normalized.startsNow = true;
  return normalized;
}

function stableIntervalKey(spec: NormalizedIntervalSpec): string {
  const entries = Object.entries(spec)
    .filter(([, value]) => value !== undefined)
    .sort(([a], [b]) => a.localeCompare(b));
  const normalized: Record<string, unknown> = {};
  for (const [key, value] of entries) {
    normalized[key] = value;
  }
  return JSON.stringify(normalized);
}

function stableUuidFromSeed(seed: string): string {
  const hash = crypto.createHash("sha256").update(seed).digest();
  const bytes = Buffer.from(hash);
  bytes[6] = ((bytes[6] ?? 0) & 0x0f) | 0x40;
  bytes[8] = ((bytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = bytes.toString("hex").slice(0, 32);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

function normalizeWeekdayIndex(
  weekday: Weekday | undefined,
): number | undefined {
  if (weekday === undefined) return undefined;
  if (typeof weekday === "number") return weekday;
  const idx = WEEKDAY_NAMES.indexOf(weekday);
  return idx === -1 ? undefined : idx;
}

function weekdayIndex(date: Date): number {
  const jsIndex = date.getDay();
  return (jsIndex + 6) % 7;
}

function clampDay(year: number, monthIndex: number, day: number): number {
  const maxDay = new Date(year, monthIndex + 1, 0).getDate();
  return Math.min(day, maxDay);
}

function buildDate(
  year: number,
  monthIndex: number,
  day: number,
  hour: number,
  minute: number,
  second: number,
): Date {
  return new Date(year, monthIndex, day, hour, minute, second, 0);
}

function computeNextRun(spec: NormalizedIntervalSpec, from: Date): Date {
  const base = new Date(from.getTime());
  base.setMilliseconds(0);
  const second = spec.second ?? base.getSeconds();
  const minute = spec.minute ?? base.getMinutes();
  const hour = spec.hour ?? base.getHours();

  if (spec.unit === "minute") {
    const candidate = new Date(base.getTime());
    candidate.setSeconds(second, 0);
    if (candidate <= base) {
      candidate.setMinutes(candidate.getMinutes() + 1);
    }
    return candidate;
  }

  if (spec.unit === "hour") {
    const candidate = new Date(base.getTime());
    candidate.setMinutes(minute, second, 0);
    if (candidate <= base) {
      candidate.setHours(candidate.getHours() + 1);
    }
    return candidate;
  }

  if (spec.unit === "day") {
    const candidate = new Date(base.getTime());
    candidate.setHours(hour, minute, second, 0);
    if (candidate <= base) {
      candidate.setDate(candidate.getDate() + 1);
    }
    return candidate;
  }

  if (spec.unit === "week") {
    const targetWeekday =
      normalizeWeekdayIndex(spec.weekday) ?? weekdayIndex(base);
    const currentWeekday = weekdayIndex(base);
    const delta = (targetWeekday - currentWeekday + 7) % 7;
    const candidate = new Date(base.getTime());
    candidate.setDate(candidate.getDate() + delta);
    candidate.setHours(hour, minute, second, 0);
    if (candidate <= base) {
      candidate.setDate(candidate.getDate() + 7);
    }
    return candidate;
  }

  const desiredMonth = spec.month ?? base.getMonth() + 1;
  const desiredDay = spec.day ?? base.getDate();

  if (spec.unit === "year") {
    let year = base.getFullYear();
    let monthIndex = desiredMonth - 1;
    let day = clampDay(year, monthIndex, desiredDay);
    let candidate = buildDate(year, monthIndex, day, hour, minute, second);
    if (candidate <= base) {
      year += 1;
      monthIndex = desiredMonth - 1;
      day = clampDay(year, monthIndex, desiredDay);
      candidate = buildDate(year, monthIndex, day, hour, minute, second);
    }
    return candidate;
  }

  if (isMultiYearUnit(spec.unit)) {
    const size = MULTI_YEAR_UNIT_SIZE[spec.unit];
    const currentYear = base.getFullYear();
    const baseYear = Math.floor(currentYear / size) * size;
    const yearInUnit = spec.yearInUnit ?? currentYear - baseYear;
    let targetYear = baseYear + yearInUnit;
    let monthIndex = desiredMonth - 1;
    let day = clampDay(targetYear, monthIndex, desiredDay);
    let candidate = buildDate(
      targetYear,
      monthIndex,
      day,
      hour,
      minute,
      second,
    );
    if (candidate <= base) {
      targetYear += size;
      monthIndex = desiredMonth - 1;
      day = clampDay(targetYear, monthIndex, desiredDay);
      candidate = buildDate(targetYear, monthIndex, day, hour, minute, second);
    }
    return candidate;
  }

  return new Date(base.getTime() + 60_000);
}

function parseTimestamp(value: unknown): number {
  if (typeof value === "number") return value;
  if (typeof value === "string" && value) return Number(value);
  return Number.NaN;
}

function parseNullableTimestamp(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const parsed = parseTimestamp(value);
  return Number.isFinite(parsed) ? parsed : null;
}

class BackupsScheduleStore {
  private _layer: SourceLayer;
  private _ensured?: Promise<void>;

  constructor(layer: SourceLayer) {
    this._layer = layer;
  }

  async ensure(): Promise<void> {
    if (this._ensured) {
      return this._ensured;
    }
    this._ensured = this._createTable();
    return this._ensured;
  }

  private _sqliteDb(): SqliteDb | undefined {
    const db = (this._layer as { _db?: unknown })._db;
    if (
      !db ||
      typeof (db as { run?: unknown }).run !== "function" ||
      typeof (db as { prepare?: unknown }).prepare !== "function"
    ) {
      return undefined;
    }
    return db as SqliteDb;
  }

  private _pgDb(): PgDb | undefined {
    const db = (this._layer as { _db?: unknown })._db;
    if (!db || typeof (db as { unsafe?: unknown }).unsafe !== "function") {
      return undefined;
    }
    return db as PgDb;
  }

  private _mysqlPool(): MysqlPool | undefined {
    const pool = (this._layer as { _pool?: unknown })._pool;
    if (!pool || typeof (pool as { query?: unknown }).query !== "function") {
      return undefined;
    }
    return pool as MysqlPool;
  }

  private async _createTable(): Promise<void> {
    if (this._layer.type === "sqlite") {
      const db = this._sqliteDb();
      if (!db) {
        throw new Error(
          `Backups require a sqlite layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      db.run(
        `CREATE TABLE IF NOT EXISTS "${BACKUPS_TABLE}" (
                    "schedule_id" TEXT PRIMARY KEY,
                    "layer_ident" TEXT NOT NULL,
                    "interval_spec" TEXT NOT NULL,
                    "next_run_at" INTEGER NOT NULL,
                    "last_run_at" INTEGER,
                    "created_at" INTEGER NOT NULL,
                    "updated_at" INTEGER NOT NULL
                )`,
      );
      return;
    }
    if (this._layer.type === "pg") {
      const db = this._pgDb();
      if (!db) {
        throw new Error(
          `Backups require a pg layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      await db.unsafe(
        `CREATE TABLE IF NOT EXISTS "${BACKUPS_TABLE}" (
                    "schedule_id" TEXT PRIMARY KEY,
                    "layer_ident" TEXT NOT NULL,
                    "interval_spec" JSONB NOT NULL,
                    "next_run_at" BIGINT NOT NULL,
                    "last_run_at" BIGINT,
                    "created_at" BIGINT NOT NULL,
                    "updated_at" BIGINT NOT NULL
                )`,
      );
      return;
    }
    if (this._layer.type === "mysql") {
      const pool = this._mysqlPool();
      if (!pool) {
        throw new Error(
          `Backups require a mysql layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      await pool.query(
        `CREATE TABLE IF NOT EXISTS \`${BACKUPS_TABLE}\` (
                    \`schedule_id\` VARCHAR(64) PRIMARY KEY,
                    \`layer_ident\` VARCHAR(255) NOT NULL,
                    \`interval_spec\` JSON NOT NULL,
                    \`next_run_at\` BIGINT NOT NULL,
                    \`last_run_at\` BIGINT,
                    \`created_at\` BIGINT NOT NULL,
                    \`updated_at\` BIGINT NOT NULL,
                    INDEX \`idx_next_run_at\` (\`next_run_at\`)
                )`,
      );
      return;
    }
    throw new Error(`Backups do not support layer type "${this._layer.type}".`);
  }

  async list(): Promise<BackupScheduleRow[]> {
    await this.ensure();
    if (this._layer.type === "sqlite") {
      const db = this._sqliteDb();
      if (!db) return [];
      const rows = db
        .prepare(
          `SELECT schedule_id, next_run_at, last_run_at FROM "${BACKUPS_TABLE}"`,
        )
        .all();
      return rows
        .map((row) => ({
          scheduleId: row.schedule_id as string,
          nextRunAt: parseTimestamp(row.next_run_at),
          lastRunAt: parseNullableTimestamp(row.last_run_at),
        }))
        .filter((row) => Number.isFinite(row.nextRunAt));
    }
    if (this._layer.type === "pg") {
      const db = this._pgDb();
      if (!db) return [];
      const rows = (await db.unsafe(
        `SELECT schedule_id, next_run_at, last_run_at FROM "${BACKUPS_TABLE}"`,
      )) as Array<{
        schedule_id: string;
        next_run_at: unknown;
        last_run_at?: unknown;
      }>;
      return rows
        .map((row) => ({
          scheduleId: row.schedule_id,
          nextRunAt: parseTimestamp(row.next_run_at),
          lastRunAt: parseNullableTimestamp(row.last_run_at),
        }))
        .filter((row) => Number.isFinite(row.nextRunAt));
    }
    if (this._layer.type === "mysql") {
      const pool = this._mysqlPool();
      if (!pool) return [];
      const [rows] = (await pool.query(
        `SELECT schedule_id, next_run_at, last_run_at FROM \`${BACKUPS_TABLE}\``,
      )) as [
        { schedule_id: string; next_run_at: unknown; last_run_at?: unknown }[],
      ];
      return rows
        .map((row) => ({
          scheduleId: row.schedule_id,
          nextRunAt: parseTimestamp(row.next_run_at),
          lastRunAt: parseNullableTimestamp(row.last_run_at),
        }))
        .filter((row) => Number.isFinite(row.nextRunAt));
    }
    return [];
  }

  async insertMany(entries: BackupScheduleInsert[]): Promise<void> {
    if (entries.length === 0) return;
    await this.ensure();
    const now = Date.now();
    if (this._layer.type === "sqlite") {
      const db = this._sqliteDb();
      if (!db) {
        throw new Error(
          `Backups require a sqlite layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      for (const entry of entries) {
        db.run(
          `INSERT INTO "${BACKUPS_TABLE}" ("schedule_id", "layer_ident", "interval_spec", "next_run_at", "last_run_at", "created_at", "updated_at")
                     VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT("schedule_id") DO NOTHING`,
          [
            entry.scheduleId,
            entry.layerIdent,
            entry.intervalJson,
            entry.nextRunAt,
            entry.lastRunAt,
            now,
            now,
          ],
        );
      }
      return;
    }
    if (this._layer.type === "pg") {
      const db = this._pgDb();
      if (!db) {
        throw new Error(
          `Backups require a pg layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      for (const entry of entries) {
        await db.unsafe(
          `INSERT INTO "${BACKUPS_TABLE}" ("schedule_id", "layer_ident", "interval_spec", "next_run_at", "last_run_at", "created_at", "updated_at")
                     VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7)
                     ON CONFLICT ("schedule_id") DO NOTHING`,
          [
            entry.scheduleId,
            entry.layerIdent,
            entry.intervalJson,
            entry.nextRunAt,
            entry.lastRunAt,
            now,
            now,
          ],
        );
      }
      return;
    }
    if (this._layer.type === "mysql") {
      const pool = this._mysqlPool();
      if (!pool) {
        throw new Error(
          `Backups require a mysql layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      for (const entry of entries) {
        await pool.query(
          `INSERT IGNORE INTO \`${BACKUPS_TABLE}\` (\`schedule_id\`, \`layer_ident\`, \`interval_spec\`, \`next_run_at\`, \`last_run_at\`, \`created_at\`, \`updated_at\`)
                     VALUES (?, ?, ?, ?, ?, ?, ?)`,
          [
            entry.scheduleId,
            entry.layerIdent,
            entry.intervalJson,
            entry.nextRunAt,
            entry.lastRunAt,
            now,
            now,
          ],
        );
      }
    }
  }

  async updateRun(
    scheduleId: string,
    nextRunAt: number,
    lastRunAt: number | null,
  ): Promise<void> {
    await this.ensure();
    const now = Date.now();
    if (this._layer.type === "sqlite") {
      const db = this._sqliteDb();
      if (!db) {
        throw new Error(
          `Backups require a sqlite layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      db.run(
        `UPDATE "${BACKUPS_TABLE}" SET "next_run_at" = ?, "last_run_at" = ?, "updated_at" = ? WHERE "schedule_id" = ?`,
        [nextRunAt, lastRunAt, now, scheduleId],
      );
      return;
    }
    if (this._layer.type === "pg") {
      const db = this._pgDb();
      if (!db) {
        throw new Error(
          `Backups require a pg layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      await db.unsafe(
        `UPDATE "${BACKUPS_TABLE}" SET "next_run_at" = $1, "last_run_at" = $2, "updated_at" = $3 WHERE "schedule_id" = $4`,
        [nextRunAt, lastRunAt, now, scheduleId],
      );
      return;
    }
    if (this._layer.type === "mysql") {
      const pool = this._mysqlPool();
      if (!pool) {
        throw new Error(
          `Backups require a mysql layer with a live connection for "${this._layer.identifier}".`,
        );
      }
      await pool.query(
        `UPDATE \`${BACKUPS_TABLE}\` SET \`next_run_at\` = ?, \`last_run_at\` = ?, \`updated_at\` = ? WHERE \`schedule_id\` = ?`,
        [nextRunAt, lastRunAt, now, scheduleId],
      );
    }
  }
}

/**
 * Backup manager for scheduling and restore operations.
 * Use via `korm.pool().backups(...)` or attach with `pool.configureBackups(...)`.
 */
export class BackMan {
  private _intervals: { layerIdent: string; interval: IntervalSpec }[] = [];
  private _layers?: Map<string, SourceLayer>;
  private _depot?: Depot;
  private _depotIdent?: string;
  private _retentionByLayer: Map<string, BackupRetention> = new Map();
  private _scheduleSeeds: BackupScheduleSeed[] = [];
  private _store?: BackupsScheduleStore;
  private _locker?: BackupsLocker;
  private _timers = new Map<string, BackupTimerState>();
  private _claimInterval?: ReturnType<typeof setInterval>;
  private _started = false;
  private _closed = false;

  /** @internal */
  conveyDepot(depot?: Depot, depotIdent?: string): this {
    this._depot = depot;
    this._depotIdent = depotIdent;
    return this;
  }

  /** @internal */
  conveyLayers(layers: Map<string, SourceLayer>): this {
    this._layers = layers;
    return this;
  }

  /**
   * Register a backup interval for a specific layer.
   * Next: call `addInterval(...)` again or use the configured BackMan in a pool.
   */
  /** @internal */
  addInterval(layerIdent: string, interval: IntervalSpec): this {
    if (layerIdent !== "*" && this._layers && !this._layers.has(layerIdent)) {
      throw new Error(
        `Backups layer "${layerIdent}" is not present in the pool.`,
      );
    }
    this._intervals.push({ layerIdent, interval });
    return this;
  }

  /** @internal */
  setRetentionForLayer(layerIdent: string, retention: BackupRetention): this {
    if (layerIdent !== "*" && this._layers && !this._layers.has(layerIdent)) {
      throw new Error(
        `Backups layer "${layerIdent}" is not present in the pool.`,
      );
    }
    if (retention.mode === "count") {
      assertNonNegativeInteger(retention.count, "backup retention count");
    }
    if (retention.mode === "days") {
      assertNonNegativeInteger(retention.days, "backup retention days");
    }
    this._retentionByLayer.set(layerIdent, retention);
    return this;
  }

  /** @internal */
  getPoolConfig(): {
    intervals: { layerIdent: string; interval: IntervalSpec }[];
    retention: { layerIdent: string; retention: BackupRetention }[];
  } {
    return {
      intervals: this._intervals.map((entry) => ({
        layerIdent: entry.layerIdent,
        interval: entry.interval,
      })),
      retention: Array.from(this._retentionByLayer.entries()).map(
        ([layerIdent, retention]) => ({ layerIdent, retention }),
      ),
    };
  }

  /** @internal */
  async start(metaLayer: SourceLayer, locker: BackupsLocker): Promise<void> {
    if (this._started || this._closed) return;
    if (!this._layers) {
      throw new Error("Backups require pool layers to be attached.");
    }
    this._started = true;
    this._locker = locker;
    this._store = new BackupsScheduleStore(metaLayer);
    this._scheduleSeeds = this._buildScheduleSeeds();
    if (this._scheduleSeeds.length === 0) return;
    const entries = await this._syncSchedules();
    if (this._closed) return;
    await this._claimSchedules(entries);
    if (this._closed) return;
    this._claimInterval = setInterval(() => {
      if (this._closed) return;
      void this._refreshClaims();
    }, BACKUP_CLAIM_SWEEP_MS);
  }

  /** @internal */
  async close(): Promise<void> {
    this._closed = true;
    if (this._claimInterval) {
      clearInterval(this._claimInterval);
      this._claimInterval = undefined;
    }
    for (const state of this._timers.values()) {
      if (state.timer) {
        clearTimeout(state.timer);
      }
      try {
        state.releaseLock();
      } catch {
        // ignore
      }
    }
    this._timers.clear();
  }

  private _buildScheduleSeeds(): BackupScheduleSeed[] {
    if (!this._layers) return [];
    const layerIdents = Array.from(this._layers.keys()).sort((a, b) =>
      a.localeCompare(b),
    );
    const expanded: Array<{
      layerIdent: string;
      interval: NormalizedIntervalSpec;
    }> = [];
    for (const entry of this._intervals) {
      if (entry.layerIdent === "*") {
        for (const layerIdent of layerIdents) {
          expanded.push({
            layerIdent,
            interval: normalizeIntervalSpec(entry.interval),
          });
        }
        continue;
      }
      if (!this._layers.has(entry.layerIdent)) {
        throw new Error(
          `Backups layer "${entry.layerIdent}" is not present in the pool.`,
        );
      }
      expanded.push({
        layerIdent: entry.layerIdent,
        interval: normalizeIntervalSpec(entry.interval),
      });
    }

    const grouped = new Map<
      string,
      {
        layerIdent: string;
        interval: NormalizedIntervalSpec;
        intervalKey: string;
        count: number;
      }
    >();
    for (const entry of expanded) {
      const intervalKey = stableIntervalKey(entry.interval);
      const key = `${entry.layerIdent}::${intervalKey}`;
      const group = grouped.get(key) ?? {
        layerIdent: entry.layerIdent,
        interval: entry.interval,
        intervalKey,
        count: 0,
      };
      group.count += 1;
      grouped.set(key, group);
    }

    const sortedGroups = Array.from(grouped.values()).sort((a, b) => {
      const layerCmp = a.layerIdent.localeCompare(b.layerIdent);
      if (layerCmp !== 0) return layerCmp;
      return a.intervalKey.localeCompare(b.intervalKey);
    });

    const seeds: BackupScheduleSeed[] = [];
    for (const group of sortedGroups) {
      for (let i = 0; i < group.count; i++) {
        const scheduleId = stableUuidFromSeed(
          `${group.layerIdent}:${group.intervalKey}:${i}`,
        );
        seeds.push({
          scheduleId,
          layerIdent: group.layerIdent,
          interval: group.interval,
          intervalKey: group.intervalKey,
        });
      }
    }
    return seeds;
  }

  private async _syncSchedules(): Promise<BackupScheduleEntry[]> {
    if (!this._store) {
      throw new Error("Backups schedule store is not configured.");
    }
    if (this._scheduleSeeds.length === 0) return [];
    const rows = await this._store.list();
    const rowMap = new Map(rows.map((row) => [row.scheduleId, row]));
    const now = Date.now();
    const inserts: BackupScheduleInsert[] = [];
    const updates: Array<{
      scheduleId: string;
      nextRunAt: number;
      lastRunAt: number | null;
    }> = [];
    const entries: BackupScheduleEntry[] = [];

    for (const seed of this._scheduleSeeds) {
      const row = rowMap.get(seed.scheduleId);
      let nextRunAt = row?.nextRunAt;
      let lastRunAt = row?.lastRunAt ?? null;
      if (!Number.isFinite(nextRunAt) || (nextRunAt ?? 0) <= 0) {
        const next = seed.interval.startsNow
          ? now
          : computeNextRun(seed.interval, new Date(now)).getTime();
        nextRunAt = next;
        lastRunAt = null;
        if (row) {
          updates.push({ scheduleId: seed.scheduleId, nextRunAt, lastRunAt });
        } else {
          inserts.push({
            scheduleId: seed.scheduleId,
            layerIdent: seed.layerIdent,
            intervalJson: JSON.stringify(seed.interval),
            nextRunAt,
            lastRunAt,
          });
        }
      }
      entries.push({
        ...seed,
        nextRunAt: nextRunAt ?? now,
        lastRunAt,
      });
    }

    if (inserts.length > 0) {
      await this._store.insertMany(inserts);
    }
    if (updates.length > 0) {
      for (const update of updates) {
        await this._store.updateRun(
          update.scheduleId,
          update.nextRunAt,
          update.lastRunAt,
        );
      }
    }

    return entries;
  }

  private async _refreshClaims(): Promise<void> {
    try {
      const entries = await this._syncSchedules();
      if (this._closed) return;
      await this._claimSchedules(entries);
    } catch {
      // ignore
    }
  }

  private async _claimSchedules(entries: BackupScheduleEntry[]): Promise<void> {
    if (!this._locker) {
      throw new Error("Backups locker is not configured.");
    }
    for (const entry of entries) {
      if (this._closed) return;
      if (this._timers.has(entry.scheduleId)) continue;
      const rn = RN.create("korm", "backup", entry.scheduleId).unwrap();
      let release: (() => void) | undefined;
      try {
        release = await this._locker.acquire(rn, BACKUP_CLAIM_TIMEOUT_MS);
      } catch (error) {
        const err = error as Error;
        if (err?.name === "LockTimeoutError") {
          continue;
        }
        throw error;
      }
      if (this._closed) {
        release();
        return;
      }
      const state: BackupTimerState = {
        ...entry,
        releaseLock: release,
        running: false,
      };
      this._timers.set(entry.scheduleId, state);
      this._scheduleTimer(state);
    }
  }

  private _scheduleTimer(state: BackupTimerState): void {
    if (this._closed) return;
    if (state.timer) {
      clearTimeout(state.timer);
    }
    const delay = Math.max(0, state.nextRunAt - Date.now());
    const wait = Math.min(delay, MAX_TIMER_DELAY_MS);
    state.timer = setTimeout(() => {
      void this._handleTimer(state);
    }, wait);
  }

  private async _handleTimer(state: BackupTimerState): Promise<void> {
    if (this._closed) return;
    const now = Date.now();
    if (now < state.nextRunAt) {
      this._scheduleTimer(state);
      return;
    }
    if (state.running) {
      this._scheduleTimer(state);
      return;
    }
    state.running = true;
    try {
      await this._executeBackup(state.layerIdent);
    } catch {
      // ignore
    } finally {
      const completedAt = Date.now();
      const next = computeNextRun(
        state.interval,
        new Date(completedAt),
      ).getTime();
      state.nextRunAt = next;
      state.lastRunAt = completedAt;
      try {
        await this._store?.updateRun(
          state.scheduleId,
          state.nextRunAt,
          completedAt,
        );
      } catch {
        // ignore
      }
      state.running = false;
      if (!this._closed) {
        this._scheduleTimer(state);
      }
    }
  }

  private async _executeBackup(layerIdent: string): Promise<void> {
    const layer = this._layers?.get(layerIdent);
    if (!layer || !this._depot || !this._depotIdent) return;
    const runner = layer as {
      backup?: (context: BackupContext) => Promise<void> | void;
    };
    if (typeof runner.backup === "function") {
      await runner.backup({
        depot: this._depot,
        depotIdent: this._depotIdent,
        layerIdent,
        now: new Date(),
      });
      await this._applyRetention(layerIdent);
    }
  }

  private _retentionForLayer(layerIdent: string): BackupRetention | undefined {
    return (
      this._retentionByLayer.get(layerIdent) ?? this._retentionByLayer.get("*")
    );
  }

  private async _applyRetention(layerIdent: string): Promise<void> {
    const retention = this._retentionForLayer(layerIdent);
    if (!retention || retention.mode === "all") return;
    if (!this._depot || !this._depotIdent) return;

    const prefix = buildBackupPrefixRn(this._depotIdent, layerIdent);
    const files = await this._depot.listFiles(prefix);
    if (files.length === 0) return;

    if (retention.mode === "none") {
      for (const file of files) {
        try {
          await this._depot.deleteFile(file.rn);
        } catch {
          // ignore
        }
      }
      return;
    }

    const entries = files
      .map((file) => ({
        rn: file.rn,
        timestamp: parseBackupTimestamp(file.rn),
      }))
      .filter(
        (entry): entry is { rn: RN; timestamp: number } =>
          typeof entry.timestamp === "number" &&
          Number.isFinite(entry.timestamp),
      );

    if (entries.length === 0) return;

    if (retention.mode === "days") {
      const cutoff = Date.now() - retention.days * 24 * 60 * 60 * 1000;
      for (const entry of entries) {
        if ((entry.timestamp as number) >= cutoff) continue;
        try {
          await this._depot.deleteFile(entry.rn);
        } catch {
          // ignore
        }
      }
      return;
    }

    if (retention.mode === "count") {
      const sorted = entries
        .slice()
        .sort((a, b) => (b.timestamp as number) - (a.timestamp as number));
      const toDelete = sorted.slice(retention.count);
      for (const entry of toDelete) {
        try {
          await this._depot.deleteFile(entry.rn);
        } catch {
          // ignore
        }
      }
    }
  }

  /**
   * Restore a backup payload from a depot file.
   * Next: call `play(...)` with another backup RN or close the pool.
   */
  async play(rn: RN | string, options?: BackupRestoreOptions): Promise<void> {
    if (!this._layers) {
      throw new Error("Backups require pool layers to be attached.");
    }
    if (!this._depot || !this._depotIdent) {
      throw new Error("Backups require a depot to be attached.");
    }
    const parsed = typeof rn === "string" ? RN.create(rn).unwrap() : rn;
    if (!parsed.isDepot() || parsed.pointsTo() !== "depotFile") {
      throw new Error(
        `Expected backup depot file RN but got "${parsed.value()}".`,
      );
    }
    if (parsed.depotIdent() !== this._depotIdent) {
      throw new Error(
        `Backup RN depot "${parsed.depotIdent() ?? "unknown"}" does not match "${this._depotIdent}".`,
      );
    }
    const file = await this._depot.getFile(parsed);
    const reader = new BackupEventReader(file.stream());
    const headerEvent = await reader.next();
    if (!headerEvent || headerEvent.t !== "header") {
      throw new Error("Backup stream is missing a header event.");
    }
    const layer = this._layers.get(headerEvent.layerIdent);
    if (!layer) {
      throw new Error(
        `Backup layer "${headerEvent.layerIdent}" is not present in the pool.`,
      );
    }
    if (layer.type !== headerEvent.layerType) {
      throw new Error(
        `Backup layer type "${headerEvent.layerType}" does not match "${layer.type}".`,
      );
    }
    const restorer = layer as {
      restore?: (
        payload: BackupRestorePayload,
        options?: BackupRestoreOptions,
      ) => Promise<void> | void;
    };
    if (typeof restorer.restore !== "function") {
      throw new Error(
        `Backup restore is not supported for layer "${headerEvent.layerIdent}".`,
      );
    }
    let release = () => {};
    if (this._locker) {
      const lockId = stableUuidFromSeed(
        `backup-play:${headerEvent.layerIdent}`,
      );
      const lockRn = RN.create("korm", "backupplay", lockId).unwrap();
      release = await this._locker.acquire(lockRn);
    }
    try {
      await restorer.restore({ header: headerEvent, reader }, options);
    } finally {
      release();
    }
  }

  /**
   * Begin an interval builder.
   * Next: call a precision method like `on(...)`, `onDate(...)`, `at(...)`, or `onSecond(...)`, or stop.
   */
  every<U extends IntervalUnit>(unit: U): IntervalBuilder<U, "start"> {
    return new IntervalBuilderImpl(unit) as unknown as IntervalBuilder<
      U,
      "start"
    >;
  }
}
