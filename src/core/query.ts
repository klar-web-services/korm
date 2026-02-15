import { Result } from "@fkws/klonk-result";
import { Item, UninitializedItem } from "./item";
import type { JSONable } from "../korm";
import { RN } from "./rn";
import type { LayerPool } from "../sources/layerPool";
import {
  cloneJson,
  deepEqualJson,
  setResolvedMeta,
  type PathKey,
  type ResolvedMeta,
} from "./resolveMeta";
import { isDepotFile } from "../depot/depotFile";
import { isUnsafeObjectKey } from "./safeObject";
import {
  cloneEncryptionMeta,
  getEncryptionMeta,
  materializeEncryptedSnapshot,
  setEncryptionMeta,
  type EncryptionMeta,
} from "./encryptionMeta";
import type { ColumnKind } from "./columnKind";

/**
 * Leaf comparison node used by query helpers like `eq`, `gt`, and `like`.
 */
export type _QueryComparison = {
  /** Node type marker. */
  type: "comparison";
  /** Comparison operator. */
  operator: "=" | "!=" | ">" | "<" | ">=" | "<=" | "LIKE" | "IN";
  /** JSON path to compare against. */
  property: string;
  /** Value to compare. */
  value: any;
};

/**
 * A query component tree used by `QueryBuilder.where(...)`.
 */
export type _QueryComponent =
  | {
      /** Node type marker. */
      type: "group";
      /** Grouping method. */
      method: "AND" | "OR" | "NOT";
      /** Child components. */
      components: _QueryComponent[];
    }
  | _QueryComparison;

/** Resolve RN paths into full objects before returning. */
export type ResolveGetOption<Paths extends readonly string[] = readonly string[]> = {
  type: "resolve";
  paths: Paths;
};

/** Keep only the first `n` rows after filtering/sorting. */
export type FirstGetOption<N extends number = number> = {
  type: "first";
  n: N;
};

/** Sorting direction for `sortBy` query options. */
export type SortDirection = "asc" | "desc";

/** Sort query results by a path in item data. */
export type SortByGetOption = {
  type: "sortBy";
  key: string;
  direction?: SortDirection;
  allowStringify?: boolean;
};

/** Fail when a resolved RN reference is missing. */
export type DisallowMissingReferencesGetOption = {
  type: "disallowMissingReferences";
};

/** All options supported by `QueryBuilder.get(...)`. */
export type QueryGetOption =
  | ResolveGetOption
  | FirstGetOption
  | SortByGetOption
  | DisallowMissingReferencesGetOption;

/** Options supported by `from.rn(...)`. */
export type RnGetOption =
  | ResolveGetOption
  | DisallowMissingReferencesGetOption;

type ResolvePathsFromOptions<Options extends readonly unknown[]> =
  Options extends readonly [infer Head, ...infer Tail]
    ? Head extends ResolveGetOption<infer Paths extends readonly string[]>
      ? Paths
      : ResolvePathsFromOptions<Tail>
    : [];

type FirstCountFromOptions<Options extends readonly unknown[]> =
  Options extends readonly [infer Head, ...infer Tail]
    ? Head extends FirstGetOption<infer N extends number>
      ? N
      : FirstCountFromOptions<Tail>
    : never;

/** Extract resolved paths from variadic get options. */
export type GetOptionResolvePaths<Options extends readonly unknown[]> =
  ResolvePathsFromOptions<Options>;

type QueryItemFromOptions<
  T extends JSONable,
  Options extends readonly unknown[],
> = Item<ResolvePaths<T, ResolvePathsFromOptions<Options>>>;

/** Resolved data shape returned by `QueryBuilder.get(...)` for a given option tuple. */
export type QueryGetResultData<
  T extends JSONable,
  Options extends readonly QueryGetOption[],
> = [FirstCountFromOptions<Options>] extends [never]
  ? QueryItemFromOptions<T, Options>[]
  : number extends FirstCountFromOptions<Options>
    ? QueryItemFromOptions<T, Options> | QueryItemFromOptions<T, Options>[]
    : FirstCountFromOptions<Options> extends 1
      ? QueryItemFromOptions<T, Options>
      : QueryItemFromOptions<T, Options>[];

type IsTuple<T extends readonly unknown[]> = number extends T["length"]
  ? false
  : true;

type Concat<A extends unknown[], B extends unknown[]> = [...A, ...B];

type SplitBracketed<
  S extends string,
  Acc extends string[] = [],
> = S extends `${infer Before}[${infer Index}]${infer After}`
  ? SplitBracketed<
      After,
      Concat<Concat<Acc, Before extends "" ? [] : [Before]>, [`[${Index}]`]>
    >
  : Concat<Acc, S extends "" ? [] : [S]>;

type SplitPath<
  P extends string,
  Acc extends string[] = [],
> = P extends `${infer Head}.${infer Tail}`
  ? SplitPath<Tail, Concat<Acc, SplitBracketed<Head>>>
  : Concat<Acc, SplitBracketed<P>>;

type UnwrapRN<T> = T extends RN<infer U> ? U : T;

/**
 * Type-level transformer that applies `resolvePaths` to a model type.
 */
export type ResolvePaths<T, Paths extends readonly string[]> =
  IsTuple<Paths> extends true ? ResolvePathsTuple<T, Paths> : T;

type ResolvePathsTuple<
  T,
  Paths extends readonly string[],
> = Paths extends readonly [
  infer Head extends string,
  ...infer Tail extends readonly string[],
]
  ? ResolvePathsTuple<ResolvePath<T, Head>, Tail>
  : T;

type ResolvePath<T, P extends string> = ResolveAt<T, SplitPath<P>>;

type ResolveAt<T, Segs extends readonly string[]> = Segs extends readonly []
  ? UnwrapRN<T>
  : T extends RN<infer U>
    ? ResolveAt<U, Segs>
    : Segs extends readonly [
          infer Head extends string,
          ...infer Tail extends readonly string[],
        ]
      ? ResolveSegment<T, Head, Tail>
      : T;

type ResolveSegment<
  T,
  Head extends string,
  Tail extends readonly string[],
> = Head extends "*"
  ? ResolveWildcard<T, Tail>
  : Head extends `[${infer Index}]`
    ? ResolveIndex<T, Index, Tail>
    : ResolveProp<T, Head, Tail>;

type ResolveProp<
  T,
  Key extends string,
  Tail extends readonly string[],
> = T extends readonly unknown[]
  ? T
  : T extends object
    ? { [P in keyof T]: P extends Key ? ResolveAt<T[P], Tail> : T[P] }
    : T;

type ResolveWildcard<
  T,
  Tail extends readonly string[],
> = T extends readonly unknown[]
  ? ResolveArrayAll<T, Tail>
  : T extends object
    ? { [P in keyof T]: ResolveAt<T[P], Tail> }
    : T;

type ResolveIndex<
  T,
  Index extends string,
  Tail extends readonly string[],
> = T extends readonly unknown[]
  ? Index extends "*"
    ? ResolveArrayAll<T, Tail>
    : Index extends `${infer N extends number}`
      ? ResolveArrayIndex<T, N, Tail>
      : T
  : T;

type ResolveArrayAll<
  T extends readonly unknown[],
  Tail extends readonly string[],
> =
  IsTuple<T> extends true
    ? { [I in keyof T]: ResolveAt<T[I], Tail> }
    : ResolveAt<T[number], Tail>[];

type ResolveArrayIndex<
  T extends readonly unknown[],
  Index extends number,
  Tail extends readonly string[],
> =
  IsTuple<T> extends true
    ? { [I in keyof T]: I extends `${Index}` ? ResolveAt<T[I], Tail> : T[I] }
    : ResolveAt<T[number], Tail>[];

/**
 * Builds and executes a query against a collection RN.
 * Use `where(...)` to add filters and `get(...)` to execute.
 */
export class QueryBuilder<T extends JSONable> {
  private _futureItem: UninitializedItem<T>;
  private _rootComponent?: _QueryComponent;
  private _rn: RN<T>;
  private _projection?: string[];

  constructor(rn: RN<T>, futureItem: UninitializedItem<T>) {
    this._futureItem = futureItem;
    this._rn = rn;
  }

  /**
   * Execute the query and return matching items.
   * Use `korm.resolve(...)` to materialize RN references in the returned data.
   */
  async get<const Options extends readonly QueryGetOption[] = []>(
    ...options: Options
  ): Promise<Result<QueryGetResultData<T, Options>>> {
    type OutputItem = Item<ResolvePaths<T, GetOptionResolvePaths<Options>>>;

    const normalizedResult = normalizeGetOptions(options, "query");
    if (normalizedResult.isErr()) {
      return new Result({
        success: false,
        error: normalizedResult.error,
      });
    }
    const normalized = normalizedResult.unwrap();
    const resolvePaths = normalized.resolvePaths;
    const allowMissing = normalized.allowMissing;

    const pool = this._futureItem.pool;
    await pool.ensureWalReady();
    const layer = pool.findLayerForRn(this.rn);
    const root = this._rootComponent;
    let columnKinds = new Map<string, ColumnKind>();
    if (root && this.rn.namespace && this.rn.kind) {
      try {
        columnKinds = await layer.getColumnKinds(
          this.rn.namespace,
          this.rn.kind,
        );
      } catch {
        columnKinds = new Map<string, ColumnKind>();
      }
    }

    const autoResolvePaths =
      resolvePaths.length === 0 && root
        ? collectResolvePathsFromQuery(root, columnKinds)
        : [];
    const shouldAutoResolve =
      resolvePaths.length === 0 && autoResolvePaths.length > 0;

    let queryForDb: QueryBuilder<T> = this;
    let needsInMemoryFilter = false;
    if (root) {
      const resolveInfo = parseResolvePaths(
        shouldAutoResolve ? autoResolvePaths : resolvePaths,
      );
      const dbResult = buildDbComponent(
        root,
        resolveInfo.firstSegments,
        columnKinds,
      );
      needsInMemoryFilter = dbResult.didFilter;
      if (dbResult.component) {
        queryForDb = new QueryBuilder<T>(this._rn, this._futureItem).where(
          dbResult.component,
        );
      } else {
        queryForDb = new QueryBuilder<T>(this._rn, this._futureItem);
      }
    }

    const baseResult = await layer.executeQuery<T>(queryForDb);

    if (baseResult.isErr()) {
      return baseResult as Result<QueryGetResultData<T, Options>>;
    }

    const baseItems = baseResult.unwrap();
    let output: OutputItem[];

    if (resolvePaths.length > 0) {
      const resolved = await resolveItems(baseItems, resolvePaths, pool, {
        allowMissing,
      });
      if (resolved.isErr()) {
        return resolved as Result<QueryGetResultData<T, Options>>;
      }
      if (!root || !needsInMemoryFilter) {
        output = resolved.unwrap() as OutputItem[];
      } else {
        const filtered = filterItemsByQuery(
          resolved.unwrap() as Item<ResolvePaths<T, GetOptionResolvePaths<Options>>>[],
          root,
        );
        output = filtered as OutputItem[];
      }
    } else if (!root || !needsInMemoryFilter) {
      output = baseItems as OutputItem[];
    } else if (shouldAutoResolve) {
      const clones = cloneItemsForFilter(baseItems);
      const resolved = await resolveItems(clones, autoResolvePaths, pool, {
        allowMissing: true,
        projection: "auto",
      });
      if (resolved.isErr()) {
        return resolved as Result<QueryGetResultData<T, Options>>;
      }
      const filtered = filterItemsByQuery(
        resolved.unwrap() as Item<ResolvePaths<T, GetOptionResolvePaths<Options>>>[],
        root,
      );
      const matched = new Set(
        filtered
          .map((item) => item.rn?.value())
          .filter((value): value is string => Boolean(value)),
      );
      output = baseItems.filter(
        (item) => item.rn && matched.has(item.rn.value()),
      ) as OutputItem[];
    } else {
      const filtered = filterItemsByQuery(
        baseItems as Item<ResolvePaths<T, GetOptionResolvePaths<Options>>>[],
        root,
      );
      output = filtered as OutputItem[];
    }

    if (normalized.sortBy) {
      const sortClones = cloneItemsForFilter(output as Item<JSONable>[]);
      const sortResolved = await resolveItems(
        sortClones,
        [normalized.sortBy.key] as const,
        pool,
        { allowMissing },
      );
      if (sortResolved.isErr()) {
        return sortResolved as Result<QueryGetResultData<T, Options>>;
      }
      const sorted = sortItemsByOption(
        output,
        sortResolved.unwrap() as Item<JSONable>[],
        normalized.sortBy,
      );
      if (sorted.isErr()) {
        return sorted as Result<QueryGetResultData<T, Options>>;
      }
      output = sorted.unwrap();
    }

    if (normalized.firstN === undefined) {
      return new Result({
        success: true,
        data: output as QueryGetResultData<T, Options>,
      });
    }

    if (normalized.firstN === 1) {
      if (output.length === 0) {
        return new Result({
          success: false,
          error: new Error("first(1) expected one item but query returned 0."),
        });
      }
      return new Result({
        success: true,
        data: output[0] as QueryGetResultData<T, Options>,
      });
    }

    return new Result({
      success: true,
      data: output.slice(0, normalized.firstN) as QueryGetResultData<T, Options>,
    });
  }

  /**
   * Set the root query component.
   * Next: call `.get(...)` to execute.
   */
  where(root: _QueryComponent): QueryBuilder<T> {
    this._rootComponent = root;
    return this;
  }

  /** @internal */
  withProjection(columns: string[]): QueryBuilder<T> {
    const unique = new Set<string>();
    for (const column of columns) {
      const trimmed = column.trim();
      if (!trimmed || trimmed === "rnId") continue;
      unique.add(trimmed);
    }
    this._projection = unique.size > 0 ? Array.from(unique) : undefined;
    return this;
  }

  /** Collection RN this query targets. */
  get rn(): RN<T> {
    return this._rn;
  }

  /** Root query component, if any. */
  get root(): _QueryComponent | undefined {
    return this._rootComponent;
  }

  /** Columns to select when projection is enabled. */
  get projection(): string[] | undefined {
    return this._projection;
  }

  /** The UninitializedItem that spawned this query builder. */
  get item(): UninitializedItem<T> {
    return this._futureItem;
  }
}

const tsIdentifierUnicodeRegex =
  /^[\p{ID_Start}_$][\p{ID_Continue}\u200C\u200D$_]*$/u;

function isValidTsIdentifier(name: string): boolean {
  if (name.length === 0) return false;
  let hasNonAscii = false;
  for (let i = 0; i < name.length; i++) {
    if (name.charCodeAt(i) > 127) {
      hasNonAscii = true;
      break;
    }
  }
  if (!hasNonAscii) {
    const first = name.charCodeAt(0);
    const isStart =
      first === 36 ||
      first === 95 ||
      (first >= 65 && first <= 90) ||
      (first >= 97 && first <= 122);
    if (!isStart) return false;
    for (let i = 1; i < name.length; i++) {
      const code = name.charCodeAt(i);
      const isContinue =
        code === 36 ||
        code === 95 ||
        (code >= 65 && code <= 90) ||
        (code >= 97 && code <= 122) ||
        (code >= 48 && code <= 57);
      if (!isContinue) return false;
    }
    return true;
  }
  return tsIdentifierUnicodeRegex.test(name);
}

type PathSegment =
  | { kind: "prop"; key: string }
  | { kind: "wildcard" }
  | { kind: "index"; index: number }
  | { kind: "indexWildcard" };

type NormalizedSortBy = {
  key: string;
  direction: SortDirection;
  allowStringify: boolean;
  parsedPath: PathSegment[];
};

type NormalizedGetOptions = {
  resolvePaths: string[];
  allowMissing: boolean;
  firstN?: number;
  sortBy?: NormalizedSortBy;
};

type Setter = (value: unknown) => void;

type Task = {
  value: unknown;
  set: Setter;
  remaining: PathSegment[];
  path: PathKey[];
  meta: ResolvedMeta;
};

type PendingRn = {
  rn: RN<JSONable>;
  targets: {
    set: Setter;
    remaining: PathSegment[];
    path: PathKey[];
    meta: ResolvedMeta;
  }[];
};

type ResolvedPayload = {
  data: JSONable;
  encryptionMeta?: EncryptionMeta;
};

type EncryptLike = { __ENCRYPT__?: true; reveal?: () => unknown };

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null;
}

function unwrapEncrypted(value: unknown): unknown {
  if (!value || typeof value !== "object") return value;
  const maybeEncrypt = value as EncryptLike;
  if (
    maybeEncrypt.__ENCRYPT__ === true &&
    typeof maybeEncrypt.reveal === "function"
  ) {
    return maybeEncrypt.reveal();
  }
  return value;
}

function parseResolvePath(path: string): PathSegment[] {
  const trimmed = path.trim();
  if (trimmed.length === 0) {
    throw new Error("Resolve path cannot be empty.");
  }
  if (trimmed.endsWith(".")) {
    throw new Error(`Invalid resolve path "${path}": trailing ".".`);
  }

  const segments: PathSegment[] = [];
  let buffer = "";

  const pushBuffer = () => {
    if (buffer.length === 0) return;
    if (buffer === "*") {
      segments.push({ kind: "wildcard" });
    } else if (isUnsafeObjectKey(buffer)) {
      throw new Error(`Invalid resolve path part: "${buffer}"`);
    } else if (!isValidTsIdentifier(buffer)) {
      throw new Error(`Invalid resolve path part: "${buffer}"`);
    } else {
      segments.push({ kind: "prop", key: buffer });
    }
    buffer = "";
  };

  let i = 0;
  while (i < trimmed.length) {
    const ch = trimmed[i]!;
    if (ch === ".") {
      if (buffer.length === 0 && trimmed[i - 1] !== "]") {
        throw new Error(`Invalid resolve path "${path}": empty segment.`);
      }
      pushBuffer();
      i += 1;
      continue;
    }

    if (ch === "[") {
      pushBuffer();
      const end = trimmed.indexOf("]", i + 1);
      if (end === -1) {
        throw new Error(`Invalid resolve path "${path}": missing closing "]".`);
      }
      const content = trimmed.slice(i + 1, end);
      if (content.length === 0) {
        throw new Error(`Invalid resolve path "${path}": empty index.`);
      }
      if (content === "*") {
        segments.push({ kind: "indexWildcard" });
      } else if (/^\d+$/.test(content)) {
        segments.push({ kind: "index", index: Number(content) });
      } else {
        throw new Error(
          `Invalid resolve path "${path}": invalid index "${content}".`,
        );
      }
      i = end + 1;
      if (i < trimmed.length && trimmed[i] !== "." && trimmed[i] !== "[") {
        throw new Error(
          `Invalid resolve path "${path}": missing "." before "${trimmed[i]}".`,
        );
      }
      continue;
    }

    buffer += ch;
    i += 1;
  }

  pushBuffer();

  if (segments.length === 0) {
    throw new Error(`Invalid resolve path "${path}": no segments.`);
  }

  return segments;
}

function parseSortPath(path: string): PathSegment[] {
  const segments = parseResolvePath(path);
  for (const segment of segments) {
    if (segment.kind === "wildcard" || segment.kind === "indexWildcard") {
      throw new Error(
        `Invalid sort path "${path}": wildcards are not allowed.`,
      );
    }
  }
  return segments;
}

/**
 * Normalize and validate variadic options passed to query reads.
 * @internal
 */
export function normalizeGetOptions(
  options: readonly unknown[],
  mode: "query" | "rn" = "query",
): Result<NormalizedGetOptions> {
  try {
    const seen = new Set<string>();
    let resolvePaths: string[] = [];
    let allowMissing = true;
    let firstN: number | undefined;
    let sortBy: NormalizedSortBy | undefined;

    for (const option of options) {
      if (!isRecord(option)) {
        throw new Error("Invalid get option: expected an object.");
      }
      const type = option.type;
      if (typeof type !== "string" || type.length === 0) {
        throw new Error("Invalid get option: missing \"type\".");
      }
      if (seen.has(type)) {
        throw new Error(`Duplicate get option "${type}" is not allowed.`);
      }
      seen.add(type);

      if (type === "resolve") {
        const rawPaths = (option as { paths?: unknown }).paths;
        const input = Array.isArray(rawPaths)
          ? rawPaths
          : typeof rawPaths === "string"
            ? [rawPaths]
            : null;
        if (!input) {
          throw new Error(
            'Invalid "resolve" option: expected "paths" to be a string or string array.',
          );
        }
        const nextPaths: string[] = [];
        for (const entry of input) {
          if (typeof entry !== "string") {
            throw new Error(
              'Invalid "resolve" option: every path must be a string.',
            );
          }
          const trimmed = entry.trim();
          if (trimmed.length === 0) {
            throw new Error(
              'Invalid "resolve" option: paths cannot include empty strings.',
            );
          }
          nextPaths.push(trimmed);
        }
        resolvePaths = nextPaths;
        continue;
      }

      if (type === "disallowMissingReferences") {
        allowMissing = false;
        continue;
      }

      if (type === "first") {
        if (mode === "rn") {
          throw new Error(
            'Get option "first" is not supported for from.rn(...).',
          );
        }
        const n = (option as { n?: unknown }).n;
        if (
          typeof n !== "number" ||
          !Number.isFinite(n) ||
          !Number.isInteger(n) ||
          n <= 0
        ) {
          throw new Error(
            'Invalid "first" option: "n" must be a positive integer.',
          );
        }
        firstN = n;
        continue;
      }

      if (type === "sortBy") {
        if (mode === "rn") {
          throw new Error(
            'Get option "sortBy" is not supported for from.rn(...).',
          );
        }
        const key = (option as { key?: unknown }).key;
        if (typeof key !== "string" || key.trim().length === 0) {
          throw new Error('Invalid "sortBy" option: "key" must be a string.');
        }
        const directionRaw = (option as { direction?: unknown }).direction;
        const direction: SortDirection =
          directionRaw === undefined ? "asc" : (directionRaw as SortDirection);
        if (direction !== "asc" && direction !== "desc") {
          throw new Error(
            'Invalid "sortBy" option: "direction" must be "asc" or "desc".',
          );
        }
        const allowStringifyRaw = (option as { allowStringify?: unknown })
          .allowStringify;
        if (
          allowStringifyRaw !== undefined &&
          typeof allowStringifyRaw !== "boolean"
        ) {
          throw new Error(
            'Invalid "sortBy" option: "allowStringify" must be a boolean.',
          );
        }
        const keyTrimmed = key.trim();
        sortBy = {
          key: keyTrimmed,
          direction,
          allowStringify: allowStringifyRaw === true,
          parsedPath: parseSortPath(keyTrimmed),
        };
        continue;
      }

      throw new Error(`Unknown get option "${type}".`);
    }

    return new Result({
      success: true,
      data: {
        resolvePaths,
        allowMissing,
        firstN,
        sortBy,
      },
    });
  } catch (error) {
    return new Result({
      success: false,
      error: error instanceof Error ? error : new Error(String(error)),
    });
  }
}

function parseQueryPath(path: string): PathSegment[] {
  const trimmed = path.trim();
  if (trimmed.length === 0) {
    throw new Error("Query path cannot be empty.");
  }
  if (trimmed.endsWith(".")) {
    throw new Error(`Invalid query path "${path}": trailing ".".`);
  }

  const segments: PathSegment[] = [];
  let buffer = "";

  const pushBuffer = () => {
    if (buffer.length === 0) return;
    if (buffer === "*") {
      segments.push({ kind: "wildcard" });
    } else {
      segments.push({ kind: "prop", key: buffer });
    }
    buffer = "";
  };

  let i = 0;
  while (i < trimmed.length) {
    const ch = trimmed[i]!;
    if (ch === ".") {
      if (buffer.length === 0 && trimmed[i - 1] !== "]") {
        throw new Error(`Invalid query path "${path}": empty segment.`);
      }
      pushBuffer();
      i += 1;
      continue;
    }

    if (ch === "[") {
      pushBuffer();
      const end = trimmed.indexOf("]", i + 1);
      if (end === -1) {
        throw new Error(`Invalid query path "${path}": missing closing "]".`);
      }
      const content = trimmed.slice(i + 1, end);
      if (content.length === 0) {
        throw new Error(`Invalid query path "${path}": empty index.`);
      }
      if (content === "*") {
        segments.push({ kind: "indexWildcard" });
      } else if (/^\d+$/.test(content)) {
        segments.push({ kind: "index", index: Number(content) });
      } else {
        throw new Error(
          `Invalid query path "${path}": invalid index "${content}".`,
        );
      }
      i = end + 1;
      if (i < trimmed.length && trimmed[i] !== "." && trimmed[i] !== "[") {
        throw new Error(
          `Invalid query path "${path}": missing "." before "${trimmed[i]}".`,
        );
      }
      continue;
    }

    buffer += ch;
    i += 1;
  }

  pushBuffer();

  if (segments.length === 0) {
    throw new Error(`Invalid query path "${path}": no segments.`);
  }

  return segments;
}

function parseResolvePaths(paths: readonly string[]): {
  parsed: PathSegment[][];
  firstSegments: PathSegment[];
} {
  const parsed: PathSegment[][] = [];
  const firstSegments: PathSegment[] = [];
  const seen = new Set<string>();

  for (const rawPath of paths) {
    const trimmed = rawPath.trim();
    if (trimmed.length === 0) continue;
    if (seen.has(trimmed)) continue;
    const segments = parseResolvePath(trimmed);
    parsed.push(segments);
    if (segments.length > 0) {
      firstSegments.push(segments[0]!);
    }
    seen.add(trimmed);
  }

  return { parsed, firstSegments };
}

function collectResolvePathsFromQuery(
  root: _QueryComponent,
  columnKinds: Map<string, ColumnKind>,
): string[] {
  const paths: string[] = [];
  const seen = new Set<string>();

  const visit = (node: _QueryComponent) => {
    if (node.type === "comparison") {
      const prop = node.property.trim();
      if (!prop) return;
      let segments: PathSegment[];
      try {
        segments = parseResolvePath(prop);
      } catch {
        return;
      }
      if (segments.length <= 1) return;
      const first = segments[0];
      if (!first || first.kind !== "prop") return;
      const baseKind = columnKinds.get(first.key);
      if (baseKind === "json" || baseKind === "encrypted") return;
      if (seen.has(prop)) return;
      seen.add(prop);
      paths.push(prop);
      return;
    }
    for (const child of node.components) {
      visit(child);
    }
  };

  visit(root);
  return paths;
}

function expandSegment(
  value: unknown,
  seg: PathSegment,
): { value: unknown; set: Setter; pathKey: PathKey }[] {
  if (seg.kind === "prop") {
    if (isUnsafeObjectKey(seg.key)) return [];
    if (!isRecord(value)) return [];
    return [
      {
        value: value[seg.key],
        set: (next) => {
          value[seg.key] = next;
        },
        pathKey: seg.key,
      },
    ];
  }

  if (seg.kind === "index") {
    if (!Array.isArray(value)) return [];
    const arr = value as unknown[];
    return [
      {
        value: arr[seg.index],
        set: (next) => {
          arr[seg.index] = next;
        },
        pathKey: seg.index,
      },
    ];
  }

  if (seg.kind === "indexWildcard") {
    if (!Array.isArray(value)) return [];
    const arr = value as unknown[];
    return arr.map((entry, idx) => ({
      value: entry,
      set: (next) => {
        arr[idx] = next;
      },
      pathKey: idx,
    }));
  }

  if (Array.isArray(value)) {
    const arr = value as unknown[];
    return arr.map((entry, idx) => ({
      value: entry,
      set: (next) => {
        arr[idx] = next;
      },
      pathKey: idx,
    }));
  }

  if (!isRecord(value)) return [];

  return Object.keys(value)
    .filter((key) => !isUnsafeObjectKey(key))
    .map((key) => ({
      value: value[key],
      set: (next) => {
        value[key] = next;
      },
      pathKey: key,
    }));
}

function cloneItemsForFilter<T extends JSONable>(items: Item<T>[]): Item<T>[] {
  return items.map((item) => {
    const clone = new Item<T>(item.pool, item.data as T, item.rn);
    const encryptionMeta = getEncryptionMeta(item);
    if (encryptionMeta) {
      setEncryptionMeta(clone, cloneEncryptionMeta(encryptionMeta));
    }
    return clone;
  });
}

function isDbSafeComparison(
  comparison: _QueryComparison,
  resolveFirstSegments: PathSegment[],
  columnKinds: Map<string, ColumnKind>,
): boolean {
  const prop = comparison.property;
  if (prop.includes("[") || prop.includes("*")) return false;
  const hasDot = prop.includes(".");
  const base = hasDot ? prop.split(".")[0]! : prop;
  const baseKind = columnKinds.get(base);
  if (!hasDot) {
    return baseKind !== "encrypted";
  }

  let parsed: PathSegment[];
  try {
    parsed = parseQueryPath(prop);
  } catch {
    return false;
  }
  const first = parsed[0];
  if (!first || first.kind !== "prop") return false;

  for (const resolveFirst of resolveFirstSegments) {
    if (segmentsMatch(resolveFirst, first)) {
      return false;
    }
  }
  if (baseKind === "json") return true;
  if (baseKind === "encrypted" || baseKind === "rn") return false;
  if (!baseKind) return false;
  return false;
}

function segmentsMatch(resolveSeg: PathSegment, propSeg: PathSegment): boolean {
  if (resolveSeg.kind === "wildcard" || resolveSeg.kind === "indexWildcard")
    return true;
  if (propSeg.kind === "wildcard" || propSeg.kind === "indexWildcard")
    return true;
  if (resolveSeg.kind === "prop" && propSeg.kind === "prop") {
    return resolveSeg.key === propSeg.key;
  }
  if (resolveSeg.kind === "index" && propSeg.kind === "index") {
    return resolveSeg.index === propSeg.index;
  }
  return false;
}

type BuildResult = { component: _QueryComponent | null; didFilter: boolean };

function buildDbComponent(
  node: _QueryComponent,
  resolveFirstSegments: PathSegment[],
  columnKinds: Map<string, ColumnKind>,
): BuildResult {
  if (node.type === "comparison") {
    const safe = isDbSafeComparison(node, resolveFirstSegments, columnKinds);
    return { component: safe ? node : null, didFilter: !safe };
  }

  if (node.method === "NOT") {
    const child = node.components[0];
    if (!child) return { component: null, didFilter: true };
    const childResult = buildDbComponent(
      child,
      resolveFirstSegments,
      columnKinds,
    );
    if (!childResult.component) return { component: null, didFilter: true };
    return {
      component: {
        type: "group",
        method: "NOT",
        components: [childResult.component],
      },
      didFilter: childResult.didFilter,
    };
  }

  const childResults = node.components.map((child) =>
    buildDbComponent(child, resolveFirstSegments, columnKinds),
  );
  const children = childResults
    .map((result) => result.component)
    .filter((child): child is _QueryComponent => Boolean(child));
  const didFilter =
    childResults.some((result) => result.didFilter) ||
    children.length !== node.components.length;

  if (children.length === 0) return { component: null, didFilter: true };

  if (node.method === "OR" && children.length !== node.components.length) {
    return { component: null, didFilter: true };
  }

  return {
    component: { type: "group", method: node.method, components: children },
    didFilter,
  };
}

function coerceRn(
  value: unknown,
  cache: Map<string, RN<JSONable>>,
): RN<JSONable> | undefined {
  if (isRecord(value) && "__RN__" in value) {
    return value as unknown as RN<JSONable>;
  }

  if (typeof value === "string" && value.startsWith("[rn]")) {
    const cached = cache.get(value);
    if (cached) return cached;

    const parsed = RN.create(value);
    if (parsed.isErr()) return undefined;
    const rn = parsed.unwrap() as RN<JSONable>;
    cache.set(value, rn);
    return rn;
  }

  return undefined;
}

function filterItemsByQuery<T extends JSONable>(
  items: Item<T>[],
  root: _QueryComponent,
): Item<T>[] {
  const pathCache = new Map<string, PathSegment[]>();
  const likeCache = new Map<string, RegExp>();

  return items.filter((item) => {
    const data = item.data as JSONable | undefined;
    if (data === undefined) return false;
    return evaluateComponent(root, data, pathCache, likeCache);
  });
}

function evaluateComponent(
  node: _QueryComponent,
  data: JSONable,
  pathCache: Map<string, PathSegment[]>,
  likeCache: Map<string, RegExp>,
): boolean {
  if (node.type === "comparison") {
    return evaluateComparison(node, data, pathCache, likeCache);
  }

  if (node.method === "NOT") {
    const child = node.components[0];
    if (!child) return true;
    return !evaluateComponent(child, data, pathCache, likeCache);
  }

  if (node.method === "AND") {
    return node.components.every((child) =>
      evaluateComponent(child, data, pathCache, likeCache),
    );
  }

  return node.components.some((child) =>
    evaluateComponent(child, data, pathCache, likeCache),
  );
}

function evaluateComparison(
  comparison: _QueryComparison,
  data: JSONable,
  pathCache: Map<string, PathSegment[]>,
  likeCache: Map<string, RegExp>,
): boolean {
  let segments = pathCache.get(comparison.property);
  if (!segments) {
    try {
      segments = parseQueryPath(comparison.property);
    } catch {
      return false;
    }
    pathCache.set(comparison.property, segments);
  }

  const values = collectValuesAtPath(data, segments);
  const candidates = values.length === 0 ? [undefined] : values;
  const target = normalizeComparable(comparison.value);

  if (comparison.operator === "!=") {
    return !candidates.some((value) =>
      compareValues(value, target, "=", likeCache),
    );
  }

  return candidates.some((value) =>
    compareValues(value, target, comparison.operator, likeCache),
  );
}

function collectValuesAtPath(
  value: unknown,
  segments: PathSegment[],
): unknown[] {
  let current: unknown[] = [value];
  for (const seg of segments) {
    const next: unknown[] = [];
    for (const entry of current) {
      const expanded = expandSegment(unwrapEncrypted(entry), seg);
      for (const child of expanded) {
        next.push(child.value);
      }
    }
    current = next;
    if (current.length === 0) break;
  }
  return current;
}

function normalizeComparable(value: unknown): unknown {
  const unwrapped = unwrapEncrypted(value);
  if (unwrapped !== value) return unwrapped;
  if (!value || typeof value !== "object") return value;
  if (isDepotFile(value)) {
    return value.rn.value();
  }
  if (
    "__RN__" in (value as Record<string, unknown>) &&
    typeof (value as { value?: () => string }).value === "function"
  ) {
    return (value as { value: () => string }).value();
  }
  return value;
}

type SortComparable =
  | { missing: true; value?: undefined }
  | { missing: false; value: string | number | boolean };

function normalizeSortComparable(
  value: unknown,
  sortBy: NormalizedSortBy,
): Result<SortComparable> {
  const normalized = normalizeComparable(value);
  if (normalized === null || normalized === undefined) {
    return new Result({ success: true, data: { missing: true } });
  }
  if (
    typeof normalized === "string" ||
    typeof normalized === "number" ||
    typeof normalized === "boolean"
  ) {
    return new Result({ success: true, data: { missing: false, value: normalized } });
  }
  if (sortBy.allowStringify) {
    let stringValue: string;
    try {
      const encoded = JSON.stringify(normalized);
      stringValue = encoded === undefined ? String(normalized) : encoded;
    } catch {
      stringValue = String(normalized);
    }
    return new Result({ success: true, data: { missing: false, value: stringValue } });
  }
  return new Result({
    success: false,
    error: new Error(
      `Sort path "${sortBy.key}" resolved to a non-scalar value. Use allowStringify to permit this.`,
    ),
  });
}

function readSortComparable(
  data: JSONable | undefined,
  sortBy: NormalizedSortBy,
): Result<SortComparable> {
  if (data === undefined) {
    return new Result({ success: true, data: { missing: true } });
  }
  const values = collectValuesAtPath(data, sortBy.parsedPath);
  if (values.length === 0) {
    return new Result({ success: true, data: { missing: true } });
  }
  return normalizeSortComparable(values[0], sortBy);
}

function compareSortPrimitives(
  left: string | number | boolean,
  right: string | number | boolean,
): number {
  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }
  if (typeof left === "boolean" && typeof right === "boolean") {
    return Number(left) - Number(right);
  }
  if (typeof left === "string" && typeof right === "string") {
    return left.localeCompare(right);
  }
  return String(left).localeCompare(String(right));
}

function compareSortValues(
  left: SortComparable,
  right: SortComparable,
  direction: SortDirection,
): number {
  if (left.missing && right.missing) return 0;
  if (left.missing) return direction === "asc" ? 1 : -1;
  if (right.missing) return direction === "asc" ? -1 : 1;

  const compared = compareSortPrimitives(left.value, right.value);
  return direction === "asc" ? compared : -compared;
}

function sortItemsByOption<T extends JSONable>(
  baseItems: Item<T>[],
  resolvedItems: Item<JSONable>[],
  sortBy: NormalizedSortBy,
): Result<Item<T>[]> {
  try {
    if (baseItems.length !== resolvedItems.length) {
      throw new Error(
        `Sort preparation mismatch: expected ${baseItems.length} items but got ${resolvedItems.length}.`,
      );
    }

    const decorated: {
      item: Item<T>;
      index: number;
      comparable: SortComparable;
    }[] = [];

    for (let index = 0; index < baseItems.length; index += 1) {
      const item = baseItems[index]!;
      const resolved = resolvedItems[index];
      const comparableResult = readSortComparable(
        resolved?.data as JSONable | undefined,
        sortBy,
      );
      if (comparableResult.isErr()) {
        return new Result({
          success: false,
          error: comparableResult.error,
        });
      }
      decorated.push({
        item,
        index,
        comparable: comparableResult.unwrap(),
      });
    }

    decorated.sort((a, b) => {
      const cmp = compareSortValues(a.comparable, b.comparable, sortBy.direction);
      if (cmp !== 0) return cmp;
      return a.index - b.index;
    });

    return new Result({
      success: true,
      data: decorated.map((entry) => entry.item),
    });
  } catch (error) {
    return new Result({
      success: false,
      error: error instanceof Error ? error : new Error(String(error)),
    });
  }
}

function compareValues(
  rawValue: unknown,
  target: unknown,
  operator: _QueryComparison["operator"],
  likeCache: Map<string, RegExp>,
): boolean {
  const value = normalizeComparable(rawValue);

  if (target === null || target === undefined) {
    if (operator === "=") return value === null || value === undefined;
    if (operator === "!=") return !(value === null || value === undefined);
    return false;
  }

  if (operator === "IN") {
    if (!Array.isArray(target)) return false;
    for (const entry of target) {
      if (compareValues(value, normalizeComparable(entry), "=", likeCache)) {
        return true;
      }
    }
    return false;
  }

  if (operator === "=") {
    return deepEqualJson(value as JSONable, target as JSONable);
  }

  if (operator === "LIKE") {
    if (typeof value !== "string" || typeof target !== "string") return false;
    let regex = likeCache.get(target);
    if (!regex) {
      regex = new RegExp(`^${escapeLikePattern(target)}$`);
      likeCache.set(target, regex);
    }
    return regex.test(value);
  }

  if (operator === ">") {
    return (value as any) > (target as any);
  }
  if (operator === "<") {
    return (value as any) < (target as any);
  }
  if (operator === ">=") {
    return (value as any) >= (target as any);
  }
  if (operator === "<=") {
    return (value as any) <= (target as any);
  }

  return false;
}

function escapeLikePattern(pattern: string): string {
  const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return escaped.replace(/%/g, ".*").replace(/_/g, ".");
}

function modsKey(mods: Readonly<Map<string, string>>): string {
  let out = "";
  for (const [key, value] of mods.entries()) {
    out += `[${key}::${value}]`;
  }
  return out;
}

function chunkArray<T>(values: T[], size: number): T[][] {
  if (size <= 0) return [values];
  const out: T[][] = [];
  for (let i = 0; i < values.length; i += size) {
    out.push(values.slice(i, i + size));
  }
  return out;
}

async function resolveItems<
  T extends JSONable,
  Paths extends readonly string[],
>(
  items: Item<T>[],
  paths: Paths,
  pool: LayerPool,
  opts: { allowMissing?: boolean; projection?: "auto" } = {},
): Promise<Result<Item<ResolvePaths<T, Paths>>[]>> {
  try {
    const parsedPaths: PathSegment[][] = [];
    const seen = new Set<string>();
    const allowMissing = opts.allowMissing ?? true;
    const projectionMode = opts.projection;

    for (const rawPath of paths) {
      const trimmed = rawPath.trim();
      if (trimmed.length === 0) {
        throw new Error("Resolve path cannot be empty.");
      }
      if (seen.has(trimmed)) continue;
      parsedPaths.push(parseResolvePath(trimmed));
      seen.add(trimmed);
    }

    if (parsedPaths.length === 0 || items.length === 0) {
      return new Result({
        success: true,
        data: items as Item<ResolvePaths<T, Paths>>[],
      });
    }

    const refPathCache = new WeakMap<ResolvedMeta, Set<string>>();
    const addRefPath = (meta: ResolvedMeta, path: PathKey[]) => {
      const key = JSON.stringify(path);
      let set = refPathCache.get(meta);
      if (!set) {
        set = new Set<string>();
        refPathCache.set(meta, set);
      }
      if (set.has(key)) return;
      set.add(key);
      meta.refs.push({ path: [...path] });
    };

    const noop: Setter = () => {};
    let pending: Task[] = [];

    for (const item of items) {
      if (item.data === undefined) continue;
      const resolvedData = cloneJson(item.data as JSONable);
      const encryptionMeta = getEncryptionMeta(item);
      const rawData = encryptionMeta
        ? materializeEncryptedSnapshot(resolvedData, encryptionMeta)
        : cloneJson(resolvedData);
      const meta: ResolvedMeta = { rawData, resolvedData, refs: [] };
      setResolvedMeta(item, meta);
      for (const segments of parsedPaths) {
        pending.push({
          value: resolvedData,
          set: noop,
          remaining: segments,
          path: [],
          meta,
        });
      }
    }

    const rnParseCache = new Map<string, RN<JSONable>>();
    const resolvedCache = new Map<string, ResolvedPayload>();

    const createResolvedNode = (
      rn: RN<JSONable>,
      rawData: JSONable,
      encryptionMeta?: EncryptionMeta,
    ): { value: JSONable; meta: ResolvedMeta } => {
      const resolvedClone = cloneJson(rawData);
      const rawClone = encryptionMeta
        ? materializeEncryptedSnapshot(resolvedClone, encryptionMeta)
        : cloneJson(resolvedClone);
      const meta: ResolvedMeta = {
        rn,
        rawData: rawClone,
        resolvedData: resolvedClone,
        refs: [],
      };
      if (resolvedClone !== null && typeof resolvedClone === "object") {
        setResolvedMeta(resolvedClone as object, meta);
        if (encryptionMeta) {
          setEncryptionMeta(
            resolvedClone as object,
            cloneEncryptionMeta(encryptionMeta),
          );
        }
      }
      return { value: resolvedClone, meta };
    };

    while (pending.length > 0) {
      const next: Task[] = [];
      const pendingRns = new Map<string, PendingRn>();

      const enqueue = (
        rn: RN<JSONable>,
        set: Setter,
        remaining: PathSegment[],
        path: PathKey[],
        meta: ResolvedMeta,
      ) => {
        const key = rn.value();
        const existing = pendingRns.get(key);
        if (existing) {
          existing.targets.push({ set, remaining, path, meta });
        } else {
          pendingRns.set(key, {
            rn,
            targets: [{ set, remaining, path, meta }],
          });
        }
      };

      for (const task of pending) {
        if (task.remaining.length === 0) {
          const rn = coerceRn(task.value, rnParseCache);
          if (rn) {
            const cached = resolvedCache.get(rn.value());
            if (cached !== undefined) {
              const { value: resolvedValue } = createResolvedNode(
                rn,
                cached.data,
                cached.encryptionMeta,
              );
              task.set(resolvedValue);
              addRefPath(task.meta, task.path);
            } else {
              enqueue(rn, task.set, [], task.path, task.meta);
            }
          }
          continue;
        }

        const seg = task.remaining[0]!;
        const tail = task.remaining.slice(1);
        const children = expandSegment(task.value, seg);
        for (const child of children) {
          const childPath = [...task.path, child.pathKey];
          const rn = coerceRn(child.value, rnParseCache);
          if (rn) {
            const cached = resolvedCache.get(rn.value());
            if (cached !== undefined) {
              const { value: resolvedValue, meta: childMeta } =
                createResolvedNode(rn, cached.data, cached.encryptionMeta);
              child.set(resolvedValue);
              addRefPath(task.meta, childPath);
              if (
                !rn.isDepot() &&
                tail.length > 0 &&
                typeof resolvedValue === "object" &&
                resolvedValue !== null
              ) {
                next.push({
                  value: resolvedValue,
                  set: noop,
                  remaining: tail,
                  path: [],
                  meta: childMeta,
                });
              }
            } else {
              enqueue(rn, child.set, tail, childPath, task.meta);
            }
          } else if (tail.length > 0) {
            next.push({
              value: child.value,
              set: child.set,
              remaining: tail,
              path: childPath,
              meta: task.meta,
            });
          }
        }
      }

      if (pendingRns.size > 0) {
        const resolvedResult = await fetchResolvedData(pool, pendingRns, {
          projectionMode,
        });
        if (resolvedResult.isErr()) {
          return new Result({ success: false, error: resolvedResult.error });
        }

        const resolved = resolvedResult.unwrap();
        for (const pendingEntry of pendingRns.values()) {
          const key = pendingEntry.rn.value();
          const data = resolved.get(key);
          if (!data) {
            if (allowMissing) {
              continue;
            }
            return new Result({
              success: false,
              error: new Error(`Failed to resolve RN "${key}".`),
            });
          }
          resolvedCache.set(key, data);
          for (const target of pendingEntry.targets) {
            const { value: resolvedValue, meta: childMeta } =
              createResolvedNode(
                pendingEntry.rn,
                data.data,
                data.encryptionMeta,
              );
            target.set(resolvedValue);
            addRefPath(target.meta, target.path);
            if (
              !pendingEntry.rn.isDepot() &&
              target.remaining.length > 0 &&
              typeof resolvedValue === "object" &&
              resolvedValue !== null
            ) {
              next.push({
                value: resolvedValue,
                set: noop,
                remaining: target.remaining,
                path: [],
                meta: childMeta,
              });
            }
          }
        }
      }

      pending = next;
    }

    return new Result({
      success: true,
      data: items as Item<ResolvePaths<T, Paths>>[],
    });
  } catch (error) {
    return new Result({
      success: false,
      error: error instanceof Error ? error : new Error(String(error)),
    });
  }
}

async function fetchResolvedData(
  pool: LayerPool,
  pendingRns: Map<string, PendingRn>,
  opts: { projectionMode?: "auto" } = {},
): Promise<Result<Map<string, ResolvedPayload>>> {
  const groups = new Map<
    string,
    {
      layerId: string;
      layer: ReturnType<LayerPool["findLayerForRn"]>;
      namespace: string;
      kind: string;
      mods: Map<string, string>;
      ids: Set<string>;
      projection: Set<string> | null;
    }
  >();

  try {
    const projectionMode = opts.projectionMode;
    const resolved = new Map<string, ResolvedPayload>();
    const depotTasks: Promise<void>[] = [];

    const projectionFromTargets = (
      targets: PendingRn["targets"],
    ): { full: boolean; columns: Set<string> } => {
      const columns = new Set<string>();
      for (const target of targets) {
        if (target.remaining.length === 0) {
          return { full: true, columns };
        }
        const first = target.remaining[0];
        if (!first || first.kind !== "prop") {
          return { full: true, columns };
        }
        columns.add(first.key);
      }
      return { full: false, columns };
    };

    for (const entry of pendingRns.values()) {
      const rn = entry.rn;
      if (rn.isDepot()) {
        const key = rn.value();
        const depot = pool.findDepotForRn(rn);
        if (rn.pointsTo() === "depotFile") {
          depotTasks.push(
            depot.getFile(rn).then((file) => {
              resolved.set(key, { data: file as unknown as JSONable });
            }),
          );
          continue;
        }
        if (rn.pointsTo() === "depotPrefix") {
          depotTasks.push(
            depot.listFiles(rn).then((files) => {
              resolved.set(key, { data: files as unknown as JSONable });
            }),
          );
          continue;
        }
        throw new Error(`Unsupported depot RN "${rn.value()}".`);
      }
      if (!rn.namespace || !rn.kind || !rn.id) {
        throw new Error(`Invalid RN "${rn.value()}".`);
      }
      if (rn.pointsTo() !== "item") {
        throw new Error(`Cannot resolve collection RN "${rn.value()}".`);
      }

      const layer = pool.findLayerForRn(rn);
      const layerId = layer.identifier;
      const mods = new Map(rn.mods ?? []);
      const key = `${layerId}|${rn.namespace}|${rn.kind}|${modsKey(mods)}`;
      const projectionInfo =
        projectionMode === "auto"
          ? projectionFromTargets(entry.targets)
          : { full: true, columns: new Set<string>() };

      const existing = groups.get(key);
      if (existing) {
        existing.ids.add(rn.id);
        if (projectionMode === "auto") {
          if (existing.projection !== null) {
            if (projectionInfo.full) {
              existing.projection = null;
            } else {
              for (const column of projectionInfo.columns) {
                existing.projection.add(column);
              }
            }
          }
        }
      } else {
        groups.set(key, {
          layerId,
          layer,
          namespace: rn.namespace,
          kind: rn.kind,
          mods,
          ids: new Set([rn.id]),
          projection:
            projectionMode === "auto" && !projectionInfo.full
              ? projectionInfo.columns
              : null,
        });
      }
    }

    if (depotTasks.length > 0) {
      await Promise.all(depotTasks);
    }

    for (const group of groups.values()) {
      const ids = Array.from(group.ids);
      if (ids.length === 0) continue;

      const rnRes = RN.create(
        group.namespace,
        group.kind,
        "*",
        new Map(group.mods),
      );
      if (rnRes.isErr()) {
        return new Result({ success: false, error: rnRes.error });
      }
      const collectionRn = rnRes.unwrap();

      const chunks = chunkArray(ids, 500);
      let projection =
        group.projection && group.projection.size > 0
          ? Array.from(group.projection)
          : undefined;
      if (projectionMode === "auto" && projection) {
        try {
          const columnKinds = await group.layer.getColumnKinds(
            group.namespace,
            group.kind,
          );
          projection = projection.filter((column) => columnKinds.has(column));
          if (projection.length === 0) {
            projection = undefined;
          }
        } catch {
          projection = undefined;
        }
      }
      for (const chunk of chunks) {
        if (chunk.length === 0) continue;
        const query = new QueryBuilder<JSONable>(
          collectionRn,
          new UninitializedItem<JSONable>(pool),
        );
        if (projection) {
          query.withProjection(projection);
        }
        const root: _QueryComponent = {
          type: "comparison",
          operator: "IN",
          property: "rnId",
          value: chunk,
        };
        query.where(root);
        const result = await group.layer.executeQuery<JSONable>(query);
        if (result.isErr()) {
          return new Result({ success: false, error: result.error });
        }

        for (const item of result.unwrap()) {
          const itemRn = item.rn;
          if (!itemRn) continue;
          const key = itemRn.value();
          if (item.data !== undefined) {
            const encryptionMeta = getEncryptionMeta(item);
            resolved.set(key, {
              data: item.data as JSONable,
              encryptionMeta: encryptionMeta
                ? cloneEncryptionMeta(encryptionMeta)
                : undefined,
            });
          }
        }
      }
    }

    return new Result({ success: true, data: resolved });
  } catch (error) {
    return new Result({
      success: false,
      error: error instanceof Error ? error : new Error(String(error)),
    });
  }
}
