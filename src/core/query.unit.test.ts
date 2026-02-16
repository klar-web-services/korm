import { randomUUID } from "node:crypto";
import { describe, expect, test } from "bun:test";
import { Result } from "@fkws/klonk-result";
import { korm, type JSONable } from "../korm";
import { Item, UninitializedItem } from "./item";
import { QueryBuilder, type ResolvePaths, type _QueryComparison } from "./query";
import type { RN } from "./rn";

function makeQuery<T extends JSONable>(rows: T[]): QueryBuilder<T> {
  let layer: any;
  const pool: any = {
    ensureWalReady: async () => {},
    findLayerForRn: () => layer,
  };
  const items = rows.map(
    (data) =>
      new Item<T>(
        pool,
        data,
        korm.rn(`[rn]:tests:rows:${randomUUID()}`),
      ),
  );
  layer = {
    executeQuery: async () => new Result({ success: true, data: items }),
    getColumnKinds: async () => new Map<string, any>(),
  };
  return new QueryBuilder<T>(korm.rn("[rn]:tests:rows:*"), new UninitializedItem<T>(pool));
}

describe("query options API", () => {
  test("rejects duplicate option kinds", async () => {
    const query = makeQuery([{ score: 1 }, { score: 2 }]);
    const result = await query.get(korm.first(), korm.first(2));
    expect(result.isErr()).toBe(true);
  });

  test("validates first(n) input", async () => {
    const query = makeQuery([{ score: 1 }]);
    const badZero = await query.get(korm.first(0));
    expect(badZero.isErr()).toBe(true);
    const badNegative = await query.get(korm.first(-1));
    expect(badNegative.isErr()).toBe(true);
  });

  test("rejects wildcard sort paths", async () => {
    const query = makeQuery([{ refs: [{ name: "a" }] }]);
    const result = await query.get(korm.sortBy("refs[*].name"));
    expect(result.isErr()).toBe(true);
  });

  test("sorts with direction-based null placement", async () => {
    const query = makeQuery([
      { label: "two", score: 2 as number | null },
      { label: "null", score: null as number | null },
      { label: "one", score: 1 as number | null },
    ]);

    const asc = await query.get(korm.sortBy("score", "asc"));
    expect(asc.isOk()).toBe(true);
    expect(asc.unwrap().map((item) => item.data?.label)).toEqual([
      "one",
      "two",
      "null",
    ]);

    const desc = await query.get(korm.sortBy("score", "desc"));
    expect(desc.isOk()).toBe(true);
    expect(desc.unwrap().map((item) => item.data?.label)).toEqual([
      "null",
      "two",
      "one",
    ]);
  });

  test("errors on non-scalar sort keys unless allowStringify is enabled", async () => {
    const query = makeQuery([
      { label: "a", meta: { bucket: "b" } },
      { label: "b", meta: { bucket: "a" } },
    ]);

    const strict = await query.get(korm.sortBy("meta"));
    expect(strict.isErr()).toBe(true);

    const stringify = await query.get(
      korm.sortBy("meta", "asc", { allowStringify: true }),
    );
    expect(stringify.isOk()).toBe(true);
  });

  test("first() and first(n) return expected runtime shapes", async () => {
    const query = makeQuery([{ score: 2 }, { score: 1 }]);
    const first = await query.get(korm.sortBy("score", "asc"), korm.first());
    expect(first.isOk()).toBe(true);
    expect(first.unwrap().data?.score).toBe(1);

    const firstThree = await query.get(korm.sortBy("score", "asc"), korm.first(3));
    expect(firstThree.isOk()).toBe(true);
    expect(firstThree.unwrap().map((item) => item.data?.score)).toEqual([1, 2]);

    const emptyFirst = await makeQuery<{ score: number }>([]).get(korm.first());
    expect(emptyFirst.isErr()).toBe(true);
  });
});

type IsEqual<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false;
type IsAny<T> = 0 extends (1 & T) ? true : false;
type Assert<T extends true> = T;

type TypeModel = { owner: RN<{ name: string }>; score: number };
if (false) {
  const typeQuery = null as unknown as QueryBuilder<TypeModel>;
  const typeItem = null as unknown as UninitializedItem<TypeModel>;
  const typeRn = null as unknown as RN<TypeModel>;

  const typeSingle = typeQuery.get(korm.first());
  const typeMany = typeQuery.get(korm.first(2));
  const typeResolvedSingle = typeQuery.get(
    korm.resolve("owner.name"),
    korm.first(),
  );

  const _singleType: Assert<
    IsEqual<Awaited<typeof typeSingle>, Result<Item<TypeModel>>>
  > = true;
  const _manyType: Assert<
    IsEqual<Awaited<typeof typeMany>, Result<Item<TypeModel>[]>>
  > = true;
  const _resolvedSingleType: Assert<
    IsEqual<
      Awaited<typeof typeResolvedSingle>,
      Result<Item<ResolvePaths<TypeModel, ["owner.name"]>>>
    >
  > = true;
  const _queryValueNotAny: Assert<IsEqual<IsAny<_QueryComparison["value"]>, false>> =
    true;

  // @ts-expect-error query-only option is forbidden on from.rn
  typeItem.from.rn(typeRn, korm.first());
  // @ts-expect-error query-only option is forbidden on from.rn
  typeItem.from.rn(typeRn, korm.sortBy("score"));

  void _singleType;
  void _manyType;
  void _resolvedSingleType;
  void _queryValueNotAny;
}
