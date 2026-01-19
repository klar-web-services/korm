import { describe, expect, test } from "bun:test";
import { and, eq, gt, gte, inList, like, lt, lte, not, or } from "./queryFns";

describe("queryFns", () => {
    test("builds comparison nodes", () => {
        expect(eq("meta.count", 1)).toEqual({
            type: "comparison",
            operator: "=",
            property: "meta.count",
            value: 1,
        });
        expect(gt("score", 10)).toEqual({
            type: "comparison",
            operator: ">",
            property: "score",
            value: 10,
        });
        expect(gte("score", 10)).toEqual({
            type: "comparison",
            operator: ">=",
            property: "score",
            value: 10,
        });
        expect(lt("score", 5)).toEqual({
            type: "comparison",
            operator: "<",
            property: "score",
            value: 5,
        });
        expect(lte("score", 5)).toEqual({
            type: "comparison",
            operator: "<=",
            property: "score",
            value: 5,
        });
        expect(like("name", "%fred%")).toEqual({
            type: "comparison",
            operator: "LIKE",
            property: "name",
            value: "%fred%",
        });
        expect(inList("status", ["new", "archived"])).toEqual({
            type: "comparison",
            operator: "IN",
            property: "status",
            value: ["new", "archived"],
        });
    });

    test("builds logical group nodes", () => {
        const left = eq("a", 1);
        const right = gt("b", 2);
        expect(and(left, right)).toEqual({
            type: "group",
            method: "AND",
            components: [left, right],
        });
        expect(or(left, right)).toEqual({
            type: "group",
            method: "OR",
            components: [left, right],
        });
        expect(not(left)).toEqual({
            type: "group",
            method: "NOT",
            components: [left],
        });
    });
});
