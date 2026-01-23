import { describe, expect, test } from "bun:test";
import { BaseNeedsDanger, danger, needsDanger } from "./danger";

describe("danger", () => {
    test("executes wrapped operations", () => {
        const op = new BaseNeedsDanger((value: string) => `done:${value}`, "safe");
        expect(danger(op)).toBe("done:safe");
    });

    test("executes wrapped operations built via needsDanger", () => {
        const op = needsDanger((a: number, b: number) => a + b, 1, 2);
        expect(danger(op)).toBe(3);
    });

    test("rejects unregistered wrappers", () => {
        const fake = {} as BaseNeedsDanger<string>;
        expect(() => danger(fake)).toThrow("BaseNeedsDanger");
    });

    test("returns promises for async operations", async () => {
        const op = new BaseNeedsDanger(async (value: string) => value.toUpperCase(), "safe");
        await expect(danger(op)).resolves.toBe("SAFE");
    });
});
