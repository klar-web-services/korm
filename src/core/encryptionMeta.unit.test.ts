import { describe, expect, test } from "bun:test";
import type { JSONable } from "../korm";
import { Encrypt } from "../security/encryption";
import {
    applyEncryptionMeta,
    cloneEncryptionMeta,
    clearEncryptionMeta,
    getEncryptionMeta,
    isEncryptedPayload,
    materializeEncryptedSnapshot,
    normalizeEncryptedValues,
    setEncryptionMeta,
    type EncryptedPayload,
    type EncryptionMeta,
} from "./encryptionMeta";

const KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

describe("encryptionMeta helpers", () => {
    test("cloneEncryptionMeta deep-clones entries", () => {
        const payload: EncryptedPayload = { __ENCRYPTED__: true, type: "password", value: "hash" };
        const meta: EncryptionMeta = {
            entries: [
                { path: ["secret"], payload, plainValue: { nested: [1] } as JSONable },
            ],
        };
        const cloned = cloneEncryptionMeta(meta);
        expect(cloned).not.toBe(meta);
        expect(cloned.entries[0]).not.toBe(meta.entries[0]);
        cloned.entries[0]!.path.push("x");
        expect(meta.entries[0]!.path).toEqual(["secret"]);
        (cloned.entries[0]!.plainValue as { nested: number[] }).nested[0] = 2;
        expect((meta.entries[0]!.plainValue as { nested: number[] }).nested[0]).toBe(1);
    });

    test("stores and clears encryption metadata", () => {
        const target = {};
        const meta: EncryptionMeta = { entries: [] };
        setEncryptionMeta(target, meta);
        expect(getEncryptionMeta(target)).toBe(meta);
        clearEncryptionMeta(target);
        expect(getEncryptionMeta(target)).toBeUndefined();
    });

    test("isEncryptedPayload recognizes payloads", () => {
        const payload: EncryptedPayload = { __ENCRYPTED__: true, type: "password", value: "hash" };
        expect(isEncryptedPayload(payload)).toBe(true);
        expect(isEncryptedPayload({ type: "password", value: "hash" })).toBe(false);
    });

    test("applyEncryptionMeta no-ops with empty or missing meta", async () => {
        const data = { ok: true };
        const empty: EncryptionMeta = { entries: [] };
        const emptyResult = await applyEncryptionMeta(data, empty);
        expect(emptyResult.data).toBe(data);
        expect(emptyResult.meta).toBe(empty);
        const missingResult = await applyEncryptionMeta(data, undefined);
        expect(missingResult.data).toBe(data);
        expect(missingResult.meta).toBeUndefined();
    });

    test("applyEncryptionMeta handles payloads, wrappers, and plain values", async () => {
        const previousKey = process.env.KORM_ENCRYPTION_KEY;
        process.env.KORM_ENCRYPTION_KEY = KEY;
        try {
            const passwordPayload: EncryptedPayload = { __ENCRYPTED__: true, type: "password", value: "hash" };
            const symmetricPayload: EncryptedPayload = {
                __ENCRYPTED__: true,
                type: "symmetric",
                value: "cipher",
                iv: "iv",
                authTag: "tag",
            };
            const existingPayload: EncryptedPayload = { ...passwordPayload };
            const encryptWrapper = new Encrypt("wrapped", "password");

            const data = {
                passSame: "secret",
                passValue: "hash",
                passNew: "newpass",
                symSame: { a: 1 },
                symNew: { a: 2 },
                payloadExisting: existingPayload,
                encryptWrapper,
                nullValue: null,
            };

            const meta: EncryptionMeta = {
                entries: [
                    { path: ["passSame"], payload: passwordPayload, plainValue: "secret" },
                    { path: ["passValue"], payload: { ...passwordPayload, value: "hash" } },
                    { path: ["passNew"], payload: { ...passwordPayload, value: "old" }, plainValue: "oldpass" },
                    { path: ["symSame"], payload: symmetricPayload, plainValue: { a: 1 } },
                    { path: ["symNew"], payload: symmetricPayload, plainValue: { a: 1 } },
                    { path: ["payloadExisting"], payload: passwordPayload },
                    { path: ["encryptWrapper"], payload: passwordPayload },
                    { path: ["nullValue"], payload: passwordPayload },
                    { path: ["missing"], payload: passwordPayload },
                ],
            };

            const result = await applyEncryptionMeta(data as JSONable, meta);
            const out = result.data as Record<string, JSONable>;
            expect(isEncryptedPayload(out.passSame)).toBe(true);
            expect((out.passSame as EncryptedPayload).value).toBe("hash");
            expect(isEncryptedPayload(out.passValue)).toBe(true);
            expect(isEncryptedPayload(out.passNew)).toBe(true);
            expect(isEncryptedPayload(out.symSame)).toBe(true);
            expect((out.symSame as EncryptedPayload).type).toBe("symmetric");
            expect(isEncryptedPayload(out.symNew)).toBe(true);
            expect((out.symNew as EncryptedPayload).iv).toBeTruthy();
            expect(isEncryptedPayload(out.payloadExisting)).toBe(true);
            expect(isEncryptedPayload(out.encryptWrapper)).toBe(true);
            expect(out.nullValue).toBeNull();

            const passNewEntry = result.meta?.entries.find((entry) => entry.path[0] === "passNew");
            expect(passNewEntry?.plainValue).toBe("newpass");
        } finally {
            if (previousKey === undefined) {
                delete process.env.KORM_ENCRYPTION_KEY;
            } else {
                process.env.KORM_ENCRYPTION_KEY = previousKey;
            }
        }
    });

    test("materializeEncryptedSnapshot replaces tracked paths", () => {
        const payload: EncryptedPayload = { __ENCRYPTED__: true, type: "password", value: "hash" };
        const meta: EncryptionMeta = {
            entries: [{ path: ["secret"], payload }],
        };
        const data = { secret: "plain", other: { ok: true } };
        const snapshot = materializeEncryptedSnapshot(data, meta);
        expect(snapshot as Record<string, JSONable>).toEqual({ secret: payload, other: { ok: true } });
        const passthrough = materializeEncryptedSnapshot(data, { entries: [] });
        expect(passthrough).toBe(data);
    });

    test("normalizeEncryptedValues serializes Encrypt wrappers", async () => {
        const date = new Date("2024-01-01T00:00:00Z");
        const payload: EncryptedPayload = { __ENCRYPTED__: true, type: "password", value: "hash" };
        const wrapped = new Encrypt("secret", "password");
        const normalized = await normalizeEncryptedValues({
            wrapped,
            payload,
            list: [wrapped],
            when: date,
        } as JSONable) as Record<string, JSONable>;
        expect(isEncryptedPayload(normalized.wrapped)).toBe(true);
        expect(isEncryptedPayload(normalized.payload)).toBe(true);
        expect(Array.isArray(normalized.list)).toBe(true);
        expect(isEncryptedPayload((normalized.list as JSONable[])[0]!)).toBe(true);
        expect(normalized.when).toBe(date);
    });
});
