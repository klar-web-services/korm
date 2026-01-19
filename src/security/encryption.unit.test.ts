import { describe, expect, test } from "bun:test";
import util from "node:util";
import {
    Encrypt,
    Encrypted,
    decrypt,
    type EncryptedParams,
} from "./encryption";

const KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

describe("encryption", () => {
    test("Encrypted validates symmetric params and serializes", async () => {
        expect(() => new Encrypted({ value: "x", type: "symmetric" as "symmetric" })).toThrow();
        const password = new Encrypted({ value: "hash", type: "password" });
        expect(password.toJSON()).toEqual({ value: "hash", type: "password" });
        const symmetric = new Encrypted({ value: "cipher", type: "symmetric", iv: "iv", authTag: "tag" });
        expect(symmetric.toJSON()).toEqual({
            __ENCRYPTED__: true,
            value: "cipher",
            type: "symmetric",
            iv: "iv",
            authTag: "tag",
        });
        const badHash = new Encrypted({ value: "not-a-hash", type: "password" });
        const ok = await badHash.verifyPassword("secret");
        expect(ok).toBe(false);
        await expect(symmetric.verifyPassword("secret")).rejects.toThrow("Password verification");
    });

    test("Encrypt enforces lock before serialization", () => {
        const encrypt = new Encrypt("secret", "password");
        expect(() => encrypt.toJSON()).toThrow("lock()");
        expect(() => encrypt.safeValue()).toThrow("lock()");
        expect(encrypt.reveal()).toBe("secret");
    });

    test("Encrypt can verify password payloads", async () => {
        const encrypt = new Encrypt("secret", "password");
        await encrypt.lock();
        expect(await encrypt.verifyPassword("secret")).toBe(true);
        const encrypted = encrypt.safeValue();
        const ok = await encrypted.verifyPassword(JSON.stringify("secret"));
        expect(ok).toBe(true);
    });

    test("Encrypt rejects password verification for symmetric values", async () => {
        const previousKey = process.env.KORM_ENCRYPTION_KEY;
        process.env.KORM_ENCRYPTION_KEY = KEY;
        try {
            const encrypt = new Encrypt("secret", "symmetric");
            await encrypt.lock();
            await expect(encrypt.verifyPassword("secret")).rejects.toThrow("Password verification");
        } finally {
            if (previousKey === undefined) {
                delete process.env.KORM_ENCRYPTION_KEY;
            } else {
                process.env.KORM_ENCRYPTION_KEY = previousKey;
            }
        }
    });

    test("Encrypt.fromEncryptedPayload requires plaintext for symmetric values", () => {
        const payload: EncryptedParams<string> = { value: "hash", type: "password" };
        const password = Encrypt.fromEncryptedPayload(payload);
        expect(password.reveal()).toBe("hash");
        const symmetricPayload: EncryptedParams<string> = {
            value: "cipher",
            type: "symmetric",
            iv: "iv",
            authTag: "tag",
        };
        expect(() => Encrypt.fromEncryptedPayload(symmetricPayload)).toThrow("Cannot hydrate symmetric encryption");
        const symmetric = Encrypt.fromEncryptedPayload(symmetricPayload, "plain");
        expect(symmetric.reveal()).toBe("plain");
    });

    test("Encrypt generates keys in development when missing", async () => {
        const previousKey = process.env.KORM_ENCRYPTION_KEY;
        const previousEnv = process.env.NODE_ENV;
        process.env.NODE_ENV = "development";
        delete process.env.KORM_ENCRYPTION_KEY;
        const warn = console.warn;
        const log = console.log;
        console.warn = () => {};
        console.log = () => {};
        try {
            const encrypt = new Encrypt("secret", "symmetric");
            await encrypt.lock();
            expect(process.env.KORM_ENCRYPTION_KEY).toBeTruthy();
        } finally {
            console.warn = warn;
            console.log = log;
            if (previousKey === undefined) {
                delete process.env.KORM_ENCRYPTION_KEY;
            } else {
                process.env.KORM_ENCRYPTION_KEY = previousKey;
            }
            process.env.NODE_ENV = previousEnv;
        }
    });

    test("Encrypt throws in production without a key", async () => {
        const previousKey = process.env.KORM_ENCRYPTION_KEY;
        const previousEnv = process.env.NODE_ENV;
        delete process.env.KORM_ENCRYPTION_KEY;
        process.env.NODE_ENV = "production";
        try {
            const encrypt = new Encrypt("secret", "symmetric");
            await expect(encrypt.lock()).rejects.toThrow("KORM_ENCRYPTION_KEY is missing");
        } finally {
            if (previousKey === undefined) {
                delete process.env.KORM_ENCRYPTION_KEY;
            } else {
                process.env.KORM_ENCRYPTION_KEY = previousKey;
            }
            process.env.NODE_ENV = previousEnv;
        }
    });

    test("decrypt requires symmetric payloads and keys", async () => {
        const passwordPayload: EncryptedParams<string> = { value: "hash", type: "password" };
        await expect(decrypt(passwordPayload)).rejects.toThrow("symmetric encryption");

        const previousKey = process.env.KORM_ENCRYPTION_KEY;
        delete process.env.KORM_ENCRYPTION_KEY;
        const missingKeyPayload: EncryptedParams<string> = {
            value: "cipher",
            type: "symmetric",
            iv: "iv",
            authTag: "tag",
        };
        await expect(decrypt(missingKeyPayload)).rejects.toThrow("KORM_ENCRYPTION_KEY is missing");
        if (previousKey === undefined) {
            delete process.env.KORM_ENCRYPTION_KEY;
        } else {
            process.env.KORM_ENCRYPTION_KEY = previousKey;
        }
    });

    test("decrypt reverses symmetric encryption", async () => {
        const previousKey = process.env.KORM_ENCRYPTION_KEY;
        process.env.KORM_ENCRYPTION_KEY = KEY;
        try {
            const encrypt = new Encrypt("secret", "symmetric");
            const locked = await encrypt.lock();
            const payload: EncryptedParams<string> = {
                value: locked.value,
                type: locked.type,
                iv: locked.iv!,
                authTag: locked.authTag!,
            };
            const decrypted = await decrypt(payload);
            expect(decrypted).toBe("secret");
            const inspected = util.inspect(encrypt);
            expect(inspected).toContain("[REDACTED]");
        } finally {
            if (previousKey === undefined) {
                delete process.env.KORM_ENCRYPTION_KEY;
            } else {
                process.env.KORM_ENCRYPTION_KEY = previousKey;
            }
        }
    });
});
