import type { JSONable } from "../korm";

import { randomBytes, createCipheriv, createDecipheriv } from 'node:crypto';
import util from "node:util";
import argon2 from 'argon2';

/**
 * Serialized encrypted payload stored in the database or WAL.
 */
export type EncryptedParams<T extends JSONable> = {
    /** Encrypted payload value (hash or cipher text). */
    value: string;
    /** Original data type (for debugging/introspection). */
    dataType?: string;
    /** Encryption kind. */
    type: "password" | "symmetric";
    /** Initialization vector for symmetric encryption. */
    iv?: string;
    /** Auth tag for symmetric encryption. */
    authTag?: string;
}

/**
 * Encrypted payload with metadata.
 * Use `verifyPassword(...)` when `type` is "password".
 */
export class Encrypted<T extends JSONable> {
    readonly __ENCRYPTED__: true = true;
    value: string;
    dataType: string;
    type: "password" | "symmetric";
    salt?: string;
    iv?: string;
    authTag?: string;

    constructor(params: {
        value: string;
        dataType?: string;
        type: "password" | "symmetric";
        iv?: string;
        authTag?: string;
    }) {
        if (params.type === "symmetric" && (!params.iv || !params.authTag)) {
            throw new Error("FATAL: IV and authTag are required for symmetric encryption");
        }
        this.value = params.value;
        this.dataType = params.dataType ?? "string";
        this.type = params.type;
        this.iv = params.iv;
        this.authTag = params.authTag;
    }

    private async _verifyPassword(hash: string, plainText: string): Promise<boolean> {
        if (this.type !== "password") {
            throw new Error("FATAL: Password verification is only supported for password encryption.");
        }
        try {
          return await argon2.verify(hash, plainText);
        } catch (err) {
          return false;
        }
    };

    /**
     * Verify a password against this encrypted payload.
     * Throws if the payload is not a password hash.
     */
    async verifyPassword(plainText: string): Promise<boolean> {
        if (this.type !== "password") {
            throw new Error("FATAL: Password verification is only supported for password encryption. You can decrypt the value with `decrypt(): T` instead.");
        }
        return await this._verifyPassword(this.value, plainText);
    }

    /** Serialize to a JSON-friendly payload for storage. */
    toJSON(): JSONable {
        if (this.type === "password") {
            return {
                value: this.value,
                type: this.type,
            }
        } else {
            return {
                __ENCRYPTED__: true,
                value: this.value,
                type: this.type,
                iv: this.iv!,
                authTag: this.authTag!,
            };
        }
    }
}

/**
 * In-memory wrapper for encrypted values.
 * Create via `korm.encrypt(...)` or `korm.password(...)` and store directly in item data.
 */
export class Encrypt<T extends JSONable> {
    readonly __ENCRYPT__: true = true;
    private _value: T;
    private _type: "password" | "symmetric";
    private _encryptedValue?: Encrypted<T>;

    constructor(value: T, type: "password" | "symmetric") {
        this._value = value;
        this._type = type;
    }

    /** Reveal the cleartext value in memory. */
    reveal(): T {
        return this._value;
    }

    /**
     * Hydrate an Encrypt wrapper from a stored payload.
     * For symmetric payloads, pass the decrypted `plainValue`.
     */
    static fromEncryptedPayload<T extends JSONable>(
        payload: EncryptedParams<T>,
        plainValue?: T
    ): Encrypt<T> {
        let seedValue: T;
        if (payload.type === "password") {
            seedValue = payload.value as unknown as T;
        } else {
            if (plainValue === undefined) {
                throw new Error("FATAL: Cannot hydrate symmetric encryption without plaintext.");
            }
            seedValue = plainValue;
        }
        const encrypt = new Encrypt<T>(seedValue, payload.type);
        encrypt._encryptedValue = new Encrypted({
            value: payload.value,
            dataType: payload.dataType ?? typeof seedValue,
            type: payload.type,
            iv: payload.iv,
            authTag: payload.authTag,
        });
        return encrypt;
    }

    /**
     * Serialize to an encrypted payload for storage.
     * Call `lock()` first to ensure an encrypted value is available.
     */
    toJSON(): JSONable {
        if (!this._encryptedValue) {
            throw new Error("FATAL: Encrypted value is not available. You must call lock() first.");
        };
        return this._encryptedValue.toJSON();
    }

    [util.inspect.custom](depth: number, options: util.InspectOptionsStylized): string {
        const redacted = this._type === "symmetric" ? "[REDACTED]" : "[HASH]";
        const preview = {
            __ENCRYPT__: true,
            _type: this._type,
            _value: redacted,
            _encryptedValue: this._encryptedValue,
        };
        return `Encrypt ${util.inspect(preview, options)}`;
    }

    /** Return the encrypted payload after `lock()` has been called. */
    safeValue(): Encrypted<T> {
        if (!this._encryptedValue) {
            throw new Error("FATAL: Encrypted value is not available. You must call lock() first.");
        };
        return this._encryptedValue;
    }

    /**
     * Verify a plaintext password against this payload.
     * Throws if this Encrypt was created for symmetric encryption.
     */
    async verifyPassword(plainText: string): Promise<boolean> {
        if (this._type !== "password") {
            throw new Error("FATAL: Password verification is only supported for password encryption. You can decrypt the value with `decrypt(): T` instead.");
        }
        return await this._encryptedValue!.verifyPassword(JSON.stringify(plainText));
    }

    /**
     * Perform encryption/hash if needed and return the encrypted payload.
     */
    async lock(): Promise<Encrypted<T>> {
        if (!this._encryptedValue) {
            this._encryptedValue = await this._encrypt();
        }
        return this._encryptedValue;
    }

    private async _hashPassword (plainText: string): Promise<string> {
        return await argon2.hash(plainText, {
          type: argon2.argon2id, 
          memoryCost: 2 ** 16,
          timeCost: 3,
          parallelism: 1,
        });
    };

    private async _encrypt(): Promise<Encrypted<T>> {
        if (this._type === "symmetric") {
            if (!process.env.KORM_ENCRYPTION_KEY) {
                if (process.env.NODE_ENV === 'production') {
                    throw new Error("FATAL: KORM_ENCRYPTION_KEY is missing in PRODUCTION. Aborting to prevent data loss.");
                }
                const newKey = randomBytes(32).toString("hex");
                process.env.KORM_ENCRYPTION_KEY = newKey;
                console.warn("\x1b[31m!!! READ THIS !!!\x1b[0m -> Wanted to symmetrically encrypt a value but the KORM_ENCRYPTION_KEY environment variable is not set.");
                console.warn("korm has generated a new encryption key for you. It will stay valid for this session.");
                console.warn("Do not share this key with anyone. It can be used to decrypt the data stored in your database.");
                console.warn("You must save this key in a secure location to access data encrypted with it.");
                console.warn("To use the same key next session, set the KORM_ENCRYPTION_KEY environment variable to the value of the new key.");
                console.log(`\nTHIS IS YOUR NEW ENCRYPTION KEY:\n\n\x1b[31m '${newKey}' \x1b[0m\n\n`);
            }
            const keyBuffer = Buffer.from(process.env.KORM_ENCRYPTION_KEY, 'hex');
            const ivBuffer = randomBytes(12);
            const cipher = createCipheriv('aes-256-gcm', keyBuffer, ivBuffer);
            let encrypted = cipher.update(JSON.stringify(this._value), 'utf8', 'hex');
            encrypted += cipher.final('hex');
            const authTag = cipher.getAuthTag().toString('hex');
            return new Encrypted({
                value: encrypted,
                dataType: typeof this._value,
                type: "symmetric",
                iv: ivBuffer.toString('hex'),
                authTag: authTag,
            });
        } else {
            const encryptedValue = await this._hashPassword(JSON.stringify(this._value));
            return new Encrypted({
                value: encryptedValue,
                dataType: typeof this._value,
                type: "password",
            });
        }
    }
};

/** Alias for password-hashed values. */
export type Password<T extends JSONable> = Encrypt<T>;

/**
 * Decrypt a stored encrypted payload back into plaintext.
 * Only works for symmetric encryption and requires `KORM_ENCRYPTION_KEY`.
 */
export async function decrypt<T extends JSONable> (encrypted: EncryptedParams<T>): Promise<T> {
    if (encrypted.type !== "symmetric") {
        throw new Error("FATAL: Decryption is only supported for symmetric encryption.");
    }
    if (!process.env.KORM_ENCRYPTION_KEY) {
        throw new Error("FATAL: Cannot decrypt: KORM_ENCRYPTION_KEY is missing.");
    }
    const keyBuffer = Buffer.from(process.env.KORM_ENCRYPTION_KEY, 'hex');
    const ivBuffer = Buffer.from(encrypted.iv!, 'hex');
    const authTagBuffer = Buffer.from(encrypted.authTag!, 'hex');
    const decipher = createDecipheriv('aes-256-gcm', keyBuffer, ivBuffer);
    decipher.setAuthTag(authTagBuffer);
    let decrypted = decipher.update(encrypted.value, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    return JSON.parse(decrypted) as T;
}
