import { Result } from "@fkws/klonk-result";
import type { JSONable } from "../korm";

/** RN modifiers such as `from` (layer) or `depot` (depot identifier). */
export type Mods = Map<string, string>;

/**
 * Resource Name (RN) for items, collections, and depot files.
 * Example: `[rn][from::userdb]:users:freetier:UUID` or `[rn][depot::files]:invoices:fred:invoice.pdf`.
 */
export class RN<T extends JSONable = JSONable> {
    readonly __RN__: true = true;
    private constructor(
        private _namespace?: string,
        private _kind?: string,
        private _id?: string,
        private readonly _mods: Mods = new Map(),
        private _depotParts?: string[]
    ) { }

    /**
     * Add or update a modifier (e.g. `from` or `depot`).
     * Returns the same RN for chaining.
     */
    mod(key: string, value: string): RN<T> {
        if (key === "depot" && !this._depotParts) {
            throw new Error("Cannot add depot mod to an item RN. Create a depot RN from a string instead.");
        }
        this._mods.set(key, value);
        return this;
    }

    /**
     * Create an RN from a full RN string, or from namespace/kind/id parts.
     * Returns a Result with validation errors when the RN is malformed.
     */
    static create<T extends JSONable = JSONable>(rn: string): Result<RN<T>>;
    /** @inheritdoc */
    static create<T extends JSONable = JSONable>(namespace: string, kind: string, id: string, mods?: Mods): Result<RN<T>>;
    static create<T extends JSONable = JSONable>(
        rnOrNamespace: string,
        kindOrUndefined?: string,
        idOrUndefined?: string,
        mods?: Mods
    ): Result<RN<T>> {
        const namePattern = /^[a-z][a-z0-9]*$/;
        const modValuePattern = /^[^\[\]]+$/;
        const isRnString =
            arguments.length === 1
            && typeof rnOrNamespace === "string"
            && rnOrNamespace.startsWith("[rn]");

        if (isRnString) {
            try {
                const raw = rnOrNamespace as string;
                let idx = 4; // skip "[rn]"
                const parsedMods: Mods = new Map();

                const parseMod = (segment: string): { key: string; value: string } => {
                    const sepIdx = segment.indexOf("::");
                    if (sepIdx === -1) throw new Error(`Error parsing resource name '${raw}': Mods are malformed.`);
                    const key = segment.slice(0, sepIdx);
                    const value = segment.slice(sepIdx + 2);
                    if (!key || !namePattern.test(key)) {
                        throw new Error(`'${key}' is not allowed as mod key. Adhere to this pattern: [a-z][a-z0-9]`);
                    }
                    if (!value || !modValuePattern.test(value)) {
                        throw new Error(`'${value}' is not allowed as mod value. Adhere to this pattern: [a-zA-Z0-9_-]`);
                    }
                    return { key, value };
                };

                while (raw[idx] === "[") {
                    const close = raw.indexOf("]", idx + 1);
                    if (close === -1) {
                        throw new Error(`Error parsing resource name '${raw}': Mods are malformed.`);
                    }
                    const segment = raw.slice(idx + 1, close);
                    const { key, value } = parseMod(segment);
                    parsedMods.set(key, value);
                    idx = close + 1;
                }

                if (raw[idx] !== ":") {
                    throw new Error(`Error parsing resource name '${raw}': Missing ":" after mods.`);
                }
                const rest = raw.slice(idx + 1);
                if (rest.length === 0) {
                    throw new Error(`Error parsing resource name '${raw}': Missing RN body.`);
                }

                if (parsedMods.has("depot")) {
                    const parts = rest.split(":");
                    if (parts.length === 0) {
                        throw new Error(`Error parsing resource name '${raw}': Missing depot path.`);
                    }
                    for (let i = 0; i < parts.length; i++) {
                        const part = parts[i]!;
                        if (!part) {
                            throw new Error(`Error parsing resource name '${raw}': Empty depot segment.`);
                        }
                        if (part.includes("[") || part.includes("]")) {
                            throw new Error(`Error parsing resource name '${raw}': Depot segments cannot include "[" or "]".`);
                        }
                        if (part === "." || part === "..") {
                            throw new Error(`Error parsing resource name '${raw}': Depot segments cannot be "." or "..".`);
                        }
                        if (part.includes("/") || part.includes("\\") || part.includes("\u0000")) {
                            throw new Error(`Error parsing resource name '${raw}': Depot segments cannot include path separators.`);
                        }
                        if (part === "*" && i !== parts.length - 1) {
                            throw new Error(`Error parsing resource name '${raw}': Wildcard only allowed in final depot segment.`);
                        }
                    }
                    return new Result({
                        success: true,
                        data: new RN<T>(undefined, undefined, undefined, parsedMods, parts),
                    });
                }

                const [namespace, kind, id, ...extra] = rest.split(":");
                if (!namespace || !kind || !id || extra.length > 0) {
                    throw new Error(`Error parsing resource name '${raw}': Does not comply with RN pattern.`);
                }
                if (namespace === "undefined" || kind === "undefined") {
                    throw new Error(`'undefined' is not allowed as namespace or kind.`);
                }
                const idPattern = /^(\*|[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$/;
                if (!idPattern.test(id)) {
                    throw new Error(`'${id}' is not allowed as id. Must be a UUIDv4 or wildcard (*).`);
                }

                return new Result({
                    success: true,
                    data: new RN<T>(namespace as string, kind as string, id as string, parsedMods),
                });
            } catch (error) {
                return new Result({ success: false, error: error as Error })
            }
        } else {
            for (const e of [rnOrNamespace, kindOrUndefined]) {
                if (e === "undefined" || !namePattern.test(e!)) return new Result({ success: false, error: new Error(`'${e}' is not allowed as namespace or kind. Adhere to this pattern: [a-z][a-z0-9]`) });
            }
            const idPattern = /^(\*|[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})$/;
            if (!idPattern.test(idOrUndefined!)) return new Result({ success: false, error: new Error(`'${idOrUndefined}' is not allowed as id. Must be a UUIDv4 or wildcard (*).`) });

            const clonedMods = new Map(mods ?? []);
            for (const [key, value] of clonedMods.entries()) {
                if (!namePattern.test(key)) return new Result({ success: false, error: new Error(`'${key}' is not allowed as mod key. Adhere to this pattern: [a-z][a-z0-9]`) });
                if (!modValuePattern.test(value)) return new Result({ success: false, error: new Error(`'${value}' is not allowed as mod value. Adhere to this pattern: [a-zA-Z0-9_-]`) });
            }
            if (clonedMods.has("depot")) {
                return new Result({ success: false, error: new Error("Depot RNs must be created from an RN string.") });
            }

            return new Result({ success: true, data: new RN<T>(rnOrNamespace as string, kindOrUndefined as string, idOrUndefined as string, clonedMods) });
        }
    }

    /** Namespace segment (e.g. `users`). */
    get namespace(): string | undefined {
        return this._namespace;
    }

    /** Kind segment (e.g. `freetier`). */
    get kind(): string | undefined {
        return this._kind;
    }

    /** Item id segment (UUID) or `*` for collections. */
    get id(): string | undefined {
        return this._id;
    }

    /** Modifiers attached to this RN. */
    get mods(): Readonly<Mods> {
        return this._mods;
    }

    /** True when this RN points to a depot file or prefix. */
    isDepot(): boolean {
        return this._mods.has("depot");
    }

    /** Depot identifier if this RN targets a depot. */
    depotIdent(): string | undefined {
        return this._mods.get("depot");
    }

    /** Path segments for depot RNs (without the `depot` mod). */
    depotParts(): string[] | undefined {
        return this._depotParts ? [...this._depotParts] : undefined;
    }

    /** Serialize to RN string when JSON-stringifying. */
    toJSON(): JSONable {
        return this.value();
    }

    /** Which kind of target this RN refers to. */
    pointsTo(): "item" | "collection" | "depotFile" | "depotPrefix" {
        if (this.isDepot()) {
            const last = this._depotParts?.[this._depotParts.length - 1];
            return last === "*" ? "depotPrefix" : "depotFile";
        }
        return this.id! === "*" ? "collection" : "item"
    }

    /** Full RN string value. */
    value(): string {
        const modsString = Array.from(this._mods.entries())
            .map(([key, value]) => `[${key}::${value}]`)
            .join("");
        if (this.isDepot()) {
            const parts = this._depotParts ?? [];
            return `[rn]${modsString}:${parts.join(":")}`;
        }
        return `[rn]${modsString}:${this._namespace ?? ""}:${this.kind ?? ""}:${this.id ?? ""}`;
    }
}
