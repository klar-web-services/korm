import crypto from "node:crypto";

/**
 * Create a stable depot identifier from type and input parts.
 * Used to auto-name depots when no identifier is provided.
 */
export function createDepotIdentifier(type: string, parts: string[]): string {
    const seed = [type, ...parts].map((part) => part.trim()).join("|");
    const hash = crypto.createHash("sha256").update(seed).digest("hex").slice(0, 12);
    return `korm_${type}_${hash}`;
}
