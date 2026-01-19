const UNSAFE_OBJECT_KEYS = new Set(["__proto__", "prototype", "constructor"]);

export function isUnsafeObjectKey(key: string): boolean {
    return UNSAFE_OBJECT_KEYS.has(key);
}

export function safeAssign(target: Record<string, unknown>, key: string, value: unknown): void {
    if (isUnsafeObjectKey(key)) {
        Object.defineProperty(target, key, {
            value,
            writable: true,
            enumerable: true,
            configurable: true,
        });
        return;
    }
    target[key] = value;
}
