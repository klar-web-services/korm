import type { JSONable } from "../korm";
import type { LayerPool } from "../sources/layerPool";
import { applyEncryptionMeta, normalizeEncryptedValues, type EncryptionMeta } from "./encryptionMeta";
import { persistDepotFilesInData } from "../depot/depotPersist";
import type { DepotFileBase } from "../depot/depotFile";

export async function prepareWriteData<T extends JSONable>(
    data: T | undefined,
    encryptionMeta: EncryptionMeta | undefined,
    pool: LayerPool,
    options?: { depotOps?: DepotFileBase[]; skipDepotUpload?: boolean }
): Promise<{ data: T | undefined; meta?: EncryptionMeta; depotOps?: DepotFileBase[] }> {
    if (data === undefined) {
        return { data, meta: encryptionMeta, depotOps: options?.depotOps };
    }

    let nextData: JSONable = data;
    let nextMeta: EncryptionMeta | undefined = encryptionMeta;
    const depotOps = options?.depotOps ?? (options?.skipDepotUpload ? [] : undefined);

    if (encryptionMeta) {
        const applied = await applyEncryptionMeta(nextData as JSONable, encryptionMeta);
        nextData = applied.data as JSONable;
        nextMeta = applied.meta;
    }

    nextData = await normalizeEncryptedValues(nextData as JSONable);
    nextData = await persistDepotFilesInData(nextData as JSONable, pool, {
        depotOps,
        skipUpload: options?.skipDepotUpload,
    });

    return { data: nextData as T, meta: nextMeta, depotOps };
}
