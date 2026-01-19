import type { JSONable } from "../korm";
import type { RN } from "../core/rn";
import type { DepotFile, DepotFileBase } from "./depotFile";

/**
 * Storage backend for depot files (local or S3-compatible).
 * Use via `korm.depot.*` and include in a pool to resolve depot RNs.
 */
export interface Depot {
    readonly __DEPOT__: true;
    identifier: string;
    readonly type: "local" | "s3";

    /** Upload a depot file and return the storage key/path. */
    createFile(file: DepotFileBase): Promise<string>;
    /** Fetch a depot file by RN. */
    getFile(rn: RN<JSONable>): Promise<DepotFile>;
    /** Delete a depot file by RN. Returns true when deleted. */
    deleteFile(rn: RN<JSONable>): Promise<boolean>;
    /** List files under a depot prefix RN. */
    listFiles(rn: RN<JSONable>): Promise<DepotFile[]>;
    /** List directory names under a depot prefix RN. */
    listDirs(rn: RN<JSONable>): Promise<string[]>;
    /** Read, edit, and replace a depot file. */
    editFile(rn: RN<JSONable>, edit: (file: DepotFile) => Promise<DepotFileBase>): Promise<boolean>;
}
