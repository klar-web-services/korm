import type { JSONable } from "../korm";
import type { RN } from "../core/rn";
import type { DepotFile, DepotFileBase } from "./depotFile";

/**
 * Storage backend for depot files (local or S3-compatible).
 * Use via `korm.depots.*` and include in a pool to resolve depot RNs.
 */
export interface Depot {
  readonly __DEPOT__: true;
  identifier: string;
  readonly type: "local" | "s3";

  /**
   * Upload a depot file and return the storage key/path.
   * Next: call `getFile(...)` or keep the RN for later retrieval.
   */
  createFile(file: DepotFileBase): Promise<string>;
  /**
   * Fetch a depot file by RN.
   * Next: call `.text()`, `.stream()`, `.update(...)`, or `.delete(pool)`.
   */
  getFile(rn: RN<JSONable>): Promise<DepotFile>;
  /**
   * Delete a depot file by RN. Returns true when deleted.
   * Next: create a new file with `korm.file(...)` if needed.
   */
  deleteFile(rn: RN<JSONable>): Promise<boolean>;
  /**
   * List files under a depot prefix RN.
   * Next: inspect returned `DepotFile` entries or resolve them in item data.
   */
  listFiles(rn: RN<JSONable>): Promise<DepotFile[]>;
  /**
   * List directory names under a depot prefix RN.
   * Next: build a more specific prefix RN and call `listFiles(...)`.
   */
  listDirs(rn: RN<JSONable>): Promise<string[]>;
  /**
   * Read, edit, and replace a depot file.
   * Next: call `getFile(...)` to read the updated contents.
   */
  editFile(
    rn: RN<JSONable>,
    edit: (file: DepotFile) => Promise<DepotFileBase>,
  ): Promise<boolean>;
}
