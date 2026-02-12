import type { Depot } from "../depot";
import { DepotFile, type DepotFileBase } from "../depotFile";
import { RN } from "../../core/rn";
import { createDepotIdentifier } from "../depotIdent";
import fs from "node:fs";
import path from "node:path";

function isReadableStream(value: unknown): value is ReadableStream<Uint8Array> {
  return (
    !!value &&
    typeof value === "object" &&
    "getReader" in value &&
    typeof (value as ReadableStream).getReader === "function"
  );
}

async function writeStreamToPath(
  filePath: string,
  stream: ReadableStream<Uint8Array>,
): Promise<void> {
  const payload = new Uint8Array(await new Response(stream).arrayBuffer());
  await fs.promises.writeFile(filePath, payload);
}

async function writeBlobToPath(filePath: string, blob: Blob): Promise<void> {
  const payload = new Uint8Array(await blob.arrayBuffer());
  await fs.promises.writeFile(filePath, payload);
}

async function readBlobFromPath(filePath: string): Promise<Blob> {
  const payload = await fs.promises.readFile(filePath);
  return new Blob([payload]);
}

/**
 * Local filesystem depot.
 * Use `korm.depots.local(rootPath)` to create and include in a pool.
 */
export class LocalDepot implements Depot {
  readonly __DEPOT__: true = true;
  identifier: string;
  readonly type: "local" = "local";
  private _root: string;

  /**
   * Create a local depot.
   * Pass either `root` alone or `identifier` plus `root`.
   */
  constructor(root: string);
  /** @inheritdoc */
  constructor(identifier: string, root: string);
  constructor(rootOrIdentifier: string, rootMaybe?: string) {
    const root = rootMaybe ?? rootOrIdentifier;
    const identifier = rootMaybe ? rootOrIdentifier : undefined;
    this._root = root;
    this.identifier = identifier ?? createDepotIdentifier("local", [root]);
  }

  private _pathFromRn(rn: RN): string {
    const parts = rn.depotParts();
    if (!parts) {
      throw new Error(`RN "${rn.value()}" does not refer to a depot path.`);
    }
    const normalized =
      parts[parts.length - 1] === "*" ? parts.slice(0, -1) : parts;
    const base = path.resolve(this._root);
    const resolved =
      normalized.length === 0 ? base : path.resolve(base, ...normalized);
    const rel = path.relative(base, resolved);
    if (rel !== "" && (rel.startsWith("..") || path.isAbsolute(rel))) {
      throw new Error(`Depot RN "${rn.value()}" resolves outside depot root.`);
    }
    return resolved;
  }

  private async _ensureParentDir(filePath: string): Promise<void> {
    const dir = path.dirname(filePath);
    if (!dir) return;
    await fs.promises.mkdir(dir, { recursive: true });
  }

  /** @inheritdoc */
  async createFile(file: DepotFileBase): Promise<string> {
    if (file.rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${file.rn.value()}".`);
    }
    const path = this._pathFromRn(file.rn);
    await this._ensureParentDir(path);
    if (isReadableStream(file.file)) {
      await writeStreamToPath(path, file.file);
    } else {
      await writeBlobToPath(path, file.file as Blob);
    }
    return path;
  }

  /** @inheritdoc */
  async getFile(rn: RN): Promise<DepotFile> {
    if (rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${rn.value()}".`);
    }
    const path = this._pathFromRn(rn);
    return new DepotFile(rn, await readBlobFromPath(path));
  }

  /** @inheritdoc */
  async deleteFile(rn: RN): Promise<boolean> {
    if (rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${rn.value()}".`);
    }
    const path = this._pathFromRn(rn);
    try {
      await fs.promises.unlink(path);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return false;
      }
      throw error;
    }
  }

  /** @inheritdoc */
  async listFiles(rn: RN): Promise<DepotFile[]> {
    if (rn.pointsTo() !== "depotPrefix") {
      throw new Error(`Expected depot prefix RN but got "${rn.value()}".`);
    }
    const dirPath = this._pathFromRn(rn);
    const files: DepotFile[] = [];
    const base = rn.value().replace(/\*$/, "");
    const basePrefix = base.endsWith(":") ? base : `${base}:`;
    const walk = async (currentDir: string) => {
      let entries: fs.Dirent[];
      try {
        entries = await fs.promises.readdir(currentDir, {
          withFileTypes: true,
        });
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") {
          return;
        }
        throw error;
      }
      for (const entry of entries) {
        const entryPath = `${currentDir}/${entry.name}`;
        if (entry.isDirectory()) {
          await walk(entryPath);
          continue;
        }
        if (!entry.isFile()) continue;
        const rel = path.relative(dirPath, entryPath);
        const parts = rel.split(path.sep).filter(Boolean);
        const fileRn = `${basePrefix}${parts.join(":")}`;
        const parsed = RN.create(fileRn);
        if (parsed.isErr()) {
          throw parsed.error;
        }
        files.push(
          new DepotFile(parsed.unwrap(), await readBlobFromPath(entryPath)),
        );
      }
    };
    await walk(dirPath);
    return files;
  }

  /** @inheritdoc */
  async listDirs(rn: RN): Promise<string[]> {
    if (rn.pointsTo() !== "depotPrefix") {
      throw new Error(`Expected depot prefix RN but got "${rn.value()}".`);
    }
    const dirPath = this._pathFromRn(rn);
    const dirs: string[] = [];
    let entries: fs.Dirent[];
    try {
      entries = await fs.promises.readdir(dirPath, { withFileTypes: true });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return [];
      }
      throw error;
    }
    for (const entry of entries) {
      if (entry.isDirectory()) {
        dirs.push(entry.name);
      }
    }
    return dirs;
  }

  /** @inheritdoc */
  async editFile(
    rn: RN,
    edit: (file: DepotFile) => Promise<DepotFileBase>,
  ): Promise<boolean> {
    if (rn.pointsTo() !== "depotFile") {
      throw new Error(`Expected depot file RN but got "${rn.value()}".`);
    }
    const file = await this.getFile(rn);
    const newFile = await edit(file);
    const path = this._pathFromRn(rn);
    await this._ensureParentDir(path);
    if (isReadableStream(newFile.file)) {
      await writeStreamToPath(path, newFile.file);
    } else {
      await writeBlobToPath(path, newFile.file as Blob);
    }
    return true;
  }

  /** @internal */
  getPoolConfig(): { type: "local"; root: string } {
    return { type: "local", root: this._root };
  }
}
