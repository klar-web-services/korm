import { describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { spawnSync } from "node:child_process";
import path from "node:path";

describe("CLI bin scripts", () => {
  test("generate-encryption-key executes successfully", () => {
    const script = path.resolve(
      import.meta.dir,
      "../../scripts/generate_encryption_key.js",
    );
    expect(existsSync(script)).toBe(true);

    const result = spawnSync(script, [], {
      encoding: "utf8",
      cwd: path.resolve(import.meta.dir, "../.."),
    });

    if (result.error) {
      throw new Error(
        `Failed to spawn generate-encryption-key.\nerror:\n${String(result.error)}`,
      );
    }

    if (result.status !== 0) {
      throw new Error(
        `generate-encryption-key exited non-zero.\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
      );
    }

    expect(result.stdout).toContain("THIS IS YOUR NEW ENCRYPTION KEY:");
    expect(result.stdout).toMatch(/[a-f0-9]{64}/i);
  });
});
