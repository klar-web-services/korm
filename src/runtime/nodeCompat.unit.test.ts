import { describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { pathToFileURL } from "node:url";

describe("Node runtime compatibility", () => {
  test("imports the built package entrypoint under node", () => {
    const distEntry = path.resolve(import.meta.dir, "../../dist/index.js");
    expect(existsSync(distEntry)).toBe(true);
    const entryUrl = pathToFileURL(distEntry).href;
    const script = `
      try {
        await import(${JSON.stringify(entryUrl)});
      } catch (error) {
        const message = error instanceof Error ? error.stack ?? error.message : String(error);
        process.stderr.write(message);
        process.exit(1);
      }
    `;
    const result = spawnSync("node", ["--input-type=module", "-e", script], {
      encoding: "utf8",
      cwd: path.resolve(import.meta.dir, "../.."),
    });
    if (result.error) {
      throw new Error(
        `Node failed to spawn for dist import check.\nerror:\n${String(result.error)}`,
      );
    }
    if (result.status !== 0) {
      throw new Error(
        `Node failed to import dist entrypoint.\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
      );
    }
  });
});
