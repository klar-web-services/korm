import { readFileSync } from "node:fs";

const rawVersion = process.argv[2];
if (!rawVersion) {
  console.error("Usage: bun run release:notes <version>");
  process.exit(1);
}

const normalized = rawVersion.startsWith("v") ? rawVersion.slice(1) : rawVersion;
const changelog = readFileSync(new URL("../CHANGELOG.md", import.meta.url), "utf8");
const lines = changelog.split(/\r?\n/);

const header = `## ${normalized}`;
const startIndex = lines.findIndex((line) => line.trim() === header);
if (startIndex === -1) {
  console.error(`Version ${normalized} not found in CHANGELOG.md`);
  process.exit(1);
}

const bodyLines: string[] = [];
for (let i = startIndex + 1; i < lines.length; i += 1) {
  if (lines[i].startsWith("## ")) break;
  bodyLines.push(lines[i]);
}

const body = bodyLines.join("\n").trim();
if (!body) {
  console.error(`No notes found under ${header}`);
  process.exit(1);
}

console.log(body);
