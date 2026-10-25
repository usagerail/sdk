// Checks in CI the package that goes to npm: in the tarball pnpm pack produces, every file exports points to exists and is compiled JS,
// the type declarations have no leftover relative .ts imports, and framework-independent entry points import directly. The framework adapters need Express, Hono and so on installed, so they're checked by hand before a release
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const FRAMEWORK_ENTRIES = new Set(["./express", "./hono", "./fastify", "./next"]);
const work = mkdtempSync(join(tmpdir(), "usagerail-pack-"));
try {
  execFileSync("pnpm", ["pack", "--pack-destination", work], { stdio: ["ignore", "ignore", "inherit"] });
  const tarball = readdirSync(work).find((name) => name.endsWith(".tgz"));
  execFileSync("tar", ["-xzf", join(work, tarball), "-C", work]);
  const root = join(work, "package");
  const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
  const problems = [];
  if (manifest.private) problems.push("package.json is private");
  for (const [entry, target] of Object.entries(manifest.exports)) {
    if (typeof target !== "object") {
      problems.push(`${entry} still points at ${target}`);
      continue;
    }
    for (const file of [target.types, target.default]) if (!existsSync(join(root, file))) problems.push(`${entry}: ${file} is missing`);
    if (!FRAMEWORK_ENTRIES.has(entry)) await import(pathToFileURL(join(root, target.default)).href);
  }
  for (const name of readdirSync(join(root, "dist"))) {
    if (name.endsWith(".d.ts") && /from\s+["']\.\.?\/[^"']+\.ts["']/.test(readFileSync(join(root, "dist", name), "utf8"))) problems.push(`dist/${name} imports a .ts path`);
  }
  if (problems.length) {
    console.error(problems.join("\n"));
    process.exit(1);
  }
  console.log(`${tarball}: ${Object.keys(manifest.exports).length} entry points, all built; plain entries import cleanly`);
} finally {
  rmSync(work, { recursive: true, force: true });
}
