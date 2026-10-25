import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

// The script that rewrites relative imports in .d.ts files before publishing to npm has to find dist even from a directory with a space in its path
test("the declaration rewrite works from a directory with a space in its path", () => {
  const root = join(mkdtempSync(join(tmpdir(), "usagerail-")), "with space");
  try {
    mkdirSync(join(root, "scripts"), { recursive: true });
    mkdirSync(join(root, "dist"));
    copyFileSync(new URL("../scripts/declaration-imports.mjs", import.meta.url), join(root, "scripts", "declaration-imports.mjs"));
    writeFileSync(join(root, "dist", "index.d.ts"), 'export { a } from "./a.ts";\nexport type { B } from "../b.ts";\nexport { c } from "c.ts";\n');
    execFileSync(process.execPath, [join(root, "scripts", "declaration-imports.mjs")], { stdio: "ignore" });
    assert.equal(readFileSync(join(root, "dist", "index.d.ts"), "utf8"), 'export { a } from "./a.js";\nexport type { B } from "../b.js";\nexport { c } from "c.ts";\n');
  } finally {
    rmSync(join(root, ".."), { recursive: true, force: true });
  }
});
