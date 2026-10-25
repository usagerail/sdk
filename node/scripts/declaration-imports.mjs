// rewriteRelativeImportExtensions only rewrites the emitted .js; the .d.ts files keep relative imports like "./clock.ts".
// Installed in someone else's project, TypeScript can't find that path in node_modules (there's only clock.d.ts) and the types are lost. This rewrites them all to .js
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

// fileURLToPath rather than .pathname: with characters like spaces in the path, .pathname keeps the %20 form and the directory isn't found
const dist = fileURLToPath(new URL("../dist/", import.meta.url));
let changed = 0;
for (const name of readdirSync(dist)) {
  if (!name.endsWith(".d.ts")) continue;
  const path = join(dist, name);
  const before = readFileSync(path, "utf8");
  const after = before.replace(/(from\s+|import\()(["'])(\.\.?\/[^"']+)\.ts\2/g, "$1$2$3.js$2");
  if (after !== before) {
    writeFileSync(path, after);
    changed += 1;
  }
}
console.log(`rewrote relative .ts imports in ${changed} declaration file(s)`);
