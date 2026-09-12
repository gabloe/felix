#!/usr/bin/env node
// Hand-authored SVGs are committed twice: once under `docs/assets/` so GitHub
// renders them in the repo docs, and once under `docs-site/public/diagrams/` so
// the site serves them. Nothing generates one from the other, so editing one and
// forgetting the other leaves two diagrams claiming to be the same picture.
//
// Only diagrams that genuinely exist in both places are paired. Most do not:
// the storage-format figures are referenced from `docs/` alone, and the perf
// charts are generated. A one-sided diagram is a deliberate choice, not drift,
// so this checks the one thing that is always wrong -- two copies of the same
// name that no longer agree.
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO = dirname(dirname(fileURLToPath(import.meta.url)));
const SITE = join(REPO, 'docs-site/public/diagrams');
const DOCS = join(REPO, 'docs/assets');

/** Every `.svg` under `dir`, keyed by basename. */
function svgs(dir) {
  const found = new Map();
  const walk = (path) => {
    for (const entry of readdirSync(path)) {
      const full = join(path, entry);
      if (statSync(full).isDirectory()) walk(full);
      else if (entry.endsWith('.svg')) found.set(entry, full);
    }
  };
  try {
    walk(dir);
  } catch {
    // A missing directory is not a drift failure; there is simply nothing to pair.
  }
  return found;
}

const site = svgs(SITE);
const docs = svgs(DOCS);
const failures = [];
let paired = 0;

for (const [name, sitePath] of site) {
  const docsPath = docs.get(name);
  if (!docsPath) continue;
  paired += 1;
  if (readFileSync(sitePath, 'utf8') !== readFileSync(docsPath, 'utf8')) {
    failures.push(
      `${name}: docs/assets and docs-site/public/diagrams have drifted apart`,
    );
  }
}

for (const f of failures) console.error(`\x1b[31mFAIL\x1b[0m ${f}`);
console.log(`${paired} duplicated diagram(s) checked, ${failures.length} out of sync`);
process.exit(failures.length === 0 ? 0 : 1);
