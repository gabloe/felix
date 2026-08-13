#!/usr/bin/env node
// Parse every ```mermaid block in the repo's Markdown with the same mermaid
// version the docs site renders with, and fail on any that does not parse.
//
// This exists because a broken diagram is invisible until someone loads the
// page: Markdown renders the fenced block happily, CI never looks at it, and
// the first person to find out is a reader who wanted the diagram. Eyeballing
// mermaid is unreliable — `;` silently terminates a statement, so a note
// reading "returns immediately; a timer flushes later" parses as a note plus
// garbage rather than as an error you can see in the source.
//
// Usage: node scripts/check-mermaid.mjs [files...]   (defaults to all tracked .md/.mdx)

import { readFileSync } from 'node:fs';
import { execSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { dirname, resolve } from 'node:path';

// ESM resolves relative specifiers against this file, not the cwd, so anchor
// everything to the repo root explicitly.
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const fromDocsSite = (rel) =>
  pathToFileURL(resolve(REPO, 'docs-site/node_modules', rel)).href;

const { JSDOM } = await import(fromDocsSite('jsdom/lib/api.js'));

// mermaid.parse needs a DOM even though it never renders here.
const dom = new JSDOM('<!doctype html><html><body></body></html>');
globalThis.window = dom.window;
globalThis.document = dom.window.document;
// Node 21+ defines `navigator` as a getter-only global, so it must be replaced
// rather than assigned.
Object.defineProperty(globalThis, 'navigator', {
  value: dom.window.navigator,
  configurable: true,
});
globalThis.DOMPurify = { sanitize: (s) => s, addHook: () => {}, setConfig: () => {} };

const { default: mermaid } = await import(fromDocsSite('mermaid/dist/mermaid.core.mjs'));
mermaid.initialize({ startOnLoad: false, securityLevel: 'loose' });

const files = process.argv.slice(2).length
  ? process.argv.slice(2)
  : execSync('git ls-files "*.md" "*.mdx"', { encoding: 'utf8', cwd: REPO })
      .trim()
      .split('\n')
      .filter(Boolean)
      .map((f) => resolve(REPO, f));

const FENCE = /```mermaid\r?\n([\s\S]*?)```/g;
let blocks = 0;
const failures = [];

for (const file of files) {
  let text;
  try {
    text = readFileSync(file, 'utf8');
  } catch {
    continue;
  }
  const shown = file.startsWith(REPO) ? file.slice(REPO.length + 1) : file;
  for (const match of text.matchAll(FENCE)) {
    blocks += 1;
    const line = text.slice(0, match.index).split('\n').length;
    try {
      await mermaid.parse(match[1]);
    } catch (err) {
      failures.push({ file: shown, line, message: String(err.message ?? err).split('\n').slice(0, 6).join('\n') });
    }
  }
}

for (const f of failures) {
  console.error(`\x1b[31mFAIL\x1b[0m ${f.file}:${f.line}\n${f.message}\n`);
}
console.log(`${blocks} mermaid diagram(s) checked, ${failures.length} invalid`);
process.exit(failures.length === 0 ? 0 : 1);
