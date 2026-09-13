#!/usr/bin/env node
// `animation` is a single CSS property, not a list that accumulates. When two
// classes on the same element each declare it, the later rule replaces the
// earlier one outright — so an element carrying `.fadeIn` and `.turnRed` runs
// only one of them, and the other silently never happens.
//
// This is worth a check rather than a comment because the failure is invisible:
// the SVG is valid, nothing warns, and the diagram just quietly stops telling
// its story. It shipped once in a draft of `log-append.svg`, where three
// records animated their fill and consequently never faded in at all.
//
// The fix when this fires is always the same: merge the two into one keyframe
// set that animates both properties.
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, dirname, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO = dirname(dirname(fileURLToPath(import.meta.url)));
const ROOTS = ['docs-site/public/diagrams', 'docs/assets'];

/** Every `.svg` under `dir`, recursively. */
function svgs(dir) {
  const found = [];
  const walk = (path) => {
    for (const entry of readdirSync(path)) {
      const full = join(path, entry);
      if (statSync(full).isDirectory()) walk(full);
      else if (entry.endsWith('.svg')) found.push(full);
    }
  };
  try {
    walk(dir);
  } catch {
    // A root that does not exist yet is not a failure.
  }
  return found;
}

/**
 * Class names whose rules set `animation`.
 *
 * Deliberately includes the `animation: none` rules inside
 * `prefers-reduced-motion`: those are declarations too, and an element carrying
 * two of them has the same override problem, just with a different symptom.
 */
function animatedClasses(css) {
  const classes = new Set();
  // Strip at-rule headers so their braces do not confuse the naive rule split;
  // the rules inside them are still visited.
  const flattened = css.replace(/@media[^{]*\{/g, '').replace(/@keyframes[^{]*\{[\s\S]*?\}\s*\}/g, '');
  for (const [, selector, body] of flattened.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    if (!/\banimation\s*:/.test(body)) continue;
    for (const [, name] of selector.matchAll(/\.([A-Za-z0-9_-]+)/g)) classes.add(name);
  }
  return classes;
}

let failures = 0;
let checked = 0;

for (const root of ROOTS) {
  for (const file of svgs(join(REPO, root))) {
    const source = readFileSync(file, 'utf8');
    const css = [...source.matchAll(/<style>([\s\S]*?)<\/style>/g)].map((m) => m[1]).join('\n');
    if (!css.includes('animation')) continue;
    checked += 1;

    const animated = animatedClasses(css);
    const clashes = [];
    for (const [, attr] of source.matchAll(/class="([^"]+)"/g)) {
      const hits = attr.split(/\s+/).filter((name) => animated.has(name));
      if (hits.length > 1) clashes.push({ attr, hits });
    }

    if (clashes.length) {
      failures += 1;
      console.error(`\x1b[31mFAIL\x1b[0m ${relative(REPO, file)}`);
      for (const { attr, hits } of clashes) {
        console.error(`       class="${attr}"`);
        console.error(`       ${hits.join(' and ')} each set \`animation\`; only the last one runs.`);
        console.error('       Merge them into one keyframe set animating both properties.');
      }
    }
  }
}

console.log(
  `${checked} animated diagram(s) checked, ${failures} with conflicting animation classes`,
);
process.exit(failures ? 1 : 0);
