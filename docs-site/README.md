# Felix Documentation

The Felix documentation website is built with [Astro Starlight](https://starlight.astro.build/).

## Local development

Requires Node.js 24 or later.

```bash
npm install
npm run dev
```

The development server prints its local URL and reloads when content changes.

Build and preview the production site:

```bash
npm run build
npm run preview
```

The static site is written to `dist/`.

## Structure

```text
docs-site/
├── public/                  # Files copied directly to the site
├── src/
│   ├── assets/              # Assets imported by Astro
│   ├── content/
│   │   └── docs/            # Markdown documentation
│   └── content.config.ts    # Starlight content collection
├── astro.config.mjs         # Site, integration, and navigation config
├── package.json             # Node dependencies and scripts
└── README.md
```

Every page must have a `title` in its YAML frontmatter. Use standard fenced
code blocks and Markdown tables. Starlight asides use this syntax:

```markdown
:::caution[Important note]
Callout content.
:::
```

Mermaid diagrams use `mermaid` fenced code blocks.

The GitHub Pages workflow in `.github/workflows/pages.yml` deploys the site
when documentation changes are pushed to `main`.
