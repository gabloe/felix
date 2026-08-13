import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import mermaid from 'astro-mermaid';

export default defineConfig({
  site: 'https://gabloe.github.io',
  base: '/felix',
  integrations: [
    mermaid({ autoTheme: true, enableLog: false }),
    starlight({
      // Fonts are self-hosted via @fontsource so the site has no runtime
      // dependency on Google Fonts, and so first paint is not gated on a
      // third-party connection. Inter for UI/prose, JetBrains Mono for the
      // 1710 code fences. Order matters: these must land before custom.css,
      // which is what actually points `--sl-font`/`--sl-font-mono` at them.
      //
      // Layout is tuned for code- and table-heavy technical docs; see the
      // custom.css header for why Starlight's defaults do not fit this content.
      customCss: [
        '@fontsource-variable/inter',
        '@fontsource-variable/jetbrains-mono',
        './src/styles/custom.css',
      ],
      title: 'Felix',
      description: 'Low-latency QUIC-based pub/sub and distributed cache system',
      logo: {
        src: './src/assets/logo.png',
        alt: 'Felix',
      },
      favicon: '/logo.png',
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/gabloe/felix',
        },
      ],
      editLink: {
        baseUrl: 'https://github.com/gabloe/felix/edit/main/docs-site/',
      },
      lastUpdated: true,
      sidebar: [
        { label: 'Home', slug: 'index' },
        {
          label: 'Getting Started',
          items: [
            { label: 'What Felix Is For', slug: 'getting-started/what-felix-is-for' },
            { label: 'Overview', slug: 'getting-started/overview' },
            { label: 'Quickstart', slug: 'getting-started/quickstart' },
            { label: 'Installation', slug: 'getting-started/installation' },
          ],
        },
        {
          label: 'Demos',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'demos/overview' },
            { label: 'Slow-consumer Isolation', slug: 'demos/slow-consumer-isolation' },
            { label: 'Local State Divergence', slug: 'demos/state-divergence' },
            { label: 'Notifications', slug: 'demos/notifications' },
            { label: 'Orders Pipeline', slug: 'demos/orders' },
          ],
        },
        {
          label: 'Architecture',
          collapsed: true,
          items: [
            { label: 'System Design', slug: 'architecture/system-design' },
            { label: 'Components', slug: 'architecture/components' },
            { label: 'Wire Protocol', slug: 'architecture/wire-protocol' },
            { label: 'Semantics', slug: 'architecture/semantics' },
          ],
        },
        {
          label: 'API Documentation',
          collapsed: true,
          items: [
            { label: 'Broker API', slug: 'api/broker-api' },
            { label: 'Control Plane API', slug: 'api/control-plane-api' },
            { label: 'Client SDK', slug: 'api/client-sdk' },
          ],
        },
        {
          label: 'Features',
          collapsed: true,
          items: [
            { label: 'QUIC Transport', slug: 'features/quic-transport' },
            { label: 'Pub/Sub Streaming', slug: 'features/pubsub' },
            { label: 'Distributed Cache', slug: 'features/cache' },
            { label: 'Performance Tuning', slug: 'features/performance' },
            { label: 'Performance & Platform Notes', slug: 'features/performance-platform-notes' },
            { label: 'Benchmarks', slug: 'features/benchmarks' },
            { label: 'Observability', slug: 'features/observability' },
            { label: 'Security', slug: 'features/security' },
          ],
        },
        {
          label: 'Deployment',
          collapsed: true,
          items: [
            { label: 'Local Development', slug: 'deployment/local' },
            { label: 'Docker Compose', slug: 'deployment/docker-compose' },
            { label: 'Kubernetes', slug: 'deployment/kubernetes' },
            { label: 'Graceful Shutdown', slug: 'deployment/graceful-shutdown' },
          ],
        },
        {
          label: 'Reference',
          collapsed: true,
          items: [
            { label: 'Configuration', slug: 'reference/configuration' },
            { label: 'Environment Variables', slug: 'reference/environment-variables' },
            { label: 'Troubleshooting', slug: 'reference/troubleshooting' },
            { label: 'FAQ', slug: 'reference/faq' },
          ],
        },
        {
          label: 'Development',
          collapsed: true,
          items: [
            { label: 'Contributing', slug: 'development/contributing' },
            { label: 'Building & Testing', slug: 'development/building' },
            { label: 'Project Structure', slug: 'development/project-structure' },
            { label: 'How Felix Works', slug: 'development/how-felix-works' },
            { label: 'Internals: The Publish Path', slug: 'development/internals-publish' },
            { label: 'Internals: Subscribe & Fanout', slug: 'development/internals-subscribe' },
            { label: 'Internals: Backpressure & Core Sharding', slug: 'development/internals-concurrency' },
          ],
        },
      ],
    }),
  ],
});
