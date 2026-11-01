// @ts-check
import {relayerDark, relayerLight} from './src/prism-themes.mjs';

/**
 * Docusaurus renders a "Direct link to ..." anchor inside every heading. Strip those before
 * the llms-txt plugin converts built HTML back to Markdown, so llms-full.txt stays clean.
 *
 * @returns {(tree: any) => void}
 */
function rehypeStripHeadingAnchors() {
  const isHashLink = (/** @type {any} */ node) =>
    node.type === 'element' &&
    node.tagName === 'a' &&
    [].concat(node.properties?.className ?? []).includes('hash-link');

  return (tree) => {
    const walk = (/** @type {any} */ node) => {
      if (!Array.isArray(node.children)) {
        return;
      }
      node.children = node.children.filter((child) => !isHashLink(child));
      node.children.forEach(walk);
    };
    walk(tree);
  };
}

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Relayer',
  tagline: 'Messaging for .NET that wraps your broker’s own client',
  favicon: 'img/favicon.svg',
  url: 'https://relayer.tomlonghurst.dev',
  baseUrl: '/',
  organizationName: 'thomhurst',
  projectName: 'Relayer',
  trailingSlash: false,
  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  markdown: {
    hooks: {onBrokenMarkdownLinks: 'throw'},
  },
  i18n: {defaultLocale: 'en', locales: ['en']},
  stylesheets: [
    {rel: 'preconnect', href: 'https://fonts.googleapis.com'},
    {rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: 'anonymous'},
    {
      rel: 'stylesheet',
      href: 'https://fonts.googleapis.com/css2?family=Fragment+Mono&family=Hanken+Grotesk:ital,wght@0,400..800;1,400..800&display=swap',
    },
  ],
  plugins: [
    [
      '@signalwire/docusaurus-plugin-llms-txt',
      {
        siteTitle: 'Relayer',
        siteDescription:
          'Relayer is a .NET messaging library that wraps the native client for each broker (Kafka through Dekaf, Azure Service Bus, Amazon SQS/SNS, RabbitMQ) and adds resiliency, outbox/inbox, telemetry, serialization, topology and testing.',
        depth: 2,
        enableDescriptions: true,
        content: {
          enableMarkdownFiles: true,
          enableLlmsFullTxt: true,
          relativePaths: false,
          includeDocs: true,
          includeBlog: false,
          includePages: false,
          excludeRoutes: ['/search'],
          beforeDefaultRehypePlugins: [rehypeStripHeadingAnchors],
        },
      },
    ],
  ],
  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      /** @type {import('@easyops-cn/docusaurus-search-local').PluginOptions} */
      ({
        hashed: true,
        indexBlog: false,
        highlightSearchTermsOnTargetPage: true,
        explicitSearchResultPath: true,
      }),
    ],
  ],
  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/thomhurst/Relayer/tree/main/website/',
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {customCss: './src/css/custom.css'},
        sitemap: {changefreq: 'weekly', priority: 0.5},
      }),
    ],
  ],
  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      metadata: [
        {
          name: 'keywords',
          content: '.NET, C#, messaging, Kafka, Azure Service Bus, SQS, RabbitMQ, outbox, OpenTelemetry',
        },
      ],
      colorMode: {defaultMode: 'light', respectPrefersColorScheme: true},
      announcementBar: {
        id: 'preview-status',
        content:
          'Relayer is in early development. The core pipeline works; broker transports and NuGet packages are not released yet. <a href="/docs/roadmap">See the roadmap</a>.',
        isCloseable: true,
      },
      navbar: {
        title: 'Relayer',
        logo: {alt: 'Relayer logo', src: 'img/logo.svg', srcDark: 'img/logo-dark.svg'},
        items: [
          {type: 'docSidebar', sidebarId: 'docsSidebar', position: 'left', label: 'Docs'},
          {to: '/docs/transports', label: 'Transports', position: 'left'},
          {to: '/docs/roadmap', label: 'Roadmap', position: 'left'},
          {href: 'https://tomlonghurst.dev/', label: 'More libraries', position: 'right'},
          {href: 'https://github.com/thomhurst/Relayer', label: 'GitHub', position: 'right'},
          {href: 'https://github.com/sponsors/thomhurst', label: 'Sponsor', position: 'right'},
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Learn',
            items: [
              {label: 'Getting started', to: '/docs/getting-started'},
              {label: 'Core model', to: '/docs/concepts/core-model'},
              {label: 'Sending messages', to: '/docs/guides/sending'},
              {label: 'Failure handling', to: '/docs/guides/failure-handling'},
            ],
          },
          {
            title: 'Project',
            items: [
              {label: 'GitHub', href: 'https://github.com/thomhurst/Relayer'},
              {label: 'Design', href: 'https://github.com/thomhurst/Relayer/blob/main/docs/design.md'},
              {label: 'Decision record', href: 'https://github.com/thomhurst/Relayer/blob/main/docs/decisions.md'},
              {label: 'MIT license', href: 'https://github.com/thomhurst/Relayer/blob/main/LICENSE'},
            ],
          },
          {
            title: 'More libraries',
            items: [
              {label: 'TUnit', href: 'https://tunit.dev/'},
              {label: 'Dekaf', href: 'https://dekaf.tomlonghurst.dev/'},
              {label: 'Respire', href: 'https://respire.tomlonghurst.dev/'},
              {label: 'ModularPipelines', href: 'https://modularpipelines.tomlonghurst.dev/'},
              {label: 'All projects', href: 'https://tomlonghurst.dev/'},
            ],
          },
        ],
        copyright: `Relayer is MIT licensed and built in the open. © ${new Date().getFullYear()} Tom Longhurst.`,
      },
      prism: {
        theme: relayerLight,
        darkTheme: relayerDark,
        additionalLanguages: ['csharp', 'bash', 'json', 'powershell', 'ini'],
      },
    }),
};

export default config;
