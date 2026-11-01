/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    'intro',
    'getting-started',
    {
      type: 'category',
      label: 'Concepts',
      items: ['concepts/core-model', 'concepts/capabilities'],
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/messages',
        'guides/handlers',
        'guides/configuration',
        'guides/sending',
        'guides/failure-handling',
        'guides/topology',
        'guides/telemetry',
        'guides/health-checks',
      ],
    },
    {
      type: 'category',
      label: 'Transports',
      link: {type: 'doc', id: 'transports/index'},
      items: [
        'transports/in-memory',
        'transports/authoring',
        'transports/receive',
        'transports/consume-pipeline',
        'transports/envelopes',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: ['reference/diagnostics', 'reference/outbox-stores'],
    },
    'roadmap',
    'contributing/documentation',
  ],
};

export default sidebars;
