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
        'guides/sagas',
        'guides/saga-timeouts',
        'guides/configuration',
        'guides/sending',
        'guides/multi-tenancy',
        'guides/request-response',
        'guides/failure-handling',
        'guides/topology',
        'guides/saga-diagrams',
        'guides/telemetry',
        'guides/health-checks',
        'guides/testing',
      ],
    },
    {
      type: 'category',
      label: 'Transports',
      link: {type: 'doc', id: 'transports/index'},
      items: [
        'transports/in-memory',
        'transports/kafka',
        'transports/service-bus',
        'transports/service-bus-receive',
        'transports/rabbitmq',
          'transports/nats',
        'transports/authoring',
        'transports/conformance',
        'transports/receive',
        'transports/consume-pipeline',
        'transports/partition-completion',
        'transports/partition-scheduler',
        'transports/envelopes',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: ['reference/diagnostics', 'reference/outbox-stores', 'reference/postgres-schema', 'reference/postgres-store', 'reference/postgres-outbox', 'reference/ef-core-outbox', 'reference/tenant-stores', 'reference/direct-dispatch', 'reference/ordered-dispatch', 'reference/ef-core-inbox', 'reference/postgres-maintenance'],
    },
    'roadmap',
    'contributing/documentation',
    'contributing/performance',
    'contributing/releases',
  ],
};

export default sidebars;
