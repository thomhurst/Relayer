---
title: Contributing to the docs
description: "Where Relayer's documentation lives, how to run the site locally, and the rule that every change keeps the docs current."
---

## Where documentation lives

| Location | Audience | Content |
|---|---|---|
| `website/docs/` | Users and transport authors | This site: guides, concepts, transport contracts, diagnostics, roadmap |
| `docs/design.md` | Contributors | How Relayer works, including planned features |
| `docs/decisions.md` | Contributors | Why it works that way |
| XML doc comments in `src/` | IDE users | API-level contracts |

A user-facing feature is documented on this site. The design document describes architecture and intent; it links here for usage instead of duplicating it.

## Keep docs current with every change

A pull request that changes behavior, public API, configuration, diagnostics, defaults or status updates the matching page in the same pull request:

- New or changed public API or behavior: the guide that covers it, with a working call site.
- New analyzer or generator diagnostic: [Diagnostics](../reference/diagnostics.md). Its `helpLinkUri` points at that page's anchor.
- A feature moves from planned to implemented: [Roadmap](../roadmap.md) and the status table on the home page.
- New page: add it to `website/sidebars.js`.

Code samples must use the real API and compile against `main`. Mark anything not yet implemented as planned.

## Run the site locally

The site uses [Docusaurus](https://docusaurus.io/) and needs Node.js 22 or later.

```bash
cd website
npm ci
npm start        # live-reloading dev server at http://localhost:3000
npm run build    # production build; fails on broken links and anchors
```

CI builds the site on every pull request that touches `website/`, and deploys it to GitHub Pages from `main`.
