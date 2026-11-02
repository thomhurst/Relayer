# Documentation dependencies

Use Node.js 22 or later. From this directory, run `npm ci` and `npm run build`.
The production build checks broken links and anchors. CI and the Pages deployment
use Node.js 24 and publish only `build/`; neither `node_modules` nor a Node.js
server is deployed. The npm dependency tree is tooling for this site, separate
from the Relayer NuGet packages.

## Security overrides

`package.json` pins patched transitive dependencies under their immediate
consumers while Docusaurus 3.10.2 still requests older major versions:

- `@docusaurus/core` uses Tinypool 2.1.2 for static rendering workers. This fixes
  both prototype-pollution gadgets,
  [GHSA-5gmw-xhrv-c9v3](https://github.com/advisories/GHSA-5gmw-xhrv-c9v3) and
  [GHSA-85c8-ppgw-ccpr](https://github.com/advisories/GHSA-85c8-ppgw-ccpr).
  Its default export, worker options, `run` and `destroy` APIs match the calls
  Docusaurus makes. Worker rendering is currently disabled by default. When
  changing this override, also validate a temporary config with
  `future.faster.ssgWorkerThreads` and
  `future.v4.removeLegacyPostBuildHeadAttribute` enabled, and build with
  `DOCUSAURUS_SSG_WORKER_THREAD_COUNT=2`.
- `copy-webpack-plugin` and `css-minimizer-webpack-plugin` use
  serialize-javascript 7.1.4. It retains their CommonJS serialization API and
  fixes code injection
  ([GHSA-5c6j-r48x-rmvq](https://github.com/advisories/GHSA-5c6j-r48x-rmvq)),
  array-like object CPU exhaustion
  ([GHSA-qj8w-gfj5-8c6v](https://github.com/advisories/GHSA-qj8w-gfj5-8c6v)),
  and function serialization XSS
  ([GHSA-m5mm-p8h7-m29v](https://github.com/advisories/GHSA-m5mm-p8h7-m29v)).
- `sockjs` uses uuid 11.1.1, which retains `require('uuid').v4()` used by its
  transport. It fixes the v3/v5/v6 output-buffer bounds advisory
  [GHSA-w5hq-g745-h8pq](https://github.com/advisories/GHSA-w5hq-g745-h8pq), even
  though SockJS uses v4 without an output buffer.

These overrides protect build and local server processes. Static hosting does
not make executing untrusted build configuration or plugins safe. Keep local
development and preview servers on trusted interfaces. Remove each override
when its consumer accepts a patched release, then repeat clean installation,
audit and build validation. Do not use `npm audit fix --force` to downgrade
Docusaurus or replace incompatible APIs just to clear the audit.

## Remaining audit findings

The 2026-11-02 audit has no critical or high findings. Its 19 moderate and 3 low
package findings propagate from two advisories; they are not 22 separate
vulnerabilities. They remain visible in `npm audit`, with these dispositions:

- **minimatch**, [GHSA-g3hv-75wg-m249](https://github.com/advisories/GHSA-g3hv-75wg-m249):
  the audit flags every release below 11.0.1. The advisory describes amplification
  with attacker-controlled glob patterns and `optimizationLevel >= 2`. The locked
  tree contains only minimatch 3.1.5 through `serve-handler`; that version has no
  `optimizationLevel` or `firstPhasePreProcess` implementation. `serve-handler`,
  used by `npm run serve`, calls minimatch 3 as a function with configured patterns;
  minimatch 11 exports an object, so a global override breaks that server API.
  Retain the supported version pending a compatible upstream update; reassess if untrusted patterns
  become inputs. GitHub Pages does not run `serve-handler` or glob matching.
- **deepmerge**, [GHSA-gcqx-w48m-w2h4](https://github.com/advisories/GHSA-gcqx-w48m-w2h4):
  4.3.1 is the latest published version and no patched version is listed.
  `@svgr/plugin-svgo` declares this dependency, but its published 8.1.0
  `dist/index.js` does not import it. The advisory requires merging a malicious
  object; no such call is present in that plugin. Retain the finding pending
  an upstream patch or removal rather than downgrade solely to escape the
  reported version range. Reassess this path when updating SVGR.

Run `npm audit --json` after dependency changes; the counts and affected ranges
can change. Track the remediation and original audit in
[issue #101](https://github.com/thomhurst/Relayer/issues/101).
