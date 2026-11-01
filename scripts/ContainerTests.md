# Linux container tests

The CI build job prepares its dedicated Linux runner before restoring or running
tests:

```bash
sudo bash scripts/Prepare-ContainerTests.sh
sudo systemctl restart docker
dotnet test -c Release
```

Do not run this setup on a shared Docker host: it changes host networking settings
and restarts Docker. Ordinary local test runs do not require this setup.

Docker reads Linux's dynamic port range when its allocator initializes. The script
sets that range to 20000–60999 and adds 20000–29999 to the kernel's existing reserved
ports. Kernel-assigned outbound sockets therefore avoid the low portion, while
Docker can select and reserve published ports there. Existing reservations are
preserved. A fresh runner's previous outbound range starts at 32768, so its earlier
outbound sockets do not occupy the new low portion either.

This mitigates [moby/moby#54079](https://github.com/moby/moby/issues/54079), where
Docker can exhaust ten bind attempts on occupied outbound ports. It covers
Testcontainers' automatically published resource-reaper port as well as broker
ports. The resource reaper stays enabled; tests retain dynamic Docker port
reservation, parallel execution and all broker assertions. It does not promise
that an explicitly bound host listener or actual port exhaustion cannot fail.

Preparation failures and container startup failures remain test failures. There
is no automatic retry. Hosted failure logs do not identify their conflicting
socket owners; an isolated reproduction establishes the allocator mechanism,
not the destroyed runner's exact socket state.
