# Linux container tests

The CI build job prepares its dedicated Linux runner before restoring or running
tests:

```bash
sudo bash scripts/Prepare-ContainerTests.sh
# Start Docker 28.5.1 or later after preparation. For an existing installation:
sudo systemctl restart docker
bash scripts/Test-ContainerPortAllocation.sh
docker pull apache/kafka:4.0.2
docker pull postgres:18-alpine
dotnet test -c Release
```

Do not run this setup on a shared Docker host: it changes host networking settings
and starts or restarts Docker. Ordinary local test runs do not require this setup.

CI starts a pinned Docker 28.5.1 daemon with `docker/setup-docker-action` after
preparation. Its `set-host: true` exports `DOCKER_HOST`, so both Testcontainers
and the PostgreSQL fixture's Docker CLI use that daemon. Docker 28.0.4 does not
track conflicts between its loopback and wildcard publications correctly: ten
PostgreSQL ports can exhaust Kafka's wildcard bind attempts in the same reserved
range. Docker 28.5.1's allocator checks all addresses when allocating a wildcard
port. The smoke script holds ten Docker-owned loopback publications and then
starts custom-range and automatic wildcard publications. It removes only its two test
containers on exit. See the upstream
[allocator implementation](https://github.com/moby/moby/blob/v28.5.1/libnetwork/portallocator/portallocator.go).

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

CI pulls the pinned Kafka and PostgreSQL images before starting the test process.
On a cold runner, downloading and unpacking Kafka consumed 48 seconds of its
lifecycle tests' two-minute deadline (#195). PostgreSQL's `docker run` commands
also have a 60-second deadline that includes any missing-image acquisition.
Preparing both images removes acquisition from these budgets; it does not
guarantee container startup completes within them. Image preparation fails the
job if either pull fails; it does not retry. Keep these image tags aligned with
their fixtures. Tests retain their deadlines, parallel execution, both restart
cycles, and native protocol assertions. Ordinary local runs still let
Testcontainers and the Docker CLI pull missing images; use the commands above
when validating lifecycle timing.
