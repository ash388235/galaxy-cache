# Getting started: set up, build and test (Windows and Unix)

This is the first page for a new developer or team. In about 30 minutes you will:

- install the tools
- build the project
- run the sanity test that checks all three engines
- start each engine by hand

Commands are shown for **Windows (PowerShell)** and **Unix (Linux, macOS, WSL)**.

## Which page to read

| Page | Read it for |
| --- | --- |
| This page | Setup, build, sanity test, running locally, developer workflow |
| [README.md](../README.md) | What Galaxy Cache is, the modules, the client API |
| [PROVIDERS.md](PROVIDERS.md) | galaxy vs Apache Ignite: support matrix and Ignite setup |
| [ONBOARDING.md](ONBOARDING.md) | Deploying a grid on separate Unix hosts (systemd, TLS, tokens) |
| [WSL-LOCAL-LINUX.md](WSL-LOCAL-LINUX.md) | Linux on a Windows laptop (WSL2), and a 3-node grid in Docker (`scripts/docker-grid.sh`) |
| [RUNBOOK.md](RUNBOOK.md) | Day-to-day operations, failures, restarts |
| [SECURITY-TESTING.md](SECURITY-TESTING.md) | What has been security tested |
| [architecture/](architecture/) | One-page architecture diagram (HTML and PDF) |

## 1. The three ways to run

All three use the same client API (`GalaxyClient`, `GalaxyCache<T>`). Only the config changes.

| Mode | What runs | Config | Good for |
| --- | --- | --- | --- |
| **direct** | File storage, plus 2 index pods (HTTP, ports 7070/7071) | `config/galaxy.yaml`, namespaces `trading`, `risk` | The simplest local setup |
| **grid** | 3 galaxy grid nodes (binary TCP 7101-7103, admin HTTP 7201-7203) | `config/galaxy.yaml`, namespace `grid` | Our own distributed cache |
| **ignite** | 3 Apache Ignite 2.18 servers (ports 47500-47502, 47100-47102, 10800-10802) | `config/galaxy-ignite.yaml` | Same API on Ignite |

The **dashboard** (port 8080) shows queries for direct and grid mode.

## 2. Install the tools

You need:

- **Java 21** (a JDK, not a JRE)
- **Maven 3.9** or newer (3.8 also works)
- **Git**
- **8 GB of RAM**: the sanity test runs about 4 GB of Java processes at once

**Windows**

```powershell
winget install EclipseAdoptium.Temurin.21.JDK
winget install Git.Git
# Maven: download the binary zip from https://maven.apache.org/download.cgi, unzip it to C:\tools\maven,
# then add C:\tools\maven\bin to PATH (or use the Maven bundled with IntelliJ IDEA).
```

Open a new PowerShell window. Allow the project scripts to run in this window:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```

**Ubuntu / Debian / WSL**

```bash
sudo apt update && sudo apt install -y openjdk-21-jdk maven git curl
```

**macOS**

```bash
brew install openjdk@21 maven git
```

**Check (both)**

```
java -version     # must say 21
mvn -version      # must show Java 21 under "Java version"
```

If `mvn -version` shows another Java, set `JAVA_HOME` to the JDK 21 folder.

## 3. Get the code and build

```
git clone https://github.com/ash388235/galaxy-cache.git
cd galaxy-cache
mvn -B package -DskipTests        # fast build of all jars, about 1-2 minutes
```

The first build downloads dependencies from Maven Central. This includes Apache Ignite, about 60 MB.

The build makes these runnable jars:

| Jar | What it is |
| --- | --- |
| `galaxy-index-server/target/galaxy-index-server.jar` | Index pod |
| `galaxy-grid-node/target/galaxy-grid-node.jar` | Grid node |
| `galaxy-ignite-node/target/galaxy-ignite-node.jar` | Ignite server node |
| `galaxy-dashboard/target/galaxy-dashboard.jar` | Dashboard |
| `galaxy-examples/target/galaxy-examples.jar` | Demos and tools |

**Full build with all tests** (about 150 tests; about 3-4 minutes on a 4-core Linux machine, longer on a laptop):

```
mvn -B verify
```

The tests start real index pods, grid nodes and Ignite servers on free local ports. Close other Java servers if a
test complains about ports.

## 4. Run the sanity test

The sanity test:

1. builds the project
2. starts each engine in turn
3. runs the same demo on each one (write 5,000 trades, query, count, try every optional operation, drop)
4. checks that every engine gives the same answers
5. stops everything it started

```powershell
# Windows
powershell -ExecutionPolicy Bypass -File scripts\sanity-test.ps1
```

```bash
# Unix
scripts/sanity-test.sh
```

Options:

| Windows | Unix | What it does |
| --- | --- | --- |
| `-SkipBuild` | `--skip-build` | Use the jars you already built |
| `-WithTests` | `--with-tests` | Run the full test suite during the build |
| `-Only grid` | `--only grid` | Run one engine only (`direct`, `grid` or `ignite`) |

Expected ending:

```
== sanity test results
  direct  PASS
  grid    PASS
  ignite  PASS
```

Each engine prints the same query results: AAPL 481, NVDA and quantity > 9000 49, SELL or TS* 2,767, MSFT count
507. Operations an engine does not have are printed as `not supported`. That is expected, not a failure; see
[PROVIDERS.md](PROVIDERS.md). Logs are in `galaxy-data/run/sanity-*.log`.

## 5. Run each mode by hand

All data, logs and pid files go to `galaxy-data/`, which git ignores.

### Direct mode (file storage + index pods + dashboard)

| | Windows | Unix |
| --- | --- | --- |
| Start pods and dashboard, run the demo | `scripts\start-local.ps1 -SkipBuild` | `scripts/start-index.sh idx-a`<br>`scripts/start-index.sh idx-b`<br>`scripts/start-dashboard.sh`<br>`scripts/run-demo.sh --records 100000` |
| Dashboard | http://localhost:8080 | http://localhost:8080 |
| Stop | `scripts\stop-local.ps1` | `scripts/stop-all.sh` |

### Grid mode (3 galaxy grid nodes)

| | Windows | Unix |
| --- | --- | --- |
| Start the nodes (waits for READY) | `scripts\start-grid.ps1` | `scripts/start-grid.sh` |
| Demo: 2 writers + 1 reader + dashboard | `scripts\run-grid-demo.ps1` | `scripts/run-grid-demo.sh` |
| Dashboard | http://localhost:8080/?namespace=grid | same |
| Status | http://localhost:7201/v1/stats | `scripts/grid-status.sh` |
| Stop | `scripts\stop-local.ps1` | `scripts/stop-grid.sh` |

The grid is **fail-stop**: if one node stops, the others stop too, and clients fail with exit status 75. Restart
all nodes together.

### Ignite mode (3 Apache Ignite servers)

| | Windows | Unix |
| --- | --- | --- |
| Start the servers (waits for READY) | `scripts\start-ignite.ps1` | `scripts/start-ignite.sh` |
| Demo | see below | see below |
| Stop | `scripts\stop-ignite.ps1` | `scripts/stop-ignite.sh` |

```
java "@config/ignite-jvm.options" -cp galaxy-examples/target/galaxy-examples.jar io.galaxycache.examples.ProviderDemo --config config/galaxy-ignite.yaml
```

The same command works in PowerShell. Keep the quotes around `@config/ignite-jvm.options`, because PowerShell
treats a bare `@` as special.

Every client JVM that uses Ignite on Java 21 needs `@config/ignite-jvm.options`. Without it you get a clear error
that says so. The server jar does not need it. With `persistence: true` (the default) the data survives a stop
and start, in `galaxy-data/ignite`.

### The same demo on any engine

`ProviderDemo` is the smallest example of provider-neutral code:

```
java -cp galaxy-examples/target/galaxy-examples.jar io.galaxycache.examples.ProviderDemo --config config/galaxy.yaml --namespace trading   # direct
java -cp galaxy-examples/target/galaxy-examples.jar io.galaxycache.examples.ProviderDemo --config config/galaxy.yaml --namespace grid      # grid
```

## 6. Use it from your own code

The modules are not published to a Maven repository yet. Run `mvn -B install -DskipTests` once to put them in your
local `~/.m2`.

```xml
<!-- always -->
<dependency><groupId>io.galaxycache</groupId><artifactId>galaxy-client</artifactId><version>0.1.0-SNAPSHOT</version></dependency>
<!-- for the galaxy grid -->
<dependency><groupId>io.galaxycache</groupId><artifactId>galaxy-grid-client</artifactId><version>0.1.0-SNAPSHOT</version></dependency>
<!-- for Apache Ignite -->
<dependency><groupId>io.galaxycache</groupId><artifactId>galaxy-ignite</artifactId><version>0.1.0-SNAPSHOT</version></dependency>
```

```java
@GalaxyDomain(name = "Trade", version = 1)
public class Trade {
    @Indexed public String symbol;
    @Indexed public long quantity;
    public double price;
}

try (GalaxyClient client = GalaxyClient.fromConfig(Path.of("config/galaxy.yaml"))) {
    GalaxyCache<Trade> trades = client.cache(DatasetId.of("trading", "equities", "us", LocalDate.now()), Trade.class);
    trades.putAll(Map.of("T-1", t1, "T-2", t2));
    List<Trade> big = trades.query(Query.where(and(eq("symbol", "AAPL"), gt("quantity", 500))));

    if (trades.supports(Capability.TRANSACTIONS)) {            // true on Ignite, false on galaxy
        trades.inTransaction(tx -> { tx.put("T-1", t1); return null; });
    }
}
```

To switch engines, point at another config file. Your code stays the same.

## 7. Developer workflow

**Where things are**

| Area | Location |
| --- | --- |
| Public API (`GalaxyClient`, `GalaxyCache`, `GalaxyCluster`, `Capability`) | `galaxy-cache-api/src/main/java/io/galaxycache/client` |
| Provider SPI | `galaxy-cache-api/.../client/spi` |
| Contract tests every provider must pass | `galaxy-cache-api/src/test/java/io/galaxycache/client/ProviderContractTest.java` |
| galaxy provider | `galaxy-client/.../client/galaxy` |
| Grid client and cluster ops | `galaxy-grid-client` |
| Ignite provider | `galaxy-ignite` |
| Ignite server | `galaxy-ignite-node` |

**Useful commands**

```
mvn -B verify -pl galaxy-ignite-node -am                       # one module plus what it needs
mvn -B verify -pl galaxy-client -am -Dtest=GalaxyFileProviderContractTest -Dsurefire.failIfNoSpecifiedTests=false
mvn -B package -DskipTests -pl galaxy-examples -am             # rebuild the demo jar only
```

**Before you push**

- Run `mvn -B verify`. It must be green.
- Run the sanity test.

**On Windows, new `.sh` scripts** must be marked executable for Unix users:

```
git add --chmod=+x scripts/my-script.sh
```

**IDE**: open the root `pom.xml` in IntelliJ IDEA (or VS Code with the Java extensions) as a Maven project, and set
the project SDK to Java 21. To run Ignite tests from the IDE, add the `ignite.jvm.opens` value from the root
`pom.xml` to the run configuration's VM options. Maven adds it for you.

## 8. Troubleshooting

| Symptom | Fix |
| --- | --- |
| `Address already in use` or a node does not become READY | Something still runs from an earlier start: run the stop script for that mode. On Unix, `scripts/stop-all.sh` stops everything with a pid file in `galaxy-data/run`. |
| `provider: ignite needs extra JVM flags on Java 21` | Start the client JVM with `@config/ignite-jvm.options` (section 5). |
| `No cache provider 'ignite' ... Add the galaxy-ignite jar` | Add the `galaxy-ignite` dependency (section 6). |
| Client exits with status 75 | Fail-stop: a node of the grid or Ignite cluster went away. Restart all nodes, then the client. |
| `different configuration` at startup | The nodes or clients use different config values (servers, partitions, backups, ...). Use the same file everywhere. |
| Ignite does not come back after you changed `partitions` or `servers` | With persistence, old data in `galaxy-data/ignite` keeps the old layout. Stop the nodes and delete `galaxy-data/ignite` (this deletes the data). |
| Start over with empty data | Stop everything, then delete the `galaxy-data` folder. |
| Windows: `running scripts is disabled on this system` | Use `powershell -ExecutionPolicy Bypass -File scripts\...` or `Set-ExecutionPolicy -Scope Process Bypass`. |
| Maven `Could not transfer artifact` | A network or proxy problem reaching Maven Central. Retry, or configure your proxy in `~/.m2/settings.xml`. |
| Windows on ARM | Works with an x64 JDK under emulation, but slower; an ARM64 JDK (Temurin or Microsoft) is faster. |
