<!-- 导航中的文件链接保持绝对路径，并固定到本次发布版本。 -->

<p align="center">
  <img src="https://raw.githubusercontent.com/nanfeng-labs/miju-services/v0.3.0/assets/miju-services-header.svg" alt="miju-services" width="560" />
</p>

<h1 align="center">miju-services</h1>

<div align="center">
  <strong>Rust 应用装配、服务生命周期与受管工作</strong>
</div>

<br />

<div align="center">
  <a href="https://crates.io/crates/miju-services">
    <img src="https://img.shields.io/crates/v/miju-services.svg?label=published" alt="已发布版本" />
  </a>
  <a href="https://docs.rs/miju-services">
    <img src="https://img.shields.io/badge/docs-published-blue.svg" alt="API 文档" />
  </a>
  <a href="https://github.com/nanfeng-labs/miju-services/actions/workflows/ci.yaml">
    <img src="https://github.com/nanfeng-labs/miju-services/actions/workflows/ci.yaml/badge.svg?branch=main" alt="CI" />
  </a>
  <a href="https://www.rust-lang.org/">
    <img src="https://img.shields.io/badge/MSRV-1.89-orange.svg?logo=rust" alt="Rust 1.89 或更新版本" />
  </a>
  <a href="https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/LICENSE">
    <img src="https://img.shields.io/github/license/nanfeng-labs/miju-services" alt="MIT 许可证" />
  </a>
</div>

<br />

<div align="center">
  <a href="https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/README.md">English</a> · 简体中文
</div>

<br />

<div align="center">
  <a href="#功能">功能</a> ·
  <a href="#安装">安装</a> ·
  <a href="#使用">使用</a> ·
  <a href="#示例">示例</a> ·
  <a href="#工作原理">生命周期</a> ·
  <a href="#cargo-features">功能开关</a>
</div>

<br />

建立在 [miju](https://github.com/nanfeng-labs/miju) 之上的 Rust 服务基座。集中装配应用服务，观察就绪与健康状态，并等待后台工作和资源清理实际结束。普通 Rust 接入由应用持有 Tokio 运行时；可选 JNI 宿主为每代应用持有自己的运行时。业务配置、资源以及日志输出位置由应用负责。

## 功能

- **应用装配**：集中注册服务；名称、依赖以及各服务自身的设置在任何服务运行前完成校验。
- **就绪与健康**：确认初始化完成，观察运行期间的可用状态。
- **受管工作**：有界的异步与阻塞工作，可返回值；库不执行的外部执行体以租约（lease）纳入。
- **停止与清理**：先排空使用方，再停止提供方；每个服务拥有自己的宽限期，全部释放后才执行清理。
- **资源**：启动后取得值，在取得处登记关闭动作，并交给声明了依赖的服务使用。
- **miju 接入**：管理 Worker 和 Monitor 的生命周期，通过示例接入 miju-cron 日历 backend。
- **周期操作**：按完成后延时或固定时间网格运行一轮异步工作。
- **可选 JNI 宿主**：导出五个 Java 方法，为每代应用持有独立运行时。

## 安装

需要 **Rust 1.89+**，以及启用了时间功能的 Tokio 运行时。

下方示例使用 `0.3.0` API。0.3 与 0.2 源码不兼容；[CHANGELOG](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/CHANGELOG.md) 附有迁移对照表。

```toml
[dependencies]
miju-services = "0.3.0"
tokio = { version = "1", features = ["rt", "time", "sync", "macros", "signal"] }
```

库不安装信号处理器。示例应用自行启用 Tokio `signal` feature；使用多线程 `#[tokio::main]` 时，还需启用 `rt-multi-thread`。

## 使用

注册一个函数，运行至宿主请求停止，取得最终报告。

```rust,no_run
use miju_services::{App, Context, Result};
use std::time::Duration;

async fn heartbeat(ctx: Context) -> Result {
    ctx.ready();
    loop {
        tokio::select! {
            _ = ctx.stopping() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(5)) => println!("heartbeat"),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result {
    let mut app = App::new();
    app.service("heartbeat", heartbeat);
    // 服务停止、其拥有的一切释放后才返回。
    let report = app.run_until(tokio::signal::ctrl_c()).await?;
    println!("{report}");
    Ok(())
}
```

`cargo run --example basic` 运行同一装配。Ctrl-C 请求停止；服务、已接纳工作和已登记清理全部结束后 `run_until` 才返回，不设期限。必要服务失败也会直接结束应用，无须等待 Ctrl-C。需要有界等待或第二次信号退出策略时，见[嵌入式控制](#嵌入式控制)。

## miju 与周期操作

Worker 的 backend、middleware 和任务函数仍归 miju。工厂收到一个 `Seat`，Worker 在它上面构建：

```rust,no_run
# #[cfg(feature = "miju")]
# async fn worker_example() -> miju_services::Result {
use miju::prelude::{MemoryStorage, WorkerBuilder};
use miju_services::{App, BoxError, miju::worker};

async fn read_device(device: String) -> Result<(), BoxError> {
    println!("read {device}");
    Ok(())
}

let jobs = MemoryStorage::new();
let mut app = App::new();
app.service("devices", worker(move |seat| {
    WorkerBuilder::new(&seat)
        .backend(jobs)
        .build(read_device)
        .stream()
}));
app.run_until(tokio::signal::ctrl_c()).await?;
# Ok(())
# }
```

服务在 Worker 真正 `Start` 时就绪，这不代表远端 backend 已就绪。任务经由受管执行器运行，所有处理函数结束后服务才算排空。backend 失败会使服务失败；任务错误计入 `MijuStats`，不使服务失败。Monitor 是一个分组服务，其重启会被记录，但本身不使报告失败。Worker 或 Monitor 的宽限期到期时仍在运行的处理函数会被中断：持久化 backend 在孤儿超时后重新投递该任务，内存或 cron backend 则丢失它，报告注明该服务被强制结束。见 [worker](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/miju_worker.rs) 与 [monitor](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/miju_monitor.rs)。

简单的轮询操作不需要队列：

```rust
# #[cfg(feature = "periodic")]
# fn periodic_example() {
use miju_services::{App, periodic};
use std::time::Duration;

let mut app = App::new();
app.service("device-status", periodic::every(Duration::from_secs(5), || async {
    println!("collect device status");
    Ok(())
}));
# }
```

`every` 在每轮完成后等待一个周期；`.on_grid()` 使用固定时间槽，跳过长轮次错过的槽。若一轮在第 0 秒开始、第 8 秒结束，周期为五秒，则下一轮分别在第 13 秒或第 10 秒开始。各轮在服务内依次执行，首轮立即运行（`.first_after_period()` 可推迟），一轮的错误先记录再进入下个周期（`.stop_on_error()` 则使服务失败）。没有隐藏的重试。`every_with` 为每轮传入 `Round`，并保留具体错误类型。

日历任务[直接以 miju-cron 作为 backend](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/cron_worker.rs)。进阶的[有界队列示例](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/bounded_queue_rounds.rs)在 miju 执行外增加应用自有的接纳、同目标重叠策略和最终回执。其容量覆盖排队与运行中的工作；停止时完成已取出的任务并取消其余任务。`MemoryStorage` 本身无界，仅限制 Worker 并发不能限制积压。

## 示例

协议使用同一入口。示例内的 `HttpService::new(address, router)` 在绑定后报告就绪，并持有两类租约：每个连接一个，每个请求一个且随响应体转移。最后一个处理函数、Router 克隆和响应体全部释放后服务才排空；拿不到租约的请求在处理函数运行前即以 503 应答。WebSocket 在应答 101 前预留会话，有自己的会话上限（达到时返回 503）；会话使用有界发送队列和两秒关闭预算。UDP 过载时直接丢弃，不排队，并排空已接纳数据报。应用另行保留的资源和自行分离的任务仍由应用负责。

`cargo run --example http` 监听 `127.0.0.1:3000`，`websocket` 为 `ws://127.0.0.1:3001/ws`，`udp` 为 UDP `127.0.0.1:4000`。Ctrl-C 请求停止并等待实际清理。协议适配位于 `examples/support`，不是库的公开 API。

| 示例 | 重点 |
| --- | --- |
| [basic](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/basic.rs) | 注册一个函数，运行至 Ctrl-C。 |
| [shared_resource](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/shared_resource.rs) | 类型化设置、具体 `Service`，以及以资源形式移交预先构建的存储。 |
| [configured_app](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/configured_app/README.md) | 多配置来源、类型化设置、集中装配与文件清理。 |
| [watched_settings](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/watched_settings.rs) | 每轮不可变快照与应用自有的配置更新。 |
| [startup_failure](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/startup_failure.rs) | 必要初始化失败时无须宿主信号即退出。 |
| [managed_work](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/managed_work.rs) | 有限前置步骤与服务所属后台工作。 |
| [operational_host](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/operational_host.rs) | 从启动起观察失败，并明确选择第二次信号策略。 |
| [embedded_runtime](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/embedded_runtime.rs) | 有界等待、仍被持有的责任，以及之后的实际结束。 |
| [miju_worker](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/miju_worker.rs) | 构建一次真实 miju Worker 并管理其生命周期。 |
| [miju_monitor](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/miju_monitor.rs) | 接入分组，保留上游重启与整体结果语义。 |
| [periodic](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/periodic.rs) | 一个周期与一个异步操作，无须队列。 |
| [cron_worker](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/cron_worker.rs) | 直接通过 miju-cron backend 进行日历调度。 |
| [bounded_queue_rounds](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/bounded_queue_rounds.rs) | 进阶：有界接纳、重叠策略、最终回执与排队任务取消。 |
| [http](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/http.rs) | 提供业务路由，绑定后就绪，等待实际释放。 |
| [websocket](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/websocket.rs) | 101 前接纳，保留升级责任，限制会话关闭时间。 |
| [udp](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/udp.rs) | 丢弃过载，排空已接纳数据报。 |

## 工作原理

```mermaid
flowchart LR
    app["App<br/>注册服务与资源"] --> run["启动与运行<br/>初始化、就绪与健康"]
    run --> drain["停止与排空<br/>先使用方，各自宽限"]
    drain --> report["清理与报告<br/>释放资源、记录结果"]
    classDef assembly fill:#fff7ed,stroke:#f97316,color:#7c2d12
    classDef operation fill:#ecfdf5,stroke:#10b981,color:#064e3b
    classDef ending fill:#eff6ff,stroke:#3b82f6,color:#172554
    class app assembly
    class run operation
    class drain,report ending
```

只有所有服务排空、所有清理动作结束后才会产生报告。结果是否成功另行判断：`report.is_success()`。

### 核心概念

- **App**：收集注册、完成校验，并运行唯一的生命周期 driver，由它做出所有启动与停止决定。
- **Service**：`async fn(Context) -> Result`、闭包，或实现 `Service` 的类型（用于保留具体错误或在装配时校验设置）。`Spec` 设定依赖、可选或有限，以及工作容量。
- **Context**：服务与其生命周期交互的入口：`ready()`、`set_health()`、`stopping()`、`spawn()`、`lease()`、`defer()`。
- **Scope**：服务的接纳闸门与释放屏障。工作和租约计入其中；根、工作和租约全部消失时，服务才算排空。
- **Resource**：由一个服务在启动后取得、供其他服务读取的值，与依赖边绑定。
- **Cleanup**：所有服务排空后执行；使用方的先于提供方的，同级中后登记的先执行。

### 装配与资源

通过构造函数直接传普通值、`Arc` 或现成 watch 句柄。需要关闭的值作为资源：启动被接纳后才取得，关闭动作由取得它的代码登记。

```rust
# use miju_services::{App, BoxError, Spec};
# use std::sync::Arc;
# #[derive(Default)]
# struct Store;
# impl Store {
#     async fn open() -> Result<Self, BoxError> { Ok(Self) }
#     async fn close(&self) -> Result<(), BoxError> { Ok(()) }
# }
let mut app = App::new();
let store = app.resource("store", |ctx| async move {
    let store = Arc::new(Store::open().await?);
    let closing = store.clone();
    ctx.defer("store", async move { closing.close().await })?;
    Ok(store)
});
app.service_fn(Spec::new("counter").after(&store), move |ctx| async move {
    let _store = store.get(&ctx)?;
    ctx.ready();
    ctx.stopping().await;
    Ok(())
});
```

启动前不取得任何资源，因此启动被拒绝时无须回滚，`StartError::into_app()` 可取回未启动的 `App`。在 `App` 之前构建的存储也可以同样移交：[共享资源示例](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/shared_resource.rs)在资源运行时登记其关闭动作，此前仍由调用方负责。忘记 `.after(&store)` 的服务每次调用 `get` 都会得到错误，与服务实际启动顺序无关。

多配置来源见 [configured_app](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/configured_app/README.md)：默认值 → 显式 TOML → 应用环境变量 → CLI，解释各有效值的来源，再装配写文件的采样服务；服务在启动后打开输出文件，并在就绪前登记关闭动作。[watch 示例](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/watched_settings.rs)另行展示每轮持有一份不可变配置快照。两者都不进入库的依赖树。

### 就绪与健康

在监听绑定等真实初始化完成后调用 `ctx.ready()`。它锁存初始化，并将尚未显式声明的健康设为 `Healthy`；显式 `Unknown`、`Degraded`、`Unhealthy` 会保留。所有必要服务都已初始化且健康时，应用就绪。停止后或根返回后的调用会被忽略。

服务默认为必要的驻留服务。必要服务失败、panic、就绪超时（默认 30 秒，`app.startup_timeout(d)`）或驻留根意外返回都会停止应用。有限服务（`Spec::finite()`）是一次性步骤：成功并排空后即算初始化完成，不受启动超时限制。可选服务失败只停止并排空自身。没有自动重启。

### 受管工作与停止

`ctx.spawn(name, future)` 和 `ctx.spawn_blocking(name, closure)` 按服务容量（默认 1024）接纳工作，或直接拒绝而不执行。返回的 `Work<T>` 产出该值；丢弃它不会撤销工作，工作仍归服务所有。用 `critical(..)` 包装名称，可使该工作的失败导致服务失败。库不执行的外部执行体（例如框架的连接任务）可取得 `ctx.lease(name)` 并移入其中；租约存在期间服务不能排空。

停止是合作式的。服务在其使用方全部排空后才被要求停止，宽限期（默认 30 秒，`app.shutdown_grace(d)`）从那时起算，因此每个提供方都有完整的窗口。到期后，根与其异步工作被 abort，报告将该服务标记为被强制结束。已经开始的阻塞工作无法中断，服务和报告都等待它返回。停止时尚在排队、未开始的工作会被取消且不执行。

快照是观察结果：单个服务内一致，应用运行期间跨服务并非原子切面。最终报告是。

### 嵌入式控制

宿主需要句柄时使用 `start()`：

```rust
# use miju_services::{App, BoxError};
# use std::time::Duration;
# fn main() -> Result<(), BoxError> {
# let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
# runtime.block_on(async {
let mut app = App::new();
app.service_fn("service", |ctx| async move {
    ctx.ready();
    ctx.stopping().await;
    Ok(())
});
let handle = app.start()?;
tokio::time::timeout(Duration::from_secs(1), handle.ready()).await??;
let report = handle.shutdown().await?;
assert!(report.is_success());
# Ok::<(), BoxError>(())
# })?;
# Ok(())
# }
```

| 操作 | 含义 |
| --- | --- |
| `ready()` | 应用就绪时完成，或给出不会就绪的原因。 |
| `status()` / `changed()` | 当前或下一次变化时的阶段、就绪与健康。 |
| `snapshot()` | 当下组装的所有服务及其仍持有的责任。 |
| `stop()` | 请求按依赖顺序停止。重复调用不改变任何东西。 |
| `wait()` | 实际结束时的最终报告；driver 丢失时为 `Lost`。 |
| `shutdown()` | `stop()` 后 `wait()`。 |
| `observer()` | 只观察、不维持应用运行的句柄。 |

以上操作都不设期限。需要时用 `tokio::time::timeout` 包装：超时只结束该次观察，不影响其他任何东西。`wait()` 超时后，`snapshot().outstanding()` 列出仍被持有的责任。最后一个 `AppHandle` 被丢弃时会请求停止。须保持运行时存活至报告产生；运行时先行消失时，`wait()` 返回 `Lost`，不会返回报告。

当前没有 Tokio 运行时或运行时缺少 time driver 时，`start()` 拒绝启动并交回 `App`。`run_until` 将停止来源的错误与最终报告一并保留。从 `#[tokio::main]` 返回时仍可能等待已开始的阻塞工作；运行时与进程退出策略由应用负责，[运行宿主示例](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/operational_host.rs)展示了明确的第二次信号退出。

## Cargo features

| Feature | 默认启用 | 用途 |
| --- | --- | --- |
| `miju` | 是 | Worker 和 Monitor 生命周期接入。 |
| `periodic` | 是 | 在服务内执行完成后延时或固定时间网格的操作。 |
| `jni` | 否 | 五方法 Java bridge 及每代独立运行时。 |
| `tracing` | 否 | 发出生命周期事件，不安装 subscriber。 |
| `docsrs` | 否 | 在 rustdoc 中生成 feature 参考。 |

`default-features = false` 保留不依赖 miju 的完整生命周期。库关闭 miju 自身的默认 features，由应用选择 middleware。启用 `tracing` 后，driver 对每个记录的服务失败、到期的宽限期和失败的清理动作发出 warning，driver 丢失时发出 error，停止开始和应用结束时发出 info。配置、cron、队列辅助模块、协议及信号处理仍由应用选择。

## JNI 宿主

启用默认关闭的 `jni` feature 后，实现 `jni::AppFactory` 返回未启动的 `App`，通过一次 `jni::export!` 导出五个 Java 静态方法。Java 声明与所有数值代码与 0.2 相同。JNI owner 为每代应用持有自己的 runtime，等待工作、清理及用户引用实际释放后才允许下一代；Java stop 超时（`-124`）只结束该次观察。

工厂返回的 `App` 必须可安全同步析构；资源在启动后取得并以 `ctx.defer` 关闭时即满足此要求。完整的 Rust 装配、Java 声明与真实 UDP 调用见 [JNI 示例](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/examples/jni/README.md)。

## 开发

```sh
cargo test --all-targets --all-features --locked
cargo test --doc --all-features --locked
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
```

CI 将编译、测试、格式、Clippy、文档及未使用依赖检查拆为独立 jobs，配置 Linux/macOS/Windows 测试及 Rust 1.89 验证，并在 Linux 和 macOS 上运行真实 JVM。Windows 桌面 JNI [已为 0.2.0-beta.5 完成验收](https://github.com/nanfeng-labs/miju-services/releases/download/v0.2.0-beta.5/BETA5-WINDOWS-ACCEPTANCE.md)；该证据只覆盖那一源码版本。Android ART 和目标业务应用仍分别验收。

## 许可证

miju-services 采用 [MIT 许可证](https://github.com/nanfeng-labs/miju-services/blob/v0.3.0/LICENSE)。
