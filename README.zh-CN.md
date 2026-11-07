<p align="center">
  <img src="https://raw.githubusercontent.com/nanfeng-labs/miju-services/main/assets/miju-services-header.svg" alt="miju-services" width="560" />
</p>

<h1 align="center">miju-services</h1>

<div align="center">
  <strong>服务装配、生命周期与受管工作，等待资源实际清理完成。</strong>
</div>

<br />

<div align="center">
  <a href="https://crates.io/crates/miju-services">
    <img src="https://img.shields.io/crates/v/miju-services.svg?label=published" alt="Published crate version" />
  </a>
  <a href="https://docs.rs/miju-services">
    <img src="https://img.shields.io/badge/docs-published-blue.svg" alt="Published API documentation" />
  </a>
  <a href="https://github.com/nanfeng-labs/miju-services/actions/workflows/ci.yaml">
    <img src="https://github.com/nanfeng-labs/miju-services/actions/workflows/ci.yaml/badge.svg?branch=main" alt="CI" />
  </a>
  <a href="https://www.rust-lang.org/">
    <img src="https://img.shields.io/badge/MSRV-1.89-orange.svg?logo=rust" alt="Rust 1.89 or newer" />
  </a>
  <a href="https://github.com/nanfeng-labs/miju-services/blob/main/LICENSE">
    <img src="https://img.shields.io/github/license/nanfeng-labs/miju-services" alt="MIT license" />
  </a>
</div>

<br />

<div align="center">
  <a href="README.md">English</a> · 简体中文
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

建立在 [miju](https://github.com/nanfeng-labs/miju) 之上的 Rust 服务基座。集中装配应用服务，观察就绪与健康状态，并在停止时明确报告后台工作和资源清理的完成情况。普通 Rust 接入由应用持有 Tokio 运行时；可选 JNI 宿主为每代应用持有自己的运行时。业务配置、资源以及日志输出位置由应用负责。

## 功能

- **应用装配**：集中注册服务，校验依赖关系。
- **就绪与健康**：确认初始化完成，观察运行期间的可用状态。
- **受管工作**：限制并跟踪异步、阻塞及外部聚合工作。
- **停止与清理**：先排空使用方，再停止依赖提供方；工作释放后执行具名清理。
- **miju 接入**：管理 Worker 和 Monitor 的生命周期，通过示例接入 miju-cron 日历 backend。
- **直接周期操作**：运行一轮异步工作，支持完成后延时或固定时间网格忙则跳过。
- **可选 JNI 宿主**：导出五个 Java 方法，为每代应用持有独立运行时。

## 安装

需要 **Rust 1.89+**，以及启用了时间功能的 Tokio 运行时。

下方示例使用 `0.2.0-beta.1` API。从 `0.1.0-beta` 迁移时，请先查看 [CHANGELOG](https://github.com/nanfeng-labs/miju-services/blob/v0.2.0-beta.1/CHANGELOG.md) 中的不兼容变更。

```toml
[dependencies]
miju-services = "0.2.0-beta.1"
tokio = { version = "1", features = ["rt", "time", "sync", "macros", "signal"] }
```

库不安装信号处理器。示例应用自行启用 Tokio `signal` feature；使用多线程 `#[tokio::main]` 时，还需启用 `rt-multi-thread`。

## 使用

注册一个函数，运行至宿主请求停止，取得最终报告：

```rust,no_run
use miju_services::{App, BoxError, ServiceContext};
use std::time::Duration;

async fn heartbeat(context: ServiceContext) -> Result<(), BoxError> {
    context.ready();
    loop {
        tokio::select! {
            _ = context.stopping() => break,
            _ = tokio::time::sleep(Duration::from_secs(5)) => println!("heartbeat"),
        }
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let report = App::builder()
        .service_fn("heartbeat", heartbeat)
        .build()?
        .run_until(tokio::signal::ctrl_c())
        .await?;
    println!("{report:?}");
    Ok(())
}
```

`cargo run --example basic` 运行同一装配。Ctrl-C 请求停止；服务、已接纳工作及登记的清理实际结束后，调用才返回。必要服务失败也会结束应用，无须等到 Ctrl-C。

## miju 与周期操作

miju 继续负责 backend、middleware、作业函数及重试。`worker_from` 的工厂取得与生命周期控制共用的 context：

```rust,no_run
# #[cfg(feature = "miju")]
# async fn worker_example() -> Result<(), miju_services::BoxError> {
use miju::prelude::{MemoryStorage, WorkerBuilder};
use miju_services::{App, miju::worker_from};

let jobs = MemoryStorage::new();
App::builder()
    .service("devices", worker_from(move |context| {
        WorkerBuilder::new(context)
            .backend(jobs)
            .build(|device: String| async move { println!("Read {device}"); })
            .stream()
    }))
    .build()?
    .run_until(tokio::signal::ctrl_c())
    .await?;
# Ok(())
# }
```

工厂必须使用传入 context，不另建控制身份。Start 只确认 worker 驱动建立，不代表远端 backend 已就绪。停止后桥接继续驱动同一 stream；作业错误在快照中可见。backend 失败或 Monitor 重启历史会保留未确认的聚合责任，阻止错误地认证清理完成。Monitor 整体作为一个服务，内部 worker 故障可能被常驻兄弟延迟汇总。见 [worker](examples/miju_worker.rs) 与 [monitor](examples/miju_monitor.rs)。

简单轮询只需要周期和一轮操作，不必使用队列：

```rust
# #[cfg(feature = "periodic")]
# fn periodic_example() -> Result<(), miju_services::BoxError> {
use miju_services::{App, periodic};
use std::time::Duration;

let app = App::builder()
    .service("device-status", periodic::fixed_delay(Duration::from_secs(5), || async {
        println!("Collect device status");
        Ok(())
    })?)
    .build()?;
# let _ = app;
# Ok(())
# }
```

`fixed_delay` 在完成后等待；`fixed_rate_skip` 使用固定时槽，跳过忙碌或错过的机会。若 0 秒开始、8 秒结束、周期 5 秒，下轮分别在 13 秒或 10 秒。默认立即首轮、串行执行，业务错误记录后继续下一周期，没有隐式重试。需要延迟首轮、具体错误、合作停止或失败升级时使用 `PeriodicRule` 与 `RoundContext`。

[日历示例](examples/cron_worker.rs) 直接以 miju-cron 为 backend。[高级队列示例](examples/bounded_queue_rounds.rs) 在 miju 执行之外加入应用自己的接纳、目标重叠策略和最终回执：容量同时覆盖排队与执行；停止完成已取作业并取消未取作业；回执在完整重试链结束后发布。`MemoryStorage` 本身无界，worker 并发限制不等于积压上限。

## 示例

协议沿用同一个 App 入口。HTTP 只需提供 Router，再注册示例内的 `HttpService::new(address, router)`；它在 bind 后确认 ready，停止后等待框架及 Router、请求 future 和响应 body 实际释放。WebSocket 在返回 101 前接纳会话，满载返回 503；升级回调的 socket 转交或释放也进入 HTTP 屏障。会话使用有界出站队列与两秒关闭预算。UDP 满载丢弃、不排队，已接纳数据报参与排空。另行保留的资源和裸 spawn 仍由应用负责。

`cargo run --example http` 监听 `127.0.0.1:3000`；`websocket` 为 `ws://127.0.0.1:3001/ws`；`udp` 为 UDP `127.0.0.1:4000`。Ctrl-C 请求停止并等待实际清理。`embedded_runtime` 可直接运行，展示 Pending 后保留 runtime 继续等待。协议适配器位于 `examples/support`，不是库公开 API。

| 示例 | 重点 |
| --- | --- |
| [basic](examples/basic.rs) | 注册函数，运行至 Ctrl-C。 |
| [shared_resource](examples/shared_resource.rs) | 强类型设置、构造传参、清理和启动前回滚。 |
| [configured_app](examples/configured_app/README.md) | 多来源配置、强类型设置、集中装配与文件清理。 |
| [watched_settings](examples/watched_settings.rs) | 单轮固定快照与应用持有的配置更新。 |
| [startup_failure](examples/startup_failure.rs) | 必要初始化失败，无须宿主信号即可退出。 |
| [managed_work](examples/managed_work.rs) | 有限前置步骤及服务所属后台工作。 |
| [operational_host](examples/operational_host.rs) | 从启动起观察故障，明确第二次信号策略。 |
| [miju_worker](examples/miju_worker.rs) | 一次构造真实 miju worker 并管理其生命周期。 |
| [miju_monitor](examples/miju_monitor.rs) | 按上游重启与聚合结果语义接入一组 worker。 |
| [periodic](examples/periodic.rs) | 周期与一轮异步操作，无须队列。 |
| [cron_worker](examples/cron_worker.rs) | 直接使用 miju-cron 日历 backend。 |
| [bounded_queue_rounds](examples/bounded_queue_rounds.rs) | 高级有界接纳、重叠、最终回执及未取作业取消。 |
| [http](examples/http.rs) | 提供业务 Router，bind 后就绪，等待实际释放。 |
| [websocket](examples/websocket.rs) | 101 前接纳、升级释放屏障和有界会话关闭。 |
| [udp](examples/udp.rs) | 满载丢弃与已接纳数据报排空。 |
| [embedded_runtime](examples/embedded_runtime.rs) | Pending 后保留运行时，直到实际清理结束。 |

## 工作原理

```mermaid
flowchart LR
    app["App<br/>注册服务与清理"] --> run["启动与运行<br/>初始化、就绪与健康"]
    run --> drain["停止与排空<br/>依赖顺序、工作归属"]
    drain --> report["清理与报告<br/>释放资源、记录结果"]
    classDef assembly fill:#fff7ed,stroke:#f97316,color:#7c2d12
    classDef operation fill:#ecfdf5,stroke:#10b981,color:#064e3b
    classDef ending fill:#eff6ff,stroke:#3b82f6,color:#172554
    class app assembly
    class run operation
    class drain,report ending
```

正常合作停止时，工作实际释放后才进入清理并生成最终报告。预算到期不会让尚未结束的责任消失。

### 核心概念

- **App**：集中注册服务，校验依赖，驱动生命周期。
- **Service**：持有自己的设置和依赖。异步函数使用 `service_fn`，实现 `Service` 的对象使用 `service`。
- **WorkScope**：跟踪一个服务所属的后台工作。丢弃工作句柄或取消等待，不会撤销已接纳责任。
- **Cleanup**：服务根和所属工作实际释放责任后，按登记逆序执行具名异步清理。

### 装配与资源

通过构造函数直接传普通值、`Arc` 或现成 watch 句柄。核心不按类型查找资源，也不加载业务配置。

```rust
# use miju_services::{App, BoxError};
# use std::sync::Arc;
# #[derive(Default)]
# struct Store;
# impl Store { async fn close(&self) -> Result<(), BoxError> { Ok(()) } }
# fn main() -> Result<(), BoxError> {
let store = Arc::new(Store::default());
let cleanup = store.clone();
let app = App::builder()
    .cleanup("store", move || async move { cleanup.close().await })
    .service_fn("counter", move |context| async move {
        let _store = store;
        context.ready();
        context.stopping().await;
        Ok(())
    })
    .build()?;
# let _ = app;
# Ok(())
# }
```

`start` 接纳前，回滚归应用 bootstrap 负责。应用须保留回滚所需的独立访问能力；丢弃 builder 或尚未启动的 `App` 无法执行异步清理。缺少 Tokio 运行时导致的启动拒绝可用 `StartError::into_app()` 取回装配。接纳后，登记的清理归 driver 负责，包含部分初始化失败的收尾。[共享资源示例](examples/shared_resource.rs) 展示这个边界及具体 `Service` 实现。

[configured_app](examples/configured_app/README.md) 展示默认值 → 显式 TOML → 应用环境变量 → CLI，解释各有效值的来源，再集中装配采样服务。服务在启动接纳后打开文件，并在就绪前登记 `defer_cleanup`；尚未启动的 App 无须回滚文件资源。[watch 示例](examples/watched_settings.rs) 另行展示每轮持有一份不可变配置快照。配置依赖只用于示例，不进入普通库消费者的依赖树。

### 就绪与健康

在监听绑定等真实初始化完成后调用 `context.ready()`。它锁存初始化，并将尚未显式声明的健康设为 `Healthy`；显式 `Unknown`、`Degraded`、`Unhealthy` 会保留。之后健康更新影响当前可用性。本服务已经进入停止或根已释放后的通知不再改变状态。

默认服务为必要的驻留服务。必要服务失败、panic、Resident 就绪超时或意外正常返回会请求全局停止。Finite 表示一次性业务步骤，只有成功完成并实际排空后才初始化依赖，不受 Resident 启动超时限制。可选服务失败只停止并排空自身，不自动重启。只有需要依赖、可选服务、有限步骤或不同工作容量时，才需显式构造 `ServiceSpec`。

### 受管工作与停止

后台工作通过 `context.work().spawn(...)` 或 `spawn_blocking(...)` 接入。有界接纳被拒绝时，业务工厂不会执行。根返回自动关闭接纳，driver 继续等待已接纳工作。依赖提供方在使用方根及所属工作实际释放前保持可用。

普通异步工作可在停止宽限后被 abort；已经开始的阻塞工作须合作结束。`WorkSpec::aggregate` 以明确完成契约覆盖外部执行体：只有正常 `Ok(())` 确认实际结束，错误或 panic 保留未确认责任。普通任务不需要使用它。

宽限到期请求符合条件的 abort，不代表完成，也不允许清理越过工作。未结束的阻塞操作、外部未确认责任或挂起的清理都会保留 driver，阻止产生最终报告。

`run_until()` 和 `shutdown()` 不设等待期限。需要有界观察时，使用 `start()` 取得句柄，再调用 `shutdown_within(duration)`；`Pending` 表示责任尚未结束。宿主可以保留运行时继续等待，或像 [operational_host](examples/operational_host.rs) 一样明确选择进程退出策略。超时不会释放资源，也不会生成最终报告。

停止宽限从应用的首次停止请求起算，由全部服务共享。provider 只能使用消费者排空后的剩余时间，可能在获得合作停止的执行机会前就被 abort。必要的异步收尾应登记为 cleanup，不能只放在 provider 根的停止分支中。

快照由 driver 提供。直接读取 `ServiceSnapshot` 字段，或在解构时使用 `..`，可避免其他依赖启用适配器 feature 后导致模式匹配无法编译。

### 嵌入式控制

宿主需要控制句柄时使用 `build()?.start()?`：

```rust
# use miju_services::{App, BoxError};
# use std::time::Duration;
# fn main() -> Result<(), BoxError> {
# let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
# runtime.block_on(async {
let handle = App::builder()
    .service_fn("service", |context| async move {
        context.ready();
        context.stopping().await;
        Ok(())
    })
    .build()?
    .start()?;
handle.ready(Duration::from_secs(1)).await?;
let report = handle.shutdown().await?;
assert!(report.is_success());
# Ok::<(), BoxError>(())
# })?;
# Ok(())
# }
```

| 操作 | 含义 |
| --- | --- |
| `ready(within)` | 等待当前应用就绪；超时只结束本次观察。 |
| `snapshot()` | 读取阶段、健康和未完成责任。 |
| `stop()` | 请求一次停止；重复调用不延长内部预算。 |
| `wait()` | 观察实际最终报告，包括不成功结果。 |
| `shutdown()` | 请求停止，等实际结束，再分类失败。 |
| `shutdown_within(within)` | 在本次观察时限内返回 `Complete(report)` 或 `Pending(snapshot)`。 |

`Complete` 表示实际结束；成功与否查看 `report.is_success()`。取消观察不会取消 driver。最后一个控制句柄释放会请求停止；observer 和工作句柄不是控制租约。宿主须保持运行时存活至收尾结束；运行时丢失可以得到 `DriverLost`，不能得到清理完成证书。

宿主还必须启用 Tokio time driver。`start()` 会检查当前 runtime 是否存在，但无法确认它具备计时能力；缺少 time driver 时，服务已被接纳后，生命周期 driver 可能在首次轮询计时器时 panic 丢失。服务根任务可能继续运行而失去生命周期监管，cleanup 和最终报告均不保证完成。

`run_until` 保留停止来源错误和最终报告。终态观察与停止来源同时就绪时，终态分支优先。需要运行诊断及第二次信号强退策略的宿主可参考[运行宿主示例](examples/operational_host.rs)；强退由应用决定，不产生完成证书。

## Cargo features

| Feature | 默认启用 | 用途 |
| --- | --- | --- |
| `miju` | 是 | Worker 和 Monitor 生命周期接入。 |
| `periodic` | 是 | 串行执行完成后延时或固定时间网格忙则跳过的操作。 |
| `jni` | 否 | 五方法 Java bridge 及每代独立运行时。 |
| `tracing` | 否 | 发出生命周期事件，不安装 subscriber。 |
| `docsrs` | 否 | 在 rustdoc 中生成 feature 参考。 |

默认 features 为 `miju` 和 `periodic`，两者也可单独启用。`default-features = false` 保留不依赖 miju 的完整普通生命周期。库关闭 miju 自身的默认 features，由应用选择 middleware。`tracing` 只发出生命周期事件，不安装 subscriber；`docsrs` 生成 feature 文档。配置、cron、队列辅助模块、协议及信号处理仍由应用选择。

## JNI 宿主

普通 Rust 应用继续拥有 Tokio runtime。启用默认关闭的 `jni` feature 后，工厂实现 `jni::AppFactory` 并返回未启动 `App`，通过一次 `jni::export!` 导出五个 Java 静态方法。JNI owner 为每代应用持有自己的 runtime，等待工作、清理及用户引用实际释放后才允许下一代；Java stop 超时只结束该次观察。

工厂返回的未启动 App 必须可安全同步析构。需要异步关闭的资源在服务初始化中取得，并在 ready 前登记清理；builder cleanup 不代替启动前回滚。完整的 Rust 装配、Java bridge 与真实 UDP 调用见 [JNI 示例](examples/jni/README.md)。

## 开发

```sh
cargo test --all-targets --all-features --locked
cargo test --doc --all-features --locked
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
```

CI 将编译、测试、格式、Clippy、文档及未使用依赖检查拆为独立 jobs，并配置 Linux/macOS/Windows 测试及 Rust 1.89 验证。

## 许可证

miju-services 采用 [MIT 许可证](https://github.com/nanfeng-labs/miju-services/blob/main/LICENSE)。
