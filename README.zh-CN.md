<h1 align="center">miju-services</h1>

<p align="center">
  <strong>集中装配服务，观察就绪，等待实际清理完成。</strong>
</p>

<p align="center">
  <a href="README.md">English</a> ·
  <a href="https://github.com/nanfeng-labs/miju">miju</a> ·
  <a href="LICENSE">MIT</a> · Rust 1.89+
</p>

`miju-services` 是建立在 [miju](https://github.com/nanfeng-labs/miju) 之上的 Rust 服务基座，集中装配普通服务，观察就绪和健康，并等待工作与资源实际收尾。Tokio 运行时、业务配置、资源构造和日志输出由应用负责。

当前未发布的 `0.2` 工作区已实现普通生命周期、应用配置与资源装配示例。已发布的 `0.1.0-beta` 使用另一套 API；其他集成在后续批次接入。

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

当前重构尚未发布，可通过本地路径接入：

```toml
[dependencies]
miju-services = { path = "../miju-services" }
tokio = { version = "1", features = ["rt", "time", "sync", "macros", "signal"] }
```

库不安装信号处理器。示例应用自行启用 Tokio `signal` feature；使用多线程 `#[tokio::main]` 时，还需启用 `rt-multi-thread`。

## 核心概念

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

[configured_app](examples/configured_app/README.md) 展示默认值 → 显式 TOML → 应用环境变量 → CLI，解释各有效值的来源，在校验后打开文件并集中装配采样服务。[watch 示例](examples/watched_settings.rs) 另行展示每轮持有一份不可变配置快照。配置依赖只用于示例，不进入普通库消费者的依赖树。

### 就绪与健康

在监听绑定等真实初始化完成后调用 `context.ready()`。它锁存初始化，并将尚未显式声明的健康设为 `Healthy`；显式 `Unknown`、`Degraded`、`Unhealthy` 会保留。之后健康更新影响当前可用性。本服务已经进入停止或根已释放后的通知不再改变状态。

默认服务为必要的驻留服务。必要服务失败、panic、Resident 就绪超时或意外正常返回会请求全局停止。Finite 表示一次性业务步骤，只有成功完成并实际排空后才初始化依赖，不受 Resident 启动超时限制。可选服务失败只停止并排空自身，不自动重启。只有需要依赖、可选服务、有限步骤或不同工作容量时，才需显式构造 `ServiceSpec`。

### 受管工作与停止

后台工作通过 `context.work().spawn(...)` 或 `spawn_blocking(...)` 接入。有界接纳被拒绝时，业务工厂不会执行。根返回自动关闭接纳，driver 继续等待已接纳工作。依赖提供方在使用方根及所属工作实际释放前保持可用。

普通异步工作可在停止宽限后被 abort；已经开始的阻塞工作须合作结束。`WorkSpec::aggregate` 以明确完成契约覆盖外部执行体：只有正常 `Ok(())` 确认实际结束，错误或 panic 保留未确认责任。普通任务不需要使用它。

宽限到期请求符合条件的 abort，不代表完成，也不允许清理越过工作。未结束的阻塞操作、外部未确认责任或挂起的清理都会保留 driver，阻止产生最终报告。

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

## 示例

| 示例 | 重点 |
| --- | --- |
| [basic](examples/basic.rs) | 注册函数，运行至 Ctrl-C。 |
| [shared_resource](examples/shared_resource.rs) | 强类型设置、构造传参、清理和启动前回滚。 |
| [configured_app](examples/configured_app/README.md) | 多来源配置、强类型设置、集中装配与文件清理。 |
| [watched_settings](examples/watched_settings.rs) | 单轮固定快照与应用持有的配置更新。 |
| [startup_failure](examples/startup_failure.rs) | 必要初始化失败，无须宿主信号即可退出。 |
| [managed_work](examples/managed_work.rs) | 有限前置步骤及服务所属后台工作。 |
| [operational_host](examples/operational_host.rs) | 从启动起观察故障，明确第二次信号策略。 |

## Features 与开发

当前默认 features 为空，普通生命周期可以独立使用。`tracing` 只发事件，`docsrs` 生成 feature 文档。信号处理由应用负责。

```sh
cargo test --all-targets --all-features --locked
cargo test --doc --all-features --locked
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
```

CI 将编译、测试、格式、Clippy、文档及未使用依赖检查拆为独立 jobs，并配置 Linux/macOS/Windows 测试及 Rust 1.89 验证。工作流配置不表示托管任务已经运行通过。
