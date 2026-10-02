//! What a caller using `anyhow` or `color-eyre` gets back through `??`.

use color_eyre::eyre;
use std::{io, sync::Once};
use tracing_subscriber::layer::SubscriberExt as _;

fn write(n: i32) -> io::Result<()> {
    if n == 3 { Err(io::Error::other("disk full")) } else { Ok(()) }
}

#[test]
fn anyhow_gets_the_consumers_error_as_it_was() {
    use anyhow::Context as _;
    let run = || -> anyhow::Result<()> {
        ordair::in_order(0..10)
            .map(|n| n)
            .try_for_each(|n| write(n).with_context(|| format!("writing item {n}")))??;
        Ok(())
    };
    let error = run().unwrap_err();
    assert_eq!(format!("{error:#}"), "writing item 3: disk full");
    assert!(error.downcast_ref::<io::Error>().is_some());
}

#[test]
fn anyhow_gets_a_worker_panic_as_an_error() {
    let run = || -> anyhow::Result<()> {
        ordair::in_order(0..10)
            .map(|n| assert_ne!(n, 3, "boom"))
            .try_for_each(|()| Ok::<_, anyhow::Error>(()))??;
        Ok(())
    };
    let error = run().unwrap_err();
    assert!(error.is::<ordair::Panic>());
    assert!(error.to_string().contains("boom"), "{error}");
}

/// Only the error hook: the panic hook is global and would reformat every
/// other test's panics. Once, as `cargo test` runs these in one process.
fn install_color_eyre() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let (_, eyre_hook) = color_eyre::config::HookBuilder::default().into_hooks();
        eyre_hook.install().expect("no other eyre hook");
    });
}

#[test]
fn color_eyre_keeps_the_span_trace_from_the_consumer() {
    use eyre::WrapErr as _;
    install_color_eyre();
    let subscriber = tracing_subscriber::registry().with(tracing_error::ErrorLayer::default());
    let error = tracing::subscriber::with_default(subscriber, || {
        let run = || -> eyre::Result<()> {
            ordair::in_order(0..10).map(|n| n).try_for_each(|n| {
                let _span = tracing::info_span!("consume", n).entered();
                write(n).wrap_err_with(|| format!("writing item {n}"))
            })??;
            Ok(())
        };
        run().unwrap_err()
    });

    let chain: Vec<_> = error.chain().map(ToString::to_string).collect();
    assert_eq!(chain, ["writing item 3", "disk full"]);
    let handler = error.handler().downcast_ref::<color_eyre::Handler>().expect("color-eyre's");
    let mut spans = Vec::new();
    handler.span_trace().expect("a span trace").with_spans(|span, fields| {
        spans.push(format!("{}{{{fields}}}", span.name()));
        true
    });
    assert_eq!(spans, ["consume{n=3}"]);
}

#[test]
fn color_eyre_gets_a_worker_panic_as_an_error() {
    install_color_eyre();
    let run = || -> eyre::Result<()> {
        ordair::in_order(0..10)
            .map(|n| assert_ne!(n, 3, "boom"))
            .try_for_each(|()| Ok::<_, eyre::Report>(()))??;
        Ok(())
    };
    let error = run().unwrap_err();
    assert!(error.downcast_ref::<ordair::Panic>().is_some());
    assert!(error.to_string().contains("boom"), "{error}");
}
