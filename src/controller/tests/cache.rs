use std::sync::Arc;

use crate::controller::cache::{CacheConfig, CacheController};
use crate::ctx;
use crate::layout::PageGroupId;

#[rstest::rstest]
#[tokio::test]
async fn test_controller_create_layer(
    #[values(0, 128 << 10)] capacity: u64,
    #[values(1, 3, 7, 12)] num_pages: usize,
) {
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(CacheConfig {
        memory_allowance: capacity,
        disable_gc_worker: false,
    });
    let controller = CacheController::new(&ctx);

    let layer1 = controller
        .get_or_create_layer(PageGroupId(1), num_pages)
        .expect("layer should be created");
    let layer2 = controller
        .get_or_create_layer(PageGroupId(1), num_pages)
        .expect("layer should be created");
    assert_eq!(layer1.id(), layer2.id());
    assert!(Arc::ptr_eq(&layer1, &layer2));
}

#[rstest::rstest]
#[tokio::test]
async fn test_controller_layer_controls(#[values(0, 128 << 10)] capacity: u64) {
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(CacheConfig {
        memory_allowance: capacity,
        disable_gc_worker: false,
    });
    let controller = CacheController::new(&ctx);

    let layer1 = controller
        .get_or_create_layer(PageGroupId(1), 2)
        .expect("layer should be created");

    controller.remove_layer(PageGroupId(1));

    let layer2 = controller
        .get_or_create_layer(PageGroupId(1), 2)
        .expect("layer should be created");
    assert_ne!(layer1.id(), layer2.id());
    assert!(!Arc::ptr_eq(&layer1, &layer2));

    controller.reassign_layer(PageGroupId(1), PageGroupId(2));

    let layer3 = controller
        .get_or_create_layer(PageGroupId(2), 2)
        .expect("layer should be created");
    let layer4 = controller
        .get_or_create_layer(PageGroupId(1), 2)
        .expect("layer should be created");
    assert_eq!(layer2.id(), layer3.id());
    assert!(Arc::ptr_eq(&layer2, &layer3));

    assert_ne!(layer2.id(), layer4.id());
    assert!(!Arc::ptr_eq(&layer2, &layer4));
}

#[tokio::test]
async fn test_controller_layer_remove_non_existant_layer() {
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(CacheConfig {
        memory_allowance: 32 << 10,
        disable_gc_worker: false,
    });
    let controller = CacheController::new(&ctx);
    controller.remove_layer(PageGroupId(1));
}

#[tokio::test]
async fn test_controller_reassign_non_existant_group() {
    let ctx = ctx::FileContext::for_test(false).await;
    ctx.set_config(CacheConfig {
        memory_allowance: 32 << 10,
        disable_gc_worker: false,
    });
    let controller = CacheController::new(&ctx);
    controller.reassign_layer(PageGroupId(1), PageGroupId(2));
}
