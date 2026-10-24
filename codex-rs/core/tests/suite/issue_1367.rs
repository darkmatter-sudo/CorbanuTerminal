//! Issue #1367: the Aggressive/Moderate suites (#902) died with a bare
//! SIGSEGV on macOS, before any test finished. Their sessions run on the
//! threads the test harness and Tokio spawn, whose stack is
//! `RUST_MIN_STACK` (8 MiB in CI), not the 16 MiB the product gives its
//! threads; a frame that steps over the guard page is a SIGSEGV with no
//! overflow message. The test binaries now raise that minimum to 32 MiB
//! whatever the environment sets; these tests fail if a thread spawned the
//! way sessions' threads are gets less.

/// What sessions in tests need (#553, #1367). Spelled out here rather than
/// imported, so these tests still build, and fail, without the fix.
const TEST_THREAD_MIN_STACK_BYTES: usize = 32 * 1024 * 1024;

/// The platform may count its guard page inside a thread's stack size.
const GUARD_ALLOWANCE_BYTES: usize = 64 * 1024;

/// The stack size of the calling thread (not the main thread's).
#[cfg(target_os = "macos")]
fn current_thread_stack_bytes() -> usize {
    // Safety: `pthread_self` is always a valid thread of this process.
    unsafe { libc::pthread_get_stacksize_np(libc::pthread_self()) }
}

/// The stack size of the calling thread (not the main thread's).
#[cfg(target_os = "linux")]
fn current_thread_stack_bytes() -> usize {
    let mut attr = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
    let mut size: libc::size_t = 0;
    // Safety: `pthread_getattr_np` initializes `attr` for this thread (it
    // is read and destroyed only when that succeeded), and `size` is a
    // valid out-pointer.
    unsafe {
        assert_eq!(
            libc::pthread_getattr_np(libc::pthread_self(), attr.as_mut_ptr()),
            0
        );
        assert_eq!(libc::pthread_attr_getstacksize(attr.as_ptr(), &mut size), 0);
        libc::pthread_attr_destroy(attr.as_mut_ptr());
    }
    size
}

#[track_caller]
fn assert_session_stack(thread: &str, bytes: usize) {
    assert!(
        bytes + GUARD_ALLOWANCE_BYTES >= TEST_THREAD_MIN_STACK_BYTES,
        "{thread} has a {bytes}-byte stack, less than the \
         {TEST_THREAD_MIN_STACK_BYTES} bytes sessions in tests need (#1367); \
         RUST_MIN_STACK={:?}",
        std::env::var_os("RUST_MIN_STACK"),
    );
}

/// The test harness spawns each test's thread without a stack size.
#[test]
fn issue_1367_a_thread_spawned_without_a_size_has_the_session_stack() {
    let bytes = std::thread::spawn(current_thread_stack_bytes)
        .join()
        .expect("the thread should not panic");
    assert_session_stack("a thread spawned without a stack size", bytes);
}

/// Sessions run on Tokio's worker and blocking threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn issue_1367_tokio_workers_and_blocking_threads_have_the_session_stack() {
    let worker = tokio::spawn(async { current_thread_stack_bytes() })
        .await
        .expect("the task should not panic");
    assert_session_stack("a Tokio worker thread", worker);
    let blocking = tokio::task::spawn_blocking(current_thread_stack_bytes)
        .await
        .expect("the task should not panic");
    assert_session_stack("a Tokio blocking thread", blocking);
}
