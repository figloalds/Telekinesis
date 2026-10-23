pub mod content;
pub mod errors;
pub mod orchestrator;
pub mod protocol;
pub mod ipc {
    pub const EXIT_INTERNAL: i32 = 70;
    pub const EXIT_FAULT: i32 = 86;
}
