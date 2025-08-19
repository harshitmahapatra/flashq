# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust-based message queue implementation. The project is in early development with a minimal structure containing only a basic `main.rs` file.

## Development Commands

### Building and Running
- `cargo build` - Build the project
- `cargo run` - Build and run the application
- `cargo build --release` - Build optimized release version

### Testing and Quality
- `cargo test` - Run all tests
- `cargo test <test_name>` - Run a specific test
- `cargo clippy` - Run Rust linter for code quality checks
- `cargo fmt` - Format code according to Rust style guidelines
- `cargo check` - Quick compile check without generating binaries

### Dependencies
- `cargo add <crate_name>` - Add a new dependency
- `cargo update` - Update dependencies to latest compatible versions

## Project Structure

Following Rust best practices with library and binary crates:
- `src/lib.rs` - Library crate containing core message queue functionality
- `src/main.rs` - Binary crate with application entry point  
- `Cargo.toml` - Project configuration using Rust 2024 edition

### Library Crate (`src/lib.rs`)
- `Message` struct - Individual message with content, timestamp, and unique ID
- `MessageQueue` struct - Main queue implementation with topic-based organization
- Thread-safe using `Arc<Mutex<>>` for concurrent access
- All unit tests located here

### Binary Crate (`src/main.rs`)
- Imports and uses the library crate
- Provides example usage and CLI interface

## Architecture Notes

Current implementation features:
- **Topic-based messaging**: Messages are organized by topic strings
- **Non-destructive polling**: Messages remain in queue after being read
- **FIFO ordering**: Messages are returned in the order they were posted  
- **Thread safety**: Safe concurrent access using Arc<Mutex<>>
- **Unique message IDs**: Each message gets an incrementing ID
- **Count limiting**: Poll operations can limit number of messages returned