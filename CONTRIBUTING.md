# Contributing to Rulemorph

Thank you for your interest in contributing to Rulemorph! This document provides guidelines and instructions for contributing.

## Ways to Contribute

- **Report bugs** - Open an issue describing the problem
- **Suggest features** - Open an issue with your idea
- **Submit pull requests** - Fix bugs or implement features
- **Improve documentation** - Help make docs clearer
- **Share feedback** - Tell us about your use cases

## Development Setup

### Prerequisites

- Rust 1.85+ (edition 2024)
- Git

### Getting Started

```bash
# Clone the repository
git clone https://github.com/vinhphatfsg/rulemorph.git
cd rulemorph

# Build all crates
cargo build

# Run tests
cargo test

# Run a specific test
cargo test -p rulemorph test_name
```

### Project Structure

```
crates/
├── rulemorph/      # Core library
├── rulemorph_cli/  # CLI binary
└── rulemorph_mcp/  # MCP server
```

## Pull Request Process

1. **Fork** the repository
2. **Create a branch** for your changes (`git checkout -b feature/my-feature`)
3. **Make your changes** with clear, focused commits
4. **Add tests** for new functionality
5. **Run the test suite** (`cargo test`)
6. **Push** your branch and open a PR

### PR Guidelines

- Keep PRs focused on a single change
- Write clear commit messages
- Add tests for new features or bug fixes
- Update documentation if needed
- Ensure all tests pass

## Code Style

- Follow Rust idioms and conventions
- Use `cargo fmt` before committing
- Use `cargo clippy` to catch common issues
- Keep functions small and focused
- Add doc comments for public APIs

## Running Tests

```bash
# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p rulemorph

# Run with output
cargo test -- --nocapture

# Run performance tests
cargo test -p rulemorph --test performance -- --ignored --nocapture
```

## Reporting Issues

When reporting bugs, please include:

- **Description** - What happened vs what you expected
- **Steps to reproduce** - Minimal example to reproduce the issue
- **Environment** - OS, Rust version, crate version
- **Error messages** - Full error output if applicable

## Feature Requests

When suggesting features:

- **Use case** - Describe the problem you're trying to solve
- **Proposed solution** - Your idea for how to solve it
- **Alternatives** - Other approaches you've considered

## Areas Where Help is Appreciated

- Additional transformation operations
- Performance optimizations
- Documentation improvements
- Language bindings (Python, Node.js)
- Additional input/output formats

## Questions?

Feel free to open an issue with the `question` label if you have any questions about contributing.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
