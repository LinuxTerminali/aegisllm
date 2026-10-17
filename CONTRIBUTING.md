# Contributing to AegisLLM

Thank you for your interest in contributing to AegisLLM! This guide explains how to set up your environment, contribute code, and submit pull requests.

---

## Code of Conduct

We are committed to providing a welcoming, inclusive, and harassment-free experience for everyone. Please be respectful and constructive in all discussions, issues, and code reviews.

---

## Development Setup

### Prerequisites
- **Go**: 1.23 or newer
- **Make**
- **Docker** (optional, for container builds)

### Clone & Build
```bash
git clone https://github.com/LinuxTerminali/aegisllm.git
cd aegisllm

# Build all binaries (aegis, mockupstream, benchmark)
make build
```

---

## Coding Standards

1. **Go Idioms & Standard Library**:
   - Favor Go standard library components wherever practical.
   - Avoid external heavy runtime dependencies (e.g., Redis, external message queues). The scheduler is designed to operate as a self-contained, single-binary proxy.

2. **Concurrency & Thread Safety**:
   - All state mutations must be synchronized with standard concurrency primitives (`sync.Mutex`, `sync.RWMutex`, `atomic`).
   - Run tests with the race detector enabled before committing:
     ```bash
     make test-race
     ```

3. **Code Formatting & Linting**:
   - All code must be formatted using `gofmt`:
     ```bash
     make fmt
     make vet
     ```

4. **Security & Data Privacy**:
   - **Never** log API keys, Bearer tokens, prompt messages, or raw response bodies.
   - Always sanitize errors returned to clients and redact sensitive headers using `upstream.RedactSecrets`.

---

## Pull Request Process

1. Fork the repository and create your branch from `main`:
   ```bash
   git checkout -b feature/my-new-feature
   ```
2. Write unit and integration tests covering your changes.
3. Ensure the test suite passes cleanly:
   ```bash
   make all
   ```
4. Commit your changes with descriptive commit messages following Conventional Commits (e.g., `feat: ...`, `fix: ...`, `docs: ...`).
5. Open a Pull Request against the `main` branch with a clear summary of your changes and motivation.
