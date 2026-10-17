# Security Policy

## Supported Versions

Security updates are actively applied to the `main` branch.

| Version | Supported          |
| ------- | ------------------ |
| main    | :white_check_mark: |

---

## Reporting a Vulnerability

The AegisLLM team takes security vulnerabilities seriously.

If you believe you have found a security vulnerability in AegisLLM, please do **NOT** report it through public GitHub issues or discussions.

Instead, please report it privately:
- Open a private security report via [GitHub Security Advisories](https://github.com/LinuxTerminali/aegisllm/security/advisories/new).
- Alternatively, open a confidential security advisory or reach out through GitHub.
- Include detailed steps to reproduce the issue, attack vectors, and any relevant logs or proof of concept.

### What to Expect:
1. **Acknowledgment**: You will receive an acknowledgment of your report within 48 hours.
2. **Investigation**: We will investigate and validate the issue, providing updates on remediation timelines.
3. **Patch & Advisory**: Once a fix is ready, we will release a patch and publish a security advisory crediting your discovery (if desired).

---

## Security Architecture Principles

- **Zero API Key Leakage**: API keys and authorization headers are never logged or echoed in error diagnostics.
- **Header Sanitization**: Internal scheduling headers (`X-Run-ID`, `X-Priority`, `X-Workload-Class`) and hop-by-hop headers are removed prior to forwarding upstream.
- **Bounded In-Memory State**: Request queues, token counters, and active run metadata are strictly bounded to prevent memory exhaustion and DoS attacks.
