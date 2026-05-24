# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x     | :white_check_mark: |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to: scot.floess@gmail.com

You should receive a response within 48 hours. If for some reason you do not, please follow up via email to ensure we received your original message.

Please include:
- Type of issue (e.g., path traversal, remote code execution, arbitrary class loading)
- Full paths of source file(s) related to the issue
- Location of the affected source code (tag/branch/commit or direct URL)
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including attack scenario

## Security Considerations for Users

JClassLoader loads and executes classes from potentially untrusted sources. Users should:

### 1. **Only Load Classes from Trusted Sources**
- Validate URLs before adding them as class sources
- Use HTTPS instead of HTTP for remote sources
- Verify authentication credentials are secure

### 2. **Path Traversal Protection**
JClassLoader validates class names to prevent path traversal:
- Rejects class names containing `..`
- Rejects class names with path separators (`/`, `\`)

### 3. **SSL/TLS Validation**
Remote sources use default SSL/TLS validation:
- Certificate chain validation enabled
- Hostname verification enabled
- No custom trust managers that bypass validation

### 4. **Authentication**
When using authenticated sources:
- Use environment variables for credentials (not hardcoded)
- Rotate credentials regularly
- Use least-privilege access (read-only when possible)

## Disclosure Policy

When we receive a security report, we will:
1. Confirm the problem and determine affected versions
2. Audit code to find similar problems
3. Prepare fixes for all supported versions
4. Release patches as quickly as possible
5. Publish security advisories on GitHub

We ask that you:
- Give us reasonable time to fix the issue before public disclosure (90 days)
- Make a good faith effort to avoid privacy violations and data destruction
- Do not exploit the vulnerability beyond proof-of-concept

## Known Security Considerations

### ClassLoader Security
- **Arbitrary Code Execution**: By design, JClassLoader loads and executes arbitrary classes. Only use with trusted sources.
- **Hot Reload**: Improper hot reload can leak memory (see issue #27 for mitigation)

### Network Sources
- **Man-in-the-Middle**: Use HTTPS and verify certificates
- **Credential Exposure**: Never log or expose authentication credentials

### Caching
- **Cache Poisoning**: Validate class bytes before caching
- **Stale Classes**: Clear cache when updating remote sources

## Security Updates

Security updates are released as patch versions (e.g., 1.0.1, 1.1.2) and announced via:
- GitHub Security Advisories
- Release notes
- This SECURITY.md file (updates section)

## Contact

For security concerns, contact the project maintainer through GitHub or the email address listed in the project's pom.xml.
