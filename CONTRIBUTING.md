# Contributing to JClassLoader

Thank you for your interest in contributing to JClassLoader!

## How to Contribute

### Reporting Bugs

1. **Check existing issues** - Your bug may already be reported
2. **Use the bug report template** - Provide all requested information
3. **Include details**:
   - JClassLoader version
   - Java version (output of `java -version`)
   - Operating system
   - Full stack trace
   - Minimal reproducible example

### Suggesting Enhancements

1. **Open a GitHub Issue** with the enhancement label
2. **Describe the use case** - Why is this feature needed?
3. **Propose a solution** - How should it work?
4. **Consider alternatives** - What other approaches could solve this?

### Pull Requests

1. **Fork the repository**
   ```bash
   git clone https://github.com/YOUR-USERNAME/jclassloader.git
   cd jclassloader
   ```

2. **Create a feature branch**
   ```bash
   git checkout -b feature/my-feature
   ```

3. **Make your changes**
   - Write clean, readable code
   - Follow existing code style
   - Add JavaDoc for public APIs
   - No wildcard imports (`import java.util.*` - use specific imports)

4. **Add tests**
   - All new features require unit tests
   - Bug fixes should include regression tests
   - Aim for high test coverage (target: 80%+)
   - Run tests: `mvn clean test`

5. **Update documentation**
   - Update README.md if adding new features
   - Update JavaDoc for API changes
   - Add examples if appropriate

6. **Commit your changes**
   ```bash
   git add .
   git commit -m "Brief description of changes
   
   Detailed explanation of what changed and why.
   
   Fixes #123"
   ```

7. **Push and create Pull Request**
   ```bash
   git push origin feature/my-feature
   ```
   Then open a PR on GitHub

### Code Style Guidelines

#### General
- **No wildcard imports** - Use specific imports (`import java.util.List;` not `import java.util.*;`)
- **Prefer specific exceptions** - Don't throw or catch generic `Exception`
- **Implement AutoCloseable** - For classes that hold resources (connections, files, etc.)
- **Immutable value objects** - Make value objects `final` with `equals/hashCode/toString`

#### JavaDoc
- **All public classes** must have class-level JavaDoc
- **All public methods** must have JavaDoc with:
  - Description of what the method does
  - `@param` for each parameter
  - `@return` for return values
  - `@throws` for checked exceptions
- **Include examples** in JavaDoc when helpful

Example:
```java
/**
 * Loads class data from a remote HTTP/HTTPS source.
 * Supports optional authentication via Basic or Bearer token.
 *
 * @param className The fully qualified class name (e.g., "com.example.MyClass")
 * @return The class bytecode as a byte array
 * @throws IOException if the class cannot be loaded or network error occurs
 */
@Override
public byte[] loadClassData(String className) throws IOException {
    // Implementation
}
```

#### Testing
- **Use JUnit 5** - `@Test`, not JUnit 4
- **Mock external dependencies** - Use Mockito for mocking HTTP clients, databases, etc.
- **Test edge cases** - Null inputs, empty strings, invalid data
- **No integration tests in unit tests** - Mock external services (S3, databases, Kafka)

Example test:
```java
@Test
void testLoadClassDataThrowsIOExceptionOnNetworkError() throws IOException {
    HttpURLConnection mockConnection = mock(HttpURLConnection.class);
    when(mockConnection.getResponseCode()).thenReturn(500);
    
    RemoteClassSource source = new RemoteClassSource("https://example.com");
    
    assertThrows(IOException.class, () -> source.loadClassData("com.example.MyClass"));
}
```

### Testing Your Changes

```bash
# Run all tests
mvn clean test

# Run tests with coverage
mvn clean test jacoco:report

# View coverage report
open target/site/jacoco/index.html

# Run checkstyle (if configured)
mvn checkstyle:check

# Full verification
mvn clean verify
```

### Building the Project

```bash
# Build without tests
mvn clean install -DskipTests

# Build with tests
mvn clean install

# Generate JavaDoc
mvn javadoc:javadoc

# View JavaDoc
open target/site/apidocs/index.html
```

## Code Review Process

1. **Automated checks** run on all PRs:
   - Tests must pass (463+ tests)
   - Code must compile
   - No wildcard imports
   
2. **Maintainer review**:
   - Code quality and style
   - Test coverage
   - Documentation
   - Breaking changes

3. **Feedback and iteration**:
   - Address review comments
   - Update PR as needed
   - Maintainer will merge when ready

## Contribution Agreement

By contributing to JClassLoader, you agree that:
- Your contributions will be licensed under the **GNU General Public License v3.0**
- You have the right to contribute the code (you own it or have permission)
- You understand that contributions may be modified or rejected

## Getting Help

- **GitHub Issues** - Ask questions with the "question" label
- **GitHub Discussions** - For general discussions and design questions
- **Documentation** - Check README.md, JavaDoc, and code examples

## Code of Conduct

This project adheres to the Contributor Covenant Code of Conduct. By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainer.

## Recognition

Contributors are recognized in:
- Git commit history
- GitHub contributors page
- Release notes (for significant contributions)

Thank you for contributing to JClassLoader!
