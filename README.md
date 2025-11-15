# Journal

[![Build Status][build-img]][build-url]
[![Coverage][cov-img]][cov-url]
[![Documentation][doc-img]][doc-url]
[![License][license-img]][license-url]

[build-img]: https://github.com/sandesh-sanjeev/journal/actions/workflows/ci.yml/badge.svg?branch=master
[build-url]: https://github.com/sandesh-sanjeev/journal/actions/workflows/ci.yml

[doc-img]: https://img.shields.io/badge/crate-doc-green?style=flat
[doc-url]: https://sandesh-sanjeev.github.io/Journal/journal/index.html

[cov-img]: https://coveralls.io/repos/github/sandesh-sanjeev/journal/badge.svg?branch=master
[cov-url]: https://coveralls.io/github/sandesh-sanjeev/journal?branch=master

[license-img]: https://img.shields.io/badge/License-MIT-yellow.svg
[license-url]: https://opensource.org/licenses/MIT

## Testing

We use `cargo-nextest` to execute tests.

```bash
$ cargo install cargo-nextest
```

Now you can run tests with nextest.

```bash
$ cargo nextest run
```

### Coverage

We use `cargo-llvm-cov` to collect coverage data.

```bash
$ cargo install cargo-llvm-cov
```

Now we can run tests and gather coverage. Run with nightly toolchain for best results.

```bash
$ cargo llvm-cov nextest --lcov --output-path lcov.info
```

Or view coverage results on your browser.

```bash
$ cargo llvm-cov nextest --open
```