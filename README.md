# Spin Python SDK


This is an SDK for creating [Spin](https://github.com/fermyon/spin) apps using Python.

This has been modified to incorporate [filibuster](https://github.com/filibuster-testing/filibuster) to enable service-level fault-injection in microservices that compile to WebAssembly.

For source code and documentation on how to use the `spin_python_sdk`, please go to the [original repository](https://github.com/fermyon/spin-python-sdk).

## Usage

In order to install this and use this instead of the normal `spin_python_sdk`:

1. Uninstall `spin_python_sdk` if you have it installed using: `pip uninstall spin-sdk`.
2. Go to the root directory of this repository.
3. Run `python -m pip install .`.