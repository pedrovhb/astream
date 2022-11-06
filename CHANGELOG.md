# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).



## [0.2.0] - 2022-11-06

### Added

- Several async stream utils
- Some async non-stream utils
- StreamGrouper class - a groupby-like stream processor which splits streams with a mapping-like interface
- WorkerPool class - a pool of workers which can be used to process streams

### Changed

- Moved iteration-related utils to `stream_utils`
- Moved non-iteration-related utils to `utils`

## [0.1.0] - Initial release

### Added

- Everything :)
- Closeable queue
- Basic stream
