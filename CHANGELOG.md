# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.7.0] - TODO
### Changed
- Extracted email verification as its own recipe
- ThirdParty queries

## [1.6.0] - 2021-01-14
### Changed
- Used rowmapper interface
- Adds email verification queries
- User pagination queries

## [1.5.0] - 2020-11-06
### Added
- Support for emailpassword recipe
- Refactoring of queries to put them per recipe
- Changes base interface as per plugin interface 2.4

## [1.3.0] - 2020-05-21
### Added
- Adds check to know if in memory db should be used.

## [1.1.1] - 2020-04-08
### Fixed
- The core now waits for the PostgrSQL db to start