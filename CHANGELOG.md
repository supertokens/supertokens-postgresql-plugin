# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changes

- Fixed ResultSet instances to avoid Memory Leaks

## [1.13.0] - 2021-12-24

- added passwordless support

## [1.12.0] - 2021-12-19

### Added

- Delete user functionality

## [1.11.1] - 2021-10-07

### Changed

- Explicitly naming table constraints on creation (using the default Postgres names, so we don't break existing DBs)
- Using PSQLException to parse exception messages

## [1.11.0] - 2021-09-12

### Changed

- Added functions and other changes for the JWT recipe
- Updated to match 2.9 plugin interface to support multiple access token signing keys: https://github.com/supertokens/supertokens-core/issues/305
- Added new table to store access token signing keys (session_access_token_signing_keys)
### Breaking change:

- Changed email verification table to have user_id with max length 128

## [1.10.0] - 2021-06-20

### Changed

- Fixes https://github.com/supertokens/supertokens-core/issues/258
- Changes for pagination and count queries: https://github.com/supertokens/supertokens-core/issues/259
- Add GetThirdPartyUsersByEmail query: https://github.com/supertokens/supertokens-core/issues/277
- Add change email interface method within transaction: https://github.com/supertokens/supertokens-core/issues/275
- Added emailverification functions: https://github.com/supertokens/supertokens-core/issues/270

## [1.9.0] - 2021-06-01

### Added

- Added ability to specify a table schema: https://github.com/supertokens/supertokens-core/issues/251

## [1.8.0] - 2021-04-20

### Added

- Added ability to set table name prefix (https://github.com/supertokens/supertokens-core/issues/220)
- Added connection URI support (https://github.com/supertokens/supertokens-core/issues/221)

## [1.7.0] - 2021-02-16

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