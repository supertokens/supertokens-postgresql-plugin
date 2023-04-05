# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.4.0] - 2023-03-30

- Support for Dashboard Search

## [2.3.0] - 2023-03-27
- Support for TOTP recipe
- Support for active users

### Database changes
- Add new tables for TOTP recipe:
  - `totp_users` that stores the users that have enabled TOTP
  - `totp_user_devices` that stores devices (each device has its own secret) for each user
  - `totp_used_codes` that stores used codes for each user. This is to implement rate limiting and prevent replay attacks.
- Add `user_last_active` table to store the last active time of a user.

## [2.2.0] - 2023-02-21

- Adds support for Dashboard recipe

## [2.1.0] - 2022-11-07

- Updates dependencies as per: https://github.com/supertokens/supertokens-core/issues/525

## [2.0.0] - 2022-09-19

- Updates the `third_party_user_id` column in the `thirdparty_users` table from `VARCHAR(128)` to `VARCHAR(256)` to
  resolve https://github.com/supertokens/supertokens-core/issues/306

- Adds support for user migration
    - Updates the `password_hash` column in the `emailpassword_users` table from `VARCHAR(128)` to `VARCHAR(256)` to
      support more types of password hashes.

- For legacy users who are self hosting the SuperTokens core run the following command to update your database with the
  changes:
  `ALTER TABLE thirdparty_users ALTER COLUMN third_party_user_id TYPE VARCHAR(256); ALTER TABLE emailpassword_users ALTER COLUMN password_hash TYPE VARCHAR(256);`

## [1.20.0] - 2022-08-18

- Adds log level feature and compatibility with plugin interface 2.18

## [1.19.0] - 2022-08-10

- Adds compatibility with plugin interface 2.17

## [1.18.0] - 2022-07-25

- Adds support for UserIdMapping recipe

## [1.17.0] - 2022-06-07

- Compatibility with plugin interface 2.15 - returns only non expired session handles for a user

## [1.16.0] - 2022-05-05

- Adds support for UserRoles recipe

## [1.15.0] - 2022-03-04

- Adds support for the new usermetadata recipe
- Fixes https://github.com/supertokens/supertokens-postgresql-plugin/issues/34

## [1.14.0] - 2022-02-23

- Adds an index on device_id_hash to the codes table.
- Using lower transaction isolation level while creating passwordless device with code

## [1.13.2] - 2022-02-19

- Refactor Query Mechanism to avoid Memory Leaks
- Adds debug statement to help fix error of passwordless code creation procedure (related to https://github.
  com/supertokens/supertokens-core/issues/373).

## [1.13.1] - 2022-02-16

- Fixed https://github.com/supertokens/supertokens-core/issues/373: Catching `StorageTransactionLogicException` in
  transaction helper function for retries
- add workflow to verify if pr title follows conventional commits

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
- Updated to match 2.9 plugin interface to support multiple access token signing
  keys: https://github.com/supertokens/supertokens-core/issues/305
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
