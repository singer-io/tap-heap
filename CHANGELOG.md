# Changelog

## 1.5.0
  * Added forced-replication-method metadata field [#26](https://github.com/singer-io/tap-heap/pull/26)

## 1.4.1
  * Allows the use of --catalog as opposed to just --properties [#24](https://github.com/singer-io/tap-heap/pull/24)

## 1.4.0
  * Changes JSONFileCache location [#22](https://github.com/singer-io/tap-heap/pull/22)

## 1.3.0
  * Adds proxy AWS Account support
  * [#21](https://github.com/singer-io/tap-heap/pull/21)

# 1.2.0
  * Implement multi-processing to improve extraction performance [#20](https://github.com/singer-io/tap-heap/pull/20)

# 1.1.4
  * Handle non-union Avro types correctly during sync [#18](https://github.com/singer-io/tap-heap/pull/18)

# 1.1.3
  * Only send activate version if we sent records
  * Only filter files on the bookmark if the bookmark is in the files list
  * Sort the files explicitly using the dump id and part number instead of using string sorting
  * [#16](https://github.com/singer-io/tap-heap/pull/16)

# 1.1.2
  * Handle case where there are no `incremental=false` dumps [#14](https://github.com/singer-io/tap-heap/pull/14)

# 1.1.1
  * Check for a version bookmark and if it is not present then do a full table resync [#12](https://github.com/singer-io/tap-heap/pull/12)

# 1.1.0
  * Updated how we read manifest files and how we choose what files to sync. Added a version bookmark and tests. [#11](https://github.com/singer-io/tap-heap/pull/11)

# 1.0.2
  * Add logging of the dump id / manifest id, files, and incremental value when full sync is requested based on heap's documentation of incremental [#9](https://github.com/singer-io/tap-heap/pull/9)

# 1.0.1
  * Add logging around Activate Version Messages

# 1.0.0
  * Release as open beta

## 0.0.7
  * Use `filter_data_by_metadata()` instead of `transform()` to speed up record throughput [#7](https://github.com/singer-io/tap-heap/pull/7)
