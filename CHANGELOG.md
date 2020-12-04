# Changelog

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
