# Changelog

# 1.0.2
  * Add logging of the dump id / manifest id, files, and incremental value when full sync is requested based on heap's documentation of incremental [#9](https://github.com/singer-io/tap-heap/pull/9)

# 1.0.1
  * Add logging around Activate Version Messages

# 1.0.0
  * Release as open beta

## 0.0.7
  * Use `filter_data_by_metadata()` instead of `transform()` to speed up record throughput [#7](https://github.com/singer-io/tap-heap/pull/7)
