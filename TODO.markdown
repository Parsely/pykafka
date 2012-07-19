# Roadmap

* Add `samsa.consumer`.
* Improve documentation for `_configure` methods.
* Refactor protocol client to allow for streaming reads on fetch/multifetch requests.
* Implement compression for messages in the 0.7 format.
* Implement the 0.6 message format.
* Allow semantic partitioning on `Topic.publish`.
* Clean up logging statements, make `INFO`-level logging more useful.
* Add message batching for asynchronous send/in-memory buffering.
* Fix documentation to show correct method signatures for `requires_configuration` decorated methods, and annotate those methods in some fashion as well.
