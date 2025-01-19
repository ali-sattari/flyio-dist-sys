# txn-rw-register

## Requirements

### 6a: single node

Support `txn` workload where:
* multiple operations are requested
* each op is a list of 3 elements: op, key, value
  * e.g. `[r, 1, null]` or `[w, 2, 42]`
* Keys are implicitly created on first write.
* writes are unique per key, so each key only receives request for write of each value only once
  * e.g. key 1 only receives `[w, 1, 10]` and `[w, 1, 11]` once
* reply should be of type `txn_ok` and repeat op list
  * filling in `r` ops current value, null if unknown

#### Assumptions
* op list should be evaluated in order -> yes
* `r` further down the op list should reflect earlier `w` for the same key (read your writes?)

#### Questions
* how does this connect to previous challenges?
  * unique ID -> g-counter?
  * gossip to share storage
  * offset and commit to finalize transactions
* can I use kafka challenge app as storage here?
  * write
    * send per each op and record given offset
    * commit offsets at the end of processing list
  * read
    * ask for last committed offset
    * poll for that offset

## design
