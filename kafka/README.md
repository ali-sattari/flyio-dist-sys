# Kafka log challenge

## 5b: Multi-node

We have a KV store from Maelstrom with Read, Write, and CompareAndSwap methods.
We need to store these information and retrieve for poll and last_cmmitted_offset RPC calls:
* last committed offset per key
* list of message per key as key -> list(msg, offset)

The naive approach would be to store them as:
* one record per key for last committed offset in linKV, e.g. co_key1:12345
* one record per key for messages in seqKv, e.g. key1:[msg1:1234,msg2:2345]

But then this would be inefficient with both send and poll RPCs.
Another approach can be:
* one record per key for last committed offset in linKV, e.g. co_key1:12345
* one record per key for list of offsets in seqKv, e.g. ol_key1:12345,23456,11111
* many records per key/msg suffixed with offset in seqKv, e.g.: key1_22345:msg1, key2_23456:msg2

This way we can:
* on send write a new record for msg, and update the offset list record
* on poll read offset list key, filter offsets we need, read msg records

okay, so far I think this is a good approach:
* use g-counter via linkv CAS for offsets
* store last committed offset per key in linkv
* store offset_list in memory per node
* store msgs in seqkv (as they are immutable, and wrote only once)

then on RPCs:
* when a send comes, we need to generate new offset, write msg, update in_memory offset_list
* when commit offset comes, CAS it in linkv
* when list_committed_offset comes, read it by any node from linkv
* when poll comes, check in_memory offset list, read msgs from seqkv based on filtered list, return

if that worked, expand it for efficiency:
* gossip offset_list between nodes
* prune in-memory offset_list after each commit_offset rpc (assuming poll only needs committed msgs to return?)

### issues on 5b

* poll skipped: I suspect due to local only offset list
  maelstrom sends msg x to n0
  then asks n1 about it, n1 doesn't have the offset locally so it doesn't return anything
* we need to have some mechanism to share offsets across nodes
  * kv version can be key:list[offset]
    but then appending to the list is tricky, we either write and risk loosing offsets, or cas but risk going into cas loops
  * another option is to gossip offsets between nodes
    it has overhead and complexity of gossiping, but fits criteria of eventual consistency


### Todo:

* implement offset list gossip
  * maybe move gossip buffer to channel
    * it removes buff map and mutex, but then how to batch? on receiver end?
  * add gossip to handleSend
  * ???
  * profit!
