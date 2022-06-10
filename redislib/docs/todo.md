
tests

annotate every command's return type
* progress: keys.ttl

splited functionality:
* [x] commander
* [x] scanner
* [ ] pubsuber
* [x] transaction
* [ ] transaction as pipeline (stores operation in pipeline to reduce network roundtrips)
* [ ] lua scripting
* [ ] support redis uri: `redis://user:pass@host:port/db`
* [ ] cluster?
* [x] database
* [ ] distributed lock, as redlock requires at least 5 nodes, i just implement a `setnx` version which requires 1 node


undiscovered redis commands/topics:
* untouch
* wait
* lcs
* blocking
    * bzpopmax, bzmpop
    * blpop, blmpop, blmove
* stream?
* flushdb SYNC?
* setbit
