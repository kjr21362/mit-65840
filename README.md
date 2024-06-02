# MIT 6.5840 Distributed Systems

## Lab 2 - Key/Value Server

## Lab 3 - Raft

Helpful resources

- dslogs.py - A script from a [TA](https://blog.josejg.com/debugging-pretty/) of the class, that pretty prints log outputs in the terminal to help with debugging.

In the code, add logs
```
Debug(dLeader, "S%d is follower", rf.me)
```

parse the log file
```
VERBOSE=1 go test -run 3A | ./dslogs.py -c 3
```

![log output](/dslog_output.png)


- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/), from another TA

- [Distributed Systems 6.2: Raft](https://www.youtube.com/watch?v=uXEYuDwm7e4&t=352s), Martin Kleppmann's lecture on Raft, walks through pseudocodes.

### 3A Leader Election
```
$ go test -race -run 3A
Test (3A): initial election ...
  ... Passed --   3.0  3  112   30226    0
Test (3A): election after network failure ...
  ... Passed --   5.0  3  232   48746    0
Test (3A): multiple elections ...
  ... Passed --   6.4  7 1115  230226    0
PASS
ok      6.5840/raft     15.695s
```
### 3B Log
```
$ go test -race -run 3B
Test (3B): basic agreement ...
  ... Passed --   0.6  3   19    4988    3
Test (3B): RPC byte count ...
  ... Passed --   1.5  3   53  114606   11
Test (3B): test progressive failure of followers ...
  ... Passed --   4.6  3  196   44555    3
Test (3B): test failure of leaders ...
  ... Passed --   5.1  3  354   79666    3
Test (3B): agreement after follower reconnects ...
  ... Passed --   5.5  3  212   56803    8
Test (3B): no agreement if too many followers disconnect ...
  ... Passed --   3.5  5  321   70430    4
Test (3B): concurrent Start()s ...
  ... Passed --   0.6  3   18    4690    6
Test (3B): rejoin of partitioned leader ...
  ... Passed --  11.0  3  501  113377    4
Test (3B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  17.4  5 2631 2087583  102
Test (3B): RPC counts aren't too high ...
  ... Passed --   2.1  3   73   20008   12
PASS
ok      6.5840/raft     53.038s
```

### 3C Persistence
```
$ go test -race -run 3C
Test (3C): basic persistence ...
  ... Passed --   5.5  3  210   55870    7
Test (3C): more persistence ...
  ... Passed --  18.4  5 1799  394111   16
Test (3C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   0.9  3   46   11986    4
Test (3C): Figure 8 ...
  ... Passed --  30.9  5 1028  205417   22
Test (3C): unreliable agreement ...
  ... Passed --   3.0  5  236   79979  246
Test (3C): Figure 8 (unreliable) ...
  ... Passed --  32.4  5 4735 8622535  286
Test (3C): churn ...
  ... Passed --  16.2  5  802  495104  343
Test (3C): unreliable churn ...
  ... Passed --  16.2  5 1276 1447290  473
PASS
ok      6.5840/raft     124.857s
```

### 4A Key/Value service without snapshots
```
$ VERBOSE=1 go test -race -run 4A
Test: one client (4A) ...
  ... Passed --  15.2  5  2476  482
Test: ops complete fast enough (4A) ...
  ... Passed --  31.0  3  3041    0
Test: many clients (4A) ...
  ... Passed --  15.7  5  3176 1419
Test: unreliable net, many clients (4A) ...
  ... Passed --  16.9  5  4166  836
Test: concurrent append to same key, unreliable (4A) ...
  ... Passed --   1.5  3   183   52
Test: progress in majority (4A) ...
  ... Passed --   0.4  5    82    2
Test: no progress in minority (4A) ...
  ... Passed --   1.1  5   296    3
Test: completion after heal (4A) ...
  ... Passed --   1.1  5   156    3
Test: partitions, one client (4A) ...
  ... Passed --  22.2  5  3605  443
Test: partitions, many clients (4A) ...
  ... Passed --  23.8  5  5001 1078
Test: restarts, one client (4A) ...
  ... Passed --  29.4  5  3859  464
Test: restarts, many clients (4A) ...
  ... Passed --  54.5  5  8400 1434
Test: unreliable net, restarts, many clients (4A) ...
  ... Passed --  39.8  5  7001  818
Test: restarts, partitions, many clients (4A) ...
  ... Passed --  52.8  5  8894  975
Test: unreliable net, restarts, partitions, many clients (4A) ...
  ... Passed --  42.1  5  6659  546
Test: unreliable net, restarts, partitions, random keys, many clients (4A) ...
  ... Passed --  66.4  7 18492 1004
PASS
ok      6.5840/kvraft   415.385s
```