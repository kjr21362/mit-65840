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