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