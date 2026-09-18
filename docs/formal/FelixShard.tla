---------------------------- MODULE FelixShard ----------------------------
(***************************************************************************)
(* One shard of a Felix stream: the lease that lets a broker serve it, the *)
(* replication that puts its records on a majority, and the promotion that *)
(* picks the next leader when the lease lapses.                            *)
(*                                                                         *)
(* The model follows docs/replication-design.md. What it checks:           *)
(*                                                                         *)
(*   AtMostOneServing   -- no two brokers serve the shard at once, under   *)
(*                         clock drift, lost heartbeats, and a broker that  *)
(*                         pauses between admitting a write and committing *)
(*                         it.                                             *)
(*   AckedSurvive       -- a record acknowledged to a client is held by     *)
(*                         the broker serving the shard, always, including *)
(*                         after a promotion.                              *)
(*   AckedAgree         -- two brokers never hold different acknowledged   *)
(*                         records at one offset.                          *)
(*   NoTruncationBelowHwm -- a follower never discards a record below its   *)
(*                         high-water mark.                                *)
(*   NoStaleCommit      -- no broker commits at a generation the control    *)
(*                         plane has already superseded.                   *)
(*                                                                         *)
(* Time is discrete. `now` is real time; each broker has its own clock,    *)
(* within `Drift` of real time, which is the drift-rate assumption of the  *)
(* design in the only form a finite model needs. A broker anchors a lease  *)
(* at the instant it sent the heartbeat, on its own clock, and stops       *)
(* serving `Eps` before its own expiry; the control plane grants the next  *)
(* generation no earlier than `Margin` after the expiry it recorded.       *)
(*                                                                         *)
(* Two knobs exist to show the model has teeth. `CheckAtCommit = FALSE`    *)
(* drops the design's second lease check, and TLC finds the paused broker  *)
(* that commits after its lease lapsed. `Promotion = "leader-report"` is   *)
(* the design as written: the leader reports which followers are caught   *)
(* up, asynchronously, and the control plane promotes from the last report *)
(* it received. `Promotion = "log-order"` promotes the live replica with   *)
(* the highest (last generation, length), Raft's election restriction.     *)
(***************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Brokers,        \* the shard's replica set
    L,              \* lease length, in ticks
    Margin,         \* how long past a lapse the control plane waits before granting again
    Eps,            \* how long before its own expiry a broker stops serving
    Drift,          \* how far a broker's clock may sit from real time
    MaxTime,        \* real time runs out here; bounds the state space
    MaxWrites,      \* how many client writes the run admits
    CheckAtCommit,  \* re-check the lease before committing, or only at admission
    Quorum,         \* acknowledge on a majority (TRUE) or on the leader alone (FALSE)
    Promotion       \* "leader-report" or "log-order"

ASSUME Promotion \in {"leader-report", "log-order"}
ASSUME Eps < L /\ Margin >= 0

VARIABLES
    now,        \* real time
    clock,      \* each broker's monotonic clock
    gen,        \* the assignment generation at the control plane
    leader,     \* who the control plane assigned at gen
    cpExpiry,   \* when the lease at gen lapses, on the control plane's clock (real time)
    report,     \* the followers the last leader report named as caught up
    inflight,   \* a leader report on its way to the control plane, or <<>>
    bgen,       \* the generation each broker believes it leads; 0 means it does not
    bexpiry,    \* each broker's own belief of its lease expiry, on its own clock
    hbOut,      \* whether each broker has a heartbeat in flight
    hbAt,       \* when that heartbeat was sent, on the broker's clock
    log,        \* each broker's log: a sequence of [g |-> generation, id |-> write]
    hwm,        \* each broker's high-water mark: the prefix known committed
    halted,     \* followers that found a divergence they may not repair
    pending,    \* a write admitted by each broker and not yet committed; 0 means none
    acked,      \* writes acknowledged to a client
    writes,     \* how many writes have been admitted so far
    staleCommit \* history: a broker committed at a generation already superseded

vars == << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
           hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>

Majority(S) == Cardinality(S) * 2 > Cardinality(Brokers)

\* Brokers are interchangeable, which lets TLC fold their permutations.
Symm == Permutations(Brokers)

\* A broker serves the shard while it believes it leads and its own clock is
\* short of its own expiry by the margin it gives up.
Serving(b) == bgen[b] > 0 /\ clock[b] + Eps < bexpiry[b]

Record(g, id) == [g |-> g, id |-> id]

LastGen(b) == IF Len(log[b]) = 0 THEN 0 ELSE log[b][Len(log[b])].g

-----------------------------------------------------------------------------

Init ==
    /\ now = 0
    /\ clock = [b \in Brokers |-> 0]
    /\ gen = 1
    /\ leader \in Brokers
    /\ cpExpiry = L
    /\ report = {}
    /\ inflight = <<>>
    /\ bgen = [b \in Brokers |-> IF b = leader THEN 1 ELSE 0]
    /\ bexpiry = [b \in Brokers |-> IF b = leader THEN L ELSE 0]
    /\ hbOut = [b \in Brokers |-> FALSE]
    /\ hbAt = [b \in Brokers |-> 0]
    /\ log = [b \in Brokers |-> <<>>]
    /\ hwm = [b \in Brokers |-> 0]
    /\ halted = {}
    /\ pending = [b \in Brokers |-> 0]
    /\ acked = {}
    /\ writes = 0
    /\ staleCommit = FALSE

-----------------------------------------------------------------------------
(* Time. Real time ticks, and with it each broker's clock moves by zero,   *)
(* one or two, staying within Drift of real time. A clock that stands      *)
(* still is slow; one that moves by two is fast. Both are the design's     *)
(* assumption, and moving them in the same step as real time keeps the     *)
(* state space to what the drift can actually produce.                     *)

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ clock' \in { c \in [Brokers -> 0..(MaxTime + Drift)] :
                     \A b \in Brokers : /\ c[b] >= clock[b]
                                        /\ c[b] <= clock[b] + 2
                                        /\ c[b] >= now + 1 - Drift
                                        /\ c[b] <= now + 1 + Drift }
    /\ UNCHANGED << gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>

-----------------------------------------------------------------------------
(* The lease is the heartbeat. A broker that believes it leads sends one,  *)
(* remembering when on its own clock; the control plane accepts it only    *)
(* while the lease it granted has not lapsed, and the broker then extends  *)
(* its belief from the instant it sent, never from the instant it heard    *)
(* back. A heartbeat can be lost.                                          *)

SendHeartbeat(b) ==
    /\ bgen[b] > 0
    /\ ~hbOut[b]
    /\ hbOut' = [hbOut EXCEPT ![b] = TRUE]
    /\ hbAt' = [hbAt EXCEPT ![b] = clock[b]]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    log, hwm, halted, pending, acked, writes, staleCommit >>

AcceptHeartbeat(b) ==
    /\ hbOut[b]
    /\ leader = b /\ bgen[b] = gen
    /\ now <= cpExpiry
    /\ cpExpiry' = now + L
    /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
    /\ bexpiry' = [bexpiry EXCEPT ![b] = hbAt[b] + L]
    /\ UNCHANGED << now, clock, gen, leader, report, inflight, bgen, hbAt,
                    log, hwm, halted, pending, acked, writes, staleCommit >>

LoseHeartbeat(b) ==
    /\ hbOut[b]
    /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>

\* A broker that finds its lease lapsed, or that hears of a newer generation,
\* stops believing it leads. Modelled as the broker noticing; the safety
\* argument does not rely on it noticing in time.
StepDown(b) ==
    /\ bgen[b] > 0
    /\ (bgen[b] < gen \/ clock[b] + Eps >= bexpiry[b])
    /\ bgen' = [bgen EXCEPT ![b] = 0]
    /\ pending' = [pending EXCEPT ![b] = 0]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                    hbOut, hbAt, log, hwm, halted, acked, writes, staleCommit >>

-----------------------------------------------------------------------------
(* Writes. Admission checks the lease; the commit checks it again, or does *)
(* not, which is the knob. Anything may happen between the two: that gap   *)
(* is where a paused process lives.                                        *)

Admit(b) ==
    /\ Serving(b)
    /\ pending[b] = 0
    /\ writes < MaxWrites
    /\ writes' = writes + 1
    /\ pending' = [pending EXCEPT ![b] = writes + 1]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, acked, staleCommit >>

Commit(b) ==
    /\ pending[b] /= 0
    /\ bgen[b] > 0
    /\ CheckAtCommit => Serving(b)
    /\ log' = [log EXCEPT ![b] = Append(@, Record(bgen[b], pending[b]))]
    /\ pending' = [pending EXCEPT ![b] = 0]
    \* Under `Leader`, the leader's own durable write is the acknowledgement.
    /\ acked' = IF Quorum THEN acked ELSE acked \cup {pending[b]}
    \* History: the control plane has moved on, and this write still landed.
    /\ staleCommit' = (staleCommit \/ bgen[b] < gen)
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, halted, writes >>

-----------------------------------------------------------------------------
(* Replication. The leader ships the next record a follower is missing. A  *)
(* follower whose log disagrees with the leader's keeps what a newer       *)
(* generation than its own last accepted one says, provided the            *)
(* disagreement sits above its high-water mark; otherwise it halts. A      *)
(* follower refuses a leader older than one it has already heard from.     *)

\* The first offset at which two logs disagree, or one past the shorter.
Diverge(a, c) ==
    LET n == IF Len(a) < Len(c) THEN Len(a) ELSE Len(c)
        d == { i \in 1..n : a[i] /= c[i] }
    IN IF d = {} THEN n + 1 ELSE CHOOSE i \in d : \A j \in d : i <= j

Ship(b, f) ==
    /\ bgen[b] > 0 /\ f /= b /\ f \notin halted
    /\ bgen[f] = 0
    /\ bgen[b] >= LastGen(f)
    /\ LET i == Diverge(log[b], log[f]) IN
       \/ /\ i > Len(log[f])
          /\ i <= Len(log[b])
          /\ log' = [log EXCEPT ![f] = Append(@, log[b][i])]
          /\ UNCHANGED halted
       \/ /\ i <= Len(log[f])
          /\ i <= Len(log[b])
          /\ IF bgen[b] > LastGen(f) /\ i > hwm[f]
             THEN /\ log' = [log EXCEPT ![f] = SubSeq(@, 1, i - 1)]
                  /\ UNCHANGED halted
             ELSE /\ halted' = halted \cup {f}
                  /\ UNCHANGED log
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, pending, acked, writes, staleCommit >>

\* Under `Quorum`, a record is acknowledged once a majority including the
\* leader holds it, and the leader's mark moves up to it.
AckQuorum(b) ==
    /\ Quorum
    /\ Serving(b)
    /\ \E i \in (hwm[b] + 1)..Len(log[b]) :
        /\ Majority({ m \in Brokers : Len(log[m]) >= i /\ log[m][i] = log[b][i] } \cup {b})
        /\ acked' = acked \cup { log[b][j].id : j \in 1..i }
        /\ hwm' = [hwm EXCEPT ![b] = i]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, halted, pending, writes, staleCommit >>

\* A follower learns the mark from the leader, never past what it holds.
LearnHwm(b, f) ==
    /\ bgen[b] > 0 /\ f /= b
    /\ hwm[f] < hwm[b]
    /\ Len(log[f]) >= hwm[b]
    /\ SubSeq(log[f], 1, hwm[b]) = SubSeq(log[b], 1, hwm[b])
    /\ hwm' = [hwm EXCEPT ![f] = hwm[b]]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, halted, pending, acked, writes, staleCommit >>

-----------------------------------------------------------------------------
(* Reports. The leader tells the control plane which followers hold every  *)
(* record it does. The report travels on its own; it may arrive later than *)
(* the acknowledgements it describes, or never.                            *)

Report(b) ==
    /\ Serving(b)
    /\ inflight = <<>>
    /\ inflight' = << { f \in Brokers \ {b} : log[f] = log[b] /\ f \notin halted } >>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>

DeliverReport ==
    /\ inflight /= <<>>
    /\ report' = inflight[1]
    /\ inflight' = <<>>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>

LoseReport ==
    /\ inflight /= <<>>
    /\ inflight' = <<>>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>

-----------------------------------------------------------------------------
(* Promotion. Once the lease has lapsed and the margin has passed, the      *)
(* control plane names a new leader at the next generation. Who qualifies  *)
(* is the rule under test.                                                 *)

\* A candidate under the design as written: reported caught up by the last
\* report the control plane has, and not halted.
ByLeaderReport(f) == f \in report /\ f \notin halted

\* A candidate under the log-order rule: among the live replicas, one whose
\* (last generation, length) is greatest.
ByLogOrder(f) ==
    /\ f \notin halted
    /\ \A o \in Brokers \ (halted \cup {leader}) :
        \/ LastGen(o) < LastGen(f)
        \/ LastGen(o) = LastGen(f) /\ Len(log[o]) <= Len(log[f])

Promote(f) ==
    /\ f /= leader
    /\ now >= cpExpiry + Margin
    /\ IF Promotion = "leader-report" THEN ByLeaderReport(f) ELSE ByLogOrder(f)
    /\ gen' = gen + 1
    /\ leader' = f
    /\ cpExpiry' = now + L
    /\ bgen' = [bgen EXCEPT ![f] = gen + 1]
    /\ bexpiry' = [bexpiry EXCEPT ![f] = clock[f] + L]
    /\ pending' = [pending EXCEPT ![f] = 0]
    /\ report' = {}
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, log, hwm, halted, acked, writes, staleCommit >>

-----------------------------------------------------------------------------

Next ==
    \/ Tick
    \/ \E b \in Brokers :
        \/ SendHeartbeat(b)
        \/ AcceptHeartbeat(b)
        \/ LoseHeartbeat(b)
        \/ StepDown(b)
        \/ Admit(b)
        \/ Commit(b)
        \/ AckQuorum(b)
        \/ Report(b)
        \/ Promote(b)
        \/ \E f \in Brokers : Ship(b, f) \/ LearnHwm(b, f)
    \/ DeliverReport
    \/ LoseReport

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* What has to be true.                                                    *)

\* No two brokers serve the shard at once.
AtMostOneServing ==
    \A a, c \in Brokers : Serving(a) /\ Serving(c) => a = c

\* A record acknowledged to a client is held by whoever is serving.
AckedSurvive ==
    \A b \in Brokers : Serving(b) =>
        \A id \in acked : \E i \in 1..Len(log[b]) : log[b][i].id = id

\* Two brokers never hold different acknowledged records at one offset.
AckedAgree ==
    \A a, c \in Brokers : \A i \in 1..Len(log[a]) :
        /\ i <= Len(log[c])
        /\ log[a][i].id \in acked
        /\ log[c][i].id \in acked
        => log[a][i] = log[c][i]

\* No broker commits a write at a generation the control plane has superseded:
\* once the next leader is named, the old one's lease has run out by its own
\* clock, and its commit check says so. Without that check, a broker paused
\* between admitting and committing lands a write after its epoch ended.
NoStaleCommit == ~staleCommit

\* A follower never discards a record below its high-water mark.
NoTruncationBelowHwm ==
    \A b \in Brokers : Len(log[b]) >= hwm[b]

\* Every acknowledged record is on a majority, so it survives any minority loss.
AckedOnMajority ==
    Quorum =>
        \A id \in acked :
            Majority({ b \in Brokers : \E i \in 1..Len(log[b]) : log[b][i].id = id })

TypeOK ==
    /\ now \in 0..MaxTime
    /\ gen \in Nat
    /\ leader \in Brokers
    /\ halted \subseteq Brokers
    /\ writes \in 0..MaxWrites

=============================================================================
