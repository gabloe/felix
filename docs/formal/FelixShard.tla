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
(* With `Handoff`, the control plane may also move the shard while its     *)
(* leader is alive: it fences the leader, which stops serving when it sees *)
(* the fence but keeps shipping, and names the successor only once the    *)
(* leader has reported that its log stopped growing. `WaitForDrained =    *)
(* FALSE` cuts over as soon as the fence is written, and TLC finds the     *)
(* leader landing a write after its successor has taken over.             *)
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
    Promotion,      \* "leader-report" or "log-order"
    ReportBeforeAck, \* whether a Quorum ack waits for the report describing it
    Handoff,        \* whether the control plane may move the shard off a live leader
    WaitForDrained, \* whether a cut-over waits for the leader's drained report
    MaxMoves        \* how many planned moves the run starts; bounds the state space

ASSUME Promotion \in {"leader-report", "log-order"}
ASSUME ReportBeforeAck \in BOOLEAN
ASSUME Handoff \in BOOLEAN /\ WaitForDrained \in BOOLEAN
ASSUME Eps < L /\ Margin >= 0

VARIABLES
    now,        \* real time
    clock,      \* each broker's monotonic clock
    gen,        \* the assignment generation at the control plane
    leader,     \* who the control plane assigned at gen
    cpExpiry,   \* when the lease at gen lapses, on the control plane's clock (real time)
    report,     \* what the control plane was last told: [holders, len, drained, gen]
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
    staleCommit, \* history: a broker committed at a generation already superseded
    draining,   \* the control plane has fenced the leader so the shard can move
    successor,  \* where it is moving to; meaningful only while draining
    stopped,    \* each broker has seen the fence and stopped serving
    moves       \* how many planned moves have been started

vars == << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
           hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit,
           draining, successor, stopped, moves >>

handoffVars == << draining, successor, stopped, moves >>

NoReport == [holders |-> {}, len |-> 0, drained |-> FALSE, gen |-> 0]

Majority(S) == Cardinality(S) * 2 > Cardinality(Brokers)

\* Brokers are interchangeable, which lets TLC fold their permutations.
Symm == Permutations(Brokers)

\* A broker's lease is good while it believes it leads and its own clock is
\* short of its own expiry by the margin it gives up.
LeaseValid(b) == bgen[b] > 0 /\ clock[b] + Eps < bexpiry[b]

\* It serves the shard on that lease until it has seen a fence.
Serving(b) == LeaseValid(b) /\ ~stopped[b]

Record(g, id) == [g |-> g, id |-> id]

LastGen(b) == IF Len(log[b]) = 0 THEN 0 ELSE log[b][Len(log[b])].g

-----------------------------------------------------------------------------

Init ==
    /\ now = 0
    /\ clock = [b \in Brokers |-> 0]
    /\ gen = 1
    /\ leader \in Brokers
    /\ cpExpiry = L
    /\ report = NoReport
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
    /\ draining = FALSE
    /\ successor = leader
    /\ stopped = [b \in Brokers |-> FALSE]
    /\ moves = 0

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
    /\ UNCHANGED handoffVars

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
    /\ UNCHANGED handoffVars

AcceptHeartbeat(b) ==
    /\ hbOut[b]
    /\ leader = b /\ bgen[b] = gen
    /\ now <= cpExpiry
    /\ cpExpiry' = now + L
    /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
    /\ bexpiry' = [bexpiry EXCEPT ![b] = hbAt[b] + L]
    /\ UNCHANGED << now, clock, gen, leader, report, inflight, bgen, hbAt,
                    log, hwm, halted, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

LoseHeartbeat(b) ==
    /\ hbOut[b]
    /\ hbOut' = [hbOut EXCEPT ![b] = FALSE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

\* A broker that finds its lease lapsed, or that hears of a newer generation,
\* stops believing it leads. Modelled as the broker noticing; the safety
\* argument does not rely on it noticing in time.
StepDown(b) ==
    /\ bgen[b] > 0
    /\ (bgen[b] < gen \/ clock[b] + Eps >= bexpiry[b])
    /\ bgen' = [bgen EXCEPT ![b] = 0]
    /\ pending' = [pending EXCEPT ![b] = 0]
    /\ stopped' = [stopped EXCEPT ![b] = FALSE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bexpiry,
                    hbOut, hbAt, log, hwm, halted, acked, writes, staleCommit,
                    draining, successor, moves >>

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
    /\ UNCHANGED handoffVars

Commit(b) ==
    /\ pending[b] /= 0
    /\ bgen[b] > 0
    /\ CheckAtCommit => LeaseValid(b)
    /\ log' = [log EXCEPT ![b] = Append(@, Record(bgen[b], pending[b]))]
    /\ pending' = [pending EXCEPT ![b] = 0]
    \* Under `Leader`, the leader's own durable write is the acknowledgement.
    /\ acked' = IF Quorum THEN acked ELSE acked \cup {pending[b]}
    \* History: the control plane has moved on, and this write still landed.
    /\ staleCommit' = (staleCommit \/ bgen[b] < gen)
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, hwm, halted, writes >>
    /\ UNCHANGED handoffVars

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
    /\ UNCHANGED handoffVars

\* Under `Quorum`, a record is acknowledged once a majority including the
\* leader holds it, and the leader's mark moves up to it.
\*
\* With `ReportBeforeAck`, the mark may not move past what the control plane
\* has already been told: the leader reports who holds the record, waits for
\* that report to land, and only then releases the acknowledgement. This is
\* `publish_mark` in `services/felix-broker-service/src/replication/driver/shard.rs`, which moves
\* the mark only `if reported`, and `await_quorum`, which blocks the publish on
\* the mark. Without it a leader can tell a client its record is on a majority
\* while the control plane knows nothing about which replica holds it, and a
\* leader dying in that window is replaced from a report that predates the
\* acknowledgement.
AckQuorum(b) ==
    /\ Quorum
    /\ LeaseValid(b)
    /\ \E i \in (hwm[b] + 1)..Len(log[b]) :
        /\ Majority({ m \in Brokers : Len(log[m]) >= i /\ log[m][i] = log[b][i] } \cup {b})
        /\ ReportBeforeAck => /\ i <= report.len
                              /\ Majority(report.holders \cup {b})
        /\ acked' = acked \cup { log[b][j].id : j \in 1..i }
        /\ hwm' = [hwm EXCEPT ![b] = i]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, halted, pending, writes, staleCommit >>
    /\ UNCHANGED handoffVars

\* A follower learns the mark from the leader, never past what it holds.
LearnHwm(b, f) ==
    /\ bgen[b] > 0 /\ f /= b
    /\ hwm[f] < hwm[b]
    /\ Len(log[f]) >= hwm[b]
    /\ SubSeq(log[f], 1, hwm[b]) = SubSeq(log[b], 1, hwm[b])
    /\ hwm' = [hwm EXCEPT ![f] = hwm[b]]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, halted, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

-----------------------------------------------------------------------------
(* Reports. The leader tells the control plane which followers hold every  *)
(* record it does, at which generation, and whether its log has stopped    *)
(* growing. The report travels on its own; it may arrive later than the    *)
(* acknowledgements it describes, or never. One from a generation the      *)
(* control plane has moved past is dropped on arrival, as the store does:  *)
(* it describes a leadership that has ended, and TLC finds what believing  *)
(* it does -- a drained report from the old leader, read as the new one's, *)
(* lets the next move skip its wait.                                       *)

Report(b) ==
    /\ LeaseValid(b)
    /\ leader = b /\ bgen[b] = gen
    /\ inflight = <<>>
    /\ inflight' = << [holders |-> { f \in Brokers \ {b} :
                                        log[f] = log[b] /\ f \notin halted },
                       len     |-> Len(log[b]),
                       drained |-> stopped[b] /\ pending[b] = 0,
                       gen     |-> bgen[b]] >>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

DeliverReport ==
    /\ inflight /= <<>>
    /\ report' = IF inflight[1].gen = gen THEN inflight[1] ELSE report
    /\ inflight' = <<>>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

LoseReport ==
    /\ inflight /= <<>>
    /\ inflight' = <<>>
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit >>
    /\ UNCHANGED handoffVars

-----------------------------------------------------------------------------
(* Promotion. Once the lease has lapsed and the margin has passed, the      *)
(* control plane names a new leader at the next generation. Who qualifies  *)
(* is the rule under test.                                                 *)

\* A candidate under the design as written: reported caught up by the last
\* report the control plane has, and not halted.
ByLeaderReport(f) == f \in report.holders /\ f \notin halted

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
    /\ report' = NoReport
    /\ draining' = FALSE
    /\ stopped' = [stopped EXCEPT ![f] = FALSE]
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, log, hwm, halted, acked, writes,
                    staleCommit, successor, moves >>

-----------------------------------------------------------------------------
(* Planned handoff. The control plane fences the leader so the shard can    *)
(* move to a follower the last report says is caught up. The leader keeps  *)
(* its lease and keeps shipping; it stops serving when it sees the fence,  *)
(* and a write it admitted before that still lands. Its next report says   *)
(* whether the log has stopped growing, and the cut-over waits for that -- *)
(* or does not, which is the knob.                                         *)

Fence(f) ==
    /\ Handoff
    /\ moves < MaxMoves
    /\ ~draining
    /\ f /= leader
    /\ f \in report.holders /\ f \notin halted
    /\ draining' = TRUE
    /\ successor' = f
    /\ moves' = moves + 1
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit,
                    stopped >>

\* The leader sees the fence. Modelled as the broker noticing; the cut-over
\* below does not rely on it noticing in time.
ObserveFence(b) ==
    /\ draining /\ leader = b /\ bgen[b] = gen
    /\ ~stopped[b]
    /\ stopped' = [stopped EXCEPT ![b] = TRUE]
    /\ UNCHANGED << now, clock, gen, leader, cpExpiry, report, inflight, bgen, bexpiry,
                    hbOut, hbAt, log, hwm, halted, pending, acked, writes, staleCommit,
                    draining, successor, moves >>

CutOver(f) ==
    /\ draining /\ successor = f
    /\ f \notin halted
    /\ WaitForDrained => (report.gen = gen /\ report.drained /\ f \in report.holders)
    /\ gen' = gen + 1
    /\ leader' = f
    /\ cpExpiry' = now + L
    /\ bgen' = [bgen EXCEPT ![f] = gen + 1]
    /\ bexpiry' = [bexpiry EXCEPT ![f] = clock[f] + L]
    /\ pending' = [pending EXCEPT ![f] = 0]
    /\ report' = NoReport
    /\ draining' = FALSE
    /\ stopped' = [stopped EXCEPT ![f] = FALSE]
    /\ UNCHANGED << now, clock, inflight, hbOut, hbAt, log, hwm, halted, acked, writes,
                    staleCommit, successor, moves >>

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
        \/ Fence(b)
        \/ ObserveFence(b)
        \/ CutOver(b)
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
    /\ draining \in BOOLEAN
    /\ successor \in Brokers
    /\ moves \in 0..MaxMoves

=============================================================================
