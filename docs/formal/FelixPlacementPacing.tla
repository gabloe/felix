------------------------- MODULE FelixPlacementPacing -------------------------
(***************************************************************************)
(* How placement paces copies across shards: moves that stage a destination *)
(* and follower replacements that copy one in. The shard model next door    *)
(* checks one shard's handoff is safe; this one checks the limits hold      *)
(* across many.                                                             *)
(*                                                                         *)
(*   CopiesWithinLimit -- never more copies in flight than `MaxConcurrent`, *)
(*                        and never more than `MaxPerNode` into or out of   *)
(*                        one node.                                         *)
(*   FencedNeverTimesOut -- a move past its fence is finished, never       *)
(*                        abandoned.                                        *)
(*                                                                         *)
(* Placement counts copies from what the store says is in progress. A move *)
(* names its destination as `successor`; a replacement names the follower  *)
(* it copies in as `joining`. `CountReplacements` is whether a replacement *)
(* is visible to that count at all -- without it, TLC finds a replacement  *)
(* beside a move under a limit of one.                                      *)
(*                                                                         *)
(* A pass is modelled as its writes, one at a time, each deciding from the *)
(* store as it stands; that is what one planner does within a pass.         *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Shards,             \* the shards placement moves
    Nodes,              \* the brokers
    MaxConcurrent,      \* FELIX_SHARD_MOVES_MAX_CONCURRENT
    MaxPerNode,         \* FELIX_SHARD_MOVES_MAX_PER_NODE
    CountReplacements   \* whether a follower replacement counts as a copy in flight

ASSUME CountReplacements \in BOOLEAN

None == "none"

VARIABLES
    leader,     \* who leads each shard
    copy,       \* the node a copy is going to, or None
    kind,       \* "move" or "replacement" while a copy is in flight
    fenced      \* the move's leader has been fenced

vars == << leader, copy, kind, fenced >>

Init ==
    /\ leader \in [Shards -> Nodes]
    /\ copy = [s \in Shards |-> None]
    /\ kind = [s \in Shards |-> None]
    /\ fenced = [s \in Shards |-> FALSE]

Copying(s) == copy[s] /= None

\* What the planner counts: a copy the store names.
Counted(s) == Copying(s) /\ (kind[s] = "move" \/ CountReplacements)

Touches(s, n) == n = leader[s] \/ n = copy[s]

Room(from, to) ==
    /\ Cardinality({s \in Shards : Counted(s)}) < MaxConcurrent
    /\ \A n \in {from, to} :
        Cardinality({s \in Shards : Counted(s) /\ Touches(s, n)}) < MaxPerNode

Start(s, n, k) ==
    /\ ~Copying(s)
    /\ n /= leader[s]
    /\ Room(leader[s], n)
    /\ copy' = [copy EXCEPT ![s] = n]
    /\ kind' = [kind EXCEPT ![s] = k]
    /\ UNCHANGED << leader, fenced >>

Fence(s) ==
    /\ Copying(s) /\ kind[s] = "move" /\ ~fenced[s]
    /\ fenced' = [fenced EXCEPT ![s] = TRUE]
    /\ UNCHANGED << leader, copy, kind >>

CutOver(s) ==
    /\ fenced[s]
    /\ leader' = [leader EXCEPT ![s] = copy[s]]
    /\ copy' = [copy EXCEPT ![s] = None]
    /\ kind' = [kind EXCEPT ![s] = None]
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]

Seat(s) ==
    /\ Copying(s) /\ kind[s] = "replacement"
    /\ copy' = [copy EXCEPT ![s] = None]
    /\ kind' = [kind EXCEPT ![s] = None]
    /\ UNCHANGED << leader, fenced >>

\* Past `FELIX_SHARD_MOVE_TIMEOUT_MS`, which in this model may be any time
\* before the fence.
TimeOut(s) ==
    /\ Copying(s) /\ ~fenced[s]
    /\ copy' = [copy EXCEPT ![s] = None]
    /\ kind' = [kind EXCEPT ![s] = None]
    /\ UNCHANGED << leader, fenced >>

Next ==
    \E s \in Shards :
        \/ \E n \in Nodes : Start(s, n, "move") \/ Start(s, n, "replacement")
        \/ Fence(s)
        \/ CutOver(s)
        \/ Seat(s)
        \/ TimeOut(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

CopiesWithinLimit ==
    /\ Cardinality({s \in Shards : Copying(s)}) <= MaxConcurrent
    /\ \A n \in Nodes :
        Cardinality({s \in Shards : Copying(s) /\ Touches(s, n)}) <= MaxPerNode

FencedNeverTimesOut == \A s \in Shards : fenced[s] => Copying(s) /\ kind[s] = "move"

TypeOK ==
    /\ leader \in [Shards -> Nodes]
    /\ copy \in [Shards -> Nodes \cup {None}]
    /\ kind \in [Shards -> {"move", "replacement", None}]
    /\ fenced \in [Shards -> BOOLEAN]

Symm == Permutations(Shards) \cup Permutations(Nodes)

=============================================================================
