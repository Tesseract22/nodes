# THe SWIM protocol

The paper is very confusing. So here is a breakdown.

## The Basic SWIM

### 1) Failure Detector Component
In each protocol period, `i` does:

a. Sends ONE `ping` to random group member `j`.
b. Waits for `ack`. 
c. If timeout, `i` sends `ping-req` to `K` random member.
    c1. Upon receiving `ping-req`, sends `ping` to `j`.
    c2. Upon reciving `ack`, relay `ack` back to `i`.
    Notice that the `ack` in this case is not directly sent to `i` by `j`, but relayed through `k`. This is to avoid congestion between `i` and `j`.

d. At the end of the protocol period, checkis if recieve any `ack` either directly or indirectly.

### 2) Dissemination Component
After determining a failure, the node multicast the failture to its member. A member receiving this would remove `j` from membership list.

Group join can be implement similarly.

## The Augmented SWIM

No more multicast. Membership udpates are carried through `ping`, `ping-req`, and `ack`.

Each node maintains a list of recent membership changes. Theses changes are piggybacked on the messages sent by the failture detector.

Changes that are sent less should be prefered. A change can be removed from the list after being sent `y log N` times, where `y` is a parameter.

### Suspicion

If at the end of a protocol period, `i` does not receive `ack` from `j` either directly or indirectly. `j` (instead of being marked as `dead`) is marked as `suspected`.

This change is added to the list of membership changes, and carried through the piggybacks as something like `{ Suspect j: i suspects j }`. 

Other members upon receiving such message, would mark `j` as `suspected` as well (and added to the list of changes). A suspected member should still be valid target of `ping`.

When `i` receive a ping from a suspected member `j`, it should spreads `{ Alive j: i knows j }`. Otherwise, the entry `j` in `i` should expire after some time, where `{ Confgirm: i delcares j as dead }`.

An incarnation number is thus needed to be carried through in each of this piggybacked message. When `j` receive a suspicion about itself, it should starts piggybacking `{ Alive: j knows j }` with a incremented incarnation number.
