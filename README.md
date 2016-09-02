Mirage Xenstore server
======================

This repo contains an experimental Xenstore server implementation which uses Mirage
and [Irmin](https://github.com/mirage/irmin).

The design goals are: (in priority order)
  1. to survive crashes/unexpected restarts
  2. to make system debugging easy
  3. to run very fast

Notes on persistence modes
==========================

The server supports 2 persistence modes:
  1. none (default): nothing is remembered across server invocations
  2. crash resilient: the process may be killed and restarted at any
     time without loss of service
     
In crash resilient mode, all critical data is stored in an irminsule[1]
git database. This data includes:
  * all key/value pairs and metadata
  * all interdomain ring domid/mfn/event-channel (so connections can
    be re-established)
  * all unsent watch events (clients will see each event at least once)
  * all current watch registrations
  * any partially-read packets from rings
  * the highest used transaction id per connection
  * all untransmitted reply packets
  * all log settings
  * all quota information

The server will ensure that the side-effects of an operation will be
persisted before any reply packet is transmitted.

When the server restarts, the only visible artifacts should be:
  1. possible duplicate watch events (we assume these are idempotent)
  2. all outstanding transactions will be artificially aborted (client
     is expected to restart the transaction anyway)

The irminsule database should be persisted to storage which is cleared
on host restart. Ideally it should be stored to RAM (eg tmpfs) and not
a physical disk (slow and unnecessarily durable).

Developing with Docker
======================

This is useful when building on a host (for example a Mac or Windows laptop)
which doesn't have Xen headers installed.

Build an image containing all the dependencies:
```
docker build -t xenstore .
```

Run a fresh shell inside the image:
```
docker run -it -v `pwd`:/src xenstore bash
```

