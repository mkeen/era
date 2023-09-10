<div align="center">
    <img src="./assets/128x128@2x.png" alt="Era Logo" width="256">
    <hr />
        <h3 align="center" style="border-bottom: none">Read-optimized cache of Cardano on-chain entities</h3>
        <img alt="GitHub" src="https://img.shields.io/github/license/mkeen/era" />
    <hr/>
</div>

## Intro

_Era_ indexes Cardano's blockchain data in a high-level fashion and in a way that ensures you can effectively fetch or subscribe to whatever data you need in real-time.

The focus is on providing a high level index of data that is useful for building consumer-facing experiences.

> _Era_ is a fork of _Scrolls_. Significant differences as of now are real-time support for rollbacks via both N2C and N2N protocols, a refocused reducer-set, and a systemd/Debian-first posture. Support for Docker will be reintroduced in the next couple waves of updates.

This project serves as the index-hydrator for my own projects, and will be updated as contributers and myself see fit. Doesn't do something you need? Dig in and open a PR, or create an issue that describes your feature request.

## Building ##

1. `cargo build --all-features --release`
2. `cargo deb`
3. Use dpkg to install Era using the output from #2

## Redis

The codebase supports other backends, but development is focused on creating high-availability, read-optimized caches in Redis.

Current full-index size in Redis is around 50gig. Not a small amount, but not bad for a full-chain index. Keeping things high-level and as aggregated as possible while still being extremely useful is a goal.

## Debian/Systemd

Releases are in the form of .deb files and accompanied by source code. Installs systemd service, and the release binary and default config.

## SOAs

Era organizes information about asset/utxo ownership using SOAs. SOA stands for "Stake or Address". The concept behind this is simple: If a Cardano user has a stake key associated with their address, we will store information about what they own indexed by that stake key, in order to reduce keyspace size, and to keep queries for things like wallet UI population light. If there is no stake key associated with an address, Era will index on address. Hence: SOA.

### Don't we have tools for this already?

Kind of. This is an opinionated project that generates data-sets in ways that optimize storage, ensure consistency, and most importantly, lends itself well to many parallel subscribtions on high-level blockchain data.

Additionally, I am aiming to release more of the tools that I am using to build things like [Cardopoly](https://cardopoly.io/) and [Drop King](https://dropking.co) so that it will get that much easier for anyone to build enterprise-grade, high-performance consumer-facing blockchain-based user interfaces. Just call me the hyphenator.

WAGMI <3
