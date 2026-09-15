//! External orchestration locks for fiducia-node.
//!
//! These are intentionally **not** the node's Raft-replicated `/v1/locks`
//! implementation. They are identities for bootstrap, migration, and operator
//! maintenance that may be backed by Cloudflare Durable Objects so recovery of
//! Fiducia never requires Fiducia to already be healthy.

use fiducia_lib_core::locks::LockKey;

pub fn shard_bootstrap(shard_id: u32) -> LockKey {
    fiducia_lib_core::locks::shard_bootstrap(&shard_id.to_string())
}

pub fn migration() -> LockKey {
    fiducia_lib_core::locks::migration("node")
}

pub fn maintenance(job: &str) -> LockKey {
    fiducia_lib_core::locks::singleton_job(job)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_keys_are_not_the_raft_lock_namespace() {
        assert_eq!(
            shard_bootstrap(7).as_str(),
            "fiducia-cloud/node/shard-bootstrap:7"
        );
        assert_eq!(migration().as_str(), "fiducia-cloud/migrations/node");
    }
}
