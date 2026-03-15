use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use uuid::Uuid;

// Note: The spec shows `mpsc::Sender<WsOutbound>` in the inner map for future outbound
// delivery. We use `HashSet<u64>` for now since outbound delivery is out of scope.
// When outbound delivery is added, the inner type will change to store senders.

pub struct ClientRegistry {
    next_id: AtomicU64,
    connections: RwLock<HashMap<Uuid, HashSet<u64>>>,
}

impl Default for ClientRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            connections: RwLock::new(HashMap::new()),
        }
    }

    /// Register a connection for a client. Returns the connection ID.
    pub fn register(&self, client_id: Uuid) -> u64 {
        let conn_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.connections
            .write()
            .unwrap()
            .entry(client_id)
            .or_default()
            .insert(conn_id);
        conn_id
    }

    /// Deregister a connection. Removes the client entry if no connections remain.
    pub fn deregister(&self, client_id: Uuid, conn_id: u64) {
        let mut map = self.connections.write().unwrap();
        if let Some(conns) = map.get_mut(&client_id) {
            conns.remove(&conn_id);
            if conns.is_empty() {
                map.remove(&client_id);
            }
        }
    }

    /// Total number of active connections across all clients.
    pub fn connection_count(&self) -> usize {
        self.connections
            .read()
            .unwrap()
            .values()
            .map(|c| c.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_deregister() {
        let reg = ClientRegistry::new();
        let client = Uuid::new_v4();

        let conn1 = reg.register(client);
        assert_eq!(reg.connection_count(), 1);

        let conn2 = reg.register(client);
        assert_eq!(reg.connection_count(), 2);

        reg.deregister(client, conn1);
        assert_eq!(reg.connection_count(), 1);

        reg.deregister(client, conn2);
        assert_eq!(reg.connection_count(), 0);
    }

    #[test]
    fn deregister_unknown_is_noop() {
        let reg = ClientRegistry::new();
        reg.deregister(Uuid::new_v4(), 999);
        assert_eq!(reg.connection_count(), 0);
    }

    #[test]
    fn multiple_clients() {
        let reg = ClientRegistry::new();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();

        let a1 = reg.register(a);
        let _b1 = reg.register(b);
        let _a2 = reg.register(a);
        assert_eq!(reg.connection_count(), 3);

        reg.deregister(a, a1);
        assert_eq!(reg.connection_count(), 2);
    }
}
