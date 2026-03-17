use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use uuid::Uuid;

pub struct ClientRegistry {
    next_id: AtomicU64,
    connections: RwLock<HashMap<Uuid, HashMap<u64, mpsc::Sender<String>>>>,
    operator_ids: RwLock<HashSet<Uuid>>,
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
            operator_ids: RwLock::new(HashSet::new()),
        }
    }

    /// Register a connection. Returns (conn_id, receiver).
    /// If `is_operator`, the id is also added to the operator set.
    pub fn register(&self, id: Uuid, is_operator: bool) -> (u64, mpsc::Receiver<String>) {
        let conn_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(64);
        self.connections
            .write()
            .unwrap()
            .entry(id)
            .or_default()
            .insert(conn_id, tx);
        if is_operator {
            self.operator_ids.write().unwrap().insert(id);
        }
        (conn_id, rx)
    }

    /// Deregister a connection. Removes id from operator set if last connection.
    pub fn deregister(&self, id: Uuid, conn_id: u64) {
        let mut map = self.connections.write().unwrap();
        if let Some(conns) = map.get_mut(&id) {
            conns.remove(&conn_id);
            if conns.is_empty() {
                map.remove(&id);
                self.operator_ids.write().unwrap().remove(&id);
            }
        }
    }

    /// Send a message to all connections for a given id.
    pub fn send_to(&self, id: Uuid, payload: &str) {
        let map = self.connections.read().unwrap();
        if let Some(conns) = map.get(&id) {
            for tx in conns.values() {
                let _ = tx.try_send(payload.to_owned());
            }
        }
    }

    /// Snapshot of currently connected operator IDs.
    pub fn operator_ids(&self) -> HashSet<Uuid> {
        // TODO: use ref instead of clone?
        self.operator_ids.read().unwrap().clone()
    }

    /// Total number of active connections across all ids.
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
    fn register_returns_receiver_and_increments_count() {
        let reg = ClientRegistry::new();
        let id = Uuid::new_v4();
        let (_conn_id, _rx) = reg.register(id, false);
        assert_eq!(reg.connection_count(), 1);
    }

    #[test]
    fn deregister_decrements_count() {
        let reg = ClientRegistry::new();
        let id = Uuid::new_v4();
        let (conn1, _rx1) = reg.register(id, false);
        let (conn2, _rx2) = reg.register(id, false);
        assert_eq!(reg.connection_count(), 2);
        reg.deregister(id, conn1);
        assert_eq!(reg.connection_count(), 1);
        reg.deregister(id, conn2);
        assert_eq!(reg.connection_count(), 0);
    }

    #[test]
    fn deregister_unknown_is_noop() {
        let reg = ClientRegistry::new();
        reg.deregister(Uuid::new_v4(), 999);
        assert_eq!(reg.connection_count(), 0);
    }

    #[test]
    fn operator_ids_tracked() {
        let reg = ClientRegistry::new();
        let op = Uuid::new_v4();
        let (conn, _rx) = reg.register(op, true);
        assert!(reg.operator_ids().contains(&op));
        reg.deregister(op, conn);
        assert!(!reg.operator_ids().contains(&op));
    }

    #[test]
    fn send_to_delivers_message() {
        let reg = ClientRegistry::new();
        let id = Uuid::new_v4();
        let (_conn, mut rx) = reg.register(id, false);
        reg.send_to(id, r#"{"test":true}"#);
        assert_eq!(rx.try_recv().unwrap(), r#"{"test":true}"#);
    }

    #[test]
    fn send_to_unknown_id_is_noop() {
        let reg = ClientRegistry::new();
        reg.send_to(Uuid::new_v4(), "msg"); // should not panic
    }

    #[test]
    fn send_to_fans_out_to_multiple_connections() {
        let reg = ClientRegistry::new();
        let id = Uuid::new_v4();
        let (_c1, mut rx1) = reg.register(id, false);
        let (_c2, mut rx2) = reg.register(id, false);
        reg.send_to(id, "hello");
        assert_eq!(rx1.try_recv().unwrap(), "hello");
        assert_eq!(rx2.try_recv().unwrap(), "hello");
    }

    #[test]
    fn operator_removed_from_set_only_when_last_connection_drops() {
        let reg = ClientRegistry::new();
        let op = Uuid::new_v4();
        let (c1, _rx1) = reg.register(op, true);
        let (c2, _rx2) = reg.register(op, true);
        reg.deregister(op, c1);
        assert!(reg.operator_ids().contains(&op));
        reg.deregister(op, c2);
        assert!(!reg.operator_ids().contains(&op));
    }
}
