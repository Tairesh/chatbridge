//! Construction of `messages.external_message_id`.
//!
//! The invariant, in one place: **an external message id is unique within its
//! channel.** `UNIQUE (channel_id, external_message_id)` enforces it and
//! `insert_message` ends in `ON CONFLICT DO NOTHING`, so an id that is not unique
//! does not raise an error — it drops a message. Whatever makes the provider's id
//! unique on the provider's side belongs in here.

use uuid::Uuid;

/// A Meta `mid` is unique across the platform.
pub fn instagram(mid: &str) -> String {
    format!("instagram:{mid}")
}

/// A Bot API `message_id` is unique only *within a chat*, so two people writing to
/// the same bot both reach `message_id: 1`. The chat id is what separates them.
pub fn telegram(chat_id: i64, message_id: i64) -> String {
    format!("telegram:{chat_id}:{message_id}")
}

/// An update carrying no message has no chat either. Never persisted — this id only
/// reaches the logs.
pub fn telegram_update(update_id: i64) -> String {
    format!("telegram:update:{update_id}")
}

/// The widget's `mid` comes from the browser. `crypto.randomUUID()` today, but a
/// hand-rolled embed could send the same value for every visitor.
pub fn widget(client_id: Uuid, mid: Uuid) -> String {
    format!("widget:{client_id}:{mid}")
}

/// An operator's reply before the provider has assigned it an id.
pub fn operator(mid: &str) -> String {
    format!("operator:{mid}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_telegram_chats_reaching_the_same_message_id_do_not_collide() {
        assert_ne!(telegram(111, 1), telegram(222, 1));
    }

    #[test]
    fn two_widget_clients_sending_the_same_mid_do_not_collide() {
        let mid = Uuid::from_u128(9);
        assert_ne!(
            widget(Uuid::from_u128(1), mid),
            widget(Uuid::from_u128(2), mid)
        );
    }

    #[test]
    fn the_formats_are_stable() {
        // Ids are stored. Changing a format orphans every row already written.
        assert_eq!(instagram("mid_1"), "instagram:mid_1");
        // Group chats have negative ids; nothing may choke on the minus sign.
        assert_eq!(telegram(-100, 7), "telegram:-100:7");
        assert_eq!(operator("abc"), "operator:abc");
    }
}
