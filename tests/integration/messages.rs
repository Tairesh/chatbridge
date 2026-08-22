use crate::common;
use crate::common::*;
use crate::support::*;

use chrono::Utc;
use uuid::Uuid;

#[tokio::test]
async fn find_or_create_chat_creates_new() {
    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("chat_test_{}", Uuid::new_v4())).await;
    let client_id = chatbridge::db::create_client(&pool).await.unwrap();
    let _client_guard = TestClient { id: client_id };

    let chat_id = chatbridge::db::find_or_create_chat(&pool, client_id, channel.id)
        .await
        .unwrap();

    let _chat_guard = common::TestChat { id: chat_id };

    // Calling again returns the same chat
    let chat_id2 = chatbridge::db::find_or_create_chat(&pool, client_id, channel.id)
        .await
        .unwrap();
    assert_eq!(chat_id, chat_id2);
}

#[tokio::test]
async fn insert_message_returns_incoming_with_id() {
    use chatbridge::model::{EventKind, NewMessage, ProviderKind};

    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("msg_test_{}", Uuid::new_v4())).await;
    let client_id = chatbridge::db::create_client(&pool).await.unwrap();
    let _client_guard = TestClient { id: client_id };

    let chat_id = chatbridge::db::find_or_create_chat(&pool, client_id, channel.id)
        .await
        .unwrap();
    let _chat_guard = common::TestChat { id: chat_id };

    let new_msg = NewMessage {
        external_message_id: "widget:test-mid".into(),
        channel_id: channel.id,
        conversation: Some(chatbridge::model::Conversation::Customer(client_id)),
        sender_id: Some(client_id),
        sender_type: "client".into(),
        provider: ProviderKind::Widget,
        event: EventKind::Message,
        text: Some("hello".into()),
        raw: serde_json::json!({"action": "send", "text": "hello"}),
    };

    let incoming = chatbridge::db::insert_message(&pool, &new_msg, Some(chat_id))
        .await
        .unwrap()
        .expect("should not be a duplicate");

    let _msg_guard = common::TestMessage { id: incoming.id };

    assert_eq!(incoming.external_message_id, "widget:test-mid");
    assert_eq!(incoming.channel_id, channel.id);
    assert_eq!(incoming.chat_id, Some(chat_id));
    assert_eq!(incoming.sender_id, Some(client_id));
    assert_eq!(incoming.text.as_deref(), Some("hello"));
    assert_eq!(incoming.status, chatbridge::model::MessageStatus::New);
}

#[tokio::test]
async fn insert_message_without_sender_has_no_chat() {
    use chatbridge::model::{EventKind, NewMessage, ProviderKind};

    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("nosender_{}", Uuid::new_v4())).await;

    let new_msg = NewMessage {
        external_message_id: "instagram:mid_orphan".into(),
        channel_id: channel.id,
        conversation: None,
        sender_id: None,
        sender_type: "client".into(),
        provider: ProviderKind::Instagram,
        event: EventKind::Message,
        text: Some("orphan msg".into()),
        raw: serde_json::json!({}),
    };

    let incoming = chatbridge::db::insert_message(&pool, &new_msg, None)
        .await
        .unwrap()
        .expect("should not be a duplicate");

    let _msg_guard = common::TestMessage { id: incoming.id };

    assert!(incoming.chat_id.is_none());
    assert!(incoming.sender_id.is_none());
}

#[tokio::test]
async fn edit_message_updates_text_and_edited_at() {
    use chatbridge::model::{EventKind, NewMessage, ProviderKind};

    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("edit_test_{}", Uuid::new_v4())).await;

    let new_msg = NewMessage {
        external_message_id: "widget:edit-target".into(),
        channel_id: channel.id,
        conversation: None,
        sender_id: None,
        sender_type: "client".into(),
        provider: ProviderKind::Widget,
        event: EventKind::Message,
        text: Some("original".into()),
        raw: serde_json::json!({}),
    };
    let incoming = chatbridge::db::insert_message(&pool, &new_msg, None)
        .await
        .unwrap()
        .unwrap();
    let _msg_guard = common::TestMessage { id: incoming.id };

    let edit =
        chatbridge::db::edit_message(&pool, channel.id, "widget:edit-target", Some("updated"))
            .await
            .unwrap()
            .expect("message should exist");

    assert_eq!(edit.id, incoming.id);
    assert_eq!(edit.text.as_deref(), Some("updated"));
    assert!(edit.edited_at >= incoming.created_at);
}

#[tokio::test]
async fn edit_message_unknown_returns_none() {
    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("edit_miss_{}", Uuid::new_v4())).await;

    let result =
        chatbridge::db::edit_message(&pool, channel.id, "widget:nonexistent", Some("text"))
            .await
            .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn insert_message_dedup_returns_none() {
    use chatbridge::model::{EventKind, NewMessage, ProviderKind};

    let pool = setup_pool().await;
    let channel =
        insert_test_widget_channel(&pool, &format!("dedup_test_{}", Uuid::new_v4())).await;

    let new_msg = NewMessage {
        external_message_id: "widget:dedup-mid".into(),
        channel_id: channel.id,
        conversation: None,
        sender_id: None,
        sender_type: "client".into(),
        provider: ProviderKind::Widget,
        event: EventKind::Message,
        text: Some("first".into()),
        raw: serde_json::json!({}),
    };

    let first = chatbridge::db::insert_message(&pool, &new_msg, None)
        .await
        .unwrap();
    assert!(first.is_some());
    let _msg_guard = common::TestMessage {
        id: first.unwrap().id,
    };

    let second = chatbridge::db::insert_message(&pool, &new_msg, None)
        .await
        .unwrap();
    assert!(second.is_none(), "duplicate should return None");
}

#[tokio::test]
async fn find_last_chat_returns_closed_chat() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;
    let client = insert_test_client(&pool, "Archived Client").await;
    let chat = insert_test_chat(&pool, client.id, channel.id, "closed", Utc::now()).await;

    let found = chatbridge::db::find_last_chat(&pool, client.id, channel.id)
        .await
        .unwrap()
        .expect("a closed chat must still be returned");

    assert_eq!(found.id, chat.id);
    assert_eq!(found.status, "closed");
}

#[tokio::test]
async fn find_last_chat_returns_newest_of_several() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;
    let client = insert_test_client(&pool, "Returning Client").await;

    // Two 'new' chats are impossible — idx_chats_active forbids them.
    let old = insert_test_chat(
        &pool,
        client.id,
        channel.id,
        "closed",
        Utc::now() - chrono::Duration::hours(2),
    )
    .await;
    let recent = insert_test_chat(&pool, client.id, channel.id, "new", Utc::now()).await;

    let found = chatbridge::db::find_last_chat(&pool, client.id, channel.id)
        .await
        .unwrap()
        .expect("chat should exist");

    assert_eq!(found.id, recent.id);
    assert_ne!(found.id, old.id);
    assert_eq!(found.status, "new");
}

#[tokio::test]
async fn find_last_chat_returns_none_when_no_chats() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;
    let client = insert_test_client(&pool, "Fresh Client").await;

    let found = chatbridge::db::find_last_chat(&pool, client.id, channel.id)
        .await
        .unwrap();

    assert!(found.is_none(), "a client with no chats yields None");
}

#[tokio::test]
async fn get_chat_messages_includes_sender_name() {
    let pool = setup_pool().await;
    let widget_id = format!("test_widget_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;
    let client = insert_test_client(&pool, "Named Client").await;
    let chat = insert_test_chat(&pool, client.id, channel.id, "new", Utc::now()).await;

    let operator_id = Uuid::new_v4();
    sqlx::query("INSERT INTO operators (id, name) VALUES ($1, 'Named Operator')")
        .bind(operator_id)
        .execute(&pool)
        .await
        .unwrap();
    let _operator = TestOperator { id: operator_id };

    sqlx::query(
        "INSERT INTO messages (chat_id, external_message_id, channel_id, sender_id, sender_type, text, raw)
         VALUES ($1, $2, $3, $4, 'client', 'from the client', '{}'::jsonb)",
    )
    .bind(chat.id)
    .bind(format!("widget:{}", Uuid::new_v4()))
    .bind(channel.id)
    .bind(client.id)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO messages (chat_id, external_message_id, channel_id, sender_id, sender_type, text, raw, created_at)
         VALUES ($1, $2, $3, $4, 'operator', 'from the operator', '{}'::jsonb, now() + interval '1 second')",
    )
    .bind(chat.id)
    .bind(format!("operator:{}", Uuid::new_v4()))
    .bind(channel.id)
    .bind(operator_id)
    .execute(&pool)
    .await
    .unwrap();

    let messages = chatbridge::db::get_chat_messages(&pool, chat.id)
        .await
        .unwrap();

    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].sender_type, "client");
    assert_eq!(messages[0].sender_name.as_deref(), Some("Named Client"));
    assert_eq!(messages[1].sender_type, "operator");
    assert_eq!(messages[1].sender_name.as_deref(), Some("Named Operator"));
}

#[tokio::test]
async fn list_active_chats_excludes_chats_of_a_deleted_channel() {
    let pool = setup_pool().await;
    let widget_id = format!("api_{}", Uuid::new_v4());
    let channel = insert_test_widget_channel(&pool, &widget_id).await;
    let client = insert_test_client(&pool, "Deleted Channel Customer").await;
    let chat_id = chatbridge::db::find_or_create_chat(&pool, client.id, channel.id)
        .await
        .unwrap();
    let _chat = TestChat { id: chat_id };

    let before = chatbridge::db::list_active_chats(&pool).await.unwrap();
    assert!(
        before.iter().any(|c| c.chat_id == chat_id),
        "the chat is in the inbox while the channel is live"
    );

    chatbridge::db::soft_delete_channel(&pool, channel.id)
        .await
        .unwrap();

    let after = chatbridge::db::list_active_chats(&pool).await.unwrap();
    assert!(
        !after.iter().any(|c| c.chat_id == chat_id),
        "a deleted channel's chats must leave the inbox — they are unanswerable"
    );
}

#[tokio::test]
async fn two_widget_clients_sending_the_same_mid_both_persist() {
    use chatbridge::model::{EventKind, NewMessage, ProviderKind};

    // The mid is browser-supplied. Two visitors of one widget sending the same value
    // must not overwrite each other.
    let pool = setup_pool().await;
    let channel = insert_test_widget_channel(&pool, &format!("collide_{}", Uuid::new_v4())).await;
    let mid = Uuid::new_v4();

    let mut stored = Vec::new();
    for _ in 0..2 {
        let client_id = chatbridge::db::create_client(&pool).await.unwrap();
        let chat_id = chatbridge::db::find_or_create_chat(&pool, client_id, channel.id)
            .await
            .unwrap();
        let msg = NewMessage {
            external_message_id: chatbridge::external_id::widget(client_id, mid),
            channel_id: channel.id,
            conversation: Some(chatbridge::model::Conversation::Customer(client_id)),
            sender_id: Some(client_id),
            sender_type: "client".into(),
            provider: ProviderKind::Widget,
            event: EventKind::Message,
            text: Some("same mid".into()),
            raw: serde_json::json!({}),
        };
        stored.push(
            chatbridge::db::insert_message(&pool, &msg, Some(chat_id))
                .await
                .unwrap(),
        );
    }

    assert!(
        stored.iter().all(|m| m.is_some()),
        "the second client's message was swallowed as a duplicate"
    );
}

#[tokio::test]
async fn adopting_a_provider_id_renames_the_row() {
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "adopt").await;
    let ours = seed_message(
        &pool,
        channel.id,
        chat_id,
        None,
        "operator",
        "operator:local-1",
    )
    .await;

    assert!(
        chatbridge::db::adopt_external_message_id(&pool, ours, "instagram:mid_a")
            .await
            .unwrap()
    );

    let id: String = sqlx::query_scalar("SELECT external_message_id FROM messages WHERE id = $1")
        .bind(ours)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(id, "instagram:mid_a");
}

#[tokio::test]
async fn adopting_over_an_echo_keeps_the_operators_row() {
    // Meta echoes our own reply back. If the echo lands before the id is adopted, two
    // rows claim `instagram:<mid>` and the unique index only lets one keep it — ours,
    // because the echo's row carries no author.
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "adopt_echo").await;
    let operator_id = chatbridge::db::create_operator(&pool).await.unwrap();
    let _op_guard = TestOperator { id: operator_id };

    let ours = seed_message(
        &pool,
        channel.id,
        chat_id,
        Some(operator_id),
        "operator",
        "operator:local-2",
    )
    .await;
    let echo = seed_message(
        &pool,
        channel.id,
        chat_id,
        None,
        "operator",
        "instagram:mid_b",
    )
    .await;

    assert!(
        chatbridge::db::adopt_external_message_id(&pool, ours, "instagram:mid_b")
            .await
            .unwrap()
    );

    let rows: Vec<(Uuid, Option<Uuid>)> = sqlx::query_as(
        "SELECT id, sender_id FROM messages
         WHERE channel_id = $1 AND external_message_id = 'instagram:mid_b'",
    )
    .bind(channel.id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 1, "only one row may hold the provider's id");
    assert_eq!(
        rows[0].0, ours,
        "the operator's row is the one that survives"
    );
    assert_eq!(rows[0].1, Some(operator_id));

    let echo_left: i64 = sqlx::query_scalar("SELECT count(*) FROM messages WHERE id = $1")
        .bind(echo)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(echo_left, 0, "the duplicate echo row has to be gone");
}

#[tokio::test]
async fn adopting_a_deleted_row_deletes_nothing() {
    // The existence check is the whole reason this is a transaction: without it a
    // delivery task whose row is gone would delete whatever else holds that id.
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "adopt_gone").await;
    let echo = seed_message(
        &pool,
        channel.id,
        chat_id,
        None,
        "operator",
        "instagram:mid_c",
    )
    .await;

    assert!(
        !chatbridge::db::adopt_external_message_id(&pool, Uuid::new_v4(), "instagram:mid_c")
            .await
            .unwrap()
    );

    let still_there: i64 = sqlx::query_scalar("SELECT count(*) FROM messages WHERE id = $1")
        .bind(echo)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(still_there, 1);
}

#[tokio::test]
async fn adopting_an_id_the_row_already_has_is_a_no_op() {
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "adopt_twice").await;
    let ours = seed_message(
        &pool,
        channel.id,
        chat_id,
        None,
        "operator",
        "instagram:mid_d",
    )
    .await;

    assert!(
        chatbridge::db::adopt_external_message_id(&pool, ours, "instagram:mid_d")
            .await
            .unwrap()
    );

    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM messages WHERE id = $1")
        .bind(ours)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        left, 1,
        "the row must not delete itself as its own duplicate"
    );
}

#[tokio::test]
async fn marking_a_message_failed_never_overwrites_a_read_one() {
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "failed").await;
    // `read` goes in first: a receipt marks its anchor *and everything older*, so a
    // message written before it would be swept up and this test would prove nothing.
    let read = seed_message(&pool, channel.id, chat_id, None, "operator", "operator:f2").await;
    chatbridge::db::mark_messages_read(&pool, channel.id, "operator:f2", "client")
        .await
        .unwrap();
    let fresh = seed_message(&pool, channel.id, chat_id, None, "operator", "operator:f1").await;

    assert!(
        chatbridge::db::mark_message_failed(&pool, fresh)
            .await
            .unwrap()
    );
    assert!(
        !chatbridge::db::mark_message_failed(&pool, read)
            .await
            .unwrap()
    );

    let fresh_status: String = sqlx::query_scalar("SELECT status FROM messages WHERE id = $1")
        .bind(fresh)
        .fetch_one(&pool)
        .await
        .unwrap();
    let read_status: String = sqlx::query_scalar("SELECT status FROM messages WHERE id = $1")
        .bind(read)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(fresh_status, "failed");
    assert_eq!(read_status, "read");
}

#[tokio::test]
async fn a_failed_message_can_never_be_marked_read() {
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "failed_read").await;
    let doomed = seed_message(&pool, channel.id, chat_id, None, "operator", "operator:f3").await;
    chatbridge::db::mark_message_failed(&pool, doomed)
        .await
        .unwrap();

    let reads = chatbridge::db::mark_messages_read(&pool, channel.id, "operator:f3", "client")
        .await
        .unwrap();
    assert!(
        reads.is_empty(),
        "nobody read a message that was never delivered"
    );
}

#[tokio::test]
async fn marking_a_message_delivered_never_demotes_a_read_one() {
    // A customer with the thread open can read a reply before its delivery task gets
    // to the database. `read` is the stronger statement and has to survive.
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "delivered").await;
    let read = seed_message(&pool, channel.id, chat_id, None, "operator", "operator:d1").await;
    chatbridge::db::mark_messages_read(&pool, channel.id, "operator:d1", "client")
        .await
        .unwrap();
    let fresh = seed_message(&pool, channel.id, chat_id, None, "operator", "operator:d2").await;

    assert!(
        chatbridge::db::mark_message_delivered(&pool, fresh)
            .await
            .unwrap()
    );
    assert!(
        !chatbridge::db::mark_message_delivered(&pool, read)
            .await
            .unwrap()
    );

    let fresh_status: String = sqlx::query_scalar("SELECT status FROM messages WHERE id = $1")
        .bind(fresh)
        .fetch_one(&pool)
        .await
        .unwrap();
    let read_status: String = sqlx::query_scalar("SELECT status FROM messages WHERE id = $1")
        .bind(read)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(fresh_status, "delivered");
    assert_eq!(read_status, "read");
}

#[tokio::test]
async fn a_delivered_message_is_still_unread() {
    // One tick is not two: a receipt has to be able to move a delivered row on to
    // `read`, which it cannot do if the read queries only look at `new`.
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "delivered_read").await;
    let msg = seed_message(&pool, channel.id, chat_id, None, "operator", "instagram:d3").await;
    chatbridge::db::mark_message_delivered(&pool, msg)
        .await
        .unwrap();

    let reads = chatbridge::db::mark_messages_read(&pool, channel.id, "instagram:d3", "client")
        .await
        .unwrap();
    assert_eq!(reads.len(), 1, "a delivered message can still be read");

    let status: String = sqlx::query_scalar("SELECT status FROM messages WHERE id = $1")
        .bind(msg)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "read");
}

#[tokio::test]
async fn the_chat_fallback_also_reaches_a_delivered_message() {
    let pool = setup_pool().await;
    let (channel, client_id, chat_id) = seed_chat(&pool, "delivered_fallback").await;
    let msg = seed_message(&pool, channel.id, chat_id, None, "operator", "operator:d4").await;
    chatbridge::db::mark_message_delivered(&pool, msg)
        .await
        .unwrap();

    let reads = chatbridge::db::mark_chat_read(&pool, channel.id, client_id, "client")
        .await
        .unwrap();
    assert_eq!(reads.len(), 1);
}

#[tokio::test]
async fn a_status_outside_the_ladder_is_rejected_by_the_database() {
    // The whole point of the constraint: a typo in a status is silent otherwise. The
    // read queries match `status IN ('new', 'delivered')`, so a misspelled row can
    // never be marked read, never shows a tick, and raises nothing.
    let pool = setup_pool().await;
    let (channel, _client, chat_id) = seed_chat(&pool, "status_check").await;

    let err = sqlx::query(
        "INSERT INTO messages (chat_id, external_message_id, channel_id, sender_type, status, raw)
         VALUES ($1, 'operator:bogus', $2, 'operator', 'delivred', '{}')",
    )
    .bind(chat_id)
    .bind(channel.id)
    .execute(&pool)
    .await
    .expect_err("a status outside the ladder has to be refused");

    assert!(
        err.to_string().contains("messages_status_check"),
        "expected the status constraint to fire, got: {err}"
    );
}
