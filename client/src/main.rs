use dioxus::prelude::*;
use futures::{SinkExt, StreamExt};
use gloo_net::websocket::{Message, futures::WebSocket};

fn main() {
  launch(App);
}

#[component]
fn App() -> Element {
  rsx! {
    document::Stylesheet { href: asset!("/assets/main.css") }
    Home {}

  }
}

#[component]
fn Home() -> Element {
  // Reactive state: chat history, current draft, and the WS read half (set after connect).
  let mut message_list = use_signal(|| vec![]); // use_signal: Holds UI state; updates trigger re-renders.
  let mut message_content = use_signal(|| String::new());
  let mut receiver_ws = use_signal(|| None);

  // Name gate: user must enter a name before seeing the chat UI.
  let mut name = use_signal(|| String::new());
  let mut has_name = use_signal(|| false);

  // Outbound path: coroutine opens WS once, then sends each queued string as "name: text".
  let chat_client = use_coroutine(move |mut rx: UnboundedReceiver<String>| async move {
    let (mut sender, receiver) = WebSocket::open("ws://localhost:3000/chat").unwrap().split();
    receiver_ws.set(Some(receiver));
    while let Some(msg) = rx.next().await {
      let message = format!("{}: {}", name, msg);
      sender.send(Message::Text(message)).await.unwrap();
    }
  });

  // Inbound path: take the receiver once and append server messages to message_list.
  // use_future: Background task that listens for incoming Message::Text and grows message_list.
  let _ = use_future(move || async move {
    if let Some(mut receiver) = receiver_ws.take() {
      while let Some(msg) = receiver.next().await {
        if let Ok(msg) = msg {
          match msg {
            Message::Text(content) => {
              message_list.write().push(content);
            }
            _ => (), // Ignore binary / ping / other frame types.
          }
        }
      }
    }
  });

  rsx!(
    // Pre-join screen: name input only.
    // Two-step UX: name first, then chat.
    if !has_name() {
      div { class: "chat-container",
        div { class: "chat input-name",
          input {
            r#type: "text",
            value: name,
            placeholder: "Enter Your Name ...",
            oninput: move |e| name.set(e.value()),
          }
          button {
            onclick: move |_| has_name.set(true),
            disabled: if name().trim() == "" { true },
            "Join Chat"
          }
        }
      }
    } else {
      // Main chat: message list + compose box.
      div { class: "chat-container",
        div { class: "chat",
          div { class: "message-container",
            {
                let messages = message_list.read();
                rsx! {
                  // Newest first (server order reversed for display), shows newest messages at the top.
                  for item in messages.iter().rev() {
                    p {
                      class: "message-item",
                      // Prefix before first ":" is treated as sender; match = "user-message".
                      // Parses "Alice: hello" to style your own messages differently.
                      class: if item.split(':').next().unwrap_or("") == name() { "user-message" },
                      "{item}"
                    }
                  }
                }
            }
          }
          div { class: "input-container",
            input {
              r#type: "text",
              value: message_content,
              placeholder: name,
              oninput: move |e| message_content.set(e.value()),
            }
            button {
              onclick: move |_| {
                  chat_client.send(message_content()); // Queue send via coroutine.
                  message_content.set(String::new()); // Clear draft after send.
              },
              disabled: if message_content().trim() == "" { true },
              "Send"
            }
          }
        }
      }
    }
  )
}
