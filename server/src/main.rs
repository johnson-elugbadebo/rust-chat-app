use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::HeaderValue,
    response::IntoResponse,
    routing::get,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use tokio::sync::broadcast::{self, Sender};
use tower_http::cors::{Any, CorsLayer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("STARTING SERVER...");
    // Create a broadcast channel with room for 100 pending messages.
    // tx sends to all subscribers; we drop the receiver (_) because only handlers need to subscribe to receiver.
    let (tx, _) = broadcast::channel(100);
    // Build the Axum router and pass tx as shared state for WebSocket handlers.
    let app = app(tx);

    // Bind to all network interfaces on port 3000 and wait until the socket is ready.
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("LISTENING ON PORT 3000");
    // Runs the HTTP server; handles requests until it shuts down.
    axum::serve(listener, app).await?;
    Ok(())
}

// Build the Axum router and inject the broadcast sender as shared state.
// Creates the HTTP router; tx is the broadcast channel used to fan out chat messages.
fn app(tx: Sender<String>) -> Router {
    // CORS lets the browser frontend (on port 8080) call this API (on port 3000).
    let cors_layer = CorsLayer::new()
        .allow_methods(Any) // Allow GET, POST, OPTIONS, etc.
        .allow_origin("http://127.0.0.1:8080".parse::<HeaderValue>().unwrap()); // Restricts access to that one frontend URL.

    Router::new()
        .route("/", get(|| async { "Home" })) // Plain text response for the root path.
        .route("/chat", get(chat_handler)) // WebSocket upgrade endpoint
        .with_state(tx) // Share the broadcast sender with handlers, makes tx available via State<Sender<String>> in handlers.
        .layer(cors_layer) // Apply CORS to all routes, CORS headers are added to responses
}

async fn chat_handler(State(tx): State<Sender<String>>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(|websocket| handle_websocket(tx, websocket))
}

async fn handle_websocket(tx: Sender<String>, websocket: WebSocket) {
    // Split the connection so we can send and receive on separate tasks.
    // separates send/receive so they can run concurrently
    let (mut sender, mut receiver) = websocket.split();
    // Each client gets its own receiver on the shared broadcast channel
    // Each client listens on the shared broadcast channel
    let mut rx = tx.subscribe();

    // Forward messages from other clients to this WebSocket.
    tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            sender.send(Message::from(msg)).await.unwrap()
        }
    });

    // Read messages from this client and broadcast text to everyone else.
    while let Some(msg) = receiver.next().await {
        if let Ok(msg) = msg {
            match msg {
                Message::Text(content) => {
                    tx.send(content.to_string()).unwrap();
                }
                _ => (),
            }
        }
    }
}
