use actix::prelude::*;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Error};
use actix_web_actors::ws;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use redis::AsyncCommands;
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChatMessage {
    id: String,           // UUID (문자열)
    sender: String,       // 발신자 user_id
    receiver: String,     // 수신자 user_id
    payload: String,      // 암호화된 메시지 본문
    timestamp: u64,       // 생성 시각 (UNIX timestamp)
    attempt_count: u32,   // 재전송 횟수
}

/// 클라이언트에서 서버로 전달되는 메시지 타입 (REGISTER, CHAT, ACK)
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    Register { user_id: String },
    Chat { receiver: String, payload: String },
    Ack { message_id: String },
}

/// 서버에서 클라이언트로 전달되는 메시지 타입
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum ServerMessage {
    Chat { message: ChatMessage },
    Ack { message_id: String },
    Error { error: String },
}

/// 라우팅 서버 본체
struct ChatServer {
    sessions: HashMap<String, Addr<ChatSession>>,
    redis_client: redis::Client,
}

impl ChatServer {
    fn new(redis_client: redis::Client) -> Self {
        ChatServer {
            sessions: HashMap::new(),
            redis_client,
        }
    }
}

impl Actor for ChatServer {
    type Context = Context<Self>;
}

/// 클라이언트 연결 관련
#[derive(Message)]
#[rtype(result = "()")]
struct Connect {
    addr: Addr<ChatSession>,
    user_id: String,
}

#[derive(Message)]
#[rtype(result = "()")]
struct Disconnect {
    user_id: String,
}

/// 클라이언트로부터 받은 채팅 메시지를 포함하는 메시지
#[derive(Message)]
#[rtype(result = "()")]
struct ClientChatMessage {
    message: ChatMessage,
}

/// 클라이언트가 ACK를 보낸 경우 (수신자 쪽)
#[derive(Message)]
#[rtype(result = "()")]
struct ClientAck {
    message_id: String,
    user_id: String, // ACK를 보낸 user_id
}

/// 서버로부터 클라이언트로 채팅 메시지를 보내기 위한 Wrapper
#[derive(Message)]
#[rtype(result = "()")]
struct ServerChatMessage(ChatMessage);

#[derive(Message)]
#[rtype(result = "()")]
struct ResendMessage(ChatMessage);


impl Handler<ResendMessage> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: ResendMessage, _ctx: &mut Context<Self>) {
        let chat_msg = msg.0;
        let receiver = chat_msg.receiver.clone();
        if let Some(addr) = self.sessions.get(&receiver) {
            addr.do_send(ServerChatMessage(chat_msg));
        } else {
            println!("Receiver {} not connected during resend", receiver);
        }
    }
}


impl Handler<Connect> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) {
        self.sessions.insert(msg.user_id.clone(), msg.addr);
        println!("User {} connected", msg.user_id);
    }
}

impl Handler<Disconnect> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        self.sessions.remove(&msg.user_id);
        println!("User {} disconnected", msg.user_id);
    }
}

impl Handler<ClientChatMessage> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: ClientChatMessage, _ctx: &mut Context<Self>) {
        let chat_msg = msg.message.clone();
        let receiver = chat_msg.receiver.clone();
        let redis_client = self.redis_client.clone();

        let chat_msg_for_async = chat_msg.clone();
        let receiver_for_async = receiver.clone();

        Arbiter::current().spawn(async move {
            let mut conn = redis_client.get_multiplexed_async_connection().await.unwrap();
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let next_retry = now + 5;
            let pending_key = "pending_messages";
            let msg_json = serde_json::to_string(&chat_msg_for_async).unwrap();
            let offline_key = format!("offline:{}", receiver_for_async);
            let _: () = conn.zadd(pending_key, &msg_json, next_retry).await.unwrap();
            let _: () = conn.rpush(&offline_key, &msg_json).await.unwrap();
            let ttl: i64 = conn.ttl(&offline_key).await.unwrap();
            if ttl < 0 {
                let _: () = conn.expire(&offline_key, 43200).await.unwrap();
            }
        });

        if let Some(addr) = self.sessions.get(&receiver) {
            addr.do_send(ServerChatMessage(chat_msg));
        } else {
            println!("Receiver {} not connected. Message stored offline.", receiver);
        }
    }
}

impl Handler<ClientAck> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: ClientAck, _ctx: &mut Context<Self>) {
        let message_id = msg.message_id;
        let redis_client = self.redis_client.clone();
        // ACK 수신 시 pending 메시지 제거
        Arbiter::current().spawn(async move {
            let mut conn = redis_client.get_multiplexed_async_connection().await.unwrap();
            let pending_key = "pending_messages";
            let pending_msgs: Vec<String> = conn.zrange(pending_key, 0, -1).await.unwrap();
            for msg_json in pending_msgs {
                if let Ok(chat_msg) = serde_json::from_str::<ChatMessage>(&msg_json) {
                    if chat_msg.id == message_id {
                        let _: () = conn.zrem(pending_key, &msg_json).await.unwrap();
                        println!("Removed pending message {} after ACK", message_id);
                        break;
                    }
                }
            }
        });
    }
}

impl Handler<ServerChatMessage> for ChatSession {
    type Result = ();

    fn handle(&mut self, msg: ServerChatMessage, ctx: &mut Self::Context) {
        let server_msg = ServerMessage::Chat { message: msg.0 };
        let json = serde_json::to_string(&server_msg).unwrap();
        ctx.text(json);
    }
}

/// WebSocket 세션
struct ChatSession {
    user_id: Option<String>,
    server_addr: Addr<ChatServer>,
}

impl ChatSession {
    fn new(server_addr: Addr<ChatServer>) -> Self {
        ChatSession {
            user_id: None,
            server_addr,
        }
    }
}

impl Actor for ChatSession {
    type Context = ws::WebsocketContext<Self>;
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for ChatSession {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Text(text)) => {
                if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
                    match client_msg {
                        ClientMessage::Register { user_id } => {
                            self.user_id = Some(user_id.clone());
                            self.server_addr.do_send(Connect { addr: ctx.address(), user_id });
                        }
                        ClientMessage::Chat { receiver, payload } => {
                            // 채팅 생성
                            let chat_msg = ChatMessage {
                                id: Uuid::new_v4().to_string(),
                                sender: self.user_id.clone().unwrap_or_default(),
                                receiver,
                                payload,
                                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                                attempt_count: 0,
                            };
                            self.server_addr.do_send(ClientChatMessage { message: chat_msg });
                        }
                        ClientMessage::Ack { message_id } => {
                            if let Some(ref user_id) = self.user_id {
                                self.server_addr.do_send(ClientAck { message_id, user_id: user_id.clone() });
                            }
                        }
                    }
                }
            }
            Ok(ws::Message::Ping(msg)) => {
                ctx.pong(&msg);
            }
            Ok(ws::Message::Close(_)) => {
                if let Some(ref user_id) = self.user_id {
                    self.server_addr.do_send(Disconnect { user_id: user_id.clone() });
                }
                ctx.stop();
            }
            _ => (),
        }
    }
}

/// HTTP 핸들러
/// WebSocket 업그레이드 엔드포인트
async fn ws_index(req: HttpRequest, stream: web::Payload, data: web::Data<Addr<ChatServer>>)
                  -> Result<HttpResponse, Error> {
    let session = ChatSession::new(data.get_ref().clone());
    ws::start(session, &req, stream)
}

/// 백그라운드 테스크
/// Redis에서 pending 메시지를 풀링하고, 재전송 시도
async fn pending_message_resender(redis_client: redis::Client, server_addr: Addr<ChatServer>) {
    loop {
        sleep(Duration::from_secs(5)).await; // 5초마다 체크
        let mut conn = match redis_client.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(e) => { println!("Redis connection error: {:?}", e); continue; }
        };
        let pending_key = "pending_messages";
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        let msgs: Vec<String> = match conn.zrangebyscore(pending_key, 0, now).await {
            Ok(m) => m,
            Err(e) => { println!("Error fetching pending messages: {:?}", e); continue; }
        };
        for msg_json in msgs {
            if let Ok(mut chat_msg) = serde_json::from_str::<ChatMessage>(&msg_json) {
                if chat_msg.attempt_count >= 5 {
                    // 최대 재전송 횟수 초과 시 메시지 폐기
                    let _: () = conn.zrem(pending_key, &msg_json).await.unwrap();
                    println!("Message {} dropped after max attempts", chat_msg.id);
                    continue;
                }

                chat_msg.attempt_count += 1;
                let backoff = 5 * 2u64.pow(chat_msg.attempt_count);
                let next_retry = now as u64 + backoff;
                // 기존 엔트리 제거 후 업데이트된 메시지 삽입
                let _: () = conn.zrem(pending_key, &msg_json).await.unwrap();
                let new_msg_json = serde_json::to_string(&chat_msg).unwrap();
                let _: () = conn.zadd(pending_key, new_msg_json.clone(), next_retry).await.unwrap();
                println!("Retrying message {} (attempt {})", chat_msg.id, chat_msg.attempt_count);

                // 수신자 커넥션이 잡히면 있으면 메시지 재전송
                let receiver = chat_msg.receiver.clone();
                if let Some(addr) = server_addr.try_send(ResendMessage(chat_msg.clone())).err() {
                    println!("Failed to send message to receiver {}", receiver);
                }
            }
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let redis_client = redis::Client::open("redis://default:seacret@redis-url").unwrap();
    let server = ChatServer::new(redis_client.clone()).start();
    let server_clone = server.clone();
    actix::spawn(pending_message_resender(redis_client.clone(), server_clone));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(server.clone()))
            .route("/ws/", web::get().to(ws_index))
    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
