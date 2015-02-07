service TestService {
  i32 ping(1:i32 val),
}

service TestBroadcastService {
  oneway void ping_broadcast(1:i32 val),
}