service TestService {
  i32 ping(1:i32 val),

  oneway void ping_broadcast(1:i32 val),
}