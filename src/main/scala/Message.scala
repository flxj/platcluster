package platcluster

import scala.collection.mutable.ArrayBuffer

case class AppendEntriesReq(
    val term:Long,
    val prevLogTrem:Long,
    val prevLogIndex:Long,
    val entries:ArrayBuffer[LogEntry]
)

case class AppendEntriesResp(
    val a:Int
)

case class RequestVoteReq(
    val term:Long 
)

case class RequestVoteResp(
    val term:Long,
    val index:Long 
)