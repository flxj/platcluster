package platcluster

import scala.util.{Try}

case class Result(msg:String)
case class RaftOptions(id:String)
case class RaftState(id:String)

class RaftServer(ops:RaftOptions):
    val store:StateMachine = null // 
    val log:LogStorage = null
    //
    def run():Try[Unit] = ??? // 异步运行
    def stop():Try[Unit] = ???
    //
    def state():Option[RaftState] = ??? // 返回当前server的状态信息
    def put(key:String,value:String):Try[Unit] = ???
    def get(key:String):Try[String] = ???
    def delete(key:String):Try[Unit] = ???
    def joinMember(id:String,addr:String,port:Int):Try[Unit] = ???
    def removeMember(addr:String,port:Int):Try[Unit] = ???
    def removeMember(id:String):Try[Unit] = ???
    //
    private def apply(cmd:String):Try[Result] = ???  // 执行操作：增删改查，成员变更

