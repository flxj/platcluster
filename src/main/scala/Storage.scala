package platcluster

import scala.util.{Try}
import platdb.DB


trait StateMachine:
    def apply(log:LogEntry):Try[Result]

trait LogStorage:
    def logLength:Long 
    def get(index:Long):Try[LogEntry]
    def append(log:LogEntry):Try[Unit]
    def delete(index:Long):Try[Unit]
    def dropRight(n:Int):Try[Unit]

case class LogEntry(term:Long,index:Long,logType:Byte,content:String):
     def getBytes:Array[Byte] = ???



case class StorageOptions(path:String)

private[platcluster] class PlatStorage(ops:StorageOptions) extends LogStorage with StateMachine:
    val db:DB = null

    def apply(log:LogEntry):Try[Result] = ???

    def logLength:Long = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(log:LogEntry):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
