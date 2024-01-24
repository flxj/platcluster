/*
    Copyright (C) 2023 flxj(https://github.com/flxj)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package platcluster

import scala.concurrent.Future
import scala.util.{Try,Failure,Success}
import platdb.{DB,defaultOptions}

case class StorageOptions(driver:String,logPath:String,fsmPath:String)

private[platcluster] object Storage:
    val driverPlatdb = "platdb:platdb"
    val driverMemory = "memory:memory"
    val driverFilePlatdb = "file:platdb"

    val kvOpGet = "get"
    val kvOpPut = "put"
    val kvOpDel = "delete"

    val exceptNotSupportDriver = new Exception("not support such storage driver")

trait Storage:
    def open():Try[Unit]
    def close():Try[Unit]
    def logStorage():LogStorage
    def stateMachine():StateMachine
//
trait KVStorage:
      // Get returns the value for the given key.
      def get(key:String):Try[String]
      // Set sets the value for the given key, via distributed consensus.
      def put(key:String, value:String):Try[Unit]
      // Delete removes the given key, via distributed consensus.
      def delete (key:String):Try[Unit]

//
trait ConsensusModule extends KVStorage:
    def init():Try[Unit]
    //
    def start():Try[Unit]
    //
    def stop():Try[Unit] 
    //
    def nodeId:String 
    //
    def status:(String,String) 
    //
    def members:Seq[String]
    //
    def apply(cmd:Command):Try[Result]
    //
    def apply(cmd:Command,timeout:Option[Int]):Try[Result]
    //
    def applyAsync(cmd:Command):Future[Try[Result]]
    //
    def applyAsync(cmd:Command,timeout:Option[Int]):Future[Try[Result]]
    // joins the node, identitifed by nodeID and reachable at addr, to the cluster.
    def joinNode(id:String,ip:String,port:Int):Try[Unit]
    // 
    def leaveNode(id:String):Try[Unit]

//
trait RaftModule extends ConsensusModule:
    def term:Long 
    //
    def leader:String
    //
    def majority:Int
    //
    def heartbeatInterval:Int
    //
    def setHeartbeatInterval(d:Int):Unit 
    //
    def electionTimeout:Int
    //
    def setElectionTimeout(d:Int):Unit
    //
    def appendEntries(req:AppendEntriesReq):Try[AppendEntriesResp]
    //
    def requestVote(req:RequestVoteReq) :Try[RequestVoteResp]

//
trait StateMachine extends KVStorage:
    /**
      * 
      *
      * @return
      */
    def init():Try[Unit]
    /**
      * 
      *
      * @param log
      * @return
      */
    def apply(cmd:Command):Try[Result]
//
trait LogStorage:
    def init():Try[Unit]
    def stop():Try[Unit]
    def latest:Try[LogEntry]
    def currentIndex:Long
    def commitIndex:Long
    def updateCommitIndex(idx:Long):Try[Unit] 
    def commitLog(idx:Long):Try[Unit] // will apply the command to fsm,and set reault callback
    def get(index:Long):Try[LogEntry]
    def slice(index:Long,count:Int):Try[(Long,Array[LogEntry])]
    def append(entry:LogEntry):Try[Unit]
    def append(entries:Seq[LogEntry]):Try[Unit]
    def delete(index:Long):Try[Unit]
    def dropRight(n:Int):Try[Unit]
    def dropRightFrom(prevIdx:Long,prevTerm:Long):Try[Boolean]
    def create(term:Long,cmd:Command):Try[LogEntry]
    def registerApplyFunc(cmdType:String,applyF:(entry:LogEntry)=>Unit):Unit

//
object PlatDB:
    def apply(ops:StorageOptions):Storage = new PlatDB(new DB(ops.fsmPath))

private[platcluster] class PlatDB(db:DB) extends Storage:
    def open(): Try[Unit] = db.open()
    def close(): Try[Unit] = db.close()
    def logStorage():LogStorage = new PlatDBLog(db)
    def stateMachine():StateMachine = new PlatDBFSM(db)

object FilePlatDBStorage:
    def apply(ops:StorageOptions):Storage = new FilePlatDBStorage(ops.logPath,new DB(ops.fsmPath))

//   
private[platcluster] class FilePlatDBStorage(logPath:String,db:DB) extends Storage:
    def open(): Try[Unit] = db.open()
    def close(): Try[Unit] = db.close()
    //
    def logStorage():LogStorage = new AppendLog(logPath)
    //
    def stateMachine():StateMachine = new PlatDBFSM(db)

//
object MemoryStore:
    def apply(ops:StorageOptions):Storage = new MemoryStore()

private[platcluster] class MemoryStore() extends Storage:
    def open(): Try[Unit] = Success(None)
    def close(): Try[Unit] = Success(None)
    //
    def logStorage():LogStorage = new MemoryLog()
    //
    def stateMachine():StateMachine = new MemoryFSM()
