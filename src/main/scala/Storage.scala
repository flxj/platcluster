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
    val exceptFSMPathIsNull = new Exception("state machine storage path is null")

trait Storage:
    // open/init the storage.
    def open():Try[Unit]
    // close the storage, release all resoueces.
    def close():Try[Unit]
    // return the LogStorage instance.
    def logStorage():LogStorage
    // return the StateMachine instance.
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
    // init module.
    def init():Try[Unit]
    // run the module.
    def start():Try[Unit]
    // stop the module.
    def stop():Try[Unit] 
    // return current node id.
    def nodeId:String 
    // return current node status:(state,role)
    def status:(String,String) 
    // return all nodes id of current cluster.
    def members:Seq[String]
    // exec one command on the cluster.
    def apply(cmd:Command):Try[Result]
    // exec one command on the cluster.
    def apply(cmd:Command,timeout:Option[Int]):Try[Result]
    // exec one command on the cluster,asynchronously.
    def applyAsync(cmd:Command):Future[Try[Result]]
    // exec one command on the cluster,asynchronously, with timeout parameter.
    def applyAsync(cmd:Command,timeout:Option[Int]):Future[Try[Result]]
    // add one node to current cluster, identitifed by nodeID and reachable at addr, to the cluster.
    def joinNode(id:String,ip:String,port:Int):Try[Unit]
    // remove one node from current cluster.
    def leaveNode(id:String):Try[Unit]

//
trait RaftModule extends ConsensusModule:
    // return current node raft term value.
    def term:Long 
    // return current cluster leader id.
    def leader:String
    // return quorum.
    def majority:Int
    // heartbeat signal transmission interval,unit ms
    def heartbeatInterval:Int
    def setHeartbeatInterval(d:Int):Unit 
    // 
    def electionTimeout:Int
    def setElectionTimeout(d:Int):Unit
    // process appendEntries request.
    def appendEntries(req:AppendEntriesReq):Try[AppendEntriesResp]
    // process requestVote request.
    def requestVote(req:RequestVoteReq) :Try[RequestVoteResp]

//
trait StateMachine extends KVStorage:
    /**
      * init current StateMachine object.
      *
      * @return
      */
    def init():Try[Unit]
    /**
      * apply one command to current StateMachine.
      *
      * @param log
      * @return
      */
    def apply(cmd:Command):Try[Result]
//
trait LogStorage:
    // init log storage.
    def init():Try[Unit]
    // close log storage.
    def close():Try[Unit]
    // return the latest log entry.
    def latest:Try[LogEntry]
    // return index value of the latest log entry.
    def currentIndex:Long
    // return the largest index value of commited logs.
    def commitIndex:Long
    // setting the commit index value.
    def setCommitIndex(idx:Long):Unit
    // update the commit index value.
    def updateCommitIndex(idx:Long):Try[Unit] 
    // commit all log while its index <= idx,will apply the command to fsm,and set reault callback.
    def commitLog(idx:Long):Try[Unit] 
    // query a log entry by index.
    def get(index:Long):Try[LogEntry]
    // range query log entries by index and number.
    def slice(index:Long,count:Int):Try[(Long,Array[LogEntry])]
    // add a entry to log storage tail.
    def append(entry:LogEntry):Try[Unit]
    // add some entries to log storage tail.
    def append(entries:Seq[LogEntry]):Try[Unit]
    // remove a entry by index.
    def delete(index:Long):Try[Unit]
    // remove continue n entries from tail.
    def dropRight(n:Int):Try[Unit]
    // remove all entries after prevIndex and prevTerm.
    def dropRightFrom(prevIdx:Long,prevTerm:Long):Try[Boolean]
    // create a new entry(index=latest+1).
    def create(term:Long,cmd:Command,callback:Boolean):Try[LogEntry]
    // register apply function for log type
    def registerApplyFunc(cmdType:String,applyF:(entry:LogEntry)=>Unit):Unit

//
object PlatDB:
    def apply(ops:StorageOptions):Storage = 
        if ops.fsmPath == "" then 
            throw Storage.exceptFSMPathIsNull
        if ops.logPath != ops.fsmPath then 
            new PlatDB(new DB(ops.fsmPath),Some(new DB(ops.logPath)))
        else
            new PlatDB(new DB(ops.fsmPath),None)
//
private[platcluster] class PlatDB(db:DB,logDB:Option[DB]) extends Storage:
    def open(): Try[Unit] = 
        db.open() match
            case Failure(e) => Failure(e)
            case Success(_) => 
                logDB match
                    case None => Success(None)
                    case Some(d) => d.open()
    def close(): Try[Unit] = 
        db.close() match
            case Failure(e) => Failure(e)
            case Success(_) => 
                logDB match
                    case None => Success(None)
                    case Some(d) => d.close()
    def logStorage():LogStorage = 
        logDB match
            case None => new PlatDBLog(db)
            case Some(log) => new PlatDBLog(log)
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
