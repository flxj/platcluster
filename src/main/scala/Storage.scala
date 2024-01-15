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
    val driverLogPlatdb = "log:platdb"

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
    def id:String 
    //
    def status:(String,String) 
    //
    def leader:String
    //
    def members:Seq[String]
    //
    def apply(cmd:String):Future[Try[CommandApplyResult]]
    // joins the node, identitifed by nodeID and reachable at addr, to the cluster.
    def joinNode(id:String,ip:String,port:Int):Try[Unit]
    // 
    def removeNode(id:String):Try[Unit]
    
//
trait StateMachine:
    def init():Try[Unit]
    def apply(log:LogEntry):Try[CommandApplyResult]
//
trait LogStorage:
    def init():Try[Unit]
    def latest:Try[(Long,LogEntry)]
    def currentIndex:Long
    def commitIndex:Long
    def setCommitIndex(idx:Long):Try[Unit]
    def get(index:Long):Try[LogEntry]
    def append(entries:Seq[LogEntry]):Try[Unit]
    def delete(index:Long):Try[Unit]
    def dropRight(n:Int):Try[Unit]
    def dropRightFrom(prevIdx:Long,prevTerm:Long):Try[Boolean]

//
private[platcluster] object PlatDB:
    def apply(ops:StorageOptions):PlatDB = new PlatDB(new DB(ops.fsmPath))

private[platcluster] class PlatDB(db:DB) extends Storage:
    def open(): Try[Unit] = db.open()
    def close(): Try[Unit] = db.close()
    //
    def logStorage():LogStorage = new PlatDBLog(db)
    //
    def stateMachine():StateMachine = new PlatDBFSM(db)
//
private[platcluster] class PlatDBLog(db:DB) extends LogStorage:
    def init():Try[Unit] = ???
    def latest:Try[(Long,LogEntry)] = ???
    def currentIndex:Long = ???
    def commitIndex:Long = ???
    def setCommitIndex(idx:Long):Try[Unit] = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(entries:Seq[LogEntry]):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
    def dropRightFrom(prevIdx:Long,prevTerm:Long):Try[Boolean] = ???
    
private[platcluster] class PlatDBFSM(db:DB) extends StateMachine:
    def init(): Try[Unit] = ???
    def apply(log:LogEntry):Try[CommandApplyResult] = ???


private[platcluster] object LogPlatDBStorage:
    def apply(ops:StorageOptions):LogPlatDBStorage = 
        new LogPlatDBStorage(ops.logPath,new DB(ops.fsmPath))
    
private[platcluster] class LogPlatDBStorage(logPath:String,db:DB) extends Storage:
    def open(): Try[Unit] = db.open()
    def close(): Try[Unit] = db.close()
    //
    def logStorage():LogStorage = new AppendLog(logPath)
    //
    def stateMachine():StateMachine = new PlatDBFSM(db)

//
private[platcluster] class AppendLog(dir:String) extends LogStorage:
    def init():Try[Unit] = ???
    def latest:Try[(Long,LogEntry)] = ???
    def currentIndex:Long = ???
    def commitIndex:Long = ???
    def setCommitIndex(idx:Long):Try[Unit] = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(entries:Seq[LogEntry]):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
    def dropRightFrom(prevIdx:Long,prevTerm:Long):Try[Boolean] = ???
    
//
private[platcluster] object MemoryStore:
    def apply(ops:StorageOptions):MemoryStore = new MemoryStore()

private[platcluster] class MemoryStore() extends Storage:
    def open(): Try[Unit] = Success(None)
    def close(): Try[Unit] = Success(None)
    //
    def logStorage():LogStorage = new MemoryLog()
    //
    def stateMachine():StateMachine = new MemoryFSM()

//
private[platcluster] class MemoryLog() extends LogStorage:
    def init():Try[Unit] = ???
    def latest:Try[(Long,LogEntry)] = ???
    def currentIndex:Long = ???
    def commitIndex:Long = ???
    def setCommitIndex(idx:Long):Try[Unit] = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(entries:Seq[LogEntry]):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
    def dropRightFrom(prevIdx:Long,prevTerm:Long):Try[Boolean] = ???

//
private[platcluster] class MemoryFSM() extends StateMachine:
    def init(): Try[Unit] = ???
    def apply(log:LogEntry):Try[CommandApplyResult] = ???