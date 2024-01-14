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

import scala.util.{Try}
import scala.collection.mutable.{ArrayBuffer,Map}
import scala.concurrent.Future
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.{Try,Failure,Success}

case class RaftOptions(id:String,transportType:String,peers:List[(String,String,Int)])

class RaftState(id:String):
    var status:String = Raft.StatusUnknown
    var role:String = Raft.RoleFollower
    var leader:String = ""
    var errInfo:String = ""

object Raft:
    val RoleLeader = "leader"
    val RoleFollower = "follower"
    val RoleCandicate = "candicate"
    //
    val StatusInit = "initializing"
    val StatusRun = "running"
    val StatusFail = "failed"
    val StatusStop = "stopped"
    val StatusUnknown = "unknown"

    val defaultHeartbeatInterval = 50 // Millisecond
    val defaultElectionTimeout = 150 // 

    val exceptionAlreadyRunning = new Exception("raft node has already running")
    //
    def apply(ops:RaftOptions,fsm:StateMachine,log:LogStorage):ConsensusModule = 
        val trans = ops.transportType match
            case "http" => new HttpTransport()
            case "grpc" => new RPCTransport()
            case _ => throw new Exception(s"not support such transport type ${ops.transportType}")
        new Raft(ops,fsm,log,trans)
        

// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
private[platcluster] class Raft(ops:RaftOptions,fsm:StateMachine,log:LogStorage,trans:Transport) extends ConsensusModule:
    private val lock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    
    // candidateId that received vote in current term (or null if none)
    var voteFor:Option[String] = None
    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    var currentTerm:Long = 0L
    // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    var commitIndex:Long = 0L
    // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    var lastApplied:Long = 0L
    // 
    val peers:Map[String,Peer] = Map[String,Peer]()
    //
    var stat:RaftState = RaftState(ops.id)
    //
    def id:String = ops.id
    //
    def state:RaftState = 
        try
            lock.readLock().lock()
            stat
        finally
            lock.readLock().unlock()
    
    def status:(String,String) =
        try
            lock.readLock().lock()
            (stat.role,stat.status)
        finally
            lock.readLock().unlock()
    
    def leader: String = 
        try
            lock.readLock().lock()
            stat.leader
        finally
            lock.readLock().unlock()
    //
    def members: Seq[String] = 
        try
            lock.readLock().lock()
            (for (k,v) <- peers yield k).toArray
        finally
            lock.readLock().unlock()

    // TODO: open log
    def init():Try[Unit] = ???

    private var stopSignal:Boolean = false
    private var waitGroup:ArrayBuffer[Future[Unit]] = ArrayBuffer[Future[Unit]]()
    //
    def start():Try[Unit] = 
        val (_,s) = status()
        if s == Raft.StatusRun then
            Failure(Raft.exceptionAlreadyRunning)
        else
            stopSignal = false
            setRole(Raft.RoleFollower)
            log.latest match
                case Failure(e) => Failure(e)
                case Success(entry) =>
                    currentTerm = entry.term
                    val res = Future[Unit] {
                        loop()
                    }
                    waitGroup += res
                    Success(setStatus(Raft.StatusRun))
    //
    def stop():Try[Unit] = 
        val (_,s) = status()
        if s == Raft.StatusStop || s == Raft.StatusFail then
            Success(None)
        else
            stopSignal = true
            // TODO: wait group stop

            log.close() match
                case Failure(e) => Failure(e)
                case _ => None
            
            fsm.close() match
                case Failure(e) => Failure(e)
                case _ => None
            
            setStatus(Raft.StatusStop)
            log.latest match
                case Failure(e) => Failure(e)
                case Success(entry) =>
                    currentTerm = entry.term
                    val res = Future[Unit] {
                        loop()
                    }
                    waitGroup += res
                    Success(None)
        /**
          * if s.State() == Stopped {
		return
	}

	close(s.stopped)

	// make sure all goroutines have stopped before we close the log
	s.routineGroup.Wait()

	s.log.close()
	s.setState(Stopped)
          */
    //
    def apply(cmd:String):Future[Try[CommandApplyResult]] = ???
    //
    
    def put(key:String,value:String):Try[Unit] = ???
    def get(key:String):Try[String] = ???
    def delete(key:String):Try[Unit] = ???

    def joinNode(id:String,ip:String,port:Int):Try[Unit] = ???

    def removeNode(id:String):Try[Unit] = ???
    //
    private def setStatus(s:String):Unit = ???
    private def setRole(r:String):Unit = ???
    //
    private def getCurrentTermFromLog():Try[Long] = ???
    //
    private def loop(): Unit =
        var s = state()
        while s.status != Raft.StatusStop && s.status != Raft.StatusFail do 
            s.role match
                case Raft.RoleCandicate => runCandicate()
                case Raft.RoleFollower => runFollower()
                case Raft.RoleLeader => runLeader()
            s = state()
    //
    private def runFollower():Unit = ???
    //
    private def runCandicate():Unit = ???
    //
    private def runLeader():Unit = ???
    
    


