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
import scala.collection.mutable.{ArrayBuffer,Map,Queue}
import scala.concurrent.{Future,Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.util.{Try,Failure,Success}

case class RaftOptions(id:String,transportType:String,peers:List[(String,String,Int)])

class RaftState(id:String):
    var role:String = Raft.RoleFollower
    var status:String = Raft.StatusUnknown 
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
    var voteFor:String = ""
    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    var currentTerm:Long = 0L
    // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    var commitIndex:Long = 0L
    // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    var lastApplied:Long = 0L
    //
    var leaderId:Option[String] = None
    // 
    val peers:Map[String,Peer] = Map[String,Peer]()
    //
    var stat:RaftState = RaftState(ops.id)
    //
    private val msgQueue:Queue[Message] = new Queue[Message]()
    //
    def id:String = ops.id
    //
    def role:String = 
        try
            lock.readLock().lock()
            stat.role
        finally
            lock.readLock().unlock()
    //
    def state:RaftState = 
        try
            lock.readLock().lock()
            stat
        finally
            lock.readLock().unlock()
    //
    def status:(String,String) =
        try
            lock.readLock().lock()
            (stat.role,stat.status)
        finally
            lock.readLock().unlock()
    //
    def leader: String = 
        try
            lock.readLock().lock()
            leaderId match
                case Some(ld) => ld 
                case None => ""
        finally
            lock.readLock().unlock()
    //
    def members: Seq[String] = 
        try
            lock.readLock().lock()
            (for (k,v) <- peers yield k).toArray
        finally
            lock.readLock().unlock()

    def init():Try[Unit] = 
        try
            lock.writeLock().lock()
            if stat.status == Raft.StatusRun then
                Success(None)
            else
                //
                log.init() match
                    case Failure(e) => throw e
                    case Success(_) => None
                fsm.init() match
                    case Failure(e) => throw e
                    case Success(_) => None
                //
                stat.status = Raft.StatusInit
                stat.errInfo = ""
                waitGroup.clear()
                Success(None)
        catch
            case e:Exception => 
                stat.status = Raft.StatusFail
                stat.errInfo = e.getMessage()
                Failure(e)
        finally
            lock.writeLock().unlock()

    private var stopSignal:Boolean = false
    private var waitGroup:ArrayBuffer[Future[Unit]] = ArrayBuffer[Future[Unit]]()
    //
    def start():Try[Unit] = 
        val s = state
        if s.status == Raft.StatusRun then
            Failure(Raft.exceptionAlreadyRunning)
        else
            stopSignal = false
            setRole(Raft.RoleFollower)
            log.latest match
                case Failure(e) => Failure(e)
                case Success((_,entry)) =>
                    currentTerm = entry.term
                    val res = Future[Unit] {
                        mainLoop()
                    }
                    waitGroup += res
                    Success(setStatus(Raft.StatusRun))
    //
    def stop():Try[Unit] = 
        val (_,s) = status
        if s == Raft.StatusStop || s == Raft.StatusFail then
            Success(None)
        else
            stopSignal = true
            for w <- waitGroup do
                Await.result(w,1.hours)
            // TODO if need close logStorage??
            waitGroup.clear()
            Success(setStatus(Raft.StatusStop))
    //
    def apply(cmd:String):Future[Try[CommandApplyResult]] = ???
    //
    
    def put(key:String,value:String):Try[Unit] = ???
    def get(key:String):Try[String] = ???
    def delete(key:String):Try[Unit] = ???

    def joinNode(id:String,ip:String,port:Int):Try[Unit] = ???

    def removeNode(id:String):Try[Unit] = ???
    //
    private def term:Long = 
        try
            lock.readLock().lock()
            currentTerm
        finally
            lock.readLock().unlock()
    //
    private def setStatus(s:String):Unit = ???
    private def setRole(r:String):Unit = ???
    private def setFail(err:Throwable):Unit = ???
    //
    private def getCurrentTermFromLog():Try[Long] = ???
    //
    import MessageTypes._
    import Message._
    //
    private def mainLoop(): Unit =
        try
            var s = state
            while s.status != Raft.StatusStop && s.status != Raft.StatusFail do 
                s.role match
                    case Raft.RoleCandicate => runCandicate()
                    case Raft.RoleFollower => runFollower()
                    case Raft.RoleLeader => runLeader()
                s = state
        catch
            case e:Exception => setFail(e)
    /**
      * • Upon election: send initial empty AppendEntries RPCs(heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)
      * • If command received from client: append entry to local log,respond after entry applied to state machine (§5.3)
      * • If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
      * • If successful: update nextIndex and matchIndex for follower (§5.3)
      * • If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
      * • If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:set commitIndex = N (§5.3, §5.4)
      */
    private def runLeader():Unit = 
        val logIdx = 0L
        log.latest match 
            case Failure(e) => throw e
            case Success(logIdx) => None
        //
        for (_,p) <- peers do
            // TODO set the prevLogIndex
            // TODO Start send heartbeat to it
            None

        //
        // TODO 
        //
        while role == Raft.RoleLeader do 
            //
            if stopSignal then
                for (_,p) <- peers do 
                    // TODO stop heartbeat 
                    None
                setStatus(Raft.StatusStop)
                return
            //
            if msgQueue.length > 0 then
                val msg = msgQueue.dequeue()
                msg.msgType match 
                    case Command => None // TODO process command
                    case AppendEntriesRequest => None // TODO process aes request
                    case AppendEntriesResponse => None // TODO process aes response
                    case RequestVoteRequest => processRequestVoteRequest(msg.source,msg)

    /**
      * • Respond to RPCs from candidates and leaders
      * • If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
      */
    private def runFollower():Unit = 
        // randomly init a timer from range [electionTimeout,electionTimeout*2] for timeout.
        var timeout = randomTimer(electionTimeout,electionTimeout*2)
        // run follower.
        while role == Raft.RoleFollower do 
            if stopSignal then 
                setStatus(Raft.StatusStop)
                return 
            // if need reset the timeout timer.
            var flush = false
            // timeout condition is triggered: become candicate
            if timeout.isCompleted then 
                setRole(Raft.RoleCandicate)
            else 
                // process request from other servers.
                if msgQueue.length > 0 then 
                    val m = msgQueue.dequeue()
                    m.msgType match
                        case Command => None // TODO process join command
                        case AppendEntriesRequest => None // TODO process 
                        case RequestVoteRequest => flush = processRequestVoteRequest(m.source,m)
                        case _ => None
            if flush then 
                timeout = randomTimer(electionTimeout,electionTimeout*2)
    //
    private def randomTimer(start:Int,end:Int):Future[Int] = ???
    def electionTimeout:Int = ???
    def majority:Int = ???
    //
    private def updateCurrentTerm(termValue:Long,name:Option[String]):Unit =
        if termValue < currentTerm then
            throw new Exception("only enable update currentTrem with a larger term value") 
        
        val prevTerm = currentTerm
        val prevLeader = leaderId
        
        // if current role is leader, should convert to follower.
        if stat.role == Raft.RoleLeader then 
            for (_,p) <- peers do
                // TODO stop all heartbeat
                None
        //
        if stat.role != Raft.RoleFollower then 
            setRole(Raft.RoleFollower)
        
        lock.writeLock().lock()
        currentTerm = termValue
        leaderId = name
        voteFor = ""
        lock.writeLock().unlock()

    /* 
        • If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
        • If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
     */
    private def processVoteResponse(resp:RequestVoteResp):Boolean = 
        if resp.voteGranted && resp.term == currentTerm then
            true
        else 
            if resp.term > currentTerm then
                updateCurrentTerm(resp.term,None)
            false

    /*  
      On conversion to candidate, start election:
        • Increment currentTerm
        • Vote for self
        • Reset election timer
        • Send RequestVote RPCs to all other servers
        • If votes received from majority of servers: become leader
        • If AppendEntries RPC received from new leader: convert tofollower
        • If election timeout elapses: start new election
    */
    private def runCandicate():Unit = 
        // reset the leader is null.
        leaderId = None
        //
        var lastLogIndex = 0L
        var lastLogTerm = 0L
        log.latest match
            case Failure(e) => throw e // TODO: err handler
            case Success((lastLogIndex,lastLog)) => lastLogTerm = lastLog.term
        
        var voteGranted:Int = 0
        val resps = Map[String,Try[Unit]]()
        var timeout:Future[Int] = null
        var voteContinue = true

        while role == Raft.RoleCandicate do
            if voteContinue then
                // do once elect
                currentTerm += 1
                voteFor = id
                resps.clear()
                //
                for (name,p) <- peers do
                    //TODO: waitGroup += resp
                    val rv:Future[Try[RequestVoteResp]] = Future {
                        trans.RequestVote(p,RequestVoteReq(currentTerm,id,lastLogIndex,lastLogTerm))
                    }
                    rv.onComplete {
                        case Failure(e) => resps(name) = Failure(e)
                        case Success(r) => 
                            r match
                                case Success(resp) =>
                                    if processVoteResponse(resp) then
                                        voteGranted += 1
                                case Failure(e) => resps(name) = Failure(e)
                    }
                //
                voteGranted = 1
                timeout = randomTimer(electionTimeout, electionTimeout*2)
                voteContinue = false
            // If received enough votes then stop waiting for more votes. And return from the candidate loop
            if voteGranted >= majority then
                setRole(Raft.RoleLeader)
                return
            //
            if stopSignal then
                setStatus(Raft.StatusStop)
                return
            else if timeout.isCompleted then
                voteContinue = true
            else 
                try
                    if msgQueue.length > 0 then
                        val msg = msgQueue.dequeue()
                        msg.msgType match
                            case Command => throw new Exception("NotLeaderError")
                            case AppendEntriesRequest => processAppendEntriesRequest(msg.source,msg)
                            case RequestVoteRequest => processRequestVoteRequest(msg.source,msg)
                catch
                    case e:Exception => None // TODO  err handler
    /**
      * 1. Reply false if term < currentTerm
      * 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
      *
      * @param source
      * @param req
      */
    private def processRequestVoteRequest(source:String,req:RequestVoteReq):Boolean = 
        var termValue:Long = 0L
        var voteGranted:Boolean = false

        if req.term < term then
            // reject the request
            termValue = currentTerm
        else
            // already vote to another server in the term.
            if req.term == term && (voteFor != "" && voteFor != req.candidateId) then
                termValue = currentTerm 
            else 
                if req.term > term then
                    updateCurrentTerm(req.term,None)
                //
                termValue = currentTerm
                // compare log info,if the candicate's log is newer than current server, should vote it.
                log.latest match
                    case Failure(e) => throw e
                    case Success((lastLogIdx,lastLog)) => 
                        if lastLogIdx <= req.lastLogIndex && lastLog.term <= req.lastLogTerm then 
                            voteGranted = true
        //
        if voteGranted then
            voteFor = req.candidateId
        //
        sendRequestVoteResponse(source,RequestVoteResp(termValue,voteGranted))
        true
    
    /**
      * 1. Reply false if term < currentTerm (§5.1)
      * 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
      * 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
      * 4. Append any new entries not already in the log
      * 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
      *
      * @param source
      * @param req
      */
    private def processAppendEntriesRequest(source:String,req:AppendEntriesReq):Unit =
        //
        var updated = false
        var resp:Option[AppendEntriesResp] = None
        //
        if req.term < currentTerm then
            resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex))
        else 
            if req.term > currentTerm then
                updateCurrentTerm(req.term,Some(req.leaderId))
            else
                // if current server is candicate, it should become follower.
                if stat.role == Raft.RoleCandicate then
                    setRole(Raft.RoleFollower)
                //
                leaderId = Some(req.leaderId)
            //
            updated = true
            log.dropRightFrom(req.prevLogIndex,req.prevLogTrem) match
                case Failure(e) => resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex))  
                case Success(_) => log.append(req.entries) match
                    case Failure(e) => resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex))
                    case Success(_) => log.setCommitIndex(req.leaderCommit) match 
                        case Success(_) => resp = Some(AppendEntriesResp(currentTerm,true,log.currentIndex,log.commitIndex))
                        case Failure(e) => resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex))
        resp match
            case Some(r) => sendAppendEntriesResponse(source,r)
            case None => None
    
    //
    private def sendRequestVoteResponse(target:String,resp:RequestVoteResp):Unit = ???
    private def sendAppendEntriesResponse(target:String,resp:AppendEntriesResp):Unit = ???
    //
    

