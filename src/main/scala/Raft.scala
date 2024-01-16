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

import scala.collection.mutable.{ArrayBuffer,Map,Queue}
import scala.concurrent.{Future,Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Try,Failure,Success}
import scala.util.Random
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.lang.Thread

case class RaftOptions(id:String,transportType:String,maxLogEntriesPerRequest:Int,electionTimeout:Int,peers:List[(String,String,Int)])

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
    val exceptionNoTransport = new Exception("not found transport")
    //
    def apply(ops:RaftOptions,fsm:StateMachine,log:LogStorage):RaftConsensusModule = new Raft(ops,fsm,log)
        

// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
private[platcluster] class Raft(ops:RaftOptions,fsm:StateMachine,log:LogStorage) extends RaftConsensusModule:
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
    val peers:Map[String,RaftPeer] = Map[String,RaftPeer]()
    //
    var stat:RaftState = RaftState(ops.id)
    //
    var trans:Option[Transport] = None
    //
    private val msgLock:ReentrantLock = new ReentrantLock()
    private val msgQueue:Queue[Message] = new Queue[Message]()
    //
    def nodeId:String = ops.id
    def logStorage:LogStorage = log
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
    //
    def maxLogEntriesPerRequest:Int = ops.maxLogEntriesPerRequest
    def heartbeatInterval: Int = ???
    def setElectionTimeout(d: Int): Unit = ???                                                                                                            
    def setHeartbeatInterval(d: Int): Unit = ???

    def init():Try[Unit] = 
        try
            lock.writeLock().lock()
            if stat.status == Raft.StatusRun then
                Success(None)
            else
                //
                ops.transportType match
                    case "http" => trans = Some(new HttpTransport(this))
                    case "grpc" => trans = Some(new RPCTransport(this))
                    case _ => throw new Exception(s"not support such transport type ${ops.transportType}")
                //
                log.init() match
                    case Failure(e) => throw e
                    case Success(_) => None
                fsm.init() match
                    case Failure(e) => throw e
                    case Success(_) => None
                //
                stopSignal = false
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
                case Success(entry) =>
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
    // TODO: convert the command to a Message,send it to raft, wait the exec result
    def apply(cmd:Command):Try[Result] = ???
    //
    def apply(cmd:Command,timeout:Int):Try[Result] = ???
    //
    def applyAsync(cmd:Command):Future[Try[Result]] = ???
    //
    def applyAsync(cmd:Command,timeout:Int):Future[Try[Result]] = ???
    //
    def put(key:String,value:String):Try[Unit] = ???
    def get(key:String):Try[String] = ???
    def delete(key:String):Try[Unit] = ???
    def joinNode(id:String,ip:String,port:Int):Try[Unit] = ???
    def removeNode(id:String):Try[Unit] = ???
    // TODO: create a Message,send it to msgQueue, wait a promise to get the 
    def AppendEntries(req:AppendEntriesReq):Try[AppendEntriesResp] = ???
    //
    def RequestVote(req:RequestVoteReq) :Try[RequestVoteResp] = ???
    //
    def term:Long = 
        try
            lock.readLock().lock()
            currentTerm
        finally
            lock.readLock().unlock()
    //
    private def recvMessage():Option[Message] = 
        try 
            msgLock.lock()
            if msgQueue.length > 0 then
                Some(msgQueue.dequeue())
            else 
                None
        finally
            msgLock.unlock()
    //
    private def sendMessage(msg:Message):Unit = 
        try 
            msgLock.lock()
            msgQueue.enqueue(msg)
        finally
            msgLock.unlock()
    //
    private def setStatus(s:String):Unit = 
        try
            lock.writeLock().lock()
            stat.status = s 
        finally 
            lock.writeLock().unlock()
    //
    private def setRole(r:String):Unit = 
        try
            lock.writeLock().lock()
            stat.role = r 
            if r == leader then 
                leaderId = Some(nodeId)
                syncedPeer.clear() 
        finally 
            lock.writeLock().unlock()
    //
    private def setFail(err:Throwable):Unit = 
        try
            lock.writeLock().lock()
            stat.status = Raft.StatusFail
            stat.errInfo = err.getMessage()
        finally 
            lock.writeLock().unlock()
    //
    private def getCurrentTermFromLog():Try[Long] = 
        log.latest match
            case Failure(e) => Failure(e)
            case Success(entry) => Success(entry.term)
    //
    private def randomTimer(start:Int,end:Int):Future[Int] = Future {
        val t = Random.between(start,end)
        Thread.sleep(t)
        t
    }
    def electionTimeout:Int = ops.electionTimeout
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
      * 
      * • If command received from client: append entry to local log,respond after entry applied to state machine (§5.3)
      * 
      * • If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
      * 
      * • If successful: update nextIndex and matchIndex for follower (§5.3)
      * 
      * • If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
      * 
      * • If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:set commitIndex = N (§5.3, §5.4)
      */
    private def runLeader():Unit = 
        var logIdx = 0L
        log.latest match 
            case Failure(e) => throw e
            case Success(entry) => logIdx = entry.index
        //
        for (_,p) <- peers do
            // TODO set the prevLogIndex
            p.setPrevLogIndex(logIdx)
            // TODO Start send heartbeat to it
            p.startHeartbeat(heartbeatInterval)

        //
        // TODO 
        //
        while role == Raft.RoleLeader do 
            try
                if stopSignal then
                    for (_,p) <- peers do 
                        p.stopHeartbeat()
                    setStatus(Raft.StatusStop)
                    return
                //
                recvMessage() match
                    case None => None
                    case Some(m) => m.msgType match 
                        case Command => 
                            val ignore = m.expire match
                                case None => false
                                case Some(e) =>  if e.isCompleted then true else false
                            if !ignore then
                                processCommand(m) match
                                    case Failure(e) => None // TODO reture this to user
                                    case Success(_) => None
                        case AppendEntriesResponse => processAppendEntriesResponse(m.source,m)
                        case AppendEntriesRequest => 
                            val (resp,_) = processAppendEntriesRequest(m) // TODO reture the resp by RPC
                            
                        case RequestVoteRequest => 
                            val (resp,_) = processRequestVoteRequest(m) // TODO reture the resp by RPC
                        case _ => None
            catch
                case e:Exception => None // TODO err handler

    /**
      * • Respond to RPCs from candidates and leaders
      * 
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
                try
                    recvMessage() match
                        case None => None
                        case Some(m) => m.msgType match
                            case Command => 
                                throw new Exception("NotLeaderError")  // TODO process join command
                            case AppendEntriesRequest => 
                                val (resp,flush) = processAppendEntriesRequest(m) // TODO reture the resp by RPC
                            case RequestVoteRequest => 
                                val (resp,flush) = processRequestVoteRequest(m) // TODO reture the resp by RPC
                            case _ => None
                catch
                    case e:Exception => None // TODO err handler
            //
            if flush then 
                timeout = randomTimer(electionTimeout,electionTimeout*2)

    /**
      * On conversion to candidate, start election:
      *
      * • Increment currentTerm
      * 
      * • Vote for self
      * 
      * • Reset election timer
      * 
      * • Send RequestVote RPCs to all other servers
      * 
      * • If votes received from majority of servers: become leader
      * 
      * • If AppendEntries RPC received from new leader: convert tofollower
      * 
      *  • If election timeout elapses: start new election
      */
    private def runCandicate():Unit = 
        // reset the leader is null.
        leaderId = None
        //
        var lastLogIndex = 0L
        var lastLogTerm = 0L
        log.latest match
            case Failure(e) => throw e // TODO: err handler
            case Success(entry) => 
                lastLogIndex = entry.index
                lastLogTerm = entry.term
        
        var voteGranted:Int = 0
        val resps = Map[String,Try[Unit]]()
        var timeout:Future[Int] = null
        var voteContinue = true

        while role == Raft.RoleCandicate do
            if voteContinue then
                // do once elect
                currentTerm += 1
                voteFor = nodeId
                resps.clear()
                //
                for (name,p) <- peers do
                    //TODO: waitGroup += resp
                    val rv:Future[Try[RequestVoteResp]] = Future {
                        trans match
                            case None => Failure(Raft.exceptionNoTransport)
                            case Some(tran) =>
                                tran.RequestVote(p,RequestVoteReq(currentTerm,nodeId,lastLogIndex,lastLogTerm))
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
                    recvMessage() match
                        case None => None
                        case Some(m) => m.msgType match
                            case Command => 
                                throw new Exception("NotLeaderError") // TODO send the error to user.
                            case AppendEntriesRequest => 
                                val (resp,_) = processAppendEntriesRequest(m) // TODO reture the resp by RPC
                            case RequestVoteRequest => 
                                val (resp,_) = processRequestVoteRequest(m) // TODO reture the resp by RPC
                catch
                    case e:Exception => None // TODO  err handler
    
    /* 
      * • If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
      *
      * • If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
    */
    private def processVoteResponse(resp:RequestVoteResp):Boolean = 
        if resp.voteGranted && resp.term == currentTerm then
            true
        else 
            if resp.term > currentTerm then
                updateCurrentTerm(resp.term,None)
            false
    
    /**
      * 1. Reply false if term < currentTerm
      * 
      * 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
      *
      * @param req
      */
    private def processRequestVoteRequest(req:RequestVoteReq):(RequestVoteResp,Boolean) = 
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
                    case Success(entry) => 
                        if entry.index <= req.lastLogIndex && entry.term <= req.lastLogTerm then 
                            voteGranted = true
        //
        if voteGranted then
            voteFor = req.candidateId
        //
        (RequestVoteResp(termValue,voteGranted),voteGranted)
    
    /**
      * 1. Reply false if term < currentTerm (§5.1)
      * 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
      * 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
      * 4. Append any new entries not already in the log
      * 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
      *
      * @param req
      */
    private def processAppendEntriesRequest(req:AppendEntriesReq):(AppendEntriesResp,Boolean) =
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
            case None => throw new Exception("response append entries request failed")
            case Some(r) => (r,updated)
    //
    private val syncedPeer:Map[String,Boolean] = Map[String,Boolean]()
    //
    private def processAppendEntriesResponse(source:String,resp:AppendEntriesResp):Unit = 
        if resp.term > term then
            updateCurrentTerm(resp.term,None)
        else if resp.success then 
            syncedPeer(source) = true
            if syncedPeer.size >= majority then 
                //
                var arr = (for (_,p) <- peers yield p.getPrevLogIndex).toBuffer
                arr += log.currentIndex
                arr = arr.sortWith((x:Long,y:Long) => x < y)

                val commitMax = arr(majority-1)
                val commitIdx = log.commitIndex

                if commitMax > commitIdx then
                    // TODO log.sync()
                    log.setCommitIndex(commitMax)
    //
    private def processCommand(cmd:Command):Try[Unit] =
        log.create(cmd) match 
            case Failure(e) => Failure(e)
            case Success(entry) => log.append(entry) match 
                case Failure(e) => Failure(e)
                case Success(_) =>
                    syncedPeer(nodeId) = true
                    // if only one node in cluster.
                    if peers.size == 0 then 
                        val commitIdx = log.currentIndex
                        log.setCommitIndex(commitIdx) 
                    else 
                        Success(None)
    //
    import platcluster.Message.given_Conversion_AppendEntriesResp_String
    def sendAppendEntriesRequest(target:String,req:AppendEntriesReq):Unit = 
        peers.get(target) match
            case None => throw new Exception(s"not known such peer ${target}")
            case Some(peer) => 
                trans match
                    case None => throw Raft.exceptionNoTransport
                    case Some(tran) => tran.AppendEntries(peer,req) match 
                        case Failure(e) => throw e 
                        case Success(resp) =>
                            if resp.success then
                                // 
                                if req.entries.length > 0 then
                                    peer.setPrevLogIndex(req.entries.last.index)
                                    /* 
                                    // if peer append a log entry from the current term
                                    // we set append to true
                                    if req.Entries[len(req.Entries)-1].GetTerm() == p.server.currentTerm {
                                        resp.append = true
                                    }
                                    */
                            else 
                                //
                                if resp.term > term then 
                                    // this happens when there is a new leader comes up that this *leader* has not
                                    // known yet.
                                    // this server can know until the new leader send a ae with higher term
                                    // or this server finish processing this response.
                                    None
                                else if resp.term == req.term && resp.commitIndex >= peer.prevLogIndex then
                                    //
                                    peer.setPrevLogIndex(resp.commitIndex)
                                else if peer.prevLogIndex > 0 then 
                                    //
                                    peer.setPrevLogIndex(peer.prevLogIndex-1)
                                    //
                                    if peer.prevLogIndex > resp.currentLogIndex then 
                                        peer.setPrevLogIndex(resp.currentLogIndex)
                            //
                            sendMessage(Message(peer.id,AppendEntriesResponse,resp,None,None))