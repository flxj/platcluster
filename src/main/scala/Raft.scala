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
import scala.concurrent.{Future,Promise,Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Try,Failure,Success}
import scala.util.Random
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.lang.Thread
import java.time.Instant

case class RaftOptions(
    //
    id:String,
    host:String,
    port:Int,
    //
    transportType:String,
    //
    maxLogEntriesPerRequest:Int,
    heartbeatInterval:Int,
    electionTimeout:Int,
    confDir:String,
    peers:List[(String,String,Int)]
)

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

    val transHttp = "http"
    val transGRPC = "grpc"

    val defaultHeartbeatInterval = 50 // Millisecond
    val defaultElectionTimeout = 150 // 

    val exceptionAlreadyRunning = new Exception("raft node has already running")
    val exceptionNoTransport = new Exception("not found transport")
    val exceptionNotLeader = new Exception("current server is not raft leader")
    val exceptionCmdTimeout = new Exception("command has already timeout")
    val exceptionNoSupportTrans = new Exception(s"not support such transport type")
    //
    def apply(ops:RaftOptions,fsm:StateMachine,log:LogStorage):RaftModule = new Raft(ops,fsm,log)
        

// log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
private[platcluster] class Raft(ops:RaftOptions,fsm:StateMachine,log:LogStorage) extends RaftModule:
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
    var trans:Option[TransportServer] = None
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
    def heartbeatInterval: Int = ops.heartbeatInterval
    def setElectionTimeout(d: Int): Unit = ???                                                                                                            
    def setHeartbeatInterval(d: Int): Unit = ???
    def electionTimeout:Int = ops.electionTimeout
    def majority:Int = ???
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
    // TODO: save current node info to json file.
    private def saveState(confDir:String):Try[Unit] = ???
    // TODO: load current node info from json file.
    private def loadState(confDir:String):Try[RaftNodeState] = ???
    //
    def init():Try[Unit] = 
        try
            lock.writeLock().lock()
            if stat.status == Raft.StatusRun then
                Success(None)
            else
                //
                ops.transportType match
                    case Raft.transHttp => trans = Some(new HttpTransport(ops.host,ops.port,this))
                    case Raft.transGRPC => trans = Some(new RPCTransport(ops.host,ops.port,this))
                    case _ => throw Raft.exceptionNoSupportTrans
                // init peers
                for (name,ip,port) <- ops.peers do
                    peers(name) = RaftPeer(name,ip,port,this)
                
                loadState(ops.confDir) match 
                    case Failure(e) => throw e 
                    case Success(s) => 
                        commitIndex = s.commitIndex
                        for p <- s.peers do 
                            val peer = RaftPeer(p.id,p.ip,p.port,this)
                            peer.setNextLogIndex(p.nextIndex)
                            peers(p.id) = peer 
                //
                saveState(ops.confDir) match 
                    case Failure(e) => throw e 
                    case Success(_) => None
                //
                fsm.init() match
                    case Failure(e) => throw e
                    case Success(_) => None
                //
                log.updateCommitIndex(commitIndex) match
                    case Success(_) => None
                    case Failure(e) => throw e
                // TODO
                log.init(fsm) match
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
                    //
                    trans match
                        case None => Failure(Raft.exceptionNoTransport)
                        case Some(svc) => svc.startAsync() match
                            case Failure(e) => Failure(e)
                            case Success(_) => 
                                // start main loop.
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
            setStatus(Raft.StatusStop)
            trans match
                case None => Failure(Raft.exceptionNoTransport)
                case Some(svc) => svc.stop() 
    //
    import MessageTypes._
    import Message._
    import Message.msgToResult
    import Message.cmdToJson 
    import Message.cmdToMsg
    //
    def apply(cmd:Command,timeout:Option[Int]):Try[Result] = // TODO: use a timeout Future to process timeout
        if cmd.op == Storage.kvOpGet then
            get(cmd.key) match
                case Failure(e) => Failure(e)
                case Success(v) => Success(Result(true,"",v))
        else 
            val resp = Promise[Try[Message]]()
            val msg = Message(nodeId,Cmd,cmd,Instant.now(),timeout,Some(resp))
            sendMessage(msg)
            //
            var res:Option[Try[Result]] = None
            resp.future.foreach { 
                case Failure(e) => res = Some(Failure(e))
                case Success(m) => res = Some(Success(msgToResult(m)))
            }
            res match
                case Some(r) => r 
                case None => Failure(new Exception("apply command failed"))
    //
    def apply(cmd:Command):Try[Result] = apply(cmd,None)
    //
    def applyAsync(cmd:Command):Future[Try[Result]] = Future {
        apply(cmd,None)
    }
    //
    def applyAsync(cmd:Command,timeout:Option[Int]):Future[Try[Result]] = Future {
        apply(cmd,timeout)
    }
    //
    def get(key:String):Try[String] = fsm.get(key)
    //
    def put(key:String,value:String):Try[Unit] = 
        apply(Command(Storage.kvOpPut,key,value)) match
            case Failure(e) => Failure(e)
            case Success(res) => 
                if res.success then 
                    Success(None) 
                else 
                    Failure(new Exception(s"${res.err} ${res.content}"))
    //
    def delete(key:String):Try[Unit] = 
        apply(Command(Storage.kvOpDel,key,"")) match
            case Failure(e) => Failure(e)
            case Success(res) => 
                if res.success then 
                    Success(None) 
                else 
                    Failure(new Exception(s"${res.err} ${res.content}"))
    //
    def joinNode(id:String,ip:String,port:Int):Try[Unit] = 
        try
            lock.writeLock().lock()
            if !peers.contains(id) && id != nodeId then
                val peer = RaftPeer(id, ip,port,this)
                if role == Raft.RoleLeader then
                    peer.startHeartbeat(heartbeatInterval)
                peers(id) = peer 
            //
            saveState(ops.confDir) 
        catch
            case e:Exception => Failure(e)
        finally
            lock.writeLock().unlock()

    //
    def removeNode(id:String):Try[Unit] = 
        try
            lock.writeLock().lock()
            if id != nodeId && peers.contains(id) then
                val p = peers(id)
                if role == Raft.RoleLeader then
                    p.stopHeartbeat()
                peers -= id
            //
            saveState(ops.confDir) 
        catch
            case e:Exception => Failure(e)
        finally
            lock.writeLock().unlock()
    
    import Message.appendReqToJson
    import Message.msgToAppendResp
    // create a Message,send it to msgQueue, wait a promise to get the result.
    def appendEntries(req:AppendEntriesReq):Try[AppendEntriesResp] = 
        //
        val resp = Promise[Try[Message]]()
        val msg = Message("",AppendEntriesRequest,req,Instant.now(),None,Some(resp))
        sendMessage(msg)
        //
        var res:Option[Try[AppendEntriesResp]] = None
        resp.future.foreach { 
            case Failure(e) => res = Some(Failure(e))
            case Success(m) => res = Some(Success(msgToAppendResp(m)))
        }
        //
        res match
            case Some(r) => r 
            case None => Failure(new Exception("process request failed"))
    //
    import Message.voteReqToJson
    import Message.msgToVoteResp
    def requestVote(req:RequestVoteReq):Try[RequestVoteResp] = 
        val resp = Promise[Try[Message]]()
        val msg = Message("",RequestVoteRequest,req,Instant.now(),None,Some(resp))
        sendMessage(msg)
        //
        var res:Option[Try[RequestVoteResp]] = None
        resp.future.foreach { 
            case Failure(e) => res = Some(Failure(e))
            case Success(m) => res = Some(Success(msgToVoteResp(m)))
        }
        //
        res match
            case Some(r) => r 
            case None => Failure(new Exception("process request failed"))
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
    //
    import Message.appendRespToMsg
    import Message.voteRespToMsg
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
                        case Cmd => 
                            if !Message.expired(m) then
                                processCommandMsg(m) match
                                    case Failure(e) => 
                                        m.response match
                                            case Some(p) => p.failure(e)
                                            case None => None
                                    case Success(_) => None
                            else 
                                m.response match
                                    case Some(p) => p.failure(Raft.exceptionCmdTimeout)
                                    case None => None
                        case AppendEntriesResponse => processAppendEntriesResponse(m)
                        case AppendEntriesRequest => 
                            val (resp,_) = processAppendEntriesRequest(m)
                            m.response match
                                case Some(p) => p.success(Success(appendRespToMsg(resp)))
                                case None => None
                        case RequestVoteRequest => 
                            val (resp,_) = processRequestVoteRequest(m)
                            m.response match
                                case Some(p) => p.success(Success(voteRespToMsg(resp)))
                                case None => None
                        case _ => None
            catch
                case e:Exception => None // TODO: error handler: for example, record then to logger

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
                            case Cmd => 
                                m.response match
                                    case Some(p) => p.failure(Raft.exceptionNotLeader)
                                    case None => None
                            case AppendEntriesRequest => 
                                val (resp,flush) = processAppendEntriesRequest(m)
                                m.response match
                                    case Some(p) => p.success(Success(appendRespToMsg(resp)))
                                    case None => None
                            case RequestVoteRequest => 
                                val (resp,flush) = processRequestVoteRequest(m)
                                m.response match
                                    case Some(p) => p.success(Success(voteRespToMsg(resp)))
                                    case None => None
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
                                tran.requestVote(p,RequestVoteReq(currentTerm,nodeId,lastLogIndex,lastLogTerm))
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
                            case Cmd => 
                                m.response match
                                    case Some(p) => p.failure(Raft.exceptionNotLeader)
                                    case None => None
                            case AppendEntriesRequest => 
                                val (resp,_) = processAppendEntriesRequest(m)
                                m.response match
                                    case Some(p) => p.success(Success(appendRespToMsg(resp)))
                                    case None => None
                            case RequestVoteRequest => 
                                val (resp,_) = processRequestVoteRequest(m)
                                m.response match
                                    case Some(p) => p.success(Success(voteRespToMsg(resp)))
                                    case None => None
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
            resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex,nodeId))
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
                case Failure(e) => resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex,nodeId))  
                case Success(_) => log.append(req.entries) match
                    case Failure(e) => resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex,nodeId))
                    case Success(_) => log.commitLog(req.leaderCommit) match 
                        case Success(_) => resp = Some(AppendEntriesResp(currentTerm,true,log.currentIndex,log.commitIndex,nodeId))
                        case Failure(e) => resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex,nodeId))
        resp match
            case None => throw new Exception("response append entries request failed")
            case Some(r) => (r,updated)
    //
    private val syncedPeer:Map[String,Boolean] = Map[String,Boolean]()
    //
    private def processAppendEntriesResponse(resp:AppendEntriesResp):Unit = 
        if resp.term > term then
            updateCurrentTerm(resp.term,None)
        else if resp.success then 
            syncedPeer(resp.nodeId) = true
            if syncedPeer.size >= majority then 
                //
                var arr = (for (_,p) <- peers yield p.getPrevLogIndex).toBuffer
                arr += log.currentIndex
                arr = arr.sortWith((x:Long,y:Long) => x < y)

                val commitMax = arr(majority-1)
                val commitIdx = log.commitIndex

                if commitMax > commitIdx then
                    // TODO log.sync()
                    log.commitLog(commitMax)
    //
    import Message.resultToMsg
    private def processCommandMsg(msg:Message):Try[Unit] =
        log.create(currentTerm,msg) match 
            case Failure(e) => Failure(e)
            case Success(entry) => 
                entry.response match
                    case None => None
                    case Some(p) => 
                        // TODO waitGroup + f
                        val f = Future {
                            msg.response match
                                case None => None
                                case Some(resp) =>
                                    p.future.foreach {
                                        case Failure(e) => 
                                            if !resp.isCompleted then
                                                resp.failure(e)
                                        case Success(r) =>
                                            if !resp.isCompleted then
                                                resp.success(Success(resultToMsg(r)))
                                    }
                        } 
                log.append(entry) match 
                case Failure(e) => Failure(e)
                case Success(_) =>
                    syncedPeer(nodeId) = true
                    // if only one node in cluster.
                    if peers.size == 0 then 
                        val commitIdx = log.currentIndex
                        log.commitLog(commitIdx) 
                    else 
                        Success(None)
    //
    import Message.appendRespToJson
    def sendAppendEntriesRequest(target:String,req:AppendEntriesReq):Try[Unit] = 
        peers.get(target) match
            case None => Failure(throw new Exception(s"not known such peer ${target}"))
            case Some(peer) => 
                trans match
                    case None => Failure(Raft.exceptionNoTransport)
                    case Some(tran) => tran.appendEntries(peer,req) match 
                        case Failure(e) => Failure(e) 
                        case Success(resp) =>
                            try
                                val timestamp = Instant.now()
                                if resp.success then
                                    // 
                                    if req.entries.length > 0 then
                                        peer.setPrevLogIndex(req.entries.last.index)
                                        /* 
                                        // if peer append a log entry from the current term we set append to true
                                            resp.append = true
                                        */
                                else 
                                    //
                                    if resp.term > term then 
                                        // 
                                        None
                                    else if resp.term == req.term && resp.commitIndex >= peer.getPrevLogIndex then
                                        //
                                        peer.setPrevLogIndex(resp.commitIndex)
                                    else if peer.getPrevLogIndex > 0 then 
                                        //
                                        peer.setPrevLogIndex(peer.getPrevLogIndex-1)
                                        //
                                        if peer.getPrevLogIndex > resp.currentLogIndex then 
                                            peer.setPrevLogIndex(resp.currentLogIndex)
                                //
                                val msg = Message(peer.id,AppendEntriesResponse,resp,timestamp,None,None)
                                Success(sendMessage(msg))
                            catch
                                case e:Exception => Failure(e)
                            
//
