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
    //
    ip:String,
    //
    port:Int,
    //
    transportType:String,
    //
    maxLogEntriesPerRequest:Int,
    //
    heartbeatInterval:Int,
    //
    electionTimeout:Int,
    //
    confDir:String,
    // peers info (id,ip,port)
    peers:List[(String,String,Int)]
)

private[platcluster] class RaftState(id:String):
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
    val StatusStopping = "stopping"
    val StatusUnknown = "unknown"

    val transHttp = "http"
    val transGRPC = "grpc"

    val defaultHeartbeatInterval = 50 // Millisecond
    val defaultElectionTimeout = 150 // 
    val defaultMaxLog = 100

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
    private var heartbeatDuration:Int = ops.heartbeatInterval
    private var electionDuration:Int = ops.electionTimeout
    
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
    def heartbeatInterval: Int = heartbeatDuration 
    def electionTimeout:Int = electionDuration
    //
    def setElectionTimeout(d: Int): Unit = 
        try
            lock.writeLock().lock()
            electionDuration = d 
        finally
            lock.writeLock().unlock()
    //                                                                                                            
    def setHeartbeatInterval(d: Int): Unit =
        try
            lock.writeLock().lock()
            heartbeatDuration = d 
        finally
            lock.writeLock().unlock()
    //
    def majority:Int = 
        try
            lock.readLock().lock()
            (peers.size+1)/2+1
        finally
            lock.readLock().unlock()
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
            if s != Raft.StatusRun then
                leaderId = None
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
            leaderId = None
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
        //println(s"[debug] timer=${t}ms")
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
                p.stopHeartbeat()
        //
        if stat.role != Raft.RoleFollower then 
            setRole(Raft.RoleFollower)
           // println(s"[debug] updateCurrentTerm  --> follower")
        
        lock.writeLock().lock()
        currentTerm = termValue
        leaderId = name
        voteFor = ""
        lock.writeLock().unlock()
    //
    import scala.io.Source
    import java.io.{File,PrintWriter}
    import io.circe.syntax._
    import io.circe.generic.auto._
    import io.circe._
    import io.circe.literal._
    import io.circe.parser.decode
    // save current node info to json file.
    private def saveState(confDir:String):Try[Unit] = 
        val path = confDir+File.separator+"snapshort"
        var p:PrintWriter = null 
        try 
            lock.readLock().lock()
            p = new PrintWriter(new File(path))
            val infos = (
                for (_,p) <- peers yield 
                    val (ip,port) = p.addr
                    RaftPeerInfo(p.id,ip,port,p.getNextLogIndex)
                ).toArray
            val s = RaftNodeState(commitIndex,lastApplied,0,infos)
            p.write(s.asJson.toString())
            Success(None)
        catch
            case e:Exception => Failure(e)
        finally
            if p != null then 
               p.close()
            lock.readLock().unlock()
    // load current node info from json file.
    private def loadState(confDir:String):Try[RaftNodeState] =
        val path = confDir+File.separator+"snapshort"
        try
            val content = Source.fromFile(path).mkString
            decode[RaftNodeState](content) match
                case Left(e) => throw e
                case Right(s) => Success(s)
        catch
            case e: java.io.FileNotFoundException => 
                val s = RaftNodeState(0L,0L,0L,Array[RaftPeerInfo]())
                Success(s)
            case e1:Exception => Failure(e1)
    //
    private def applyKV(entry:LogEntry):Unit = 
        val res = fsm.apply(entry.cmd) 
        entry.response match
            case None => None
            case Some(p) =>
                if !p.isCompleted then 
                    p.success(res)
                    println(s"[debug] applyKV set response")
    //
    private def applyChange(entry:LogEntry):Unit = 
        val res = entry.cmd.op match
            case "join" => 
                //
                val i = entry.cmd.value.indexOf(":")
                if i <= 0 || i == entry.cmd.value.length() then 
                    throw new Exception(s"known add node addr ${entry.cmd.value}")
                val ip = entry.cmd.value.substring(0,i)
                val port = entry.cmd.value.substring(i+1).toInt 
                addNode(entry.cmd.key,ip,port) 
            case "leave" => removeNode(entry.cmd.key)
            case _ => Success(None)
        entry.response match
            case None => None
            case Some(p) =>
                if !p.isCompleted then 
                    res match
                        case Failure(e) => p.failure(e)
                        case Success(_) => p.success(Success(Result(true,"","")))
    //
    def init():Try[Unit] = 
        try
            lock.writeLock().lock()
            if stat.status == Raft.StatusRun then
                Success(None)
            else
                //
                ops.transportType match
                    case Raft.transHttp => trans = Some(new HttpTransport(ops.ip,ops.port,this))
                    case Raft.transGRPC => trans = Some(new RPCTransport(ops.ip,ops.port,this))
                    case _ => throw Raft.exceptionNoSupportTrans
                
                // if snapshort exists, use it overwrite ops.
                var prevIndex:Long = 0L
                loadState(ops.confDir) match 
                    case Failure(e) => throw e 
                    case Success(s) => 
                        commitIndex = s.commitIndex
                        lastApplied = s.appliedIndex
                        prevIndex = s.prevIndex
                        for p <- s.peers do 
                            val peer = RaftPeer(p.id,p.ip,p.port,this)
                            peer.setNextLogIndex(p.nextIndex)
                            peers(p.id) = peer 
                
                // first init cluster.
                if peers.size == 0 then 
                    // create peers
                    for (name,ip,port) <- ops.peers do
                        peers(name) = RaftPeer(name,ip,port,this)
                
                saveState(ops.confDir) match 
                    case Failure(e) => throw e 
                    case Success(_) => None
                //
                fsm.init() match
                    case Failure(e) => throw new Exception(s"init fsm failed ${e}")
                    case Success(_) => None
                //
                //log.updateAppliedIndex(commitIndex) match
                //    case Success(_) => None
                //    case Failure(e) => throw e
                //log.updatePrevIndex() match
                //    case Success(_) => None
                //    case Failure(e) => throw e

                log.registerApplyFunc(cmdTypeKVRW,applyKV)
                log.registerApplyFunc(cmdTypeChange,applyChange)

                log.setCommitIndex(commitIndex) 

                log.init() match
                    case Failure(e) => throw new Exception(s"init log storage failed ${e}")
                    case Success(_) => None
                //
                log.updateCommitIndex(commitIndex) match
                    case Success(_) => None
                    case Failure(e) => throw new Exception(s"update commit index failed ${e}")
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
            init() match
                case Failure(e) => return Failure(e)
                case Success(_) => None 
            //
            stopSignal = false
            setRole(Raft.RoleFollower)
            log.latest match
                case Failure(e) => Failure(e)
                case Success(entry) =>
                    currentTerm = entry.term
                    // start the tranport module.
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
        else if s == Raft.StatusStopping then 
            Failure(new Exception("raft is stopping"))
        else
            try
                stopSignal = true
                println(s"waiting future completed...")
                for w <- waitGroup do
                    Await.result(w,1.hours)
                //
                waitGroup.clear()
                setStatus(Raft.StatusStop)
                println(s"stopping transport...")
                trans match
                    case None => None
                    case Some(svc) => svc.stop() 
                log.close()
            finally
                saveState(ops.confDir) match
                    case Success(_) => None
                    case Failure(e) => None
    //
    import MessageTypes._
    import Message._
    import Message.msgToResult
    import Message.cmdToJson 
    import Message.cmdToMsg
    //
    def apply(cmd:Command,timeout:Option[Int]):Try[Result] = 
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

            try
                val t = timeout match
                case None => 1.hours
                case Some(d) => d.milliseconds

                Await.result(resp.future,t) match
                    case Success(m) =>  res = Some(Success(msgToResult(m)))
                    case Failure(e) => res = Some(Failure(e))
            catch
                case e:Exception => res = Some(Failure(e))
            //
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
        apply(Command(cmdTypeKVRW,Storage.kvOpPut,key,value)) match
            case Failure(e) => Failure(e)
            case Success(res) => 
                if res.success then 
                    Success(None) 
                else 
                    Failure(new Exception(s"${res.err} ${res.content}"))
    //
    def delete(key:String):Try[Unit] = 
        apply(Command(cmdTypeKVRW,Storage.kvOpDel,key,"")) match
            case Failure(e) => Failure(e)
            case Success(res) => 
                if res.success then 
                    Success(None) 
                else 
                    Failure(new Exception(s"${res.err} ${res.content}"))
    //
    def joinNode(id:String,ip:String,port:Int):Try[Unit] =
        val cmd = Command(cmdTypeChange,opJoin,id,addrFmt.format(ip,port))
        apply(cmd,None) match 
            case Success(_) => Success(None)
            case Failure(e) => Failure(e)
    //
    def leaveNode(id:String):Try[Unit] = 
        val cmd = Command(cmdTypeChange,opLeave,id,"")
        apply(cmd,None) match 
            case Success(_) => Success(None)
            case Failure(e) => Failure(e)
    //
    private def addNode(id:String,ip:String,port:Int):Try[Unit] = 
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
    private def removeNode(id:String):Try[Unit] = 
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
        try
            Await.result(resp.future,Duration(10, MINUTES)) match
                case Failure(e) => res = Some(Failure(e))
                case Success(m) =>  
                    val vr = msgToAppendResp(m)
                    res = Some(Success(vr))
                    //println(s"[debug] get append message process RESULT ${vr}")
        catch
            case e:Exception => res = Some(Failure(e))
        //
        res match
            case Some(r) => r 
            case None => Failure(new Exception("process appendEntries request failed"))
    //
    import Message.voteReqToJson
    import Message.msgToVoteResp
    def requestVote(req:RequestVoteReq):Try[RequestVoteResp] = 
        val resp = Promise[Try[Message]]()
        val msg = Message("",RequestVoteRequest,req,Instant.now(),None,Some(resp))
        sendMessage(msg)
        //
        //println(s"[debug] call raft requestVote method, wait message process")
        //
        var res:Option[Try[RequestVoteResp]] = None
        try
            Await.result(resp.future,Duration(5, MINUTES)) match
                case Failure(e) => res = Some(Failure(e))
                case Success(m) =>  
                    val vr = msgToVoteResp(m)
                    res = Some(Success(vr))
                    //println(s"[debug] get vote message process RESULT ${vr}")
        catch
            case e:Exception => res = Some(Failure(e))

        //println(s"[debug] get request vote res")
        res match
            case Some(r) => r 
            case None => Failure(new Exception("process requestVote request failed"))
    //
    private def mainLoop(): Unit =
        try
            var s = state
            while !stopSignal && s.status != Raft.StatusStop && s.status != Raft.StatusFail do 
                s.role match
                    case Raft.RoleCandicate => runCandicate()
                    case Raft.RoleFollower => runFollower()
                    case Raft.RoleLeader => runLeader()
                s = state
        catch
            case e:Exception => setFail(e)
        finally
            saveState(ops.confDir) match
                case Success(_) => None
                case Failure(e) => None
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
        //println(s"[debug] run as leader")
        var logIdx = 0L
        log.latest match 
            case Failure(e) => throw e
            case Success(entry) => logIdx = entry.index
        //
        for (name,p) <- peers do
            // init the peer's prevLogIndex with leader's logIndex
            p.setPrevLogIndex(logIdx)
            // start heartbeat to the peer.
            p.startHeartbeat(heartbeatInterval)
            //println(s"[debug] start heartbeat to ${name}")
        //
        // TODO: Upon election
        //val e = Future {
        //    apply(Command(cmdTypeNone,"","",""),None) match
        //        case Failure(e) => println(s"[debug] apply none cmd error ${e}")
        //        case Success(_) => println(s"[debug] applied none cmd")
       // }
        //
        while role == Raft.RoleLeader && !stopSignal do 
            try
                if stopSignal then
                    for (_,p) <- peers do 
                        p.stopHeartbeat()
                    setStatus(Raft.StatusStop)
                else
                    // process a mesage.
                    recvMessage() match
                        case None => None
                        case Some(m) => m.msgType match 
                            case Cmd => 
                                if !Message.expired(m) then
                                    processCommandMsg(m) match
                                        case Success(_) => None
                                        case Failure(e) => m.response match
                                            case None => None
                                            case Some(p) => 
                                                if !p.isCompleted then
                                                    p.failure(e)
                                else 
                                    m.response match
                                        case None => None
                                        case Some(p) => 
                                            if !p.isCompleted then
                                                p.failure(Raft.exceptionCmdTimeout)
                            case AppendEntriesResponse => 
                                processAppendEntriesResponse(m)
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
                case e:Exception => println(s"[debug] leader process message error ${e}") // TODO: error handler: for example, record then to logger
        //
        syncedPeer.clear()

    /**
      * • Respond to RPCs from candidates and leaders
      * 
      * • If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
      */
    private def runFollower():Unit = 
        println(s"[debug] run as follower")
        // randomly init a timer from range [electionTimeout,electionTimeout*2] for timeout.
        var timeout = randomTimer(electionTimeout,electionTimeout*2)
        // run follower.
        while role == Raft.RoleFollower && !stopSignal do 
            if stopSignal then 
                setStatus(Raft.StatusStop)
            else 
                // if need reset the timeout timer.
                var flush = false
                // timeout condition is triggered: become candicate
                if timeout.isCompleted then 
                    setRole(Raft.RoleCandicate)
                    println(s"[debug] follower --> candicate")
                else 
                    // process request from other servers.
                    try
                        recvMessage() match
                            case None => None
                            case Some(m) => m.msgType match
                                case Cmd => m.response match
                                    case None => None
                                    case Some(p) => p.failure(Raft.exceptionNotLeader) 
                                case AppendEntriesRequest => 
                                    val (resp,f) = processAppendEntriesRequest(m) 
                                    flush = f 
                                    m.response match
                                        case Some(p) => p.success(Success(appendRespToMsg(resp)))
                                        case None => None
                                    //println(s"[debug] after process app req,flush value is ${flush}")
                                case RequestVoteRequest => 
                                    val (resp,f) = processRequestVoteRequest(m)
                                    flush = f 
                                    m.response match
                                        case Some(p) => p.success(Success(voteRespToMsg(resp)))
                                        case None => None
                                    //println(s"[debug] after process vote req,flush value is ${flush}")
                                case _ => None
                    catch
                        case e:Exception => println(s"[debug] follower process message error ${e}") // TODO err handler
                //
                if flush then 
                    timeout = randomTimer(electionTimeout,electionTimeout*2)
                    //println(s"[debug] flushed timer")
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
        var timeout:Future[Int] = null
        var voteContinue = true

        while role == Raft.RoleCandicate && !stopSignal do
            if voteContinue then
                //println(s"[debug] start once elect...(electionTimeout=${electionTimeout}ms)")
                // do once elect
                currentTerm += 1
                voteFor = nodeId
                for (name,p) <- peers do
                    //TODO: waitGroup += resp
                    val rv:Future[Try[RequestVoteResp]] = Future {
                        trans match
                            case None => Failure(Raft.exceptionNoTransport)
                            case Some(tran) => 
                                val req = RequestVoteReq(currentTerm,nodeId,lastLogIndex,lastLogTerm)

                                //println(s"[debug] send vote request ${req} to peer ${name}")
                                tran.requestVote(p,req)
                    }
                    rv.onComplete {
                        case Failure(e) => println(s"[debug] request vote error ${e}") // TODO: maybe we need record the failure info to logger
                        case Success(r) => r match
                            case Failure(e) => None //println(s"[debug] requestVote error ${e}")
                            case Success(resp) =>
                                if processVoteResponse(resp) then
                                    voteGranted += 1
                                    //println(s"[debug] receved a vote ${voteGranted}")       
                    }
                //
                voteGranted = 1
                timeout = randomTimer(electionTimeout, electionTimeout*2)
                voteContinue = false
                //println(s"[debug] vote self ${voteGranted}")
            
            // If received enough votes then stop waiting for more votes. 
            // And return from the candidate loop.
            if voteGranted >= majority then
                setRole(Raft.RoleLeader)
                println(s"[debug] candicate --> leader")
            else if stopSignal then
                setStatus(Raft.StatusStop)
            else if timeout.isCompleted then
                // current election timeout,we need start next round.
                voteContinue = true
                //println(s"[debug] elect timeout")
            else 
                try
                    recvMessage() match
                        case None => None
                        case Some(m) => m.msgType match
                            case Cmd => m.response match
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
                                    case Some(p) => 
                                        p.success(Success(voteRespToMsg(resp)))
                                        //println(s"[debug] set vote callback success")
                                    case None => None
                            case _ => None 
                catch
                    case e:Exception => println(s"[debug] candicate process message error ${e}") // TODO  err handler
    
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

        //println(s"[debug] process vote request ${req}")
        // reject the vote if request term is smaller than current term.
        if req.term < term then
            termValue = currentTerm
        else
            // already vote to another server for the same term value. so also reject it.
            if req.term == term && (voteFor != "" && voteFor != req.candidateId) then
                termValue = currentTerm 
            else 
                // find a larger term value, shpuld update current server.
                if req.term > term then
                    updateCurrentTerm(req.term,None)
                //
                termValue = currentTerm
                // compare log info,if the candicate's log is newer than current server,should vote it.
                log.latest match
                    case Failure(e) => throw e
                    case Success(entry) => 
                        if entry.index <= req.lastLogIndex && entry.term <= req.lastLogTerm then 
                            voteGranted = true
        //
        if voteGranted then
            voteFor = req.candidateId
            //println(s"[debug] vote for ${voteFor}")
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
        // reject the request,because the term is too small.
        if req.term < currentTerm then
            resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex,nodeId))
        else 
            if req.term > currentTerm then
                // update term info and leader id.
                updateCurrentTerm(req.term,Some(req.leaderId))
            else
                // the request's term == currentTerm, which means its come from a leader.
                // if current server is candicate, it should become follower.
                if stat.role == Raft.RoleCandicate then
                    setRole(Raft.RoleFollower)
                    println(s"[debug] candicate --> follower")
                //
                leaderId = Some(req.leaderId)
            //
            updated = true
            // try to replication the request.entries:
            // 1. delete log from index prevLogIndex 
            // 2. append the entries to current log
            // 3. commit some entries according the leaderCommit value.
            log.dropRightFrom(req.prevLogIndex,req.prevLogTrem) match
                case Failure(e) => 
                    println(s"[debug] dropRight error ${e}")
                    resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex,nodeId))  
                case Success(_) => log.append(req.entries) match
                    case Failure(e) => 
                        println(s"[debug] append entries error ${e}")
                        resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex,nodeId))
                    case Success(_) => log.commitLog(req.leaderCommit) match 
                        case Success(_) => resp = Some(AppendEntriesResp(currentTerm,true,log.currentIndex,log.commitIndex,nodeId))
                        case Failure(e) => 
                            println(s"[debug] commit error ${e}")
                            resp = Some(AppendEntriesResp(currentTerm,false,log.currentIndex,log.commitIndex,nodeId))
        resp match
            case None => throw new Exception("response append entries request failed")
            case Some(r) => (r,updated)

    // to record if the peer has replicated log from current leader's term.
    private val syncedPeer:Map[String,Boolean] = Map[String,Boolean]()
    // only leader need process this message.
    private def processAppendEntriesResponse(resp:AppendEntriesResp):Unit = 
        //println(s"[debug] process appendEntries response")
        if resp.term > term then
            // if the term value lager than current server, hhe server need become follower.
            updateCurrentTerm(resp.term,None)
        else if resp.success then 
            // check if replication in current term.
            if resp.term == currentTerm then
                syncedPeer(resp.nodeId) = true
            // if majority servers synced some log for current term, we need verify which can be commited,
            // find the largest log index, that replicated by majority servers. 
            if syncedPeer.size >= majority then 
                // sort the peers log index.
                var arr = (for (_,p) <- peers yield p.getPrevLogIndex).toBuffer
                arr += log.currentIndex
                arr = arr.sortWith((x:Long,y:Long) => x < y)

                val commitMax = arr(majority-1)
                val commitIdx = log.commitIndex

                if commitMax > commitIdx then 
                    log.commitLog(commitMax)
                    commitIndex = commitMax 
                    println(s"[debug] set commitLog ${commitMax}")
                
        // we ignore the response if resp.success == false.
        // because update peers log index info has completed in method sendAppendEntriesRequest
    //
    import Message.resultToMsg
    // only leader can process such message.
    private def processCommandMsg(msg:Message):Try[Unit] = 
        println(s"[debug] process command msg")
        // create a log entry according to the command message.
        val callback = msg.response match 
            case None => false
            case Some(_) => true
        // 
        log.create(currentTerm,msg,callback) match 
            case Failure(e) => Failure(e)
            case Success(entry) => entry.response match
                case None => None
                case Some(ep) => 
                    // TODO: add f to waitGroup

                    // add a callback to transport the command result to caller.
                    val f = Future { 
                        println(s"[debug] start callback future")
                        msg.response match
                            case None => None
                            case Some(resp) => ep.future.foreach {
                                case Failure(e) => 
                                    if !resp.isCompleted then
                                        resp.failure(e)
                                case Success(r) =>
                                    if !resp.isCompleted then
                                        resp.success(Success(resultToMsg(r)))
                                        println(s"[debug] set callback resultToMsg")
                            }
                    }
                    
                    // 
                // append the entry to local logStorage,
                // and then the background heartbeat mechanism try to replication it to followers.
                log.append(entry) match 
                    case Failure(e) => Failure(e)
                    case Success(_) =>
                        println(s"[debug] append a log entry (${entry.term},${entry.index})")
                        // set self has synced.
                        syncedPeer(nodeId) = true
                        // if only one node in cluster,we donot need replicate it,just commit it immediately.
                        if peers.size == 0 then 
                            val commitIdx = log.currentIndex
                            log.commitLog(commitIdx) match
                                case Failure(e) => return Failure(e)
                                case Success(_) => None
                            commitIndex = log.commitIndex
                        Success(None)
    // send appendEntries to follower/candicate, and process the response.
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
                                    // if replication some entries success, leader need update current peers prevLogIndex info.
                                    if req.entries.length > 0 then
                                        peer.setPrevLogIndex(req.entries.last.index) 
                                else 
                                    // replication failed: need update peer's info.
                                    if resp.term > term then 
                                        // follower's term larger than leader, it has ignored the request.
                                        // this happens when there is a new leader running and current server has not known yet.
                                        // this server can know until the new leader send a heartbeat with higher term or this server finish processing this response.
                                        None
                                    else if resp.term == req.term && resp.commitIndex >= peer.getPrevLogIndex then
                                        // maybe the peer has committed some logs but leader did not receive the response,
                                        // so leader still keep a older log index for the peer.

                                        // peer reject replication and sent a fail response at this time,
                                        // so leader need to update peer's prevLogIndex to commitIndex.
                                        peer.setPrevLogIndex(resp.commitIndex)
                                    else if peer.getPrevLogIndex > 0 then 
                                        // leader sended log is too newer for the peer, so decrease the prevLogIndex,expect the next appendEntries to match log.
                                        val n = peer.getPrevLogIndex
                                        peer.setPrevLogIndex(n-1)
                                        // if prevLogIndex lager than peer's current log index, we need continue to decrease prevLogIndex value.
                                        if peer.getPrevLogIndex > resp.currentLogIndex then 
                                            peer.setPrevLogIndex(resp.currentLogIndex)
                                
                                // send the response to mesage queue, the serevr will process it later.
                                val msg = Message(peer.id,AppendEntriesResponse,resp,timestamp,None,None)
                                Success(sendMessage(msg))
                            catch
                                case e:Exception => Failure(e)                
//
