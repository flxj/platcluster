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

import scala.util.{Try,Success,Failure}
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.locks.ReentrantReadWriteLock

trait Peer:
    def id:String
    def addr:(String,Int)

//
private[platcluster] class RaftPeer(nodeId:String,ip:String,port:Int,server:Raft) extends Peer:
    private val lock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    // index of the next log entry to send to that server (initialized to leader last log index + 1)
    private var nextIndex:Long = 0L
    // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    private var matchIndex:Long = 0L
    //
    private var heartbeatStop:Boolean = false 
    private var timer:Timer = null
    //
    def id:String = nodeId
    def addr:(String,Int) = (ip,port)
    //
    def getNextLogIndex:Long = nextIndex
    //
    def setNextLogIndex(idx:Long):Unit = 
        try
            lock.writeLock().lock()
            nextIndex = idx
        finally
            lock.writeLock().unlock()
    //
    def getPrevLogIndex:Long = 
        try
            lock.readLock().lock()
            if nextIndex > 0 then (nextIndex - 1) else 0L
        finally
            lock.readLock().unlock()
    //
    def setPrevLogIndex(idx:Long):Unit = 
        try
            lock.writeLock().lock()
            nextIndex = idx+1
        finally
            lock.writeLock().unlock()
    // 
    def startHeartbeat(interval:Int):Unit = 
        try
            lock.writeLock().lock()
            heartbeatStop = false 
            timer = new Timer()
            val task = new TimerTask {
                def run():Unit = heartbeat()
            }
            timer.schedule(task,0,interval.toLong)
        catch
            case e:Exception => throw e
        finally
            lock.writeLock().unlock()
            
    //
    def stopHeartbeat():Unit = 
        try
            lock.writeLock().lock()
            heartbeatStop = true
            if timer != null then
                timer.cancel()
        finally
            lock.writeLock().unlock()
    //
    private def stopped:Boolean = 
        try
            lock.readLock().lock()
            heartbeatStop
        finally
            lock.readLock().unlock()

    //
    private def heartbeat():Unit = 
        try
            if !stopped then
                val prevLogIdx = getPrevLogIndex
                // try to copy maxLogEntriesPerRequest log entries and send them to target peer for replication.
                // the entries start at prevLogIdx.target peer will check log use pervTerm and prevLogIdx,
                // if not match, the attach it's term and logIndex in response,then leader update peer's prevLogIdx and prevTerm info,and resend appendEntries rpc.
                // if match success, the peer will storage the entries in local logStorage, and send back success response. 
                server.logStorage.slice(prevLogIdx, server.maxLogEntriesPerRequest) match
                    case Failure(e) => throw e
                    case Success((prevTerm,entries)) =>
                        val term = server.currentTerm
                        val commitIdx = server.commitIndex
                        val req = AppendEntriesReq(server.nodeId,term,prevTerm,prevLogIdx,commitIdx,entries)
                        server.sendAppendEntriesRequest(nodeId,req) match 
                            case Failure(e) => throw e 
                            case Success(_) => None
        catch
            case e:Exception => println(s"[debug] send heartbeat error ${e}") // TODO err handler
    //
