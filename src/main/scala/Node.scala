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

private[platcluster] class RaftPeer(nodeId:String,ip:String,port:Int,server:Raft) extends Peer:
    // index of the next log entry to send to that server (initialized to leader last log index + 1)
    var nextIndex:Long = 0L
    // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    var matchIndex:Long = 0L
    //
    var prevLogIndex:Long = 0L
    
    private val lock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    private var heartbeatStop:Boolean = false 
    private var timer:Timer = null
    //
    def id:String = nodeId
    def addr:(String,Int) = (ip,port)
    def getPrevLogIndex:Long = ???
    def setPrevLogIndex(idx:Long):Unit = ???
    // 
    def startHeartbeat(interval:Int):Unit = 
        try
            lock.writeLock().lock()
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
            if timer!=null then
                timer.cancel()
        finally
            lock.writeLock().unlock()
    //
    private def getStopSignal:Boolean = 
        try
            lock.readLock().lock()
            heartbeatStop
        finally
            lock.readLock().unlock()

    //
    private def heartbeat():Unit = 
        try
            if !getStopSignal then
                val prevLogIdx = getPrevLogIndex
                val term = server.currentTerm

                server.logStorage.slice(prevLogIdx, server.maxLogEntriesPerRequest) match
                    case Failure(e) => throw e
                    case Success((prevTerm,entries)) =>
                        val commitIdx = server.commitIndex
                        server.sendAppendEntriesRequest(nodeId,AppendEntriesReq(server.nodeId,term,prevTerm,prevLogIdx,commitIdx,entries))
        catch
            case e:Exception => None // TODO err handler
    //
