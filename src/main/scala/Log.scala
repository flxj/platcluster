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

import scala.collection.mutable.ArrayBuffer
import scala.util.{Try,Success,Failure}
import scala.util.control.Breaks._
import java.util.concurrent.locks.ReentrantReadWriteLock
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe._
import io.circe.literal._
import io.circe.parser.decode
import platdb._
import platdb.Collection._

import platcluster.Message.logEntryEntityDecoder
import platcluster.Message.logEntryEntityEncoder
//
private[platcluster] class PlatDBLog(db:DB) extends LogStorage:
    private var initialized:Boolean = false
    private var fsm:Option[StateMachine] = None
    private val lock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    private val entries:ArrayBuffer[LogEntry] = new ArrayBuffer[LogEntry]()
    private var commitIdx:Long = 0L
    private var prevIndex:Long = 0L // the index before the first entry in the Log entries
    private var prevTerm:Long = 0L

    private val name = "log"
    private val meta = "logMeta"
    private val commitIdxKey = "commitIdx"
    private val emptyCmd = Command("","","")
    private var position:Long = 0L // the prevIndex entry location in BList.

    def init(sm:StateMachine):Try[Unit] = 
        try
            lock.writeLock().lock()
            if initialized then 
                return Success(None)
            fsm = Some(sm)
            //
            db.createCollection(name,DB.collectionTypeBList,0,true) match
                case Failure(e) => return Failure(e)
                case Success(_) => 
                    db.get(meta,commitIdxKey) match
                        case Success((_,n)) =>
                            val idx = n.toLong 
                            if idx > commitIdx then 
                                commitIdx = idx 
                        case Failure(e) => 
                            if !DB.isNotExists(e) then 
                                return Failure(e)
           
            // Load logEntry from platdb list
            db.view(
                (tx:Transaction) =>
                    given t:Transaction = tx 
                    val list = openList(name)
                    //
                    for elem <- list.iterator do 
                        elem match
                            case (_,None) => None
                            case (None,_) => None
                            case (Some(n),Some(s)) =>
                                val entry:LogEntry = null // TODO: to json
                                if entry.index == prevIndex then 
                                    position = n.toLong 
                                if entry.index > prevIndex then 
                                    entries += entry
                                    if entry.index <= commitIdx then
                                        // apply the log entry to fsm.
                                        fsm match
                                            case None => None
                                            case Some(sm) => sm.apply(entry) match
                                                case Success(_) => None
                                                case Failure(e) => throw new Exception(s"recovery from log failed ${e}")
            ) match
                case Success(_) => None
                case Failure(e) => throw e 
            
            initialized = true
            Success(None)
        catch
            case e:Exception => Failure(e)
        finally
            lock.writeLock().unlock()
    //
    def sync():Try[Unit] = 
        try
            lock.writeLock().lock()
            db.put(meta,commitIdxKey,commitIdx.toString)
            //
        finally
            lock.writeLock().unlock()

    def close():Try[Unit] = sync()
    //
    def compress(index:Long,term:Long):Try[Unit] = 
        try 
            lock.writeLock().lock()
            //
            var entryList = ArrayBuffer[LogEntry]()
            val currentIdx = if entries.length > 0 then entries.last.index else prevIndex
            if index < currentIdx then 
                entryList = entries.slice((index-prevIndex).toInt,entries.length)
            //
            db.update(
                (tx:Transaction) =>
                    given t:Transaction = tx
                    val list = openList(name)
                    // clear
                    list.drop((list.length).toInt) match
                        case Failure(e) => throw e
                        case Success(_) => None
                    for e <- entryList do
                        list :+= e.asJson.toString()
            ) match
                case Failure(e) => Failure(e)
                case Success(_) =>
                    entries.clear()
                    entries ++= entryList
                    prevIndex = index 
                    prevTerm = term 
                    position = 0L
                    Success(None)
        catch
            case e:Exception => Failure(e)
        finally
            lock.writeLock().unlock()
    //
    //
    def latest:Try[LogEntry] = 
        try
            lock.readLock().lock()
            if entries.length > 0 then
                Success(entries.last)
            else
                if !initialized then
                    Failure(new Exception("log storage not init"))
                else
                    Success(LogEntry(prevTerm,prevIndex,0,emptyCmd,None)) 
        finally
            lock.readLock().unlock()
    //  
    def currentIndex:Long = 
        try
            lock.readLock().lock()
            if entries.length > 0 then 
                entries.last.index
            else
                prevIndex
        finally
            lock.readLock().unlock()
    //
    def commitIndex:Long = 
        try
            lock.readLock().lock()
            commitIdx
        finally
            lock.readLock().unlock()
    //
    def updateCommitIndex(idx:Long):Try[Unit] = 
        try
            lock.writeLock().lock()
            if idx > commitIdx then
                commitIdx = idx
                db.put(meta,commitIdxKey,commitIdx.toString)
            else 
                Success(None)
        finally
            lock.writeLock().unlock()
    //
    import Message.resultToMsg
    // when init raft, readConf well set the commitIndex.
    def commitLog(idx:Long):Try[Unit] = 
        try
            lock.writeLock().lock()
            if !initialized then 
                return Failure(new Exception("log storage not init"))
            
            // set commitIdx value,and apply log entries to fsm.
            var i = idx
            if i > prevIndex+ entries.length then 
                i = prevIndex+ entries.length
            //
            val oldCommit = commitIdx 
            // if there are log entries already commited,we cannot commit them again.
            if i >= commitIdx then  
                breakable(
                    for j <- Range((commitIdx+1).toInt,(i+1).toInt,1) do 
                        val k = i - prevIndex -1
                        val entry = entries(k.toInt)
                        //
                        commitIdx = entry.index 
                        //
                        fsm match
                            case None => None
                            case Some(sm) => sm.apply(entry) match
                                case Failure(e) => 
                                    entry.response match
                                        case None => None
                                        case Some(r) =>
                                            if !r.isCompleted then 
                                                r.failure(e)
                                case Success(res) =>
                                    entry.response match
                                        case None => None
                                        case Some(r) =>
                                            if !r.isCompleted then 
                                                r.success(Success(res))
                        //
                        if entry.logType == 1  then 
                            break
                )
            //
            if oldCommit != commitIdx then
                db.put(meta,commitIdxKey,commitIdx.toString) match
                    case Success(_) => None
                    case Failure(_) => None
            Success(None)
        catch
            case e:Exception => Failure(e)
        finally
            lock.writeLock().unlock()
    //
    def get(index:Long):Try[LogEntry] = 
        try
            lock.readLock().lock()
            //
            if index <= prevIndex || index > (prevIndex+entries.length) then
                Failure(new Exception("index out of range"))
            else
                Success(entries((index-prevIndex-1).toInt))
        finally
            lock.readLock().unlock()
    //
    private def appendEntries(entry:Seq[LogEntry]):Try[Unit] = 
        db.update(
            (tx:Transaction) =>
                val entryList = (for en <- entry yield en.asJson.toString()).toList
                tx.openList(name) match 
                    case Failure(e) => throw e 
                    case Success(list) => 
                        list.append(entryList) match
                            case Success(_) => None
                            case Failure(e) => throw e
        ) match
            case Success(_) => Success(entries++=entry)
            case Failure(e) => Failure(e)
    //
    def append(entry:LogEntry):Try[Unit] = ???
    //
    def append(entrySeq:Seq[LogEntry]):Try[Unit] = 
        try
            lock.writeLock().lock()
            if !initialized then 
                throw new Exception("log storage not init")
            //
            if entrySeq.length == 0 then 
                return Success(None)
            //
            val entry = entrySeq(0)
            if entries.length > 0 then 
                val e = entries.last
                if entry.term < e.term then 
                    throw new Exception()
                else if entry.term == e.term && entry.index <= e.index then 
                    throw new Exception()
            //
            appendEntries(entrySeq)
        catch
            case e:Exception => Failure(e)
        finally
            lock.writeLock().unlock()
    //
    def delete(index:Long):Try[Unit] = Failure(new Exception("not support delete by index"))
    def dropRight(n:Int):Try[Unit] = Failure(new Exception("not support dropRight"))
    // drop log entries from
    def dropRightFrom(index:Long,term:Long):Try[Boolean] = 
        try
            lock.writeLock().lock()
            if !initialized then 
                throw new Exception("log storage not init")
            //
            if index < commitIdx then 
                throw new Exception("log is already committed")
            //
            if index > prevIndex+entries.length || index < prevIndex then 
                throw new Exception("index out of range, log entries not exists")
            //
            if index == prevIndex then 
                for e <- entries do
                    e.response match
                        case None => None
                        case Some(r) =>
                            if !r.isCompleted then
                                r.failure(new Exception("cannot exec current command"))
                entries.clear()
            else 
                val e = entries((index-prevIndex-1).toInt)
                if entries.length > 0 && e.term != term then 
                    throw new Exception(s"(idx:${index},term:${term}) cannot match log term ${e.term}")
                //
                if index < prevIndex + entries.length then 
                    for i <- Range((index-prevIndex).toInt,entries.length,1) do
                        e.response match
                            case None => None
                            case Some(r) =>
                                if !r.isCompleted then
                                    r.failure(new Exception("cannot exec current command"))
                    //
                    entries.dropRight((entries.length+prevIndex-index).toInt)
            Success(true)
        catch
            case e:Exception => Failure(e)
        finally
            lock.writeLock().unlock()
    //
    def create(term:Long,cmd:Command):Try[LogEntry] = 
        try
            lock.readLock().lock()
            //
            var nextIdx = prevIndex+1
            if entries.length > 0 then 
                nextIdx = entries.last.index+1
            Success(LogEntry(term,nextIdx,0,cmd,None))
        finally
            lock.readLock().unlock()
    // cannot contains index
    def slice(index:Long,count:Int):Try[(Long,Array[LogEntry])] = 
        try
            lock.readLock().lock()
            //
            if index < prevIndex then 
                Success((0,Array[LogEntry]()))
            else 
                if index > prevIndex+entries.length then 
                    throw new Exception(s"index ${index} out of range ${prevIndex+entries.length}")
                //
                if index == prevIndex then 
                    Success((prevTerm,entries.toArray))
                else 
                    val start = (index-prevIndex).toInt
                    val end = (index-prevIndex+count).toInt
                    if end >= entries.length then
                        Success((entries(start-1).term,entries.toArray))
                    else 
                        Success((entries(start-1).term,entries.slice(start,end).toArray))
        catch
            case e:Exception => Failure(e)
        finally
            lock.readLock().unlock()

//
private[platcluster] class AppendLog(dir:String) extends LogStorage:
    def init(fsm:StateMachine):Try[Unit] = ???
    def latest:Try[LogEntry] = ???
    def currentIndex:Long = ???
    def commitIndex:Long = ???
    def updateCommitIndex(idx:Long):Try[Unit] = ???
    def commitLog(idx:Long):Try[Unit] = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(entry:LogEntry):Try[Unit] = ???
    def append(entries:Seq[LogEntry]):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
    def dropRightFrom(prevIndex:Long,prevTerm:Long):Try[Boolean] = ???
    def create(term:Long,cmd:Command):Try[LogEntry] = ???
    def slice(index:Long,count:Int):Try[(Long,Array[LogEntry])] = ???

//
private[platcluster] class MemoryLog() extends LogStorage:
    def init(fsm:StateMachine):Try[Unit] = ???
    def latest:Try[LogEntry] = ???
    def currentIndex:Long = ???
    def commitIndex:Long = ???
    def updateCommitIndex(idx:Long):Try[Unit] = ???
    def commitLog(idx:Long):Try[Unit] = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(entry:LogEntry):Try[Unit] = ???
    def append(entries:Seq[LogEntry]):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
    def dropRightFrom(prevIndex:Long,prevTerm:Long):Try[Boolean] = ???
    def create(term:Long,cmd:Command):Try[LogEntry] = ???
    def slice(index:Long,count:Int):Try[(Long,Array[LogEntry])] = ???