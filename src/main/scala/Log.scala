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

import scala.collection.mutable.{ArrayBuffer,Map}
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
    private val lock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    private val entries:ArrayBuffer[LogEntry] = new ArrayBuffer[LogEntry]()
    private var commitIdx:Long = 0L
    private var prevIndex:Long = 0L // the index before the first entry in the Log entries --> this is the lastApplied log
    private var prevTerm:Long = 0L
    private var appliedIdx:Long = 0L

    private val name = "log"
    private val meta = "logMeta"
    private val commitIdxKey = "commitIdx"
    private val lastAppliedKey = "appliedIdx"
    private val emptyCmd = Command(cmdTypeNone,"","","")

    private val funcs:Map[String,(LogEntry)=>Unit] = Map[String,(LogEntry)=>Unit]()
    //
    def registerApplyFunc(cmdType:String,applyF:(LogEntry)=>Unit):Unit = funcs(cmdType) = applyF
    //
    def init():Try[Unit] = 
        try
            lock.writeLock().lock()
            if initialized then 
                return Success(None)
            // init log list.
            db.createCollection(name,DB.collectionTypeBList,0,true) match
                case Failure(e) => return Failure(new Exception(s"create log storage error ${e}"))
                case Success(_) => None
            //
            db.createCollection(meta,DB.collectionTypeBucket,0,true) match
                case Failure(e) => return Failure(new Exception(s"create log meta error ${e}"))
                case Success(_) => None
            // load meta info.
            db.view(
                (tx:Transaction) => 
                    //
                    given t:Transaction = tx 
                    val bk = openBucket(meta)
                    bk.get(commitIdxKey) match
                        case Failure(e) =>
                            if !DB.isNotExists(e) then 
                                throw new Exception(s"get commit idx error ${e}")
                        case Success(n) =>
                            val idx = n.toLong 
                            if idx > commitIdx then 
                                commitIdx = idx 
            ) match
                case Failure(e) => return Failure(e)
                case Success(_) => None
            //
            db.view(
                (tx:Transaction) =>
                    //
                    given t:Transaction = tx 
                    val bk = openBucket(meta)
                    bk.get(lastAppliedKey) match
                        case Failure(e) =>
                            if !DB.isNotExists(e) then 
                                throw new Exception(s"get applied idx error ${e}")
                        case Success(n) =>
                            val idx = n.toLong 
                            if idx > appliedIdx then 
                                appliedIdx = idx
            ) match
                case Failure(e) => return Failure(e)
                case Success(_) => None
            //
            prevIndex = appliedIdx
           
            // Load logEntry from platdb list
            db.view(
                (tx:Transaction) =>
                    given t:Transaction = tx 
                    val list = openList(name)
                    //
                    var applied:Long = 0L
                    for elem <- list.iterator do elem match
                        case (_,None) => None
                        case (None,_) => None
                        case (Some(n),Some(s)) => decode[LogEntry](s) match 
                            case Left(_) => None
                            case Right(entry) =>
                                if entry.index == prevIndex then 
                                    prevTerm = entry.term 
                                else if entry.index > prevIndex then 
                                    entries += entry
                                    if appliedIdx < entry.index && entry.index <= commitIdx then
                                        funcs.get(entry.cmdType) match 
                                            case None => throw new Exception(s"recovery from log failed: not support command type ${entry.cmdType}")
                                            case Some(applyLog) => 
                                                applyLog(entry)
                                                applied = entry.index
                    if applied > appliedIdx then 
                        appliedIdx = applied        
            ) match
                case Success(_) => None
                case Failure(e) => throw e 
            //
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
            db.put(meta,lastAppliedKey,appliedIdx.toString)
        finally
            lock.writeLock().unlock()
    //
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
                    Success(LogEntry(prevTerm,prevIndex,emptyCmd,None)) 
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
    def setCommitIndex(idx:Long):Unit = 
        try
            lock.writeLock().lock()
            if idx > commitIdx then
                commitIdx = idx
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
                    for j <- Range((oldCommit+1).toInt,(i+1).toInt,1) do  
                        val k = j-prevIndex-1
                        val entry = entries(k.toInt)
                        //
                        commitIdx = entry.index 
                        appliedIdx = entry.index
                        //
                        funcs.get(entry.cmdType) match
                            case None => entry.response match 
                                case None => None
                                case Some(r) =>
                                    if !r.isCompleted then 
                                        r.failure(new Exception(s"not support such command type ${entry.cmdType}"))
                            case Some(applyLog) => 
                                applyLog(entry)
                        //
                        if entry.cmdType == cmdTypeChange  then 
                            break
                )
            //
            db.put(meta,lastAppliedKey,appliedIdx.toString) match
                case Success(_) => None
                case Failure(_) => None
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
                given t:Transaction = tx
                val list = openList(name) 

                val entryList = (for en <- entry yield en.asJson.toString()).toList
                list.append(entryList) match
                    case Success(_) => None
                    case Failure(e) => throw e
        ) match
            case Success(_) => Success(entries++=entry)
            case Failure(e) => Failure(e)
        
    //
    def append(entry:LogEntry):Try[Unit] = append(List[LogEntry](entry))
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
                    throw new Exception(s"append entry term ${entry.term } < latest log term ${e.term}") 
                else if entry.term == e.term && entry.index <= e.index then 
                    throw new Exception(s"append entry index ${entry.index } < latest log index ${e.index}")
            //
            appendEntries(entrySeq)
        catch
            case e:Exception => Failure(e)
        finally
            lock.writeLock().unlock()
    //
    def delete(index:Long):Try[Unit] = Failure(new Exception("not support delete by index"))
    def dropRight(n:Int):Try[Unit] = Failure(new Exception("not support dropRight"))
    private def remove(index:Long):Try[Unit] =
        db.update(
            (tx:Transaction) =>
                given t:Transaction = tx 
                val list = openList(name)

                var start:Long = -1L
                breakable(
                    for elem <- list.iterator do elem match 
                        case (_,None) => None
                        case (None,_) => None
                        case (Some(n),Some(l)) => decode[LogEntry](l) match
                            case Right(entry) => 
                                if entry.index == index then 
                                    start = n.toInt
                                    break()
                            case Left(e) => throw new Exception(e)
                )
                //
                if start >= 0 then 
                    list.dropRight((list.length-start-1).toInt) match 
                        case Success(_) => None
                        case Failure(e) => throw e 
        )
    //  delete log from index location 'index' (dnot include index).
    def dropRightFrom(index:Long,term:Long):Try[Boolean] = 
        try
            lock.writeLock().lock()
            if !initialized then 
                throw new Exception("log storage not init.")
            // cannot delete log whichhas been commited.
            if index < commitIdx then 
                throw new Exception("log is already committed,cannot delete it.")
            // current log storage entries less than index. 
            if index > prevIndex+entries.length || index < prevIndex then 
                throw new Exception("index out of range, log entries not exists,cannot delete it.")
            // just remove all chached entries.
            if index == prevIndex then 
                for e <- entries do
                    // if some callback on the entry, we should return a error to it: the command cannot be apply.
                    e.response match
                        case None => None
                        case Some(r) =>
                            if !r.isCompleted then
                                r.failure(new Exception("cannot exec current command"))
                // delete log entries from db.
                remove(index) match
                    case Failure(e) => throw e
                    case Success(_) => entries.clear() // clean cache.
            else 
                //  
                if index < prevIndex then 
                    // if log index smaller than prevIndex,which means its already commited?
                    throw new Exception("log is already committed,cannot delete it.")
                // find the first delete location.
                val en = entries((index-prevIndex-1).toInt)
                // cannot truncate if the entry does not match term.
                if entries.length > 0 && en.term != term then 
                    throw new Exception(s"(idx:${index},term:${term}) cannot match log term ${en.term}")
                //
                if index < prevIndex + entries.length then 
                    for i <- Range((index-prevIndex).toInt,entries.length,1) do
                        entries(i).response match
                            case None => None
                            case Some(r) =>
                                if !r.isCompleted then
                                    r.failure(new Exception("cannot exec current command"))
                    // delete log from db.
                    remove(index) match
                        case Failure(e) => throw e
                        case Success(_) => entries.dropRight((entries.length+prevIndex-index).toInt)
                            
            Success(true)
        catch
            case e:Exception => Failure(e)
        finally
            lock.writeLock().unlock()
    //
    import scala.concurrent.Promise
    def create(term:Long,cmd:Command,callback:Boolean):Try[LogEntry] =  
        try
            lock.readLock().lock()
            //
            var nextIdx = prevIndex+1
            if entries.length > 0 then 
                nextIdx = entries.last.index+1
            if callback then 
                val r = Promise[Try[Result]]()
                Success(LogEntry(term,nextIdx,cmd,Some(r))) 
            else 
                Success(LogEntry(term,nextIdx,cmd,None))
        finally
            lock.readLock().unlock()
    // limit take continue count entries start at index, dnot contains 'index'.
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
                        Success((entries(start-1).term,entries.slice(start,entries.length).toArray))
                    else 
                        Success((entries(start-1).term,entries.slice(start,end).toArray))
        catch
            case e:Exception => Failure(e)
        finally
            lock.readLock().unlock()
//
private[platcluster] class AppendLog(dir:String) extends LogStorage:
    def init():Try[Unit] = ???
    def close():Try[Unit] = ???
    def latest:Try[LogEntry] = ???
    def currentIndex:Long = ???
    def commitIndex:Long = ???
    def setCommitIndex(idx:Long):Unit = ???
    def updateCommitIndex(idx:Long):Try[Unit] = ???
    def commitLog(idx:Long):Try[Unit] = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(entry:LogEntry):Try[Unit] = ???
    def append(entries:Seq[LogEntry]):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
    def dropRightFrom(prevIndex:Long,prevTerm:Long):Try[Boolean] = ???
    def create(term:Long,cmd:Command,callback:Boolean):Try[LogEntry] = ???
    def slice(index:Long,count:Int):Try[(Long,Array[LogEntry])] = ???
    def registerApplyFunc(cmdType:String,applyF:(entry:LogEntry)=>Unit):Unit = ???

//
private[platcluster] class MemoryLog() extends LogStorage:
    def init():Try[Unit] = ???
    def close():Try[Unit] = ???
    def latest:Try[LogEntry] = ???
    def currentIndex:Long = ???
    def commitIndex:Long = ???
    def setCommitIndex(idx:Long):Unit = ???
    def updateCommitIndex(idx:Long):Try[Unit] = ???
    def commitLog(idx:Long):Try[Unit] = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(entry:LogEntry):Try[Unit] = ???
    def append(entries:Seq[LogEntry]):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
    def dropRightFrom(prevIndex:Long,prevTerm:Long):Try[Boolean] = ???
    def create(term:Long,cmd:Command,callback:Boolean):Try[LogEntry] = ???
    def slice(index:Long,count:Int):Try[(Long,Array[LogEntry])] = ???
    def registerApplyFunc(cmdType:String,applyF:(entry:LogEntry)=>Unit):Unit = ???
