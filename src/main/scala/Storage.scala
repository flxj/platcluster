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

private[platcluster] object StorageInfo:
    val driverPlatdb = "platdb:platdb"
    val driverMemory = "memory:memory"
    val driverFilePlatdb = "file:platdb"

    val kvOpGet = "get"
    val kvOpPut = "put"
    val kvOpDel = "delete"

    val exceptNotSupportDriver = new Exception("not support such storage driver")
    val exceptFSMPathIsNull = new Exception("state machine storage path is null")

//
object PlatDB:
    def apply(ops:StorageOptions):Storage = 
        if ops.fsmPath == "" then 
            throw StorageInfo.exceptFSMPathIsNull
        if ops.logPath != ops.fsmPath then 
            new PlatDB(new DB(ops.fsmPath),Some(new DB(ops.logPath)))
        else
            new PlatDB(new DB(ops.fsmPath),None)
//
private[platcluster] class PlatDB(db:DB,logDB:Option[DB]) extends Storage:
    def open(): Try[Unit] = 
        db.open() match
            case Failure(e) => Failure(e)
            case Success(_) => 
                logDB match
                    case None => Success(None)
                    case Some(d) => d.open()
    def close(): Try[Unit] = 
        db.close() match
            case Failure(e) => Failure(e)
            case Success(_) => 
                logDB match
                    case None => Success(None)
                    case Some(d) => d.close()
    def logStorage():LogStorage = 
        logDB match
            case None => new PlatDBLog(db)
            case Some(log) => new PlatDBLog(log)
    def stateMachine():StateMachine = new PlatDBFSM(db)

object FilePlatDBStorage:
    def apply(ops:StorageOptions):Storage = new FilePlatDBStorage(ops.logPath,new DB(ops.fsmPath))

//   
private[platcluster] class FilePlatDBStorage(logPath:String,db:DB) extends Storage:
    def open(): Try[Unit] = db.open()
    def close(): Try[Unit] = db.close()
    //
    def logStorage():LogStorage = new AppendLog(logPath)
    //
    def stateMachine():StateMachine = new PlatDBFSM(db)

//
object MemoryStore:
    def apply(ops:StorageOptions):Storage = new MemoryStore()

private[platcluster] class MemoryStore() extends Storage:
    def open(): Try[Unit] = Success(None)
    def close(): Try[Unit] = Success(None)
    //
    def logStorage():LogStorage = new MemoryLog()
    //
    def stateMachine():StateMachine = new MemoryFSM()
