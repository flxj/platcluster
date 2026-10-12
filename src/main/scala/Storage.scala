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

case class StorageOptions(logDriver:String,dataDriver:String)

private[platcluster] object StorageInfo:
    val driverPlatdb = "platdb"
    val driverMemory = "memory"
    val driverFile = "file"

    val kvOpGet = "get"
    val kvOpPut = "put"
    val kvOpDel = "delete"

    val errNotSupportDriver = new Exception("not support such storage driver now")
    val errFSMDriverIsNull = new Exception("state machine storage driver info is null")
    val errLogDriverIsNull = new Exception("log storage driver info is null")

//
object PlatDB:
    def apply(ops:StorageOptions):Storage = 
        if ops.dataDriver == ""  then 
            throw StorageInfo.errFSMDriverIsNull
        if ops.logDriver == "" then
            throw StorageInfo.errLogDriverIsNull

        val data = ops.dataDriver.split(":")
        if data == null || data.length != 2 then
            throw new Exception(s"not support data driver formet:${ops.dataDriver}")
        data(0).toLowerCase() match
            case StorageInfo.driverPlatdb => 
                if data(1) == "" then 
                    throw StorageInfo.errFSMDriverIsNull
            case _ => throw StorageInfo.errNotSupportDriver
        
        val log = ops.logDriver.split(":")
        if log == null || log.length != 2 then
            throw new Exception(s"not support log driver formet:${ops.logDriver}")
        log(0).toLowerCase() match
            case StorageInfo.driverPlatdb => 
                if log(1) == "" then
                    throw StorageInfo.errLogDriverIsNull
            case _ => throw StorageInfo.errNotSupportDriver
        //
        if log(1) != data(1) then 
            new PlatDB(new DB(data(1)),Some(new DB(log(1))))
        else
            new PlatDB(new DB(data(1)),None)
//
private[platcluster] class PlatDB(data:DB,log:Option[DB]) extends Storage:
    def open(): Try[Unit] = 
        data.open() match
            case Failure(e) => Failure(e)
            case Success(_) => 
                log match
                    case None => Success(None)
                    case Some(d) => d.open()
    def close(): Try[Unit] = 
        data.close() match
            case Failure(e) => Failure(e)
            case Success(_) => 
                log match
                    case None => Success(None)
                    case Some(d) => d.close()
    def logStorage():LogStorage = 
        log match
            case None => new PlatDBLog(data)
            case Some(db) => new PlatDBLog(db)
    def stateMachine():StateMachine = new PlatDBFSM(data)
