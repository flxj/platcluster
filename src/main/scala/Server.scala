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

case class ServerOptions(name:String,store:StorageOptions,raft:RaftOptions)

//
private[platcluster] object PlatServer:
    def apply(ops:ServerOptions):Try[PlatServer] = 
        val db:Try[Storage] = ops.store.driver match
            case Storage.driverPlatdb => Success(PlatDB(ops.store))
            case Storage.driverMemory => Success(MemoryStore(ops.store))
            case Storage.driverLogPlatdb => Success(LogPlatDBStorage(ops.store))
            case _ => Failure(Storage.exceptNotSupportDriver)
        db match
            case Failure(e) => Failure(e)
            case Success(store) =>
                val raft = Raft(ops.raft,store.stateMachine(),store.logStorage())
                Success(new PlatServer(ops.name,store,raft))
//
private[platcluster] class PlatServer(name:String,store:Storage,cm:ConsensusModule):
    //
    def run():Unit = 
        store.open() 
        cm.start()
        None