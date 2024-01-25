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
import platdb.DB 
import platdb._

private[platcluster] class PlatDBFSM(db:DB) extends StateMachine:
    val bk = "data"
    //
    def init(): Try[Unit] = db.createCollection(bk,DB.collectionTypeBucket,0,true)
    //
    def apply(cmd:Command):Try[Result] = 
        cmd.op match
            case "get" => 
                get(cmd.key) match
                    case Failure(e) => Failure(e)
                    case Success(value) => Success(Result(true,"",value))
            case "put"|"set" =>
                put(cmd.key,cmd.value) match
                    case Failure(e) => Failure(e)
                    case Success(_) => Success(Result(true,"",""))
            case "delele" | "del" | "remove" =>
                delete(cmd.key) match
                    case Failure(e) => Failure(e)
                    case Success(_) => Success(Result(true,"","")) 
            case op => Failure(new Exception(s"current StateMachine not support operation ${op}"))
    //
    def get(key:String):Try[String] = 
        db.get(bk,key) match
            case Failure(e) => Failure(e)
            case Success((_,v)) => Success(v)
    //
    def put(key:String, value:String):Try[Unit] = db.put(bk,key,value)
    //
    def delete (key:String):Try[Unit] = db.delete(bk,true,List[String](key))

//
private[platcluster] class MemoryFSM() extends StateMachine:
    def init(): Try[Unit] = ???
    def apply(cmd:Command):Try[Result] = ???
    def get(key:String):Try[String] = ???
    def put(key:String, value:String):Try[Unit] = ???
    def delete (key:String):Try[Unit] = ???