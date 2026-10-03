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

import scala.collection.mutable.Map
import scala.util.{Try,Success,Failure}
import java.util.concurrent.locks.ReentrantReadWriteLock
import platdb.DB 
import platdb._
import platdb.Collection._

private[platcluster] class PlatDBFSM(db:DB) extends StateMachine:
    val bk = "data"
    //
    def init(): Try[Unit] = db.createCollection(bk,CollectionType.Bucket,0,true)
    //
    def apply(cmd:Command):Try[ExecResult] = 
        try
            val res = cmd.opType match
                case OperationType.Get => 
                    get(cmd.key) match
                        case Failure(e) => throw e
                        case Success(value) => ExecResult(true,"",value)
                case OperationType.Put =>
                    put(cmd.key,cmd.value) match
                        case Failure(e) => throw e 
                        case Success(_) => ExecResult(true,"","")
                case OperationType.Delete =>
                    delete(cmd.key) match
                        case Failure(e) => throw e 
                        case Success(_) => ExecResult(true,"","")
                case op => throw new Exception(s"current StateMachine not support operation ${op}")
            Success(res)
        catch
            case e:Exception => Failure(e)
    //
    def get(key:String):Try[String] = 
        var s:Option[String] = None
        db.view((tx:Transaction) =>
            given t:Transaction = tx 
            val b = openBucket(bk)
            s = b.get(key)
        ) match
            case Failure(e) => return Failure(e)
            case Success(_) => None
        s match
            case None => Failure(new Exception("key not exists"))
            case Some(v) => Success(v)
    //
    def put(key:String, value:String):Try[Unit] = 
        db.update((tx:Transaction) =>
            given t:Transaction = tx 
            val b = openBucket(bk)
            b.put(key,value)
        )
    //
    def delete (key:String):Try[Unit] = 
        db.update((tx:Transaction) =>
            given t:Transaction = tx 
            val b = openBucket(bk)
            b.delete(key)
        )
//
private[platcluster] class MemoryFSM() extends StateMachine:
    private val lock:ReentrantReadWriteLock = new ReentrantReadWriteLock()
    private val data = Map[String,String]()
    def init(): Try[Unit] = Success(None)
    def apply(cmd:Command):Try[ExecResult] = ???
    def get(key:String):Try[String] = ???
    def put(key:String, value:String):Try[Unit] = ???
    def delete (key:String):Try[Unit] = ???