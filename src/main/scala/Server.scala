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

import akka.NotUsed
import akka.Done
import akka.util.ByteString
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.common.JsonEntityStreamingSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.stream.scaladsl.Source
import akka.stream.scaladsl._
import scala.concurrent.ExecutionContext
import scala.concurrent.{Future,Promise,Await}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.sys.ShutdownHookThread
import scala.util.{Try,Success,Failure}
import spray.json._
import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat


case class ServerOptions(name:String,host:String,port:Int,store:StorageOptions,raft:RaftOptions)

/**
  * 
  */
private[platcluster] object ServerMessage:
    case class KVPair(key:String,value:String)
    case class KVPutOptions(elems:Array[KVPair])
    case class KVRemoveOptions(keys:Array[String])
    case class KVOperationResult(success:Boolean,err:String,data:Array[KVPair])
/**
  * 
  */
private[platcluster] trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
    implicit val kvPairFormat: RootJsonFormat[ServerMessage.KVPair] = jsonFormat2(ServerMessage.KVPair.apply)
    implicit val kvPutFormat: RootJsonFormat[ServerMessage.KVPutOptions] = jsonFormat1(ServerMessage.KVPutOptions.apply)
    implicit val kvRemoveFormat: RootJsonFormat[ServerMessage.KVRemoveOptions] = jsonFormat1(ServerMessage.KVRemoveOptions.apply)
    implicit val kvOperationResultFormat: RootJsonFormat[ServerMessage.KVOperationResult] = jsonFormat3(ServerMessage.KVOperationResult.apply)
    //
    //implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()
}

//
private[platcluster] object PlatServer:
    def apply(ops:ServerOptions):Try[PlatServer] = 
        val db:Try[Storage] = ops.store.driver match
            case Storage.driverPlatdb => Success(PlatDB(ops.store))
            case Storage.driverMemory => Success(MemoryStore(ops.store))
            case Storage.driverFilePlatdb => Success(FilePlatDBStorage(ops.store))
            case _ => Failure(Storage.exceptNotSupportDriver)
        db match
            case Failure(e) => Failure(e)
            case Success(store) =>
                val raft = Raft(ops.raft,store.stateMachine(),store.logStorage())
                Success(new PlatServer(ops.name,ops.host,ops.port,store,raft))
//
private[platcluster] class PlatServer(name:String,host:String,port:Int,store:Storage,cm:ConsensusModule) extends JsonSupport:
    // needed to run the route
    implicit val system: ActorSystem[_] = ActorSystem(Behaviors.empty, "platcluster-server")
    // needed for the future map/flatmap in the end and future in fetchItem and saveOrder
    implicit val executionContext: ExecutionContext = system.executionContext
    // 
    import ServerMessage._
    
    /**
      * 
      */
    def run():Unit = 
        store.open() match 
            case Failure(e) => throw e 
            case Success(_) => println(s"open storage success")
    
        val route =
            pathPrefix("v1"){
                concat (
                    (pathEnd | pathSingleSlash) {
                        get {
                            complete(HttpEntity(
                            ContentTypes.`text/html(UTF-8)`, 
                            s"<h1>hello platcluster sever ${name}</h1>"))
                        }
                    },
                    path("pairs")(routeKV()),
                    path("status")(routeStatus()),
                )
            }
        //
        cm.init() match
            case Failure(e) => throw new Exception(s"init consensus module failed:${e}")
            case Success(_) => println(s"init consensus module success")
        //
        cm.start() match
            case Failure(e) => throw new Exception(s"start consensus module failed:${e}")
            case Success(_) => println(s"start consensus module success")
        //
        val bindingFuture = Http().newServerAt(host, port).bind(route)
        println(s"Server now online. Please navigate to http://${host}:${port}/v1\nPress CTR+C to stop...")

        val waitOnFuture = Promise[Done].future 
        val shutdownHook = ShutdownHookThread{
                println("Shutdown hook is running")
                // cleanup logic
                val unbind = bindingFuture.flatMap(_.unbind())
                
                Await.ready(unbind, Duration.Inf) 
                println("unbind")
                //
                println("stop consensus module...")
                cm.stop() match
                    case Failure(e) => println(s"stop consensus module failed:${e}")
                    case Success(_) => println("consensus module stopped")
                //
                println("close storage...")
                store.close() match
                    case Failure(e) => println(s"close storage failed:${e}")
                    case Success(_) => println("storage closed")

                println("terminate system...")
                system.terminate()
                println(s"shutdown platcluster server ${name}")
        }
        println(s"waiting for connection...")
        Await.ready(waitOnFuture, Duration.Inf)
    //
    import ServerMessage._
    private def routeKV():Route = 
        concat(
            pathEnd{
                concat(
                    (get & parameter("key")) { key =>
                        val res:Future[Try[KVPair]] = Future{
                            cm.get(key) match
                                case Failure(e) => Failure(e)
                                case Success(value) => Success(KVPair(key,value))
                        }
                        onSuccess(res) {
                            case Success(value) => complete(StatusCodes.OK,value)
                            case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                        }
                    },
                    post {
                        entity(as[KVPutOptions]) { ops => 
                            val add: Future[Try[Unit]] = Future {
                                try
                                    for kv <- ops.elems do
                                        cm.put(kv.key,kv.value) match 
                                            case Failure(e) => throw e
                                            case Success(_) => None
                                    Success(None)
                                catch
                                    case e:Exception => Failure(e)
                            }
                            onSuccess(add) { 
                                case Success(_) => complete(StatusCodes.OK,s"put elements success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    },
                    delete {
                        entity(as[KVRemoveOptions]) { ops => 
                            val add: Future[Try[Unit]] = Future {
                                try
                                    for key <- ops.keys do
                                        cm.delete(key) match 
                                            case Failure(e) => throw e
                                            case Success(_) => None
                                    Success(None)
                                catch
                                    case e:Exception => Failure(e)
                            }
                            onSuccess(add) { 
                                case Success(_) => complete(StatusCodes.OK,s"remove elements success\n")
                                case Failure(e) => complete(StatusCodes.InternalServerError,e.getMessage())
                            }
                        }
                    }
                )
            }
        )
    //
    private def routeStatus():Route = 
        concat(
            pathEnd{
                concat(
                    get {
                        val (role,state) = cm.status
                        complete(StatusCodes.OK,s"name:${name} state:${state} role:${role}\n")
                    }
                )
            }
        )
//
