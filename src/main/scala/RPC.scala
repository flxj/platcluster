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

import raft.transport._
import scala.util.{Try,Success,Failure}
import scala.concurrent.{ExecutionContext,Future,Await}

import akka.stream.Materializer
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import scala.concurrent.duration._
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import akka.http.scaladsl.Http
import com.typesafe.config.ConfigFactory

class GRPCRaftServiceImpl(cm:RaftModule)(implicit mat: Materializer) extends RaftService {
  import mat.executionContext

  override def requestVote(in:RequestVoteRequest): Future[RequestVoteResponse] =
      val req = RequestVoteReq(in.term,in.candicateId,in.lastLogIndex,in.lastLogTerm)
      Future {
          cm.requestVote(req) match
              case Failure(e) => throw e 
              case Success(resp) => RequestVoteResponse(resp.term,resp.voteGranted)
      }

  override def appendEntries(in:AppendEntriesRequest): Future[AppendEntriesResponse] = 
      val es = for le <- in.entries yield  le.cmd match 
            case None => 
                platcluster.LogEntry(le.term,le.index,platcluster.Command("none","","",""),None)
            case Some(c) => 
                platcluster.LogEntry(le.term,le.index,platcluster.Command(c.cmdType,c.op,c.key,c.value),None)
      val req = AppendEntriesReq(in.leaderId,in.term,in.prevLogTrem,in.prevLogIndex,in.leaderCommit,es.toArray)
      Future {
          cm.appendEntries(req) match
              case Failure(e) => throw e 
              case Success(r) => AppendEntriesResponse(r.success,r.term,r.currentLogIndex,r.commitIndex,r.nodeId)
      }
}

private[platcluster] class RaftGRPCTransportServer(system: ActorSystem,ip:String,port:Int,rm:RaftModule) {
  def run(): Future[Http.ServerBinding] = {
    // Akka boot up code
    implicit val sys: ActorSystem = system
    implicit val ec: ExecutionContext = sys.dispatcher

    // Create service handlers
    val service: HttpRequest => Future[HttpResponse] =
      RaftServiceHandler(new GRPCRaftServiceImpl(rm))

    // Bind service handler servers to localhost:8080/8081
    val binding = Http().newServerAt(ip, port).bind(service)

    // report successful binding
    binding.foreach { binding => println(s"gRPC server bound to: ${binding.localAddress}") }

    binding
  }
}

//
private[platcluster] class RPCTransport(ip:String,port:Int,rm:RaftModule) extends TransportServer:
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    //
    var sys:Option[ActorSystem] = None
    //val svc:Option[RaftGRPCTransportServer] = None 
    //
    def start():Unit = 
        // Important: enable HTTP/2 in ActorSystem's config
        // We do it here programmatically, but you can also set it in the application.conf
        val conf = ConfigFactory.parseString("akka.http.server.enable-http2 = on").withFallback(ConfigFactory.defaultApplication())
        // ActorSystem threads will keep the app alive until `system.terminate()` is called
        val system = ActorSystem("RaftTransportGRPC", conf)
        sys = Some(system)
        //
        val svc = new RaftGRPCTransportServer(system,ip,port,rm)
        svc.run()
        
    //
    def startAsync():Try[Unit] = 
        val f = Future { start() }
        Success(None)
    //
    def stop():Try[Unit] = 
        sys match
            case None => Success(None)
            case Some(s) => 
                try
                    s.terminate()
                    Success(None)
                catch
                    case e:Exception => Failure(e)
    //
    def requestVote(peer:Peer, req:RequestVoteReq): Try[RequestVoteResp] = 
        sys match 
            case None => Failure(new Exception("transport not running"))
            case Some(system) => 
                given s:ActorSystem = system

                val (ip,port) = peer.addr 
                // Configure the client by code:
                val clientSettings = GrpcClientSettings.connectToServiceAt(ip, port).withTls(false)
                // Or via application.conf:
                // val clientSettings = GrpcClientSettings.fromConfig(GreeterService.name)

                // Create a client-side stub for the service
                val client = RaftServiceClient(clientSettings)
                
                val r = RequestVoteRequest(req.term,req.lastLogIndex,req.lastLogTerm,req.candidateId)

                val vote = client.requestVote(r)
                try 
                    val resp = Await.result(vote,30.minutes) 
                    Success(RequestVoteResp(resp.term,resp.voteGranted))
                catch
                    case e:Exception => Failure(e)
    //
    def appendEntries(peer: Peer, req: AppendEntriesReq): Try[AppendEntriesResp] = 
        sys match 
            case None =>  Failure(new Exception("transport not running"))
            case Some(system) =>
                given s:ActorSystem = system

                val (ip,port) = peer.addr 
                // Configure the client by code:
                val clientSettings = GrpcClientSettings.connectToServiceAt(ip, port).withTls(false)
                // Or via application.conf:
                // val clientSettings = GrpcClientSettings.fromConfig(GreeterService.name)

                // Create a client-side stub for the service
                val client = RaftServiceClient(clientSettings)

                val es = for le <- req.entries yield
                    raft.transport.LogEntry(le.term,le.index,"",Some(raft.transport.Command(le.cmd.cmdType,le.cmd.op,le.cmd.key,le.cmd.value)))
                
                val r = AppendEntriesRequest(req.term,req.prevLogTrem,req.prevLogIndex,req.leaderCommit,req.leaderId,es)

                val app = client.appendEntries(r)
                try
                    val resp = Await.result(app,30.minutes) 
                    Success(AppendEntriesResp(resp.term,resp.success,resp.currentLogIndex,resp.commitIndex,resp.nodeId))
                catch
                    case e:Exception => Failure(e)
    //
    def requestVoteHandler():Try[Unit] = ???
    def appendEntriesHandler():Try[Unit] = ???
