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

import scala.util.{Try,Failure,Success}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent._
import cats.effect._
import cats.syntax.all._
import cats.effect.Blocker
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.blaze.server._
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.blaze.client._
import org.http4s.client._
import org.http4s.client.dsl.io._
import org.http4s.circe._
import org.http4s.Method._
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe._
import io.circe.literal._

import platcluster.Message.logEntryEntityDecoder
import platcluster.Message.logEntryEntityEncoder

/**
  * 
  *
  * @param host
  * @param port
  * @param cm
  */
private[platcluster] class HttpTransport(host:String,port:Int,cm:RaftModule) extends TransportServer:
    implicit val requestVoteReqEntityDecoder: EntityDecoder[IO, RequestVoteReq] = jsonOf[IO, RequestVoteReq]
    implicit val requestVoteRespEntityDecoder: EntityDecoder[IO, RequestVoteResp] = jsonOf[IO, RequestVoteResp]
    implicit val commandEntityDecoder: EntityDecoder[IO, Command] = jsonOf[IO, Command]
    implicit val appendEntriesReqEntityDecoder: EntityDecoder[IO, AppendEntriesReq] = jsonOf[IO, AppendEntriesReq]
    implicit val appendEntriesRespEntityDecoder: EntityDecoder[IO, AppendEntriesResp] = jsonOf[IO, AppendEntriesResp]
    
    /**
      * process vote request.
      */
    val requestVoteService = HttpRoutes.of[IO] {
        case GET -> Root / "requestVote" => Ok("")
        case req @ POST -> Root / "requestVote" =>
            try
                val vote = req.as[RequestVoteReq].unsafeRunSync()
                //
                cm.requestVote(vote) match
                    case Failure(e) => throw e
                    case Success(resp) => Ok(resp.asJson)
            catch
                case e:Exception => InternalServerError(e.getMessage())
    }
    /**
      * process append log entries request.
      */
    val appendEntriesService = HttpRoutes.of[IO] {
        case GET -> Root / "appendEntries"  => Ok("")
        case req @ POST -> Root / "appendEntries" =>
            try
                val app = req.as[AppendEntriesReq].unsafeRunSync()
                //
                cm.appendEntries(app) match
                    case Failure(e) => throw e
                    case Success(resp) => Ok(resp.asJson)
            catch
                case e:Exception => InternalServerError(e.getMessage())
    }
    // root 
    val rootService = HttpRoutes.of[IO] {
        case GET -> Root => Ok(s"This is PlatCluster Raft Service\n")
    }

    implicit val cs: ContextShift[IO] = IO.contextShift(global)
    implicit val timer: Timer[IO] = IO.timer(global)

    //
    private val services = appendEntriesService <+> requestVoteService
    private val httpApp = Router("/" -> rootService, "/raft" -> services).orNotFound
    private var fiber:Option[Fiber[IO, Nothing]] = None
    // create a http server and run it sync.
    def start():Unit = 
        val server = BlazeServerBuilder[IO](global)
            .bindHttp(port, host)
            .withHttpApp(httpApp)
            .resource
        val fiber = server.use(_ => IO.never).start.unsafeRunSync()
    // create a http server and run it async.
    def startAsync():Try[Unit] = 
        val server = BlazeServerBuilder[IO](global)
            .bindHttp(port, host)
            .withHttpApp(httpApp)
            .resource
        
        val f = Future {
            fiber = Some(server.use(_ => IO.never).start.unsafeRunSync())
        }
        Success(None)
    // stop the http server.
    def stop():Try[Unit] = 
        fiber match
            case None => None
            case Some(f) => f.cancel.unsafeRunSync()
        Success(None)
    //
    private val blockingPool = Executors.newFixedThreadPool(16)
    private val blocker = Blocker.liftExecutorService(blockingPool)
    private val httpClient: Client[IO] = JavaNetClientBuilder[IO](blocker).create
    //
    private val voteUrl = "http://%s:%d/raft/requestVote"
    private val entriesUrl = "http://%s:%d/raft/appendEntries"
    // 
    def requestVote(peer:Peer,req:RequestVoteReq):Try[RequestVoteResp] = 
        val (h,p) = peer.addr
        Uri.fromString(voteUrl.format(h,p)) match
            case Left(e) => Failure(new Exception(e))
            case Right(u) =>
                val postReq = POST(req.asJson,u)
                try
                    httpClient.run(postReq).use {
                        case Status.Successful(r) => r.attemptAs[RequestVoteResp].leftMap(_.message).value
                        case r => r.as[String].map(b => Left(s"Request $postReq failed with status ${r.status.code} and body $b"))
                    }.unsafeRunSync() match
                        case Left(e) => throw new Exception(e)
                        case Right(resp) => Success(resp)
                catch
                    case e:Exception => Failure(e)
    //
    def appendEntries(peer:Peer,req:AppendEntriesReq):Try[AppendEntriesResp] = 
        val (h,p) = peer.addr
        Uri.fromString(entriesUrl.format(h,p)) match
            case Left(e) => Failure(new Exception(e))
            case Right(u) =>
                val postReq = POST(req.asJson,u)
                try
                    httpClient.run(postReq).use {
                        case Status.Successful(r) => r.attemptAs[AppendEntriesResp].leftMap(_.message).value
                        case r => r.as[String].map(b => Left(s"Request $postReq failed with status ${r.status.code} and body $b"))
                    }.unsafeRunSync() match
                        case Left(e) => throw new Exception(e)
                        case Right(resp) => Success(resp)
                catch
                    case e:Exception => Failure(e)
    //
    def requestVoteHandler():Try[Unit] = Success(None)
    def appendEntriesHandler():Try[Unit] = Success(None)

    def snapshot(peer:Peer,req:SnapshotReq):Try[SnapshotResp] = ???

    def installSnapshot(peer:Peer,req:InstallSnapshotReq):Try[InstallSnapshotResp] = ???

private[platcluster] class HttpTransport2(host:String,port:Int):
    implicit val requestVoteReqEntityDecoder: EntityDecoder[IO, RequestVoteReq] = jsonOf[IO, RequestVoteReq]
    implicit val requestVoteRespEntityDecoder: EntityDecoder[IO, RequestVoteResp] = jsonOf[IO, RequestVoteResp]
    implicit val commandEntityDecoder: EntityDecoder[IO, Command] = jsonOf[IO, Command]
    implicit val appendEntriesReqEntityDecoder: EntityDecoder[IO, AppendEntriesReq] = jsonOf[IO, AppendEntriesReq]
    implicit val appendEntriesRespEntityDecoder: EntityDecoder[IO, AppendEntriesResp] = jsonOf[IO, AppendEntriesResp]
    
    /**
      * process vote request.
      */
    val requestVoteService = HttpRoutes.of[IO] {
        case GET -> Root / "requestVote" => Ok("")
        case req @ POST -> Root / "requestVote" =>
            try
                val vote = req.as[RequestVoteReq].unsafeRunSync()
                //
                val resp = RequestVoteResp(vote.term+1,true)
                Ok(resp.asJson)
            catch
                case e:Exception => InternalServerError(e.getMessage())
    }
    /**
      * process append log entries request.
      */
    val appendEntriesService = HttpRoutes.of[IO] {
        case GET -> Root / "appendEntries"  => Ok("")
        case req @ POST -> Root / "appendEntries" =>
            try
                val app = req.as[AppendEntriesReq].unsafeRunSync()
                //
                val resp = AppendEntriesResp(app.term+1,true,100,100,"kkkkk")
                Ok(resp.asJson)
            catch
                case e:Exception => InternalServerError(e.getMessage())
    }
    // root 
    val rootService = HttpRoutes.of[IO] {
        case GET -> Root => Ok(s"This is PlatCluster Raft Service\n")
    }

    implicit val cs: ContextShift[IO] = IO.contextShift(global)
    implicit val timer: Timer[IO] = IO.timer(global)

    //
    private val services = appendEntriesService <+> requestVoteService
    private val httpApp = Router("/" -> rootService, "/raft" -> services).orNotFound
    private var fiber:Option[Fiber[IO, Nothing]] = None
    // create a http server and run it sync.
    def start():Unit = 
        val server = BlazeServerBuilder[IO](global)
            .bindHttp(port, host)
            .withHttpApp(httpApp)
            .resource
        val fiber = server.use(_ => IO.never).start.unsafeRunSync()
    // create a http server and run it async.
    def startAsync():Try[Unit] = 
        val server = BlazeServerBuilder[IO](global)
            .bindHttp(port, host)
            .withHttpApp(httpApp)
            .resource
        
        val f = Future {
            fiber = Some(server.use(_ => IO.never).start.unsafeRunSync())
        }
        Success(None)
    // stop the http server.
    def stop():Try[Unit] = 
        fiber match
            case None => None
            case Some(f) => f.cancel.unsafeRunSync()
        Success(None)
    //
    private val blockingPool = Executors.newFixedThreadPool(16)
    private val blocker = Blocker.liftExecutorService(blockingPool)
    private val httpClient: Client[IO] = JavaNetClientBuilder[IO](blocker).create
    //
    private val voteUrl = "http://%s:%d/raft/requestVote"
    private val entriesUrl = "http://%s:%d/raft/appendEntries"
    // 
    def requestVote(h:String,p:Int,req:RequestVoteReq):Try[RequestVoteResp] = 
        Uri.fromString(voteUrl.format(h,p)) match
            case Left(e) => Failure(new Exception(e))
            case Right(u) =>
                val postReq = POST(req.asJson,u)
                try
                    httpClient.run(postReq).use {
                        case Status.Successful(r) => r.attemptAs[RequestVoteResp].leftMap(_.message).value
                        case r => r.as[String].map(b => Left(s"Request $postReq failed with status ${r.status.code} and body $b"))
                    }.unsafeRunSync() match
                        case Left(e) => throw new Exception(e)
                        case Right(resp) => Success(resp)
                catch
                    case e:Exception => Failure(e)
    //
    def appendEntries(h:String,p:Int,req:AppendEntriesReq):Try[AppendEntriesResp] = 
        Uri.fromString(entriesUrl.format(h,p)) match
            case Left(e) => Failure(new Exception(e))
            case Right(u) =>
                val postReq = POST(req.asJson,u)
                try
                    httpClient.run(postReq).use {
                        case Status.Successful(r) => r.attemptAs[AppendEntriesResp].leftMap(_.message).value
                        case r => r.as[String].map(b => Left(s"Request $postReq failed with status ${r.status.code} and body $b"))
                    }.unsafeRunSync() match
                        case Left(e) => throw new Exception(e)
                        case Right(resp) => Success(resp)
                catch
                    case e:Exception => Failure(e)
    //
    def requestVoteHandler():Try[Unit] = Success(None)
    def appendEntriesHandler():Try[Unit] = Success(None)
    def snapshot(peer:Peer,req:SnapshotReq):Try[SnapshotResp] = ???
    def installSnapshot(peer:Peer,req:InstallSnapshotReq):Try[InstallSnapshotResp] = ???
