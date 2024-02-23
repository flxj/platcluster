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
                    case Success(resp) => 
                        //println(s"[debug] HTTP get requestVote response ${resp}")
                        Ok(resp.asJson)
            catch
                case e:Exception => 
                    //println(s"[debug] HTTP requestVote error ${e}")
                    IO(Response(Status(500)))
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
                //println(s"[debug] HTTP get app resp ${app}")
                //
                cm.appendEntries(app) match
                    case Failure(e) => throw e
                    case Success(resp) => Ok(resp.asJson)
            catch
                case e:Exception => 
                    //println(s"[debug] HTTP appendEntries error ${e}")
                    IO(Response(Status(500)))
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

//////////////////////////////
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
                //println(s"[debug] receved vote request: ${vote}")
                //
                val resp = RequestVoteResp(vote.term+1,true)
                Ok(resp.asJson)
            catch
                case e:Exception => IO(Response(Status(500)))
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
                //println(s"[debug] receved appendEntries request: ${app}")
                //
                val resp = AppendEntriesResp(app.term+1,true,100,100,"kkkkk")
                Ok(resp.asJson)
            catch
                case e:Exception => IO(Response(Status(500)))
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
    def testVoteSvc():Unit = 
        println("[1] test vote GET")
        val req = Request[IO](Method.GET, uri"/requestVote")
        val resp = requestVoteService.orNotFound.run(req).unsafeRunSync()
        println(resp)
        //
        println("==============================")
        println("[2] test vote POST")
        val voteR = RequestVoteReq(1,"abc",100,200)
        val req2 = POST(voteR.asJson, uri"/requestVote")
        val resp2 = requestVoteService.orNotFound.run(req2).unsafeRunSync()
        val r = resp2.as[RequestVoteResp].unsafeRunSync()
        println(r)
        println("==============================")
        println("[3] test vote POST2")
        val voteR2 = Command("rw","get","abc","xxx")
        val req3 = POST(voteR2.asJson, uri"/requestVote")
        try
            val resp3 = requestVoteService.orNotFound.run(req3).unsafeRunSync() 
            val r = resp3.as[RequestVoteResp].unsafeRunSync()
            println(r)
        catch
            case e:Exception => println(e)
    //
    def testAppendSvc():Unit = 
        println("[1] test append GET")
        val req = Request[IO](Method.GET, uri"/appendEntries")
        val resp = appendEntriesService.orNotFound.run(req).unsafeRunSync()
        val rr = resp.as[AppendEntriesResp].unsafeRunSync()
        println(rr)
        //
        println("==============================")
        println("[2] test append POST")
        val ent = LogEntry(1,2,Command("rw","put","kkk","vvv"),None)
        val appR = AppendEntriesReq("iiiii",1,100,200,300,Array[LogEntry](ent))
        //
        val req2 = POST(appR.asJson, uri"/appendEntries")
        val resp2 = appendEntriesService.orNotFound.run(req2).unsafeRunSync()
        val r = resp2.as[AppendEntriesResp].unsafeRunSync()
        println(r)

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

    //def get(cli:Client[IO],url:String):IO[String] = cli.expect[String](url)
    def testClient(url:String):Try[Unit] = 
        Uri.fromString(url) match
            case Left(e) => Failure(new Exception(e))
            case Right(u) =>
                val req = Request[IO](Method.GET,u)
                try
                    httpClient.run(req).use {
                        case Status.Successful(r) => r.attemptAs[String].leftMap(_.message).value
                        case r => r.as[String].map(b => Left(s"Request $req failed with status ${r.status.code} and body $b"))
                    }.unsafeRunSync() match
                        case Left(e) => 
                            Failure(new Exception(e))
                        case Right(ss) => 
                            Success(None)
                catch
                    case e:Exception => Failure(e)