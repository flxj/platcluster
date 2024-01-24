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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future,Promise}
import scala.util.Try
import java.time.Instant
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe._
import io.circe.literal._
import io.circe.parser.decode

val cmdTypeKVRW = "rw"
val cmdTypeChange = "change"
val cmdTypeNone = "none"
val opJoin = "join"
val opLeave = "leave"

val addrFmt = "%s:%d"

/**
  * Represents a command that can be executed by a state machine.
  *
  * @param op  
  * @param key for join or leave type, the key should be node id. 
  * @param value for join operation,the value should be node connection info, format is 'ip:port'
  */
case class Command(cmdType:String,op:String,key:String,value:String)

/**
  * Indicates the result of command execution.
  *
  * @param success
  * @param err
  * @param content
  */
case class Result(success:Boolean,err:String,content:String)

/**
  * Represents a log entry in the Raft log replication module.
  *
  * @param term
  * @param index
  * @param cmd
  * @param response
  */
case class LogEntry(term:Long,index:Long,cmd:Command,response:Option[Promise[Try[Result]]]):
    def cmdType:String = cmd.cmdType

case class AppendEntriesReq(
    // leaderId so follower can redirect clients.
    leaderId:String,
    // leader’s term.
    term:Long,
    // term of prevLogIndex entry
    prevLogTrem:Long,
    // index of log entry immediately preceding new ones
    prevLogIndex:Long,
    // the commit log index, leader use the field to inform followers which log can apply in local.
    leaderCommit:Long,
    // log entries to store (empty for heartbeat; may send more than one for efficiency)
    entries:Array[LogEntry]
)

case class AppendEntriesResp(
    // currentTerm, for leader to update itself
    term:Long,
    // true if follower contained entry matching prevLogIndex and prevLogTerm
    success:Boolean,
    // server currnt log index.
    currentLogIndex:Long,
    // return server commit index
    commitIndex:Long,
    // server id, so reciver can known the response come from which peer.
    nodeId:String
)

case class RequestVoteReq(
    // candidate’s term
    term:Long,
    // candidate requesting vote
    candidateId:String,
    // index of candidate’s last log entry
    lastLogIndex:Long,
    // term of candidate’s last log entry
    lastLogTerm:Long
)

case class RequestVoteResp(
    // term currentTerm, for candidate to update itself
    term:Long,
    // true means candidate received vote
    voteGranted:Boolean
)

//
private[platcluster] object MessageTypes extends Enumeration {
 type MessageType = Value
 val Cmd,Res, AppendEntriesRequest,AppendEntriesResponse,RequestVoteRequest,RequestVoteResponse = Value
}

import MessageTypes._
//
private[platcluster] case class Message(
    //uid:String,
    source:String,
    //
    msgType:MessageType,
    //
    content:String,
    //
    createAt:Instant,
    //
    timeout:Option[Int],
    //
    response:Option[Promise[Try[Message]]]
)

//
private[platcluster] object Message:
    // check the mesage already expired.
    def expired(msg:Message):Boolean =
        msg.timeout match 
            case None => false
            case Some(t) => 
                if t <= 0 then
                    false
                else
                    val insNow = Instant.now()
                    val exp = msg.createAt.plusMillis(t)
                    insNow.isBefore(exp)
    // 
    given logEntryEntityEncoder: Encoder[LogEntry] = new Encoder[LogEntry] {
        final def apply(a: LogEntry): Json = Json.obj(
            ("term", Json.fromLong(a.term)),
            ("index", Json.fromLong(a.index)),
            ("cmd", a.cmd.asJson),
        )
    }
    given logEntryEntityDecoder: Decoder[LogEntry] = new Decoder[LogEntry] {
        final def apply(c: HCursor): Decoder.Result[LogEntry] =
            for {
                term <- c.downField("term").as[Long]
                idx <- c.downField("index").as[Long]
                cmd <- c.downField("cmd").as[Command]
            } yield {
                LogEntry(term, idx,cmd,None)
            }
    }
    // message convert to request
    given msgToResult:Conversion[Message,Result] =  (msg:Message) => 
        if msg.content != "" then 
            decode[Result](msg.content) match
                case Right(r) => r
                case Left(e) => throw new Exception(e)
        else 
            Result(false,"","")
    //
    given msgToCmd:Conversion[Message,Command] =  (msg:Message) =>
        decode[Command](msg.content) match
            case Right(c) => c
            case Left(e) => throw new Exception(e)
    //
    given msgToAppendReq:Conversion[Message,AppendEntriesReq] =  (msg:Message) =>
        decode[AppendEntriesReq](msg.content) match
            case Right(a) => a
            case Left(e) => throw new Exception(e)
    //
    given msgToAppendResp:Conversion[Message,AppendEntriesResp] = (msg:Message) =>
        decode[AppendEntriesResp](msg.content) match
            case Right(a) => a
            case Left(e) => throw new Exception(e)
    //
    given msgToVoteReq:Conversion[Message,RequestVoteReq] = (msg:Message) =>
        decode[RequestVoteReq](msg.content) match
            case Right(r) => r
            case Left(e) => throw new Exception(e)
    //
    given msgToVoteResp:Conversion[Message,RequestVoteResp] = (msg:Message) =>
        decode[RequestVoteResp](msg.content) match
            case Right(r) => r 
            case Left(e) => throw new Exception(e)
    //
    // convert to json
    given voteRespToJson:Conversion[RequestVoteResp,String] = (resp:RequestVoteResp) => resp.asJson.toString()
    given voteReqToJson:Conversion[RequestVoteReq,String] = (req:RequestVoteReq) => req.asJson.toString()
    given appendRespToJson:Conversion[AppendEntriesResp,String] = (resp:AppendEntriesResp) => resp.asJson.toString()
    given appendReqToJson:Conversion[AppendEntriesReq,String] = (req:AppendEntriesReq) => req.asJson.toString()
    given cmdToJson:Conversion[Command,String] = (cmd:Command) => cmd.asJson.toString()
    given resultToJson:Conversion[Result,String] = (res:Result) => res.asJson.toString()
    // convert request to message.
    given voteRespToMsg:Conversion[RequestVoteResp,Message] = (resp:RequestVoteResp) => Message("",RequestVoteRequest,resp,Instant.now(),None,None)
    given voteReqToMsg:Conversion[RequestVoteReq,Message] = (req:RequestVoteReq) => Message("",RequestVoteResponse,req,Instant.now(),None,None)
    given appendRespToMsg:Conversion[AppendEntriesResp,Message] = (resp:AppendEntriesResp) => Message("",AppendEntriesResponse,resp,Instant.now(),None,None)
    given appendReqToMsg:Conversion[AppendEntriesReq,Message] = (req:AppendEntriesReq) => Message("",AppendEntriesRequest,req,Instant.now(),None,None)
    given cmdToMsg:Conversion[Command,Message] = (cmd:Command) => Message("",Cmd,cmd,Instant.now(),None,None)
    given resultToMsg:Conversion[Result,Message] = (res:Result) => Message("",Res,res,Instant.now(),None,None)

private[platcluster] case class RaftPeerInfo(id:String,ip:String,port:Int,nextIndex:Long)
private[platcluster] case class RaftNodeState(commitIndex:Long,appliedIndex:Long,prevIndex:Long,peers:Array[RaftPeerInfo])