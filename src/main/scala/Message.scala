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

case class Command(op:String,key:String,value:String)

case class Result(success:Boolean,err:String,content:String)

case class LogEntry(term:Long,index:Long,logType:Byte,cmd:String):
    def getBytes:Array[Byte] = ???

case class AppendEntriesReq(
    // server id.
    leaderId:String,
    // leader’s term leaderId so follower can redirect clients
    term:Long,
    // term of prevLogIndex entry
    prevLogTrem:Long,
    // index of log entry immediately preceding new ones
    prevLogIndex:Long,
    //
    leaderCommit:Long,
    // log entries to store (empty for heartbeat; may send more than one for efficiency)
    entries:Array[LogEntry]
)

case class AppendEntriesResp(
    // currentTerm, for leader to update itself
    term:Long,
    // true if follower contained entry matching prevLogIndex and prevLogTerm
    success:Boolean,
    //
    currentLogIndex:Long,
    //
    commitIndex:Long
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
 val Cmd,Res, AppendEntriesRequest,AppendEntriesResponse, RequestVoteRequest,RequestVoteResponse = Value
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
    //
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

    // message convert to request
    given msgToResult:Conversion[Message,Result] =  ???
    given msgToCmd:Conversion[Message,Command] =  ???
    given msgToAppendReq:Conversion[Message,AppendEntriesReq] =  ???
    given msgToAppendResp:Conversion[Message,AppendEntriesResp] = ???
    given msgToVoteReq:Conversion[Message,RequestVoteReq] = ???
    given msgToVoteResp:Conversion[Message,RequestVoteResp] = ???
    //
    // convert to json
    given voteRespToStr:Conversion[RequestVoteResp,String] = ???
    given voteReqToStr:Conversion[RequestVoteReq,String] = ???
    given appendRespToStr:Conversion[AppendEntriesResp,String] = ???
    given appendReqToStr:Conversion[AppendEntriesReq,String] = ???
    given cmdToStr:Conversion[Command,String] = ???
    given resultToStr:Conversion[Result,String] = ???
    // convert request to message.
    given voteRespToMsg:Conversion[RequestVoteResp,Message] = ???
    given voteReqToMsg:Conversion[RequestVoteReq,Message] = ???
    given appendRespToMsg:Conversion[AppendEntriesResp,Message] = ???
    given appendReqToMsg:Conversion[AppendEntriesReq,Message] = ???
    given cmdToMsg:Conversion[Command,Message] = ???
    given resultToMsg:Conversion[Result,Message] = ???

