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
 val Command, AppendEntriesRequest,AppendEntriesResponse, RequestVoteRequest,RequestVoteResponse = Value
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
    expire:Option[Future[Int]],
    //
    response:Option[Promise[Message]]
)

//
private[platcluster] object Message:
    // message convert to request
    given Conversion[Message,Command] =  ???
    given Conversion[Message,AppendEntriesReq] =  ???
    given Conversion[Message,AppendEntriesResp] = ???
    given Conversion[Message,RequestVoteReq] = ???
    given Conversion[Message,RequestVoteResp] = ???
    //
    // convert to json
    given Conversion[RequestVoteResp,String] = ???
    given Conversion[RequestVoteReq,String] = ???
    given Conversion[AppendEntriesResp,String] = ???
    given Conversion[AppendEntriesReq,String] = ???
    given Conversion[Command,String] = ???
    // convert request to message.
    given Conversion[RequestVoteResp,Message] = ???
    given Conversion[RequestVoteReq,Message] = ???
    given Conversion[AppendEntriesResp,Message] = ???
    given Conversion[AppendEntriesReq,Message] = ???
    given Conversion[Command,Message] = ???

