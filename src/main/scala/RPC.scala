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

import scala.util.{Try}

//
private[platcluster] class RPCTransport(ip:String,port:Int,raft:RaftModule) extends TransportServer:
    def start():Unit = ???
    def startAsync():Try[Unit] = ???
    def stop():Try[Unit] = ???
    def requestVote(peer: Peer, req: RequestVoteReq): Try[RequestVoteResp] = ???
    def appendEntries(peer: Peer, req: AppendEntriesReq): Try[AppendEntriesResp] = ???
    def requestVoteHandler():Try[Unit] = ???
    def appendEntriesHandler():Try[Unit] = ???