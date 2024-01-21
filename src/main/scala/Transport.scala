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

/**
  * Module for communicating with peer nodes.
  */
trait Transport:
    /**
      * Send the raft RequestVote request to the peer and wait for a response, then return it.
      *
      * @param peer
      * @param req
      * @return
      */
    def requestVote(peer:Peer,req:RequestVoteReq):Try[RequestVoteResp]
    /**
      * Send the raft AppendEntries request to the peer and wait for a response, then return it.
      *
      * @param peer
      * @param req
      * @return
      */
    def appendEntries(peer:Peer,req:AppendEntriesReq):Try[AppendEntriesResp]

/**
  * 
  */
trait TransportServer extends Transport:
    /**
      * 
      *
      * @return
      */
    def start():Unit
    /**
      * 
      *
      * @return
      */
    def startAsync():Try[Unit]
    /**
      * 
      *
      * @return
      */
    def stop():Try[Unit]
    /**
      * 
      *
      * @return
      */
    def requestVoteHandler():Try[Unit]
    /**
      * 
      *
      * @return
      */
    def appendEntriesHandler():Try[Unit]