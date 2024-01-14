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

private[platcluster] class Peer(nodeId:String,ip:String,port:Int):
    // index of the next log entry to send to that server (initialized to leader last log index + 1)
    var nextIndex:Long = 0
    // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    var matchIndex:Long = 0
    //
    def id():String = nodeId
    def addr:(String,Int) = (ip,port)