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

import scala.util.{Success,Failure}
import java.lang.Thread


object PlatCluster:
    @main def main(args:String*) =
        println("hello,platcluster")

        import Message.appendReqToMsg
        import Message.msgToAppendReq
       
        val cmd = Command("rw","get","kkkk","vvvv")
        val logs = Array[LogEntry](LogEntry(100,200,cmd,None))
        val req = AppendEntriesReq("iiii",100,200,300,400,logs)

        val msg = appendReqToMsg(req)
        println(msg)
        println("--------------------")

        val r = msgToAppendReq(msg)
        println(r.entries(0))


