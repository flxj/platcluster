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

import scala.util.Try
import platdb.DB 

private[platcluster] class PlatDBFSM(db:DB) extends StateMachine:
    def init(): Try[Unit] = ???
    def apply(log:LogEntry):Try[Result] = ???
    def get(key:String):Try[String] = ???
    def put(key:String, value:String):Try[Unit] = ???
    def delete (key:String):Try[Unit] = ???

//
private[platcluster] class MemoryFSM() extends StateMachine:
    def init(): Try[Unit] = ???
    def apply(log:LogEntry):Try[Result] = ???
    def get(key:String):Try[String] = ???
    def put(key:String, value:String):Try[Unit] = ???
    def delete (key:String):Try[Unit] = ???