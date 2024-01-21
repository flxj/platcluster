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


//
private[platcluster] class PlatDBLog(db:DB) extends LogStorage:
    def init(fsm:StateMachine):Try[Unit] = ???
    def latest:Try[LogEntry] = ???
    def currentIndex:Long = ???
    def commitIndex:Long = ???
    def setCommitIndex(idx:Long):Try[Unit] = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(entry:LogEntry):Try[Unit] = ???
    def append(entries:Seq[LogEntry]):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
    def dropRightFrom(prevIdx:Long,prevTerm:Long):Try[Boolean] = ???
    def create(cmd:Command):Try[LogEntry] = ???
    def slice(index:Long,count:Int):Try[(Long,Array[LogEntry])] = ???


//
private[platcluster] class AppendLog(dir:String) extends LogStorage:
    def init(fsm:StateMachine):Try[Unit] = ???
    def latest:Try[LogEntry] = ???
    def currentIndex:Long = ???
    def commitIndex:Long = ???
    def setCommitIndex(idx:Long):Try[Unit] = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(entry:LogEntry):Try[Unit] = ???
    def append(entries:Seq[LogEntry]):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
    def dropRightFrom(prevIdx:Long,prevTerm:Long):Try[Boolean] = ???
    def create(cmd:Command):Try[LogEntry] = ???
    def slice(index:Long,count:Int):Try[(Long,Array[LogEntry])] = ???

//
private[platcluster] class MemoryLog() extends LogStorage:
    def init(fsm:StateMachine):Try[Unit] = ???
    def latest:Try[LogEntry] = ???
    def currentIndex:Long = ???
    def commitIndex:Long = ???
    def setCommitIndex(idx:Long):Try[Unit] = ???
    def get(index:Long):Try[LogEntry] = ???
    def append(entry:LogEntry):Try[Unit] = ???
    def append(entries:Seq[LogEntry]):Try[Unit] = ???
    def delete(index:Long):Try[Unit] = ???
    def dropRight(n:Int):Try[Unit] = ???
    def dropRightFrom(prevIdx:Long,prevTerm:Long):Try[Boolean] = ???
    def create(cmd:Command):Try[LogEntry] = ???
    def slice(index:Long,count:Int):Try[(Long,Array[LogEntry])] = ???