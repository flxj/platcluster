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

import scala.util.{Try,Success,Failure}
import scala.collection.mutable.{Map,ArrayBuffer}
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.locks.ReentrantReadWriteLock

private[platcluster] enum SnapshotState:
    case Created
    case Writing
    case Available
    case Removed
    case Failed

private[platcluster] case class SnapshotMeta(
    nodeId:String,
    lastIndex:Long,
    lastTerm:Long,
    peers:Array[String],
    config:String,
    size:Long,
    state:SnapshotState
)

private[platcluster] trait Snapshot:
    def id:String 
    def meta:SnapshotMeta
    def state:SnapshotState
    def open():Unit
    def close():Unit
    def write(offset:Long,data:Array[Byte]):Unit
    def read(chunkSize:Int):Iterator[(Array[Byte],Long)]
    def setPeers(p:Array[String]):Unit
    def setConfig(c:String):Unit
    def save():Unit
    def clear():Unit

private[platcluster] class FileSnapshot(dir:String,name:String,fileSize:Long,nodeId:String,index:Long,term:Long) extends Snapshot:
    private val lock = new ReentrantReadWriteLock()
    private val chunks = new ArrayBuffer[InstallSnapshotReq]()
    private val peers = Array[String]()
    private var file:Files = null 
    private var path:Path = null
    private var stat:SnapshotState = SnapshotState.Created
    def open(): Unit = ???
    def close(): Unit = ???
    def id:String = nodeId
    def state:SnapshotState = stat
    def meta: SnapshotMeta = ???
    def write(offset:Long,data:Array[Byte]):Unit = ???
    //
    def read(chunkSize:Int):Iterator[(Array[Byte],Long)] =
        new Iterator[(Array[Byte],Long)] {
            private var offset:Long = 0L
            private val buff = ByteBuffer.allocate(chunkSize)
            private val channel = FileChannel.open(path, StandardOpenOption.READ)
            def hasNext: Boolean = offset < fileSize
            def next(): (Array[Byte],Long) = 
                val n = channel.read(buff)
                if n < 0 then
                    offset = fileSize
                    (null,0) // EOF
                else if n == 0 then
                    (Array[Byte](),offset)
                else
                    buff.flip()
                    val bts = new Array[Byte](buff.remaining())
                    buff.get(bts)

                    val oldOff = offset
                    offset += bts.length;
                    buff.clear()
                    (bts,oldOff)
        }
    def setPeers(p:Array[String]):Unit = ???
    def setConfig(c:String):Unit = ???
    def save():Unit = ??? // save writes the snapshot to file.
    def clear():Unit = ??? // remove deletes the snapshot file.
