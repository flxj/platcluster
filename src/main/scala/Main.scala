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

import scala.collection.mutable.Map
import scala.collection.JavaConverters._
import scala.util.{Success,Failure}
import java.io.File
import java.lang.Thread
import com.typesafe.config.ConfigFactory

object PlatCluster:
    @main def main(args:String*) =
        //
        if args.length == 0 || args(0) == "" then
            throw new Exception("not found config file path parameter")
        
        val config = ConfigFactory.parseFile(new File(args(0)))

        // service options.
        val name = config.getString("service.name")
        val host = config.getString("service.host")
        val port = config.getInt("service.port")
        if port <= 0 then 
            throw new Exception(s"Illegal service.port parameter ${port}: the port cannot be negative")

        // storage options
        val driver = config.getString("storage.driver")
        val logPath = config.getString("storage.logPath")
        val dataPath = config.getString("storage.dataPath")

        // raft options
        val id = config.getString("raft.id")
        if id == "" then 
            throw new Exception("Illegal raft.id parameter: id cannot be empty")

        val transport = config.getString("raft.transport")
        val confDir = config.getString("raft.confDir")
        if confDir == "" then 
            throw new Exception("Illegal raft.confDir parameter: confDir cannot be empty")
        //
        var maxLog = config.getInt("raft.maxLogEntriesPerRequest")
        if maxLog < 0 then 
            throw new Exception(s"Illegal raft.maxLogEntriesPerRequest parameter ${maxLog}: cannot be negative")
        else if maxLog == 0 then 
            maxLog = Raft.defaultMaxLog
        //
        var heartbeat = config.getInt("raft.heartbeatInterval")
        if heartbeat < 0 then
            throw new Exception(s"Illegal raft.heartbeatInterval parameter ${heartbeat}: cannot be negative")
        else if heartbeat == 0 then
            heartbeat = Raft.defaultHeartbeatInterval
        //
        var timeout = config.getInt("raft.electionTimeout")
        if timeout < 0 then 
            throw new Exception(s"Illegal raft.electionTimeout parameter ${timeout}: cannot be negative")
        else if timeout == 0 then
            timeout = Raft.defaultElectionTimeout
        //
        val servers = config.getConfigList("raft.servers")
        val mp = Map[String,String]()
        var localIP = ""
        var localPort = 0
        var peers = List[(String,String,Int)]()

        servers.asScala.toList.map { server =>
            val sid = server.getString("id")
            val sip = server.getString("ip")
            val sport = server.getInt("port")
            if sid == "" then 
                throw new Exception("Illegal raft.servers.id parameter: id cannot be empty")
            if mp.contains(sid) then 
                throw new Exception("Illegal raft.servers.id parameter: id cannot be duplicate")
            if sip == "" then 
                throw new Exception("Illegal raft.servers.ip parameter: ip cannot be empty")
            if sport <= 0 then 
                throw new Exception("Illegal raft.servers.port parameter: port cannot be empty")
            mp(sid) = sip
            if sid == id then 
                localIP = sip 
                localPort = sport 
            else
               peers:+=(sid,sip,sport)
        }
        if localIP == "" then 
            throw new Exception("Illegal id parameter: node info cannot in servers")

        val storeOps = StorageOptions(driver,logPath,dataPath)
        val raftOps = RaftOptions(id,localIP,localPort,transport,maxLog,heartbeat,timeout,confDir,peers)
        val ops = ServerOptions(name,host,port,storeOps,raftOps)

        println(ops)

        //PlatServer(ops) match
        //    case Failure(e) => throw e 
        //    case Success(svc) => svc.run()
