## PlatCluster

PlatCluster is a distributed KV storage service based on the Raft algorithm, created to learn distributed coordination algorithms.

Compiling this project requires: jdk11+, scala3, sbt1.8.2

Clone the project locally and run the sbt assembly command to obtain the program run jar package ` platcluster-0.2.0-SNAPSHOT.jar` in the target/target/scala-3.2.2/directory

Running the program requires preparing several configuration files in advance. The file examples are as follows：

```yaml
service {
    name = "server2"
    host = "localhost"
    port = 8082
}
storage {
    driver = "platdb:platdb"
    logPath = "/tmp/server2/data.db"
    dataPath = "/tmp/server2/data.db"

}
raft {
    id = "2"
    transport = "http"
    confDir = "/tmp/server2"
    maxLogEntriesPerRequest = 100
    heartbeatInterval = 0
    electionTimeout = 0
    servers = [
        {
            id = "1"
            ip = "localhost"
            port = 3901
        },
        {
            id = "2"
            ip = "localhost"
            port = 3902
        },
        {
            id = "3"
            ip = "localhost"
            port = 3903
        }
    ]
}
```
The `service` section is used to configure the client interface of the service. After the program runs, users can access the node service through this HTTP port, such as checking the node status. In the leader node, they can also access the Key-value object.


The `storage` section is used to configure the storage of services, namely log storage and state machine storage. Currently, platcluster only supports persistent components of the platdb type, so the driver field is "platdb: platdb".


The `raft` section is used to configure parameters related to the Raft algorithm, such as node ID, election timeout, heartbeat signal transmission interval, etc. At the same time, it is necessary to configure the raft communication addresses of all nodes in the format of triples (node ID, IP, port).




After writing the configuration file, you can run the platcluster instance in sequence. The command format is `java -jar platcluster 0.2.0 SNAPSHOT.jar <path/to/you/conf/file>`. The example is as follows:
```shell
java -jar platcluster-0.2.0-SNAPSHOT.jar  /tmp/server1.conf
```

When you see output similar to the following, it indicates that the service has run successfully.
```shell
ServerOptions(server1,localhost,8081,StorageOptions(platdb:platdb,/tmp/server1/data.db,/tmp/server1/data.db),RaftOptions(1,localhost,3901,http,100,50,150,/tmp/server1,List((2,localhost,3902), (3,localhost,3903))))
14:11:05.899 [platcluster-server-akka.actor.default-dispatcher-3] INFO  akka.event.slf4j.Slf4jLogger - Slf4jLogger started
open storage success
init consensus module success
start consensus module success
[debug] run as follower
14:11:06.658 [scala-execution-context-global-20] INFO  o.h.b.c.nio1.NIO1SocketServerGroup - Service bound to address /127.0.0.1:3901
14:11:06.673 [scala-execution-context-global-20] INFO  o.h.blaze.server.BlazeServerBuilder - 
14:11:06.682 [scala-execution-context-global-20] INFO  o.h.blaze.server.BlazeServerBuilder - http4s v0.22.15 on blaze v0.15.3 started at http://127.0.0.1:3901/
[debug] follower --> candicate
Server now online. Please navigate to http://localhost:8081/v1
Press CTR+C to stop...
waiting for connection...

```
After all instances have run successfully, the instance status can be viewed through the HTTP interface. The command example is as follows:
```shell
curl -X GET "http://localhost:8081/v1/status"

name:server1 state:running role:leader
```

```shell
curl -X GET "http://localhost:8082/v1/status"

name:server2 state:running role:follower
```

```shell
curl -X GET "http://localhost:8083/v1/status"

name:server3 state:running role:follower
```

As can be seen, there are three nodes in the cluster, among which server1 is the leader, server2 and server3 are the followers.


When most instances of the cluster, including the leader, are in the running state, key-value access services can be provided externally. Users can access the HTTP interface of the leader node to submit or query key-value objects. An example is as follows:

Create or update key value
```shell
curl -H "Content-Type: application/json" -X POST -d '{"elems":[{"key":"kkk", "value":"vvvvv"}]}' "http://localhost:8081/v1/pairs"

put elements success
```

query key-value
```shell
curl -X GET "http://localhost:8081/v1/pairs?key=kkk"


{"key":"kkk","value":"vvvvv"}
```


You can try using the ctr+c or kill command to stop the platcluster instance.


