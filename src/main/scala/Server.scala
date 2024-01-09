package platcluster

case class ServerOptions(name:String)

class HttpServer(ops:ServerOptions):
    val raft:RaftServer = null
    //
    def run():Unit = ???