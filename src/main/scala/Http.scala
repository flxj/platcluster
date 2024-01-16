

package platcluster

import scala.util.{Try}

//
private[platcluster] class HttpTransport(cm:RaftConsensusModule) extends Transport:
    def RequestVote(peer: Peer, req: RequestVoteReq): Try[RequestVoteResp] = ???
    def AppendEntries(peer: Peer, req: AppendEntriesReq): Try[AppendEntriesResp] = ???