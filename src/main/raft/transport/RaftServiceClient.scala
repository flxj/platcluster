
// Generated by Akka gRPC. DO NOT EDIT.
package raft.transport

import scala.concurrent.ExecutionContext

import akka.actor.ClassicActorSystemProvider

import akka.grpc.GrpcChannel
import akka.grpc.GrpcClientCloseException
import akka.grpc.GrpcClientSettings

import akka.grpc.scaladsl.AkkaGrpcClient

import akka.grpc.internal.NettyClientUtils

import akka.grpc.AkkaGrpcGenerated

import akka.grpc.scaladsl.SingleResponseRequestBuilder
import akka.grpc.internal.ScalaUnaryRequestBuilder

// Not sealed so users can extend to write their stubs
@AkkaGrpcGenerated
trait RaftServiceClient extends RaftService with RaftServiceClientPowerApi with AkkaGrpcClient

@AkkaGrpcGenerated
object RaftServiceClient {
  def apply(settings: GrpcClientSettings)(implicit sys: ClassicActorSystemProvider): RaftServiceClient =
    new DefaultRaftServiceClient(GrpcChannel(settings), isChannelOwned = true)
  def apply(channel: GrpcChannel)(implicit sys: ClassicActorSystemProvider): RaftServiceClient =
    new DefaultRaftServiceClient(channel, isChannelOwned = false)

  private class DefaultRaftServiceClient(channel: GrpcChannel, isChannelOwned: Boolean)(implicit sys: ClassicActorSystemProvider) extends RaftServiceClient {
    import RaftService.MethodDescriptors._

    private implicit val ex: ExecutionContext = sys.classicSystem.dispatcher
    private val settings = channel.settings
    private val options = NettyClientUtils.callOptions(settings)

    
    private def requestVoteRequestBuilder(channel: akka.grpc.internal.InternalChannel) =
    
      new ScalaUnaryRequestBuilder(requestVoteDescriptor, channel, options, settings)
    
    
    private def appendEntriesRequestBuilder(channel: akka.grpc.internal.InternalChannel) =
    
      new ScalaUnaryRequestBuilder(appendEntriesDescriptor, channel, options, settings)
    
    

    
    /**
     * Lower level "lifted" version of the method, giving access to request metadata etc.
     * prefer requestVote(raft.transport.RequestVoteRequest) if possible.
     */
    
    override def requestVote(): SingleResponseRequestBuilder[raft.transport.RequestVoteRequest, raft.transport.RequestVoteResponse] =
      requestVoteRequestBuilder(channel.internalChannel)
    

    /**
     * For access to method metadata use the parameterless version of requestVote
     */
    def requestVote(in: raft.transport.RequestVoteRequest): scala.concurrent.Future[raft.transport.RequestVoteResponse] =
      requestVote().invoke(in)
    
    /**
     * Lower level "lifted" version of the method, giving access to request metadata etc.
     * prefer appendEntries(raft.transport.AppendEntriesRequest) if possible.
     */
    
    override def appendEntries(): SingleResponseRequestBuilder[raft.transport.AppendEntriesRequest, raft.transport.AppendEntriesResponse] =
      appendEntriesRequestBuilder(channel.internalChannel)
    

    /**
     * For access to method metadata use the parameterless version of appendEntries
     */
    def appendEntries(in: raft.transport.AppendEntriesRequest): scala.concurrent.Future[raft.transport.AppendEntriesResponse] =
      appendEntries().invoke(in)
    

    override def close(): scala.concurrent.Future[akka.Done] =
      if (isChannelOwned) channel.close()
      else throw new GrpcClientCloseException()

    override def closed: scala.concurrent.Future[akka.Done] = channel.closed()
  }
}

@AkkaGrpcGenerated
trait RaftServiceClientPowerApi {
  
  /**
   * Lower level "lifted" version of the method, giving access to request metadata etc.
   * prefer requestVote(raft.transport.RequestVoteRequest) if possible.
   */
  
  def requestVote(): SingleResponseRequestBuilder[raft.transport.RequestVoteRequest, raft.transport.RequestVoteResponse] = ???
  
  
  /**
   * Lower level "lifted" version of the method, giving access to request metadata etc.
   * prefer appendEntries(raft.transport.AppendEntriesRequest) if possible.
   */
  
  def appendEntries(): SingleResponseRequestBuilder[raft.transport.AppendEntriesRequest, raft.transport.AppendEntriesResponse] = ???
  
  

}