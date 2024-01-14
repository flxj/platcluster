

```python
实现：

raftServer:
    # server会维护一个event channel，后面的loop()逻辑就是循环消费这个channel里的ev对象
    # 
    --> start():
        1.初始化一些东西：
		  - 读取配置文件
		  - init statemachine
		  - init log
		  - 从日志中加载currentTerm

		2.将自己的状态设置为follower --> 更新状态需要加锁, 然后生成一个状态改变事件并调度(通知给所有listeners)
		3.开始循环：
		    state = s.State()
      		for state != Stopped {
        		switch state {
        		case Follower:
        			s.followerLoop()  // 以follower角色开始运行
        		case Candidate:
        			s.candidateLoop() // 以candicate角色开始运行
        		case Leader:
        			s.leaderLoop()    // 以leader角色开始运行
        		case Snapshotting:
        			s.snapshotLoop()
        		}
        		state = s.State()
	        }
	#
	--> followerLoop():
	    1. 初始化一些超时参数 electionTimeout
		2. 开始循环：
		    for s.State() == Follower {
			    需要监听三个channel:
				  1. stop channel接收server停止信号 ---> 将server状态设置为Stopped,然后退出循环

				  2. event channel 接收来自peer的请求,需要根据event的类型分别处理
				     ---> Command: follower不能处理用户命令，应该返回错误
				     ---> VoteRequest: 处理投票请求 processRequestVoteRequest(req)
				     ---> AppendEntries: 处理heratbeat或日志复制请求 processAppendEntriesRequest()
					 ---> SnapshortRequest: TODO

				  3. electionTimeout channel ---> 如果该超时条件触发，则将server状态设置为candicate, 并退出循环
				重置electionTimeout，继续循环(需要根据处理的peer请求类型和结果来确定是否重置超时时间)：
				  ---> 
			}
	#
	--> leaderLoop():
	    1. 查询日志索引
		2. 更新自己维护的peers列表的prevLogIndex (日志一致性检查机制)
		3. 开始向peers发送心跳
		4. 开始循环：
           	for s.State() == Leader {
				需要监听两个channel:
           		  1. stop channel接收server停止信号 ---> 先向peers发送停止信号，然后将自身server状态设置为Stopped,然后退出循环
           		  2. event channel 处理来自客户端或者peers的请求
					 ---> Command: 处理客户命令 processCommand()
					 ---> ApendEntriesRequest: processAppendEntriesRequest()
					 ---> AppendEntriesResponse: 处理日志复制响应processAppendEntriesResponse(resp)
					 ---> VoteRequest: processRequestVoteRequest(req)
           	}
	#
	--> candidateLoop():
	    1. 重置server的leader信息
		2. 上一个任期，和上一个日志的信息
		3. 开始发起选举：
		    for s.State() == Candidate {
                增加当前任期
				并发向所有peers发送投票请求(用一个resp channel 接收响应)
				开始记录投票超时
				投自己一票，然后开始等待peers的响应：
				---> stop channel接收server停止信号 ---> 将server状态设置为Stopped,然后退出循环
				---> resp channel接收peers的投票响应 ---> processVoteResponse(resp),累计票数，如果获得超过半数票，将状态设置为Leader,退出选举
				---> event channel接收来自peer的请求,需要根据event的类型分别处理：
				    ---> Command: candicate不能处理用户请求，应该返回一个错误信息
					---> AppendEntriesRequest: processAppendEntriesRequest(req)
					---> VoteRequest: processRequestVoteRequest(req)
				---> timeout ---> 投票超时了,继续发起下一轮选举
         	}
	#
	--> 处理投票请求 processRequestVoteRequest(req):
	    1. 如果请求中的term小于当前term --->  返回投票拒绝响应
		2. 使用请求中的term大于当前term --->  更新当前term
		3. 如果req.term == currentTerm ---> 检查server是否已经针对该term投过票，如果已投，那么返回一个拒绝响应
		4. 比较日志信息， 如果请求中的日志比当前旧 ---> 返回投票拒绝响应
		5. 投它一票(记录candicate的id,返回一个投票成功响应)
	########################################################################################
	--> processAppendEntriesRequest(req):
	    1. 如果req.term < currentTerm ---> 返回拒绝复制响应(将自己的term和日志信息返回给请求者)
		2. 如果req.term == currentTerm ---> 如果当前server是candicate,那么需要变成follower
		3. if req.term > currentTerm ---> 更新本地的term和leader信息
		4. 使用req中的pervLogIndex和prevLogTerm来和本地日志比较 ---> 如果没有本地的新, 拒绝复制请求（响应中包含本地日志的索引信息）
		5. 复制req中的entries到本地log
		6. 根据req.CommitIndex设置本地log的commit index
		7. 返回复制成功响应
	#
	--> processAppendEntriesResponse(resp):
	    1. if resp.term > currentTerm ---> update current term
		2. if resp.appendSuccess == true ---> 更新本地位和peerSyncd信息，表示该peer同步成功
		3. if 已经同步的peer个数小于法定人数 ---> 结束本处理过程
		4. 更新commitIndex， 提交可以应用的日志
	#
	--> processCommand(cmd):
	    1. 创建一条log entry
		2. 将该log entry追加写到log
		3. 如果集群中就一个节点，那么更新下commitIndex
    #
	--> processRequestVoteRequest(req)
	    1. if req.term == currentTerm ---> 如果req是赞同票，返回true
		2. if req.term > currentTrrm ---> 更新当前term
		3. 返回false
				  
```