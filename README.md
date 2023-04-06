# Distributed File System

## How to run

1. Start the controller first by running: go run controller.go 26999 26998, where 26999 is the port listening to heartbeat of storage nodes, and 26998 is the port listening to client's request

2. Start the storage nodes
    - You may start manully one by one, by go to dfs/node folder, and run: go run node.go orion01:26999,
    where orion01 is the host name on which runs the controller; and 26998 is the port that controller is listening to the heartbeat.
    - Alternatively, you may batch start nodes by going to dfs/test folder, and run ./start.sh. Before running, you need to config host:port in start.sh , the same setting as in starting manully.

3. Start the client
    - Go to dfs/client, and run: go run glient.go orion01:26998. Again, orion01 is the host of controller and port 26998 is where the controller listens to client's request.

4. In client, you can use the following command:
    - store [filename] [chunksize in byte], for example, store test.txt 5. Default chunk size is 64MB
    - get [filename]
    - delete [filename]
    - ls, list of files
    - lsn, list of nodes

## Entity Functions

### Controller:

- Responsibility
    - Handle node join request
    - Handle node heartbeat report
        - Chunk instruction distribution
    - Handle client request
        - Get file list
        - Store file
        - Retrieve file
        - Delete
        - List active nodes (and occupancy)
        - Check free space
        - Get total number of requests processed
    - Regularly check node activity
        - Handle node failure
- Maintain information
    - List of active storage node
    - List of archive metadata
        - File names
        - Chunks
        - Relevant storage node
    - List of chunks update for next heartbeat

### Storage Node:

- Responsibility
    - Send join request
    - Send heartbeat report (5s)
        - Free space available
        - Total number of requests processed
        - New added chunk list
    - Handle chunk request
        - Store chunk
        - Retrieve chunk
    - Forward chunk
    - Request chunk (When find corrupted chunk)
- Maintain information
    - List of local chunks
        - Chunk id
        - Source file name
        - Checksum(Hash)
        - Chunk size
    - List of new added chunks
        - (Same with above)

### Client:

- Store file 
    - Request to controller, file chunks to nodes.
    - Chunk size customizable
    - Send once, replication taken care by nodes.
    - No file of the same name allowed; or delete before storing
- Retrieve file
    - Parallel, one thread per chunk
- Delete
- List all files (i.e. ls)
- List of active nodes
- Total disk space
- \# of requests handled by each node

## Program flow diagram: 

![Controller](/images/01.controller_flow_diagram.jpg)
![Node](/images/02.node_flow_diagram.jpg)
![Client](/images/03.client_flow_diagram.jpg)


## Strategy

### Fault tolerance

- Node failure: The system keeps running as along as the number of nodes is >= 3. When a storage node dies, the controller would notices soon, and assign the replication jobs of the chunk that stores on the dead node to other active nodes.

- Chunk failure: When a chunk is retrived in a clinet request, the node would check its chunksum. If the checksum is wrong, meaning the chunk broke, the node would reject the request, and start to send request to controller of fixing the chunk. The client will send request to the next node that has this chunk of data.

### Data distribution

- Uniformly distribution over active nodes is better than offering the most vacant node a higher priority as more nodes would join the work which saves processing time.