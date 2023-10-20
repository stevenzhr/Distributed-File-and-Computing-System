# Distributed File and Computation Engine

## How to run
- The Distributed Computation Engine is run on top of the Distributed File System, so make sure to start the DFS first.

- There are 5 components to start overall - DFS Controller, DFS storage nodes, MapReduce Manager(MRManger), Workers(computation nodes), and Client.

- You may follow the steps below to start the system.
  (I would suggest openning 5 terminal windows, one for each component, so you may have a clear view of each one's log.)

1. **Controller** <br/>
    ```
    $ cd dfs/controller/
    $ go run controller.go 26999 26998
    ```
    *26999 - port for incoming Node connection* <br/>
    *26998 - port for incoming Client connection*

2. **Storage Nodes** <br/>
    ```
    $ cd dfs/test/
    $ ./start.sh
    ```

3. **MRManager** <br/>
    ```
    $ cd dfs/MRManager/
    $ go run manager.go 26997 26996
    ```
    *26997 - port for incoming Worker connection* <br/>
    *26996 - port for incoming Client connection*

4. **Worker** <br/>
    ```
    $ cd dfs/test/
    $ ./start_worker.sh
    ```
    To clean up the all workers' ports over the cluster, you may use
    ```
    $ ./shut.sh
    ```

5. **Client** <br/>
    ```
    $ cd dfs/Client/
    $ go run client.go orion02:26998 orion02:26996
    ```
    *orion02:26998 - host and port of controller* <br/>
    *orion02:26996 - host and port of MRManager*


### How to run MapReduce Job

- Before you run MapReduce Job, make sure the input file is stored in the DFS system.

1. Create .so file
    - You may find examples of MapReduce program under 'dfs/client/mapreduce'

    - DO NOT change the structure. You may only edit r (number of reducer), map(), reduce(), compare()

    - Build the .so file using:
        ```
        $ go build -buildmode=plugin -o la.so logAnalyzer1.go
        ```
        *la.so - name of .so file* <br/>
        *logAnalyzer1.go -  name of your MR program*

2. Run MapReduce

    ```
    $ mr ./la1.so url-dataset-1m.txt output_name
    ```
    *./la1.so - the path from 'client' folder to your .so file* <br/>
    *url-dataset-1m.txt - input file name (in DFS)* <br/>
    *output_name - output file name*

- The output file (.txt) will be stored in DFS automatically, you may use ```$ ls``` to look up, and use ```$ get filename``` to retrieve the file.


## Entity Functions

### MRManager

- Handle MapReduce request from client
- Assign takes with load balancing and locality
- Monitor computing progress and report to client
- Keep track of all workers
- Store following data:
    - Map Tasks: key is chunk name, value is worker name and staatus
    - Reduce Tasks: same as Map Tasks
    - Workers: key is worker's host name, value is host & port
### Worker

- Handle Map and Reduce jobs
- communicate with MRManager

### Client

- Handle user input command
- communicate with MRManager

## Program Flow Diagrams

![MapReduce Manager](/images/manager.png)
![Worker](/images/worker.png)

## Messages in communication

1. **Chunk** - .so file, partition file <br/>
    *Multipul progresses involves sending a .so file, such as, client sends to MRManager, and MRManager sends to Workers* <br/>
    *Partition files sends between Mapper and Reducer*
    - File Name
    - Checksum
    - Size
    - Data Stream

1. **ChunkReq** - Chunk Request <br/>
    *Reducer ask for partitions from Mappers*
    - Get or Put
    - Chunk

1. **ChunkRes** - Chunk Response<br/>
    *Mapper responds to Reducer's request of partition files*
    - has this chunk or not
    - Chunk

1. **MapredReq** - Map Reduce Request
    - .so file
    - Input File Name
    - Output File Name
    - Parameters (no use in this project)
    - Controller Host (for MRManager store output file to DFS)

1. **General Request** - Reneral Request <br/>
    *Worker sends Map and Reduce task report to MRManager*
    - Request Type 
    - Request Message List


3. **GeneralRes** - General Response
    - Responose Type
    - Response Message List <br/>
    
    Used in
    - MRManager sends confirmation to Client on receving MR Request
    - MRManager sends progress report of MR jobs to Client, with type "report" or "complete", and message
    - Worker sends execution result to MRManager
    - MRManager responds to Worker's join request

4. **MapTaskReq** - Map Task Request <br/>
    *Send from MRManager to Workers assigning Map jobs*
    - .so File
    - Input Chunk Name List
    - Parameters (no use in this project)

1. **RedTaskReq** - Reduce Task Request <br/>
    *Send from MRManager to Workers assigning reduce jobs*
    - Map Task Id
    - Reduce Task Id
    - Mapper Host
    - Number of chunks
    - Controller Host
    - Output File Name

1. **JoinReq** - Join Request <br/>
    *Workers send to MRManger at begining*
    - Host and port 


## Design Decisions

1. Roles 
    - The MapReduce Manager and DFS Controller share many similarities in terms of functionality, as do workers and storage nodes, such as node registration, heartbeat monitoring, and fault tolerance. 
    - However, we have decided to separate their roles to make their functionalities more independent. This allows us to run the DFS module separately without the need to start the MapReduce module.

1. Who checks input file’s existance?
    - We choose client instead of MRManager, to minimize the communication.
    - When client receives a MapReduce command, it asks the controller whether the input file is in the system. If the controller answers "NO", client could directly reject the request without talking to MRManager.


1. How to assign map tasks to worker?
    - MRManager gets a map of each chunk stores on which 3 nodes.
    - It turn the map into "each node has which chunks".
    - loop:
        - assign a chunk to the node (1) which has the least number of unassigned chunks (2) the number of assigned chunks of which should be no more than the average assigned number of the remaining nodes.
        - once a chunk has been assigned to a nodes, delete the replica information to avoid reassignment
        - once a node has no more chunks available, delete it from the node list
    - for example, considering 3 nodes with 3 chunks c1, c2, c3
        - node1 : c1, c2, c3
        - node2 : c1, c2, c3
        - node3 : c1, c2, c3

        At the begining all the nodes have equal number of choices, so we could assign any chunk to anyone. 

        assume we assign chunk1 to node1 : [c1 - node1]

        delete all c1's replica on other nodes, we have
        - node1 : c2, c3
        - node2 : c2, c3
        - node3 : c2, c3

        Although node1 still have the least number of choices(same as other 2 nodes), but node1 has been assigned 1 chunk, which is larger than the average number of chunk assigned on the remaining nodes (1 > 0.333), so node1 cannot be assigned. Node2 and node3 has same priority.

        assume we assign chunk2 to node2 : [c1 - node1, c2 - node2]

        delete all c2's replica on other nodes, we have
        - node1 : c3
        - node2 : c3
        - node3 : c3

        Alought 3 nodes have same number of choice, but node1 and node2 have been assigned more chunks than average (1 > 0.6666), so we assign node3.

        [c1 - node1, c2 - node2, c3 - node3]

    - This strategy relies on a relatively balanced initial chunk allocation, which our DFS system does achieve. 
    - Additionally, the more chunks there are, the better this strategy performs, with a high likelihood of keeping the difference in allocated chunks among all the nodes to within 1. 
    - The downside is that when there are relatively few chunks, there is a small chance that certain allocation steps may result in a difference of more than 1 in the final number of allocated chunks among nodes.

1. How to assign reduce jobs?

    - We face a natural trade-off when it comes to selecting reducers: 
        - if we want to maximize locality, we need to choose workers with the highest number of maps, but this also means that this worker will bear a heavier computational load, resulting in an imbalance. 
        - Conversely, selecting workers with fewer maps leads to poor locality. 
        - Ultimately, since the computational load is not too significant, we opted for the former approach - the one that offers the best locality.
    - number of reducer = MIN( user input R, number of ndoes involved)
    - choose the top R nodes which has the most number of map tasks

1. When to send tasks to workers?
    - We choose to allocate all Map jobs at the beginning and send the relevant nodes a single message containing this information, to minimize communication.
    - Another approach could be to issue a new job every time we receive a completed job. However, I do not see the need for dynamic allocation in this case.
