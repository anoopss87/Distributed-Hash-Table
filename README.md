# Distributed-Hash-Table

A ring based distributed hash table is implemented in a distributed environment which consists of
•	Datanodes – to store data/keys.
•	A control client - to perform operations like add a node, delete a node and load balancing of a node.
•	Regular Client – to perform key look up.

Datanodes:
A data node will be listening on a specific port to accept socket connections. It takes port number as the command line argument. 
A Data node can independently perform add a node, delete a node and load balancing operation. The control client on receiving request 
from the user delegates the requested operation to the randomly selected data node. The data node will perform the requested operation 
locally and updates its local data structures and sends the updated data structure to all other data nodes. The other data nodes on 
receiving data structure update message will update its data structure. A data node will be continuously listening for socket connections
from other data nodes, control client and regular clients. One each connection request, it will spawn a new thread which will be in 
blocking read for the data input stream and process the read message accordingly. For a data node, all spawned thread access a shared 
data node object and update the data structure of the shared data node object in a synchronized way.

Control Client:
The control client reads from the initial configuration to establish initial ring structure. It takes initial configuration file as the 
command line argument. 
 
a)	Add a node: The control client requests the nodeId, IP address and port number from the user and then it asks all other data nodes
to establish a socket connection to the newly added node. The control client also establishes socket connection to the newly added node. Once all the socket connections are established, the control client sends add a node command to any one of the randomly selected data node which will first updates its local data struc-tures and propagate the updated data structures to all other data nodes.
b)	Delete a node: The control client requests the nodeId to be deleted from the user and then it randomly selects one data node and 
sends delete a node command with the nodeId. The data node will remove the nodeId from its local data structures and then it propagates
to all other data nodes which also updates its data structures accordingly.
c)	Load Balancing: The control client requests the user to enter the nodeId which is overloaded and then it randomly selects one data
node and sends a load balance command with the nodeId. The data node gets the predecessor of the nodeId and compute the key range between
the predecessor and overloaded nodeId. The data node then take some fixed percentage say 25% of the key range and adds to the predecessor
of the overloaded nodeId which will become the new nodeId of the predecessor and updates its local data structures and propagates the 
updated data structure to all other data nodes.
d)	Print the table: Whenever a data node updates its local data structures, it also sends the updated data structure to control client
along with other data nodes. 

Regular Client:

The regular client reads from the initial configuration file to know about data nodes present in the ring structure so that it can query
the data node for look up and table update request operation. It takes initial configuration file as the command line argument.
 

a)	Print the table: The regular client maintains the local cache of the ring data structure and prints it on the console. Initially, 
the table will be empty and it maintains the version number of the table. The data nodes and control client never knows about regular 
client and hence the regular client doesn’t receive data structure updates from the data nodes. The local table cache of the regular 
client might be outdated.
b)	Lookup: The regular client asks the user to enter the data to be looked up and it then performs hashing of the data to compute the
key. It then searches the local table and displays all the replicas with the version number of the local table. It also sends look up 
request for the primary data node to perform look up for the key and returns all the replicas of the key with the version number of the
data node table. If there is a version number mismatch between local table and data node table then the client can request for table 
update request.
c)	Table update request: The table update request command will randomly selects one data node and ask it to send its latest data 
structure. The client on re-ceiving the updated data structure will update its local data structure along with the version number of 
the data node table.    
Note: The data structures updated in a coarse grained way. On each fine grained updation of the local data structure, the data node will
send the entire data structure to all other nodes. The nodes on receiving data structure will clear its current entries and updates its 
entries based on the received data.

