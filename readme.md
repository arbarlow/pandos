# Pandos
Pandos is a highly scalable messaging system, based on distributed transaction logs.

## Design
Pandos’ primary goals are to achieve high write and read throughput, whilst being highly fault tolerant, ideally with little or no intervention.

*Clients* (consumers of topics) and *Producers* (writers to topics) communicate via gRPC with any node, the nodes handle the balancing and forwarding or requests to the correct node. In the event of failure of a node, a client simply needs to re-connect to another node.

Pandos has *topics*, each of which has one or more partitions, if you’re familiar with Kafka, this design borrows heavily from their excellent work.
However, whilst Kafka topics are divided into partitions across different machine and a partition is then divided into smaller chunks on a single disk, Pandos takes that one step further and divides partitions into ranges, which are then spread among machines.

Pandos splits partitions into ranges of around 64MB, spreads and replicates these ranges amongst the nodes. Single keys and values can be of arbitrary length, a range doesn’t always start and end at defined numbers. For example, if your messages around a 1MB then you should expect to find 0-63 in the first range and 64-127 in the next range.

Topics can provide absolute ordering if the partition count is set to 1, but provide absolute ordering only by partition otherwise.

A single partition can only be consumed by one consumer per consumer group, that is to say that many consumers can process from a single partition, but they all will receive the same messages.

A topic can set with a custom amount of partitions, or can also be set to “auto”, in this case Pandos will attempt to increase the amount of partitions based on the amount of consumers available. Since a consumer can always consume from multiple partitions, over partitioning is not particularly an issue unless you need absolute ordering of messages. But ideally this number of partitions is the same as the amount of consumers. If you have more consumers than you do partitions, some consumers will not perform any work. When set to “auto”, Pandos will, after a grace period, add more partitions to the topic to meet concurrency demands, the aggressiveness of this scaling up can be configured.

## Storage
Storage is based on all messages being written to an append only transaction log, split into ranges and then replicated among ma

## Clients
Clients (consumers) operate a mostly pull based model, asking for “slices” of a range (i.e, 10 at a time) and then committing their offset back to pandos. In order to reduce the latency of a message being pulled, consumers can long poll a node and will receive a reply when there are new messages to process.

## Nodes
Pandos nodes communicate with each other in order to rebalance and reach consensus about the cluster’s topological information.

This “chatter” information represents the current state of the system and indicates what all nodes currently agree on.

This state includes information such as:

* current nodes and which topics, partitions and ranges they currently hold and the nodes dns/ip
* consumer connections to nodes and what partitions they’re currently listening to changes for
* offsets for consumers

### Rebalancing within a node
Events can occur than will trigger rebalancing of ranges amongst nodes, these are:

*Node added*: The new node communicates information about itself to other nodes, indicating that it has space available and how much. The cluster then rebalances some replicas onto the new node after a grace period.

*Nodes going offline*: If a member of a Raft group ceases to respond, after 5 minutes, the cluster begins to rebalance by replicating the data the downed node held onto other nodes, if there are sufficient nodes and space available.

### Node UI
Each node provides an admin UI that should cluster state and can also be used to manage topics/partitions, change settings and set consumer offsets (to re-process messages)

## Scalability/Throughput
Storage scalability is achieved by adding more nodes, which in turn add more storage, adding more nodes increases the capacity of the cluster by the amount of storage on each node, divided by a replication factor.

High write throughput is achieved, due to topics being partitioned and effectively parallelising write among machines, and therefor disks.
