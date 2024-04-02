# ETCD Cluster Files

Folder containing files for the etcd cluster setup.

## Node Preparation

Clone the repository and install the etcd client:


`git clone repo_path/patroni_cluster.git`


`sudo apt install etcd-client`


Each folder (node01, node02, node03) contains a docker-compose.yaml for launching an etcd instance.

## Cluster Launch

Nodes are added to the cluster sequentially.

### Node01

- Navigate to node01 and start the first cluster node:


`cd ~/patroni_cluster/etcd_cluster/node01`

`docker-compose up -d`

- Verify etcd is running:

`etcdctl member list`

- Add the second node:

`etcdctl member add etcd_node02 http://$NODE02_IP:2380`

### Node02

- Launch the second node:

`cd ~/patroni_cluster/etcd_cluster/node02`

`docker-compose up -d`

### Verification and Adding More Nodes

- Return to node01 for verification 

`etcdctl cluster-health`

and add the third node similarly.

- Follow the process for each node, checking the cluster health after each addition.


## Documentation
For etcd documentation, refer to 
https://manpages.org/etcd