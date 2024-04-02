To deploy Patroni with docker-compose across multiple nodes (1 master + 1 or more replicas), navigate to the node-specific folder and execute 

`docker-compose up -d`. 

Manage the cluster via patronictl inside the container. 

For example, to view the current node list on node01, run 

`docker exec -it patroni_node01 /bin/bash` 

followed by 

`patronictl -c /etc/patroni/postgres.yaml list batman`. 

To initiate a master switchover, use 

`patronictl -c /etc/patroni/postgres.yaml switchover`. 

For a full command list, refer to the official documentation or use 

`patronictl --help`.