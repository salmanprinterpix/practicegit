To set up HAProxy across several nodes, the `haproxy.cfg` file is used for configuration, and unique `docker-compose.yaml` files on each node specify distinct container names. 

Launch HAProxy with 

`docker-compose up -d` 

in the node-specific directory. 

HAProxy sends REST API requests to nodes on port 8008 to identify the master patroni node by response HTTP codes, redirecting traffic accordingly. 

It utilizes an official HAProxy image with curl installed and monitors port 8888 for container health checks and keepalived to ensure HAProxy is functioning correctly on the selected node.