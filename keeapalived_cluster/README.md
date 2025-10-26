The folder contains files for deploying keepalived on multiple nodes, with node01 as the master and node02 as the backup instance, necessitating different configurations for each. 

To launch keepalived, navigate to the respective node's folder and run 

`docker-compose up -d`. 

Keepalived raises a VIP, utilized by HAProxy on both nodes as the entry point to the cluster. 

Normally, the VIP is on the master server, but it switches to the backup if the master or HAProxy on node01 fails, ensuring cluster operations continue uninterrupted. 

Health checks in the container verify DNS server availability on port 53.