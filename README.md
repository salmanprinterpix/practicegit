# Patroni Cluster

This repository is dedicated to supporting files for a **Postgres cluster managed by Patroni**. Each service within the cluster—**etcd**, **keepalived**, **haproxy**, and **patroni**—has its dedicated folder containing relevant files.

## Structure
Each service folder is equipped with a `README.md`, providing detailed instructions on setup and maintenance of the respective component.

## Deployment Order
To ensure a smooth cluster deployment, follow this order:
1. **etcd**: Distributed Configuration Store (DCS) system, managing key-value data for cluster state, including master node identity.
2. **Patroni**: Enhances Postgres, facilitating cluster operations via command line.
3. **Keepalived**: Manages VIP (Virtual IP) for failover handling.
4. **Haproxy**: Balances and proxies incoming connections, ensuring high availability.

### Current Setup
- **etcd** nodes: 3
- **Patroni, Keepalived, Haproxy** nodes: 2

## TODO
- [ ] Integrate Grafana for advanced monitoring.
- [ ] Implement `.env` file management for node IPs (`$NODE01_IP`, `$NODE02_IP`, `$NODE03_IP`).

> **Note**: For a successful setup, ensure to follow the instructions provided in each service's `README.md`.
