# Live Demo Runbook

Step-by-step commands for running the pipeline in front of the examiner. Every machine uses the same repository; what a machine does depends only on which script it runs.

| Machine | Person | Runs |
| --- | --- | --- |
| VM1 | Dhriti | `scripts/vm1_stage1_produce.sh`, `scripts/vm1_stage2.sh` |
| VM2 | Vivian | `kafka/start_broker.sh vm2` (controller + broker 1) |
| VM3 | Dhanya | `kafka/start_broker.sh vm3` (broker 2, stopped in the failure test) |
| VM4 | Navya | `scripts/vm4_stage1.sh` |

All commands run from the repository root.

## 1. Join the ZeroTier network (every machine, once)

Ask Vivian for the network ID, then:

| System | Command |
| --- | --- |
| Ubuntu / Debian | `curl -s https://install.zerotier.com \| sudo bash` then `sudo zerotier-cli join <network-id>` |
| Fedora | `sudo dnf install zerotier-one`, `sudo systemctl enable --now zerotier-one`, `sudo zerotier-cli join <network-id>` |
| Windows / macOS | Install from zerotier.com/download, then **Join New Network** from the tray or menu bar icon. On Windows, pick **Private** when asked about the network. |

If Kafka runs inside a virtual machine, install ZeroTier inside that VM, not on the host.

Vivian approves each new member. Check that you got an address in `192.168.193.0/24` with `sudo zerotier-cli listnetworks`, and that every machine can ping the others. `sudo zerotier-cli peers` should show `DIRECT` for the other members; `RELAY` means a slow path, so try another network (a phone hotspot) before the demo.

The addresses each role uses are in `config/cluster.env`. If an address changes, update that file, commit it, and `git pull` on every machine.

## 2. Open the firewall for ZeroTier (Linux, once)

On Linux the ZeroTier interface for this network is called `ztbpajn2rk` on every machine (check with `ip -br addr | grep zt`).

| System | Command |
| --- | --- |
| Fedora (firewalld) | `sudo firewall-cmd --permanent --zone=trusted --add-interface=ztbpajn2rk && sudo firewall-cmd --reload` |
| Ubuntu (ufw) | Only if `sudo ufw status` says active: `sudo ufw allow in on ztbpajn2rk` |

The `zt+` wildcard does not work with firewalld; use the exact interface name. Brokers listen on port 9092 and the controller on VM2 on port 9093.

## 3. Software and broker setup (once)

Every machine:

```bash
git clone https://github.com/VivianSobers/distributed-food-ordering.git
cd distributed-food-ordering
python3.10 -m venv .venv && .venv/bin/pip install -r requirements.txt
java -version     # must be 11; otherwise: export JAVA_HOME=/path/to/jdk-11
```

VM2 and VM3 only:

```bash
bash kafka/install_kafka.sh          # Kafka 3.9.2 into ~/kafka_2.12-3.9.2, checksum verified
bash kafka/format_storage.sh vm2     # on VM2 (use vm3 on VM3)
bash kafka/start_broker.sh vm2       # on VM2 (use vm3 on VM3)
```

`format_storage.sh` stamps the data folder with the cluster ID from `config/cluster.env`. It is safe to run again: it reports "already formatted" and leaves existing data alone. Start VM2 before VM3, because broker 2 registers with the controller on VM2.

Once both brokers are up, create the topics from any machine:

```bash
bash kafka/create_topics.sh          # orders + order_metrics, 2 partitions, RF 2
bash kafka/describe_topics.sh        # every partition should show "in sync 1,2"
```

`create_topics.sh` refuses with "Only 1 of 2 brokers are up" until VM3 has joined, since replication factor 2 needs both brokers.

To rehearse the whole pipeline on one laptop without the other machines, run `bash scripts/local_e2e.sh --down`. It starts two brokers on ports 19092 to 19094, runs every stage, checks the outputs, stops broker 2, and prints `E2E OK` when everything passes.

## 4. Demo order

Run `git pull` on every machine first.

| Step | Where | Command | What the examiner sees |
| --- | --- | --- | --- |
| 1 | VM2 | `bash kafka/start_broker.sh vm2` | `vm2 broker up on port 9092` |
| 2 | VM3 | `bash kafka/start_broker.sh vm3` | `vm3 broker up on port 9092` |
| 3 | any | `bash kafka/reset_topics.sh` | Fresh `orders` and `order_metrics` topics |
| 4 | any | `bash kafka/describe_topics.sh` | 2 partitions per topic, replicas on both brokers, all in sync |
| 5 | VM4 | `bash scripts/vm4_stage1.sh` | Consumer 1 waits for orders (up to 5 minutes) |
| 6 | VM1 | `bash scripts/vm1_stage1_produce.sh` | 62 records produced, split across partitions 0 and 1 |
| 7 | VM4 | (continues by itself) | 62 consumed, Spark Job 1 writes 44 metrics, Producer 2 sends them |
| 8 | VM1 | `bash scripts/vm1_stage2.sh` | 44 consumed, Spark Job 2 prints the restaurant and city tables |
| 9 | VM1 or VM4 | `bash scripts/broker_failure_check.sh` | Counts both topics, then asks for VM3 to be stopped |
| 10 | VM3 | `bash kafka/stop_broker.sh vm3`, then press Enter on the step 9 machine | Every partition moves to broker 1; the check prints `PASS` for both topics |
| 11 | VM3 | `bash kafka/start_broker.sh vm3` | Broker 2 rejoins and catches up |

Steps 5 and 6 can run in either order; the VM4 consumer waits up to 5 minutes for data (`FOODPIPE_MAX_WAIT` changes that) and stops once the topic has been quiet for 15 seconds (`FOODPIPE_IDLE_TIMEOUT`).

To show that the pipeline keeps working with only one broker, repeat steps 5 to 8 while VM3 is still stopped, before step 11.

Each stage overwrites its files in `output/`, so a rerun starts clean. Running step 3 again empties the topics; if you skip it and produce again, the topic holds every record twice, and both Spark jobs still give the same results because they count exact duplicates once.
