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

Every machine also needs the Kafka command-line tools, because the topic checks and the examiner's own commands run from any VM:

```bash
bash kafka/install_kafka.sh          # Kafka 3.9.2 into ~/kafka_2.12-3.9.2, checksum verified
```

VM2 and VM3 only:

```bash
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

Before the demo, on every machine, in the terminal you will use for Kafka commands:

```bash
git pull
rm -rf output                        # the course asks for no old outputs; data/order.jsonl stays untouched
eval "$(bash scripts/demo_env.sh)"   # exports KAFKA_HOME and BS=<VM2 IP>:9092
```

The examiner may then run Kafka's own tools directly, for example `$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic orders`. `BS` points at VM2, which is never stopped.

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
| 9 | VM1 or VM4 | `bash scripts/broker_failure_check.sh` | Topic configuration and ISR before the failure, counts both topics, then asks for VM3 to be stopped |
| 10 | VM3 | `bash kafka/stop_broker.sh vm3`, then press Enter on the step 9 machine | Every partition led by broker 1 with ISR `1`; the check prints `PASS` for both topics |
| 11 | VM4, VM1 | Repeat steps 5 to 8 with VM3 still stopped | Pipeline continuity: the same 62 orders, 44 metrics and summary tables |
| 12 | VM3 | `bash kafka/start_broker.sh vm3` | Broker 2 starts and catches up |
| 13 | VM1 or VM4 | `bash scripts/broker_rejoin_check.sh` | Waits until every partition shows `in sync 1,2`, then reads both topics and prints `PASS` |
| 14 | VM4, VM1 | Repeat steps 5 to 8 once more | The pipeline keeps working with both brokers back |

Steps 5 and 6 can run in either order; the VM4 consumer waits up to 5 minutes for data (`FOODPIPE_MAX_WAIT` changes that) and stops once the topic has been quiet for 15 seconds (`FOODPIPE_IDLE_TIMEOUT`).

Steps 11 and 14 produce the orders again without resetting the topics, so `orders` then holds every record two or three times. Both Spark jobs count exact duplicates once, so the outputs stay byte-identical to the first run; a test checks this. Each stage overwrites its files in `output/`, and the examiner grades the files from the live run, which can be checked at the course's output evaluator.

## 5. When something goes wrong

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `Missing .../meta.properties; run bash kafka/format_storage.sh` | Storage never formatted on this machine | Run `bash kafka/format_storage.sh vm2` (or `vm3`), then start again |
| `Cluster ID mismatch in .../meta.properties` | Data folder formatted for a different cluster | Stop the broker, delete the data folder named in the message, run `format_storage.sh` again |
| Broker start times out | Java is not 11, or the port is taken | Read the log lines it prints; check `java -version` and `ss -ltn \| grep 9092` |
| `Only 1 of 2 brokers are up; start VM3 first` | VM3 is down or cannot reach VM2 | Start VM3; ping VM2 from VM3; check port 9093 is open on VM2 |
| Producer or consumer hangs, then fails to connect | ZeroTier or firewall | Ping both broker IPs; check the firewall rule from section 2; check `config/cluster.env` matches `zerotier-cli listnetworks` |
| Consumer prints 0 records and exits | Nothing was produced in time | Check the producer ran against the same cluster; raise `FOODPIPE_MAX_WAIT` |
| Spark crashes before starting | Wrong Java version | `export JAVA_HOME=/path/to/jdk-11` and rerun |
| Broker died because a laptop slept | Laptop went to sleep | `bash kafka/start_broker.sh vm2` (or `vm3`); keep laptops plugged in and awake |

**Standby machine.** A spare server is set up with the repository, Java and Kafka. If VM1, VM3 or VM4 fails, Vivian moves that role's ZeroTier IP to the standby, and the standby runs the same script after `git pull`. VM2 cannot be replaced once the demo has started, because the controller lives there.
