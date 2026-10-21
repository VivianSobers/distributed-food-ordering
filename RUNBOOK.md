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
