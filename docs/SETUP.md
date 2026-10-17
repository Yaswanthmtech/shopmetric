# ShopMetric setup guide

Everything (Kafka, Spark, Java, Python, Postgres, the data lake, the dashboard) runs **inside Docker**.
You do **not** install Java, Spark, Kafka, Python or Postgres yourself.

## Pick your path

| Your machine | Path | ShopMetric mode |
|---|---|---|
| **Windows, 8 GB RAM** | **A: Docker Engine inside Ubuntu (WSL)**, no Docker Desktop | **laptop** (picked automatically) |
| Windows, 16 GB+ RAM | B: Docker Desktop | full |
| Mac | C: Docker Desktop for Mac | laptop on 8 GB Macs, full on 16 GB+ |
| Any machine, full mode in the cloud | D: GitHub Codespaces (free monthly hours) | full |

**Laptop mode** runs the same pipeline with the same code: real Apache Kafka, Spark Structured
Streaming, stateful features, the MLlib fraud model, Postgres and the Streamlit dashboard. To fit in
~4 GB it changes four things:
- the lake is a local folder with the same parquet layout instead of the SeaweedFS S3 server
- there is no Kafka UI container (use `./shopmetric.sh topics` instead)
- Kafka and Spark get less memory and Spark uses 2 CPU cores
- `./shopmetric.sh train` pauses the stream while the model trains

Measured memory in laptop mode:

| Part | Memory |
|---|---|
| Spark stream | ~1.4 GB |
| Kafka | ~0.6 GB (estimate) |
| Dashboard | ~0.2 GB |
| Postgres and producer | ~0.2 GB |
| **Total** | **~2.5–3 GB inside WSL** |
| Training (first start, stream paused) | peaks at ~2.5 GB |

**Disk:** 15 GB free on the drive that holds WSL/Docker. **Internet:** the first start downloads ~3 GB.

---

## Path A: Windows with 8 GB RAM (recommended for low-RAM laptops)

Docker Desktop adds its own background VM and UI, which an 8 GB laptop can't spare. Docker Engine
installed directly inside Ubuntu is lighter, free for any use, and runs the same `docker` commands.

**A1. Install WSL 2 with Ubuntu.** Open **PowerShell as Administrator**:
```powershell
wsl --install -d Ubuntu
```
Restart the PC. Ubuntu opens: choose a Linux username and password. If you already have WSL, update it:
```powershell
wsl --update
```

**A2. Give WSL memory (and swap so it never crashes).** In PowerShell:
```powershell
notepad $env:USERPROFILE\.wslconfig
```
Paste, save, close:
```ini
[wsl2]
memory=4GB
processors=4
swap=8GB

[experimental]
autoMemoryReclaim=gradual
```
This leaves about 3 GB for Windows itself. `autoMemoryReclaim` hands unused memory back to Windows.

**A3. Put WSL on the bigger drive (only if C: has less than 30 GB free).** Check free space in File
Explorer → This PC. If C: is your small SSD and nearly full, move Ubuntu to the larger drive
(replace `D:` with that drive's letter):
```powershell
wsl --shutdown
wsl --manage Ubuntu --move D:\WSL\Ubuntu
```

**A4. Turn on systemd in Ubuntu.** Docker runs as a system service and needs it. Open the **Ubuntu** app:
```bash
grep -q "systemd=true" /etc/wsl.conf 2>/dev/null || printf '[boot]\nsystemd=true\n' | sudo tee -a /etc/wsl.conf
```
Then restart WSL (PowerShell) and open Ubuntu again:
```powershell
wsl --shutdown
```

**A5. Install Docker Engine inside Ubuntu.**
```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
```
The script prints **"WSL DETECTED: We recommend using Docker Desktop"** and waits 20 seconds.
Let it continue: Docker Engine is the lighter choice for an 8 GB laptop.
Restart WSL once more so your user can run docker without `sudo` (PowerShell):
```powershell
wsl --shutdown
```
Open Ubuntu and check:
```bash
docker run --rm hello-world
docker compose version
```
If Docker Desktop is also installed, quit it and turn off **Settings → Resources → WSL integration →
Ubuntu** (or uninstall it), so the two don't fight over the `docker` command.

**A6. Put the project in your Ubuntu home folder,** not under `/mnt/c/...`, where it would be slow and
could break the scripts. Download `shopmetric.zip` to Windows Downloads, then in Ubuntu:
```bash
sudo apt update && sudo apt install -y unzip
ls /mnt/c/Users/                      # find your Windows user name
cp /mnt/c/Users/YOUR_WINDOWS_NAME/Downloads/shopmetric.zip ~/
cd ~ && unzip -o shopmetric.zip && cd shopmetric
chmod +x shopmetric.sh scripts/*.sh
```
Continue with **Run it**. Open the URLs in your normal Windows browser: WSL forwards `localhost`.

## Path B: Windows with 16 GB+ RAM (Docker Desktop)

1. Install WSL as in A1.
2. Install Docker Desktop from https://www.docker.com/products/docker-desktop/ (keep "Use WSL 2"
   ticked). Then go to **Settings → Resources → WSL integration**, turn on **Ubuntu**, and click **Apply & restart**.
3. Optionally set `memory=8GB` in `.wslconfig` (A2), then run `wsl --shutdown` and restart Docker Desktop.
4. In Ubuntu, check that `docker run --rm hello-world` works, then do A6.

Docker Desktop is free for personal use, education and small businesses.

## Path C: Mac

Install Docker Desktop for Mac (Apple Silicon or Intel). Go to **Settings → Resources** and set Memory:
use **4 GB** on an 8 GB Mac (laptop mode) or **8 GB** on 16 GB+ (full mode). Then in Terminal:
```bash
docker run --rm hello-world
cd ~ && unzip -o ~/Downloads/shopmetric.zip && cd shopmetric
chmod +x shopmetric.sh scripts/*.sh
```
If Safari already unzipped it, use `mv ~/Downloads/shopmetric ~/ && cd ~/shopmetric` instead of the `unzip` line.

## Path D: Full mode in the cloud with GitHub Codespaces

Use this when you want the **full** stack (SeaweedFS S3 + Kafka UI) or a big load test without
straining the laptop.
1. Push the project to a GitHub repository (README → "Publish it").
2. On the repository page click **Code → Codespaces → ⋯ → New with options** and choose a
   **4-core** machine. Personal accounts get 120 core-hours free each month, which is about
   30 hours on a 4-core machine.
3. In the Codespace terminal:
   ```bash
   ./shopmetric.sh mode full
   ./shopmetric.sh up
   ```
4. Open the **Ports** tab and click the globe icon next to 8501 (dashboard).
5. **Stop the Codespace when you are done** (Codespaces page → ⋯ → Stop) so it does not use up your free hours.

---

## Run it (every path)

Run every command from inside the `shopmetric` folder.

**Optional: turn on the AI copilot.** Skip this and everything else still works. In `.env`, paste your
key after `ANTHROPIC_API_KEY=`, then save with **Ctrl+O, Enter, Ctrl+X**:
```bash
nano .env
```
(`.env` is created on the first `up`; to create it now, run `./shopmetric.sh mode laptop` or `./shopmetric.sh mode full`.)

**Start everything.**
```bash
./shopmetric.sh up
```
On first run it picks laptop or full mode from how much memory Docker has and says which one it picked.
The **first run takes about 15–25 minutes on an 8 GB laptop**: it builds the images, downloads
Java and the Spark/Kafka connectors, and trains the fraud model (~4–6 min). It is finished when you see:
```
✔ ShopMetric is up in …s
  Dashboard    http://localhost:8501
```
Later starts take about a minute.

**Open:**
- http://localhost:8501 is the dashboard. A green **Live** badge appears within ~1 minute.
- http://localhost:4040 is the Spark UI. Its **Structured Streaming** tab shows 2 active queries.
- http://localhost:8080 is the Kafka UI (full mode only).

**Try the demo.**
```bash
./shopmetric.sh attack card_testing 3     # fraud appears on the Fraud tab within 1-2 minutes
./shopmetric.sh topics                    # Kafka topics, message counts, sample fraud alerts
./shopmetric.sh lake                      # what the pipeline wrote to the data lake
./shopmetric.sh status                    # containers, memory per container, latest batches
./shopmetric.sh report                    # measured numbers (let it run 10+ minutes first)
```

**Stop and restart.**
```bash
./shopmetric.sh down        # stop; data is kept. Do this when you're done, to free memory
./shopmetric.sh up          # start again
./shopmetric.sh reset       # wipe all data
./shopmetric.sh mode full   # or: mode laptop (then reset + up)
```

**Tips for 8 GB laptops while it runs:**
- Close other browser tabs, IDEs and Teams/Zoom.
- Keep only the dashboard tab open.
- Use `./shopmetric.sh bench` (it defaults to 50 orders/s in laptop mode) rather than a 200/s load test.

---

## Troubleshooting

| Message or symptom | Fix |
|---|---|
| `Cannot connect to the Docker daemon` (Path A) | `sudo systemctl start docker`. If it says systemd is not running, redo A4. |
| `permission denied ... docker.sock` | You skipped the second `wsl --shutdown` in A5. Run it in PowerShell and reopen Ubuntu. |
| `WSL 2 installation is incomplete` / `Virtualization not enabled` | `wsl --update` in PowerShell. If it still fails, enable virtualization (AMD SVM) in BIOS. |
| `port(s) ... already in use` | Another program uses that port. Close it, or change the **left** number under `ports:` in `docker-compose.yml`. |
| Build stops with `Connection reset`, `timed out`, `Could not resolve` | Network blip or VPN. Disconnect the VPN and run `./shopmetric.sh up` again; finished steps are cached. |
| Laptop very slow, or a container exits with code 137 | Memory is full. Run `./shopmetric.sh status` to see memory per container. Close other apps, check `.wslconfig` (A2), and make sure `.env` is in laptop mode (`./shopmetric.sh mode laptop`). |
| `no space left on device` | Free disk space, or move WSL to the bigger drive (A3). Remove unused Docker data with `docker system prune`. |
| `/usr/bin/env: 'bash\r'` | `sed -i 's/\r$//' shopmetric.sh scripts/*.sh` |
| `Permission denied: ./shopmetric.sh` | `chmod +x shopmetric.sh scripts/*.sh` |
| Dashboard says **Waiting** or **Stalled** for more than 3 minutes | Run `docker compose logs --tail 80 spark-stream` and share the output. |

To share an error, save the logs to a file:
```bash
docker compose logs --tail 200 > shopmetric-logs.txt
```

## Optional: run the tests on your laptop

This is not needed to run the project. It needs Python 3.12 and Java 21.
```bash
# Ubuntu (WSL):
sudo apt install -y python3 python3-venv openjdk-21-jdk-headless make
# macOS (Homebrew):  brew install python@3.12 && brew install --cask temurin@21
make venv
make test
```
The Postgres end-to-end test is skipped locally (CI runs it) and never touches your running demo.
