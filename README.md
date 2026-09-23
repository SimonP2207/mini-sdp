# Overview
A small control system that automates a continual observing campaign. It conducts
observations one at a time, stores the raw visibilities, processes each finished
observation into image products (several in parallel), then waits for a user to
quality-assess each product via a bespoke web app. User-approved products are
archived and their raw visibilities deleted to free storage; rejected products 
are deleted and their raw dataset reprocessed. Observing stops if planned 
observations' raw visibilities would push the used storage space on disk above a
configured threshold. When the user approves, raw dataset are removed, and space 
if freed.

Three processes share one SQLite database (the State Store): the orchestrator
(control loop), the observation generator (queues observations), and the QA web app.
See docs/images/components.svg for the component architecture.

# Setup
Requirements:
- uv (https://docs.astral.sh/uv/): `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Docker (on Windows: WSL2 with Docker Desktop integration enabled)

Install dependencies (uv fetches Python 3.14 if needed):
```bash
uv sync
```
The docker.io/pw410/ska-sdp-mock:0.1 image is pulled automatically on the first observation.

# Scripts
Console scripts:
- mini-sdp (orchestrator)
- mini-sdp-generate (observation request generator)
- mini-sdp-qa (QA assessment web app)

# How to run
To run the orchestrator, QA web app, and associated observation generator. You can either:
1. Separate processes in separate sessions (shared --db):
```bash
export SDP_DATA_DIR=./data
export SDP_STATE_DIR=./state
mkdir -p ${SDP_DATA_DIR} ${SDP_STATE_DIR}

# Run each below in separate terminals (separate output)
uv run mini-sdp --data-dir ${SDP_DATA_DIR} --db ${SDP_STATE_DIR}/sdp.db --threshold 3221225472
uv run mini-sdp-generate --db ${SDP_STATE_DIR}/sdp.db
uv run mini-sdp-qa --db ${SDP_STATE_DIR}/sdp.db --data-dir ${SDP_DATA_DIR}
```

2. Containerised solution:
```bash
export SDP_DATA_DIR=`pwd`/data
export SDP_STATE_DIR=`pwd`/state
mkdir -p $SDP_DATA_DIR $SDP_STATE_DIR
docker compose up --build
```

After running the above, the QA web app can be accessed at `http://127.0.0.1:8000` in your browser.

To monitor active creation/deletion of products/directories, it's informative to watch the file trees update in real-time:
```bash
sudo apt install tree
watch -n -1 -d 'tree -h --filelimit 6 --du *'
```

# Tests/Linting/Formatting
The following runs pytest and ruff:
```bash
./check.sh
```

or, on Windows:
```powershell
./check.ps1
```

# Future Work/Improvements
- [ ] Parameterise `DataProcessor` so one observation can yield multiple science products (e.g. continuum vs spectral cube)
- [ ] Migrate from SQLite to Postgres to enable multi-server deployment (if not adopting etcd)
- [ ] Django backend as common point to manage DB operations through REST framework
- [ ] Authentication on the QA web app before (and if) it ever leaves a trusted network
- [ ] Replace the poll-based QA reconcile loop with event/notify to cut latency and idle DB queries (Postgres's LISTEN/NOTIFY)
