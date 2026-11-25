(() => {
  // Dynamically infer repository URL when hosted on GitHub Pages (works on forks too)
  const hostParts = window.location.hostname.split(".");
  const pathParts = window.location.pathname.split("/").filter(Boolean);
  const owner = hostParts[0] && window.location.hostname.endsWith("github.io") ? hostParts[0] : "rajathgadagkar";
  const repo = pathParts[0] && window.location.hostname.endsWith("github.io") ? pathParts[0] : "streamlog";
  const REPO_URL = [window.location.protocol === "http:" ? "http:" : "https:", "", "github.com", owner, repo].join("/");

  const navLink = document.getElementById("repo-link-nav");
  const heroLink = document.getElementById("repo-link-hero");
  if (navLink) navLink.href = REPO_URL;
  if (heroLink) heroLink.href = REPO_URL;

  // ── 1. Interactive Cluster Simulator ──────────────────────────────────────
  const state = {
    rrCounter: 0,
    brokers: [
      { id: "broker1", port: 9001, alive: true },
      { id: "broker2", port: 9002, alive: true },
      { id: "broker3", port: 9003, alive: true },
    ],
    partitions: [
      { id: 0, primary: "broker1", replica: "broker2", log: [] },
      { id: 1, primary: "broker2", replica: "broker3", log: [] },
      { id: 2, primary: "broker3", replica: "broker1", log: [] },
    ],
    groups: {
      "payment-group": {
        mode: "Partition Sharing",
        members: ["pay-c1", "pay-c2"],
        offsets: { 0: 0, 1: 0, 2: 0 },
        uncommitted: { 0: 0, 1: 0, 2: 0 },
        received: { "pay-c1": 0, "pay-c2": 0 },
      },
      "inventory-group": {
        mode: "Independent Fan-Out",
        members: ["inv-c1"],
        offsets: { 0: 0, 1: 0, 2: 0 },
        uncommitted: { 0: 0, 1: 0, 2: 0 },
        received: { "inv-c1": 0 },
      },
    },
    orderSeq: 1001,
  };

  function stableHash(str) {
    let h = 2166136261;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    return h >>> 0;
  }

  function assignmentsFor(group) {
    const members = [...group.members].sort();
    const map = {};
    members.forEach((m) => (map[m] = []));
    if (members.length > 0) {
      for (let pid = 0; pid < state.partitions.length; pid++) {
        map[members[pid % members.length]].push(pid);
      }
    }
    return map;
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }

  function addLog(op, detail) {
    const el = document.getElementById("wire-log");
    const ts = new Date().toTimeString().slice(0, 8);
    const line = document.createElement("div");
    line.className = "log-line";
    line.innerHTML = `<span class="log-ts">[${ts}]</span> <span class="log-op ${op}">${op.padEnd(9)}</span> ${escapeHtml(detail)}`;
    el.prepend(line);
  }

  function flashBroker(brokerId) {
    const card = document.querySelector(`[data-broker="${brokerId}"]`);
    if (!card) return;
    card.classList.add("flash");
    setTimeout(() => card.classList.remove("flash"), 450);
  }

  function isAlive(brokerId) {
    const b = state.brokers.find((x) => x.id === brokerId);
    return Boolean(b && b.alive);
  }

  function produceMessage(payload, key) {
    const pid = key ? stableHash(key) % state.partitions.length : state.rrCounter++ % state.partitions.length;
    const part = state.partitions[pid];
    if (!isAlive(part.primary)) {
      addLog("FAILOVER", `PRODUCE failed: primary ${part.primary} for P${pid} is DOWN (no surviving replica).`);
      return;
    }
    const offset = part.log.length;
    part.log.push({ offset, key: key || null, message: payload });
    flashBroker(part.primary);
    addLog("PRODUCE", `topic=orders part=P${pid} offset=${offset} -> ${part.primary} (${key ? "key=" + key : "round-robin"})`);

    if (part.replica && part.replica !== part.primary && isAlive(part.replica)) {
      setTimeout(() => {
        flashBroker(part.replica);
        addLog("REPLICATE", `async copy P${pid}@${offset} ${part.primary} -> ${part.replica}`);
      }, 140);
    }
    renderSim();
  }

  function toggleBroker(brokerId) {
    const broker = state.brokers.find((b) => b.id === brokerId);
    if (!broker) return;
    broker.alive = !broker.alive;

    if (!broker.alive) {
      addLog("FAILOVER", `Broker ${brokerId} missed heartbeats (BROKER_TIMEOUT=10s) — promoting replicas`);
      state.partitions.forEach((p) => {
        if (p.primary === brokerId) {
          if (isAlive(p.replica)) {
            p.primary = p.replica;
            const otherAlive = state.brokers.find((b) => b.alive && b.id !== p.primary);
            p.replica = otherAlive ? otherAlive.id : p.primary;
            addLog("FAILOVER", `Promoted ${p.primary} -> PRIMARY for P${p.id}`);
          } else {
            addLog("FAILOVER", `P${p.id} has no live replica to promote!`);
          }
        }
      });
    } else {
      addLog("REBALANCE", `Broker ${brokerId} re-registered with Coordinator`);
    }
    renderSim();
  }

  function pollAll(commit) {
    let totalFetched = 0;
    Object.entries(state.groups).forEach(([groupName, grp]) => {
      const assign = assignmentsFor(grp);
      Object.entries(assign).forEach(([cid, pids]) => {
        pids.forEach((pid) => {
          const part = state.partitions[pid];
          if (!isAlive(part.primary)) return;
          const start = grp.uncommitted[pid];
          const recs = part.log.slice(start);
          if (recs.length > 0) {
            totalFetched += recs.length;
            grp.received[cid] = (grp.received[cid] || 0) + recs.length;
            if (commit) {
              grp.offsets[pid] = part.log.length;
              grp.uncommitted[pid] = part.log.length;
              addLog("FETCH", `${cid} (${groupName}) fetched ${recs.length} from P${pid} & committed offset=${part.log.length}`);
            } else {
              addLog("FETCH", `${cid} (${groupName}) fetched ${recs.length} from P${pid} (UNCOMMITTED — will redeliver)`);
            }
          }
        });
      });
    });
    if (totalFetched === 0) {
      addLog("FETCH", "All consumer groups are caught up (0 new records).");
    }
    renderSim();
  }

  function addPaymentConsumer() {
    const grp = state.groups["payment-group"];
    if (grp.members.length >= 3) return;
    const nextId = `pay-c${grp.members.length + 1}`;
    grp.members.push(nextId);
    grp.received[nextId] = 0;
    addLog("REBALANCE", `${nextId} joined payment-group -> reassigned partitions`);
    renderSim();
  }

  function removePaymentConsumer() {
    const grp = state.groups["payment-group"];
    if (grp.members.length <= 1) return;
    const removed = grp.members.pop();
    delete grp.received[removed];
    addLog("REBALANCE", `${removed} evicted from payment-group -> reassigned partitions`);
    renderSim();
  }

  function resetSim() {
    state.rrCounter = 0;
    state.orderSeq = 1001;
    state.brokers.forEach((b) => (b.alive = true));
    state.partitions = [
      { id: 0, primary: "broker1", replica: "broker2", log: [] },
      { id: 1, primary: "broker2", replica: "broker3", log: [] },
      { id: 2, primary: "broker3", replica: "broker1", log: [] },
    ];
    state.groups["payment-group"].members = ["pay-c1", "pay-c2"];
    state.groups["payment-group"].offsets = { 0: 0, 1: 0, 2: 0 };
    state.groups["payment-group"].uncommitted = { 0: 0, 1: 0, 2: 0 };
    state.groups["payment-group"].received = { "pay-c1": 0, "pay-c2": 0 };
    state.groups["inventory-group"].offsets = { 0: 0, 1: 0, 2: 0 };
    state.groups["inventory-group"].uncommitted = { 0: 0, 1: 0, 2: 0 };
    state.groups["inventory-group"].received = { "inv-c1": 0 };
    document.getElementById("wire-log").innerHTML = "";
    addLog("REBALANCE", "Cluster reset — 3 brokers online, 3 partitions ready.");
    renderSim();
  }

  function renderSim() {
    const brokerList = document.getElementById("broker-list");
    brokerList.innerHTML = state.brokers
      .map((b) => {
        const primaries = state.partitions.filter((p) => p.primary === b.id);
        const replicas = state.partitions.filter((p) => p.replica === b.id && p.primary !== b.id);
        const chips = [
          ...primaries.map((p) => `<span class="part-chip p">P${p.id} PRIMARY (${p.log.length})</span>`),
          ...replicas.map((p) => `<span class="part-chip r">P${p.id} REPLICA</span>`),
        ].join("");
        return `
          <div class="broker-card ${b.alive ? "alive" : "dead"}" data-broker="${b.id}">
            <div class="broker-top">
              <span class="broker-name">${b.id} :${b.port}</span>
              <div style="display:flex; align-items:center; gap:8px;">
                <span class="status-badge ${b.alive ? "ok" : "dead"}">${b.alive ? "ALIVE" : "DEAD"}</span>
                <button class="btn sm ${b.alive ? "danger" : "success"}" data-toggle-broker="${b.id}">
                  ${b.alive ? "Kill" : "Restart"}
                </button>
              </div>
            </div>
            <div class="part-chips">${chips || '<span class="part-chip">No partitions</span>'}</div>
          </div>`;
      })
      .join("");

    brokerList.querySelectorAll("[data-toggle-broker]").forEach((btn) => {
      btn.addEventListener("click", () => toggleBroker(btn.getAttribute("data-toggle-broker")));
    });

    document.getElementById("partition-summary").innerHTML = state.partitions
      .map((p) => `<span class="part-chip p">orders/P${p.id} · len=${p.log.length} · leader=${p.primary}</span>`)
      .join("");

    const cgEl = document.getElementById("consumer-groups");
    cgEl.innerHTML = Object.entries(state.groups)
      .map(([name, grp]) => {
        const assign = assignmentsFor(grp);
        const rows = grp.members
          .map((cid) => {
            const pids = (assign[cid] || []).map((id) => `P${id}`).join(", ") || "idle";
            return `<div class="consumer-row"><span>${cid} <span style="color:var(--cyan)">[${pids}]</span></span><span style="color:var(--emerald)">${grp.received[cid] || 0} msgs</span></div>`;
          })
          .join("");
        const controls =
          name === "payment-group"
            ? `<div style="display:flex; gap:4px;"><button class="btn sm" id="btn-add-c">+Consumer</button><button class="btn sm danger" id="btn-rem-c">Evict</button></div>`
            : `<span class="status-badge primary">Fan-Out</span>`;
        return `
          <div class="group-box">
            <div class="group-title">
              <span>${name}</span>
              ${controls}
            </div>
            ${rows}
          </div>`;
      })
      .join("");

    const addBtn = document.getElementById("btn-add-c");
    const remBtn = document.getElementById("btn-rem-c");
    if (addBtn) addBtn.addEventListener("click", addPaymentConsumer);
    if (remBtn) remBtn.addEventListener("click", removePaymentConsumer);
  }

  document.getElementById("btn-produce").addEventListener("click", () => {
    const payloadInput = document.getElementById("msg-payload");
    const keyInput = document.getElementById("msg-key");
    const payload = payloadInput.value.trim() || `order_id=ORD-${state.orderSeq}`;
    produceMessage(payload, keyInput.value.trim());
    state.orderSeq++;
    payloadInput.value = `order_id=ORD-${state.orderSeq} product=Headphones price=299`;
  });

  document.getElementById("btn-burst").addEventListener("click", () => {
    const products = ["Laptop", "Phone", "SSD", "Monitor", "Keyboard", "Camera"];
    products.forEach((prod, i) => {
      setTimeout(() => {
        const key = i % 2 === 0 ? `user-${100 + (i % 3)}` : "";
        produceMessage(`order_id=ORD-${state.orderSeq++} product=${prod}`, key);
      }, i * 110);
    });
  });

  document.getElementById("btn-poll-commit").addEventListener("click", () => pollAll(true));
  document.getElementById("btn-poll-nocommit").addEventListener("click", () => pollAll(false));
  document.getElementById("btn-reset").addEventListener("click", resetSim);

  resetSim();

  // ── 2. Wire Protocol Inspector ────────────────────────────────────────────
  const wireInput = document.getElementById("wire-json");
  const wireHex = document.getElementById("wire-hex");
  const wireCount = document.getElementById("wire-byte-count");

  function updateWireFrame() {
    const text = wireInput.value;
    const bytes = new TextEncoder().encode(text);
    const len = bytes.length;
    const header = [
      (len >>> 24) & 0xff,
      (len >>> 16) & 0xff,
      (len >>> 8) & 0xff,
      len & 0xff,
    ]
      .map((b) => b.toString(16).padStart(2, "0"))
      .join(" ");
    const payloadHex = Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join(" ");
    wireCount.textContent = `4 + ${len} = ${len + 4} bytes`;
    wireHex.innerHTML = `<span class="hex-header">${header}</span> <span class="hex-payload">${payloadHex}</span>`;
  }

  wireInput.addEventListener("input", updateWireFrame);
  updateWireFrame();

  // ── 3. Test Suite Matrix ──────────────────────────────────────────────────
  const testCases = [
    { id: "TC-01", name: "Basic Produce & Consume", cat: "Core", count: 3, desc: "Single/multi message delivery and strictly monotone offsets." },
    { id: "TC-02", name: "Multiple Partitions", cat: "Core", count: 2, desc: "Round-robin spread and zero message loss across partitions." },
    { id: "TC-03", name: "Key-Based Routing", cat: "Core", count: 3, desc: "Consistent hash routing and keyed/keyless mixed streams." },
    { id: "TC-04", name: "Consumer Group Sharing", cat: "Groups", count: 3, desc: "Exclusive partition ownership with zero duplicate deliveries." },
    { id: "TC-05", name: "Broadcast Fan-Out", cat: "Groups", count: 2, desc: "Independent consumer groups each receive the complete stream." },
    { id: "TC-06", name: "Offset Commit & Resume", cat: "Core", count: 3, desc: "Checkpointing, mid-batch resume, and uncommitted redelivery." },
    { id: "TC-07", name: "At-Least-Once Guarantee", cat: "Core", count: 2, desc: "100-message load verification with zero dropped records." },
    { id: "TC-08", name: "Per-Partition Ordering", cat: "Core", count: 2, desc: "Monotone offsets and strict key-ordered event streams." },
    { id: "TC-09", name: "Topic Isolation", cat: "Core", count: 2, desc: "Zero cross-contamination across concurrent topics." },
    { id: "TC-10", name: "Concurrent Producers", cat: "Concurrency", count: 2, desc: "2 and 5 parallel producer threads with zero write loss." },
    { id: "TC-11", name: "Concurrent Consumers", cat: "Concurrency", count: 1, desc: "3 consumers polling simultaneously with zero duplicates." },
    { id: "TC-12", name: "Consumer Joins Mid-Stream", cat: "Groups", count: 1, desc: "Live rebalance redistributes partitions without dropping events." },
    { id: "TC-13", name: "Consumer Leaves Mid-Stream", cat: "Groups", count: 1, desc: "Graceful unsubscribe hands partitions to surviving consumers." },
    { id: "TC-14", name: "Large Payloads (64 KB)", cat: "Core", count: 2, desc: "Verifies 4-byte framing across 1 B to 64 KB records." },
    { id: "TC-15", name: "High-Volume Throughput", cat: "Concurrency", count: 2, desc: "1,000-message end-to-end delivery and throughput guard." },
    { id: "TC-16", name: "Idempotent Topic Creation", cat: "Core", count: 2, desc: "Duplicate create_topic calls preserve existing log data." },
    { id: "TC-17", name: "Round-Robin Balance", cat: "Core", count: 1, desc: "Quantitative distribution balance across 3 partitions." },
    { id: "TC-18", name: "Disk Replication Check", cat: "Fault Tolerance", count: 2, desc: "Verifies replica .log files match primary offset sets on disk." },
    { id: "TC-19", name: "Broker Crash Consume Recovery", cat: "Fault Tolerance", count: 1, desc: "Kills primary broker; consumer continues on promoted replica." },
    { id: "TC-20", name: "Broker Crash Producer Reroute", cat: "Fault Tolerance", count: 1, desc: "Producer automatically reroutes to promoted replica after timeout." },
    { id: "TC-21", name: "Multi-Broker Failure", cat: "Fault Tolerance", count: 1, desc: "2 of 3 brokers killed; surviving broker continues serving." },
    { id: "TC-22", name: "Consumer Heartbeat Eviction", cat: "Fault Tolerance", count: 1, desc: "Silent consumer crash triggers timeout eviction & rebalance." },
    { id: "TC-23", name: "Triple Fan-Out", cat: "Groups", count: 1, desc: "3 independent groups with isolated offset tracking." },
    { id: "TC-24", name: "Sequential Batch Polling", cat: "Core", count: 2, desc: "Incremental produce/poll cycles and non-blocking empty polls." },
    { id: "TC-25", name: "Metadata Integrity", cat: "Core", count: 3, desc: "Validates offset, key, message, ts, partition, topic invariants." },
  ];

  const categories = ["All", "Core", "Groups", "Concurrency", "Fault Tolerance"];
  let activeCat = "All";

  function renderTests() {
    document.getElementById("test-filters").innerHTML = categories
      .map((c) => `<button class="filter-btn ${c === activeCat ? "active" : ""}" data-cat="${c}">${c}</button>`)
      .join("");
    document.querySelectorAll("[data-cat]").forEach((btn) => {
      btn.addEventListener("click", () => {
        activeCat = btn.getAttribute("data-cat");
        renderTests();
      });
    });

    const filtered = activeCat === "All" ? testCases : testCases.filter((t) => t.cat === activeCat);
    document.getElementById("test-grid").innerHTML = filtered
      .map(
        (t) => `
        <div class="test-card">
          <div class="test-card-top">
            <span class="tc-id">${t.id}</span>
            <span class="tc-count">${t.count} ${t.count === 1 ? "test" : "tests"}</span>
          </div>
          <h4>${t.name}</h4>
          <p>${t.desc}</p>
        </div>`
      )
      .join("");
  }
  renderTests();

  // ── 4. Code Tabs & Copy ───────────────────────────────────────────────────
  const snippets = {
    "Cluster Setup": `git clone ${REPO_URL}.git
cd streamlog
pip install -e .

# Start 1 Coordinator (:9000) + 3 Brokers (:9001–9003)
bash scripts/start_cluster.sh

# In another terminal, run the e-commerce event demo
python demo/ecommerce_demo.py`,
    "Producer API": `from streamlog import Producer

producer = Producer("127.0.0.1", 9000)
producer.create_topic("orders", num_partitions=3)

# Keyless -> round-robin across partitions
offset = producer.send("orders", "order_id=ORD-1001")

# Keyed -> consistent hash routing (strict per-key ordering)
offset = producer.send("orders", "order_id=ORD-1002", key="user-42")`,
    "Consumer API": `from streamlog import Consumer

with Consumer("127.0.0.1", 9000, consumer_id="worker-1") as consumer:
    consumer.subscribe("orders", group_id="payment-group")

    while True:
        records = consumer.poll(timeout_ms=1000)
        for rec in records:
            print(f"[p={rec['partition']} @{rec['offset']}] {rec['message']}")
            consumer.commit(rec["partition"], rec["offset"] + 1)`,
    "Run Tests": `# Fast integration suite (42 real-process tests)
make test-fast

# Fault-tolerance & failover suite (TC19–TC22)
make test-fault

# Complete 46-test suite
make test`,
  };

  let activeTab = "Cluster Setup";
  function renderCodeTabs() {
    document.getElementById("code-tabs").innerHTML = Object.keys(snippets)
      .map((k) => `<button class="tab-btn ${k === activeTab ? "active" : ""}" data-tab="${k}">${k}</button>`)
      .join("");
    document.getElementById("code-display").textContent = snippets[activeTab];
    document.querySelectorAll("[data-tab]").forEach((btn) => {
      btn.addEventListener("click", () => {
        activeTab = btn.getAttribute("data-tab");
        renderCodeTabs();
      });
    });
  }
  renderCodeTabs();

  document.getElementById("btn-copy-code").addEventListener("click", async () => {
    const btn = document.getElementById("btn-copy-code");
    try {
      await navigator.clipboard.writeText(snippets[activeTab]);
      btn.textContent = "Copied!";
      setTimeout(() => (btn.textContent = "Copy"), 1400);
    } catch {
      btn.textContent = "Select & Copy";
    }
  });

  // ── 5. Chart Lightbox ─────────────────────────────────────────────────────
  const lightbox = document.getElementById("lightbox");
  const lightboxImg = document.getElementById("lightbox-img");
  document.querySelectorAll(".chart-card img").forEach((img) => {
    img.addEventListener("click", () => {
      lightboxImg.src = img.src;
      lightbox.classList.add("open");
    });
  });
  lightbox.addEventListener("click", () => lightbox.classList.remove("open"));
})();
