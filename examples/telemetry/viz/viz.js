// rhapsody live viz — event-driven port of node-slicing.html
//
// Source of truth is a stream of telemetry events ({event_type, event_time,
// task_id, node_id, attributes:{placement, executable, task_type, ...}, ...}),
// delivered either by file-drop (one-shot, sorted-by-event_time replay) or by
// the SSE bridge (server-sent).  See bridge.py.
//
// Rendering is a port of the node-slicing.html aesthetic: dark panels per
// node, GPU + CPU slot grids, tasks fly from a queue strip into a slot, halo
// + pulse during running, fall + fade on completion.  Differences from the
// AmSC slide:
//   - No "kinds" buckets / sliders. Tasks are colored by *size* (#cores +
//     gpu_weight*#gpus), bucketed into four bands.
//   - Topology (node count, cores per node, gpus per node) comes from a
//     ResourceLayout event; the grid resizes to fit.
//   - Placement comes from event.attributes.placement when present; missing
//     placement falls back to a stable per-task synthetic slot, so the viz
//     still works for backends that don't report it yet (e.g. Dragon V3).

(() => {

  // -------------------------------------------------------------------------
  //  Stage + palette
  // -------------------------------------------------------------------------
  const W = 1280, H = 720;
  const FONT      = "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
  const FONT_MONO = "ui-monospace, 'SF Mono', Menlo, Consolas, monospace";

  const COLORS = {
    bg:           '#080818',
    panel:        '#0d1525',
    frame_border: '#3865a0',   // brighter slate-blue for node panels
    frame_label:  '#94a3b8',
    text:         '#e6ecf6',
    text_dim:     '#7080a0',
    text_label:   '#a0b0c8',
    unused:       '#1f2c40',   // lighter empty-slot fill so the grid is visible
    unused_brd:   '#465670',   // lighter empty-slot border
    queue_brd:    '#506880',   // brighter queue panel border
  };

  // Size buckets — chosen so the four typical workload sizes
  // (1c/0g, 4c/1g, 16c/2g, 32c/4g) land in distinct bands.
  // weight = cores + GPU_WEIGHT * gpus.  GPU is weighted heavier because
  // a single GPU is worth more than a single core both in compute and in
  // visual significance.  Tune via the constants below.
  const GPU_WEIGHT = 8;
  const SIZE_BUCKETS = [
    { max:   2, color: '#5acd83', dim: '#1a4028', name: 'tiny'   },  // green
    { max:  12, color: '#40c8e8', dim: '#15384a', name: 'small'  },  // cyan
    { max:  30, color: '#e8a040', dim: '#4a3418', name: 'medium' },  // amber
    { max: 1e9, color: '#e84070', dim: '#4a182a', name: 'large'  },  // magenta
  ];

  function sizeWeight(p) {
    const c = (p && (p.cores ?? (p.core_ids ? p.core_ids.length : 0))) || 0;
    const g = (p && (p.gpu_ids ? p.gpu_ids.length : 0)) || 0;
    return c + GPU_WEIGHT * g;
  }
  function bucketFor(weight) {
    for (const b of SIZE_BUCKETS) if (weight <= b.max) return b;
    return SIZE_BUCKETS[SIZE_BUCKETS.length - 1];
  }

  // -------------------------------------------------------------------------
  //  Topology + layout
  //
  //  The ResourceLayout event sets {nodes, coresPerNode, gpusPerNode}.
  //  Layout (panel sizes, slot tile sizes) is recomputed every time
  //  topology changes — same code path handles 1×1 and 64×128.
  // -------------------------------------------------------------------------
  const M             = 20;
  const TITLE_H       = 46;
  const QUEUE_Y       = TITLE_H + 8;
  const QUEUE_H       = 64;
  const NODES_Y       = QUEUE_Y + QUEUE_H + 18;
  const NODES_AREA_H  = H - NODES_Y - M;

  // Topology defaults — overwritten by ResourceLayout when received.
  let nodes = [];
  let coresPerNode = 32;
  let gpusPerNode  = 4;

  // Slot tile size — adapts so the entire grid fits a node panel.
  // Recomputed in relayout().
  let nodeW = 0, nodeH = 0, nodeCols = 0, nodeRows = 0;
  let coreCols = 0, coreSize = 0, gpuSize = 0;

  function relayout() {
    // Grid bands: scale panels and tiles together by stepping through
    // fixed grid shapes.  A single node sits at the top-left of a 2×2
    // grid (panel = ½W × ½H of the node area).  Going from 2×2 → 4×2 →
    // 4×4 halves panel dimensions each step, and tile dimensions scale
    // with the panel since they're computed from innerW/innerH below.
    //
    //   N        grid     panel size (approx)
    //   0 (placeholder) ½  ×  ½
    //   1..4   → 2 × 2   ½  ×  ½
    //   5..8   → 4 × 2   ¼  ×  ½
    //   9..16  → 4 × 4   ¼  ×  ¼
    //   17+    → 4 × ⌈N/4⌉  (further shrink, still 4 cols)
    //
    // Cells beyond ``nodes.length`` are left empty (row-major fill).
    const N = Math.max(1, nodes.length);   // 0 nodes → 1 placeholder cell
    if      (N <=  4) { nodeCols = 2; nodeRows = 2; }
    else if (N <=  8) { nodeCols = 4; nodeRows = 2; }
    else if (N <= 16) { nodeCols = 4; nodeRows = 4; }
    else              { nodeCols = 4; nodeRows = Math.ceil(N / 4); }

    const NODE_GAP = 12;
    nodeW = (W - 2 * M - (nodeCols - 1) * NODE_GAP) / nodeCols;
    nodeH = (NODES_AREA_H - (nodeRows - 1) * NODE_GAP) / nodeRows;

    // CPU grid: prefer a wide rectangle (cols ≈ 2 * rows).  Layout is
    // fixed by ``coresPerNode``; tile size is what changes with panel.
    coreCols = Math.max(1, Math.min(coresPerNode, Math.round(Math.sqrt(coresPerNode * 2.4))));
    const coreRows = Math.ceil(coresPerNode / coreCols);

    // Header band reserved at the top of each node panel.
    const headerH = 26;
    const innerW = nodeW - 16;
    const innerH = nodeH - headerH - 14;

    // Two stacked groups inside the node: gpus row, then cores grid.
    // Reserve roughly 32% of the inner height for GPUs (or auto if 0 gpus).
    const gpusHFrac = gpusPerNode > 0 ? 0.32 : 0;
    const gpuBandH  = Math.floor(innerH * gpusHFrac);
    const cpuBandH  = innerH - gpuBandH - (gpusPerNode > 0 ? 10 : 0);

    // No upper cap on tile size — we want tiles to grow when the panel
    // grows (single-node case) and shrink when the panel shrinks.
    if (gpusPerNode > 0) {
      const gPad = 6;
      const gw = (innerW - (gpusPerNode - 1) * gPad) / gpusPerNode;
      gpuSize = Math.max(2, Math.min(gpuBandH, gw));
    } else {
      gpuSize = 0;
    }

    const cPad = 2;
    const cw = (innerW - (coreCols - 1) * cPad) / coreCols;
    const ch = (cpuBandH - (coreRows - 1) * cPad) / coreRows;
    coreSize = Math.max(2, Math.min(cw, ch));

    // Recompute slot positions on every node.
    const NODE_GAP_ = NODE_GAP;
    for (let i = 0; i < nodes.length; i++) {
      const nd = nodes[i];
      const col = i % nodeCols;
      const row = Math.floor(i / nodeCols);
      nd.x = M + col * (nodeW + NODE_GAP_);
      nd.y = NODES_Y + row * (nodeH + NODE_GAP_);
      nd.w = nodeW; nd.h = nodeH;

      // Compute slot positions.
      const gpuRowW = gpusPerNode * gpuSize + (gpusPerNode - 1) * 6;
      const gpuX0   = nd.x + (nd.w - gpuRowW) / 2;
      const gpuY    = nd.y + headerH;
      nd.gpu = [];
      for (let g = 0; g < gpusPerNode; g++) {
        nd.gpu.push({
          type: 'gpu', id: g,
          x: gpuX0 + g * (gpuSize + 6), y: gpuY,
          w: gpuSize, h: gpuSize,
          busy: false, task: null,
        });
      }

      const coreRowW = coreCols * coreSize + (coreCols - 1) * cPad;
      const cpuX0 = nd.x + (nd.w - coreRowW) / 2;
      const cpuY  = gpuY + (gpusPerNode > 0 ? gpuSize + 10 : 0);
      nd.cpu = [];
      for (let c = 0; c < coresPerNode; c++) {
        const cc = c % coreCols;
        const cr = Math.floor(c / coreCols);
        nd.cpu.push({
          type: 'cpu', id: c,
          x: cpuX0 + cc * (coreSize + cPad),
          y: cpuY  + cr * (coreSize + cPad),
          w: coreSize, h: coreSize,
          busy: false, task: null,
        });
      }
    }
  }

  function setTopology(layoutNodes) {
    if (!layoutNodes || !layoutNodes.length) return;
    coresPerNode = Math.max(1, layoutNodes[0].cores || 1);
    gpusPerNode  = Math.max(0, layoutNodes[0].gpus  || 0);
    nodes = layoutNodes.map((n, i) => ({
      uid: n.id || `node-${i}`,
      gpu: [], cpu: [],
    }));
    relayout();
  }

  // -------------------------------------------------------------------------
  //  Animation tuneables.
  //
  //  Per-slot phase machine:
  //
  //                                 (TaskStarted)
  //               .---------------------+
  //              v                      |
  //          [ idle ] <-(fade_out done)-+
  //              |                      |
  //         fade_in (100 ms)            |
  //              |                      |
  //          [ running ] <--- ............
  //              |                      |
  //          (TaskCompleted)            |
  //              |                      |
  //          fade_out (100 ms)----------'
  //
  //  Interruptions:
  //    fade_in + TaskCompleted  -> flash_full (50 ms) -> fade_out
  //    fade_out + TaskStarted   -> flash_empty (50 ms) -> fade_in (new task)
  //
  //  Backlog events ({snap: true}) bypass animation and set the slot
  //  directly to its end state (running or idle).
  // -------------------------------------------------------------------------
  const FADE_IN_DURATION  = 0.10;
  const FADE_OUT_DURATION = 0.10;
  const FLASH_DURATION    = 0.05;
  const GLOW_DURATION     = 0.5;   // on-landing halo (decays from fade-in start)

  // -------------------------------------------------------------------------
  //  State
  // -------------------------------------------------------------------------
  let playing = true;
  let speed   = 1.0;
  let simTime = 0;          // event-stream clock; advances at dt * speed
                            //   - in replay mode this paces the JSONL feed
                            //     AND drives the on-screen ``t = `` readout
                            //   - in live mode it has no effect
  let animTime = 0;         // wall-clock animation clock; advances at dt
                            //   (NOT speed-scaled — flight/fade transitions
                            //    must stay visible no matter the slider).
                            //   Drives the ``t = `` readout in live mode.
  let firstEventTime = null;
  let connectionLabel = 'idle';
  // ``live`` or ``replay``; controls whether the speed slider is shown
  // and which clock the ``t = `` readout follows.  Defaults to replay so
  // a freshly-loaded page (drag-and-drop) sees the slider; switched to
  // ``live`` on /?live URL or any /live SSE connect.
  let mode = 'replay';

  // ``idle`` while a session is active (or before anything has arrived);
  // ``completed`` once SessionEnded / bridge shutdown lands.  When
  // ``completed``, the time counter freezes and node panels show
  // ``completed`` instead of the node uid.  Topology and resource
  // panels stay on screen — only the task tiles are cleared.
  let viewState = 'idle';

  // Tasks indexed by task_id — created on TaskCreated/Submitted/Started.
  // We don't need per-slot flying/fading arrays anymore: each slot tracks
  // its own animation phase below.
  const tasks = new Map();

  // Per-node free-slot tracking.  Synthetic placement falls back here.
  function pickFreeSlot(nodeIdx, kind, hint) {
    const nd = nodes[nodeIdx];
    if (!nd) return null;
    const pool = kind === 'gpu' ? nd.gpu : nd.cpu;
    if (hint != null && hint >= 0 && hint < pool.length && !pool[hint].busy) return pool[hint];
    for (const s of pool) if (!s.busy) return s;
    return null;
  }

  function findNodeIndex(nodeId) {
    if (nodeId == null) return -1;
    for (let i = 0; i < nodes.length; i++) if (nodes[i].uid === nodeId) return i;
    return -1;
  }

  // Stable hash for synthetic placement.
  function hashStr(s) {
    let h = 2166136261;
    for (let i = 0; i < s.length; i++) {
      h ^= s.charCodeAt(i);
      h = (h * 16777619) >>> 0;
    }
    return h >>> 0;
  }

  // -------------------------------------------------------------------------
  //  Event handling
  //
  //  Events arrive with `event_time` in seconds (wall clock).  We rebase
  //  the first event to simTime=0 so the viz keeps running with sensible
  //  numbers regardless of the absolute timestamp.
  //
  //  We also keep a small *queue strip* at the top: each task being
  //  prepared (Created/Submitted) becomes a queue tile.  TaskStarted
  //  pulls it out of the queue and flies it to its slot.
  // -------------------------------------------------------------------------
  function relTime(t) {
    if (firstEventTime == null) firstEventTime = t;
    return t - firstEventTime;
  }

  // ---- Slot phase machine -------------------------------------------------

  function acquireSlot(s, taskInfo, snap) {
    // Called when a TaskStarted event places ``taskInfo`` on slot ``s``.
    // ``snap=true`` means the event is from the backlog replay — set the
    // slot directly to ``running`` without animation.
    if (snap) {
      s.task      = taskInfo;
      s.phase     = 'running';
      s.phaseStart = animTime;
      s.haloStart  = -Infinity;       // no halo for snapped slots
      s.pendingTask = null;
      return;
    }
    const phase = s.phase || 'idle';
    if (phase === 'fade_out') {
      // Slot is still draining the previous tile — flash empty briefly
      // so the changeover is perceptible, then fade the new task in.
      s.pendingTask = taskInfo;
      s.phase       = 'flash_empty';
      s.phaseStart  = animTime;
      // task is left untouched; flash_empty paints idle regardless.
      return;
    }
    // idle (typical) or any other transient state — start fade-in fresh.
    s.task        = taskInfo;
    s.phase       = 'fade_in';
    s.phaseStart  = animTime;
    s.haloStart   = animTime;          // halo aligned with fade-in start
    s.pendingTask = null;
  }

  function releaseSlot(s, snap) {
    // Called when a terminal event arrives for the task on slot ``s``.
    if (snap) {
      s.task      = null;
      s.pendingTask = null;
      s.phase     = 'idle';
      s.phaseStart = animTime;
      s.busy      = false;
      return;
    }
    const phase = s.phase || 'idle';
    if (phase === 'fade_in') {
      // Very short task — completion arrived before the fade-in finished.
      // Flash full color so something is perceptible, then fade out.
      s.phase      = 'flash_full';
      s.phaseStart = animTime;
      return;
    }
    if (phase === 'flash_empty') {
      // The pending new task is being killed before it ever fades in.
      // Drop the pending task and fade the (still-mostly-visible) old
      // task out normally.
      s.pendingTask = null;
      s.phase       = 'fade_out';
      s.phaseStart  = animTime;
      return;
    }
    if (phase === 'fade_out' || phase === 'flash_full') {
      // Already going down — nothing to do.
      return;
    }
    // Normal completion from ``running`` (or any other live phase).
    s.phase      = 'fade_out';
    s.phaseStart = animTime;
  }

  function onEvent(ev, snap) {
    const t = relTime(typeof ev.event_time === 'number' ? ev.event_time : Date.now() / 1000);
    switch (ev.event_type) {
      case 'ResourceLayout': {
        const nl = ev.attributes && ev.attributes.nodes;
        if (Array.isArray(nl) && nl.length) setTopology(nl);
        break;
      }
      case 'TaskCreated':
      case 'TaskSubmitted': {
        if (!ev.task_id) break;
        let tk = tasks.get(ev.task_id);
        if (!tk) {
          tk = {
            uid: ev.task_id,
            state: 'queued',
            createdAt: t,
            endedAt: null,
            placement: null,
            bucket: SIZE_BUCKETS[0],
            slots: [],
          };
          tasks.set(ev.task_id, tk);
        }
        break;
      }
      case 'TaskStarted': {
        if (!ev.task_id) break;
        const tk = tasks.get(ev.task_id) || { uid: ev.task_id, slots: [] };
        if (!tasks.has(ev.task_id)) tasks.set(ev.task_id, tk);

        const placement = (ev.attributes && ev.attributes.placement) || null;
        const nodeId    = (placement && placement.node_id) || ev.node_id;
        tk.placement = placement;
        tk.bucket    = bucketFor(sizeWeight(placement || {}));
        tk.state     = 'running';

        // Resolve node — fall back to synthetic if unknown.
        let nodeIdx = findNodeIndex(nodeId);
        if (nodeIdx < 0) {
          if (nodes.length === 0) break;  // no topology yet
          nodeIdx = hashStr(ev.task_id) % nodes.length;
        }

        // Determine number of CPU + GPU slots to occupy.
        const ncpu = Math.min(
          (placement && (placement.cores ?? (placement.core_ids ? placement.core_ids.length : 0))) || 1,
          coresPerNode,
        );
        const ngpu = Math.min(
          (placement && (placement.gpu_ids ? placement.gpu_ids.length : 0)) || 0,
          gpusPerNode,
        );

        // Find ncpu free cores + ngpu free gpus on the chosen node.
        // If not enough are free, spill into the next nodes (round-robin).
        // ``slotIsFree`` here means "not held by another task" — a slot
        // mid-fade-out is still considered held; the new TaskStarted on
        // it will be handled by acquireSlot() below as a reuse.
        const acquired = [];
        let scan = nodeIdx;
        let needCpu = ncpu;
        let needGpu = ngpu;
        let safety = nodes.length;
        while ((needCpu > 0 || needGpu > 0) && safety-- > 0) {
          const nd = nodes[scan];
          if (nd) {
            while (needGpu > 0) {
              const hintGpu = (placement && placement.gpu_ids && placement.gpu_ids[ngpu - needGpu]);
              const s = pickFreeSlot(scan, 'gpu', hintGpu);
              if (!s) break;
              s.busy = true;
              acquired.push(s);
              needGpu--;
            }
            while (needCpu > 0) {
              const hintCpu = (placement && placement.core_ids && placement.core_ids[ncpu - needCpu]);
              const s = pickFreeSlot(scan, 'cpu', hintCpu);
              if (!s) break;
              s.busy = true;
              acquired.push(s);
              needCpu--;
            }
          }
          scan = (scan + 1) % nodes.length;
        }
        if (acquired.length === 0) break;

        tk.slots = acquired;
        for (const s of acquired) {
          acquireSlot(s, { uid: tk.uid, bucket: tk.bucket }, snap);
        }
        break;
      }
      case 'TaskCompleted':
      case 'TaskFailed':
      case 'TaskCanceled': {
        if (!ev.task_id) break;
        const tk = tasks.get(ev.task_id);
        if (!tk) break;
        tk.state   = ev.event_type === 'TaskCompleted' ? 'done' :
                     ev.event_type === 'TaskFailed' ? 'failed' : 'canceled';
        tk.endedAt = t;
        for (const s of tk.slots || []) {
          if (!(s && s.task && s.task.uid === tk.uid)) continue;
          releaseSlot(s, snap);
        }
        tk.slots = [];
        break;
      }
      case 'SessionEnded': {
        setStatus('session ended');
        markCompleted();
        break;
      }
      case 'SessionStarted': {
        setStatus('session running');
        // Roll out of any prior "completed" state — e.g. when a new
        // session starts against the same long-lived bridge.
        viewState = 'idle';
        break;
      }
      default:
        // Unknown event types are simply ignored — RHAPSODY allows
        // user-defined events via define_event(), and the viewer should
        // gracefully step over anything it doesn't understand.
        break;
    }
  }

  // -------------------------------------------------------------------------
  //  Tick + render
  // -------------------------------------------------------------------------
  function markCompleted() {
    // Session is done — snap every slot to idle (no lingering fades)
    // and flip the global state so the time counter freezes and the
    // panel labels switch to "completed".  Topology stays intact.
    viewState = 'completed';
    for (const nd of nodes) {
      for (const s of nd.gpu) snapSlotIdle(s);
      for (const s of nd.cpu) snapSlotIdle(s);
    }
  }

  function snapSlotIdle(s) {
    s.phase       = 'idle';
    s.phaseStart  = animTime;
    s.task        = null;
    s.pendingTask = null;
    s.busy        = false;
  }

  function tick(dtRaw, dtSim) {
    // While ``completed`` the time counter freezes and no animations
    // tick — everything stays on the canvas exactly as the session
    // left it.  A new SessionStarted clears this state.
    if (viewState === 'completed') return;
    // Two clocks advance in step but at different rates: animTime is the
    // wall-clock baseline for tweens, simTime is speed-scaled event time.
    animTime += dtRaw;
    simTime  += dtSim;
    // Advance each slot's phase machine.
    for (const nd of nodes) {
      for (const s of nd.gpu) advanceSlotPhase(s);
      for (const s of nd.cpu) advanceSlotPhase(s);
    }
    // Tasks: GC very-old terminal ones to keep the map bounded.
    if (tasks.size > 5000) {
      const cutoff = simTime - 120.0;
      for (const [uid, tk] of tasks) {
        if (tk.endedAt != null && tk.endedAt < cutoff) tasks.delete(uid);
      }
    }
  }

  function advanceSlotPhase(s) {
    const phase = s.phase;
    if (!phase || phase === 'idle' || phase === 'running') return;
    const elapsed = animTime - s.phaseStart;
    switch (phase) {
      case 'fade_in':
        if (elapsed >= FADE_IN_DURATION) {
          s.phase      = 'running';
          s.phaseStart = animTime;
        }
        break;
      case 'fade_out':
        if (elapsed >= FADE_OUT_DURATION) {
          s.phase      = 'idle';
          s.phaseStart = animTime;
          s.task       = null;
          s.busy       = false;
        }
        break;
      case 'flash_full':
        if (elapsed >= FLASH_DURATION) {
          s.phase      = 'fade_out';
          s.phaseStart = animTime;
        }
        break;
      case 'flash_empty':
        if (elapsed >= FLASH_DURATION) {
          // Swap to the pending new task and start its fade-in.
          s.task        = s.pendingTask;
          s.pendingTask = null;
          s.phase       = 'fade_in';
          s.phaseStart  = animTime;
          s.haloStart   = animTime;
        }
        break;
    }
  }

  const cv  = document.getElementById('cv');
  const ctx = cv.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  cv.width        = W * dpr;
  cv.height       = H * dpr;
  cv.style.width  = W + 'px';
  cv.style.height = H + 'px';
  ctx.scale(dpr, dpr);

  function rr(x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.arcTo(x + w, y, x + w, y + h, r);
    ctx.arcTo(x + w, y + h, x,     y + h, r);
    ctx.arcTo(x,     y + h, x,     y,     r);
    ctx.arcTo(x,     y,     x + w, y,     r);
    ctx.closePath();
  }
  function drawSpaced(text, x, y, spacing) {
    let cur = x;
    for (const ch of text) {
      ctx.fillText(ch, cur, y);
      cur += ctx.measureText(ch).width + spacing;
    }
  }
  function panel(x, y, w, h, borderColor, label, labelColor) {
    ctx.fillStyle = COLORS.panel;
    rr(x, y, w, h, 8);
    ctx.fill();
    ctx.strokeStyle = borderColor;
    ctx.lineWidth   = 1.5;
    rr(x + 0.5, y + 0.5, w - 1, h - 1, 8);
    ctx.stroke();
    if (label) {
      ctx.fillStyle    = labelColor || borderColor;
      ctx.font         = `600 11px ${FONT}`;
      ctx.textBaseline = 'top';
      ctx.textAlign    = 'left';
      drawSpaced(label.toUpperCase(), x + 14, y + 12, 0.8);
    }
  }
  function drawTile(x, y, w, h, color, alpha) {
    if (alpha !== undefined && alpha !== 1) ctx.globalAlpha = alpha;
    ctx.fillStyle = color;
    rr(x, y, w, h, 2);
    ctx.fill();
    if (alpha !== undefined && alpha !== 1) ctx.globalAlpha = 1;
  }

  function drawTitle() {
    ctx.fillStyle = '#40c8e8';
    ctx.beginPath();
    ctx.arc(M + 4, 26, 4, 0, Math.PI * 2);
    ctx.fill();

    ctx.fillStyle    = COLORS.text;
    ctx.font         = `600 18px ${FONT}`;
    ctx.textBaseline = 'middle';
    ctx.textAlign    = 'left';
    drawSpaced('RHAPSODY LIVE', M + 18, 26, 1.0);

    ctx.fillStyle    = COLORS.text_dim;
    ctx.font         = `400 12px ${FONT_MONO}`;
    ctx.textAlign    = 'right';
    const inflight = countRunning();
    // Live mode → wall-clock seconds since viewer started (animTime).
    // Replay mode → simTime, which advances at ``speed × dt`` so the
    // readout matches the playback pace the slider is dialed to.
    const t = (mode === 'live') ? animTime : simTime;
    ctx.fillText(`t = ${t.toFixed(1)} s   running = ${inflight}   tasks = ${tasks.size}`, W - M, 26);
  }
  function countRunning() {
    let c = 0;
    for (const tk of tasks.values()) if (tk.state === 'running') c++;
    return c;
  }

  function queueRect() {
    return { x: M, y: QUEUE_Y, w: W - 2*M, h: QUEUE_H };
  }

  function drawQueue() {
    // Empty queue panel — kept as a placeholder for upcoming queue-related
    // UI (size legend lives here; pending-task tiles were removed since
    // the fade-in animation now happens directly on the destination slot).
    const r = queueRect();
    // Reuse the queue panel as the session-state banner: it reads
    // ``queue`` while the session is active, ``completed`` once it ends.
    const queueLabel = viewState === 'completed' ? 'completed' : 'queue';
    panel(r.x, r.y, r.w, r.h, COLORS.queue_brd, queueLabel, COLORS.frame_label);

    // Legend on the right — size buckets.
    const legendX = r.x + r.w - 320;
    const legendY = r.y + 14;
    ctx.font = `400 10px ${FONT_MONO}`;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    SIZE_BUCKETS.forEach((b, i) => {
      const x = legendX + i * 78;
      drawTile(x, legendY - 4, 8, 8, b.color, 1);
      ctx.fillStyle = COLORS.text_dim;
      ctx.fillText(b.name, x + 14, legendY);
    });
  }

  function drawNodes() {
    if (nodes.length === 0) {
      // No topology yet — show a single bare panel where the first node
      // of the 2×2 layout would be.  No slot grid: we don't know yet
      // how many cores/gpus the real node will have.
      const label = viewState === 'completed' ? 'completed' : 'pending';
      panel(M, NODES_Y, nodeW, nodeH, COLORS.frame_border, label, COLORS.frame_label);
      return;
    }
    for (let i = 0; i < nodes.length; i++) {
      const nd = nodes[i];
      panel(nd.x, nd.y, nd.w, nd.h, COLORS.frame_border, nd.uid, COLORS.frame_label);
      for (const s of nd.gpu) drawSlot(s);
      for (const s of nd.cpu) drawSlot(s);
    }
  }

  function drawIdleSlot(s) {
    ctx.fillStyle = COLORS.unused;
    rr(s.x, s.y, s.w, s.h, 2);
    ctx.fill();
    ctx.strokeStyle = COLORS.unused_brd;
    ctx.lineWidth   = 0.5;
    rr(s.x + 0.5, s.y + 0.5, s.w - 1, s.h - 1, 2);
    ctx.stroke();
  }

  function drawTaskOverlay(s, bucket, alpha) {
    if (alpha <= 0) return;
    // Halo: glow that decays from haloStart over GLOW_DURATION.  It's a
    // shadow-painted double-fill so the glow extends beyond the slot
    // without growing the slot itself.
    const haloElapsed = animTime - (s.haloStart ?? -Infinity);
    if (haloElapsed >= 0 && haloElapsed < GLOW_DURATION) {
      const g = 1 - haloElapsed / GLOW_DURATION;
      ctx.save();
      ctx.shadowBlur  = 18 * g;
      ctx.shadowColor = bucket.color;
      ctx.globalAlpha = 0.9 * alpha;
      ctx.fillStyle   = bucket.color;
      rr(s.x, s.y, s.w, s.h, 2);
      ctx.fill();
      ctx.fill();
      ctx.restore();
    }
    if (alpha < 1) ctx.globalAlpha = alpha;
    ctx.fillStyle = bucket.color;
    rr(s.x, s.y, s.w, s.h, 2);
    ctx.fill();
    if (alpha < 1) ctx.globalAlpha = 1;
  }

  function drawSlot(s) {
    const phase = s.phase || 'idle';
    if (phase === 'idle' || phase === 'flash_empty' || !s.task) {
      drawIdleSlot(s);
      return;
    }
    const bucket = s.task.bucket || SIZE_BUCKETS[0];
    switch (phase) {
      case 'fade_in': {
        drawIdleSlot(s);
        const t = Math.min(1, (animTime - s.phaseStart) / FADE_IN_DURATION);
        drawTaskOverlay(s, bucket, t);
        break;
      }
      case 'running':
        drawTaskOverlay(s, bucket, 1);
        break;
      case 'fade_out': {
        drawIdleSlot(s);
        const t = Math.min(1, (animTime - s.phaseStart) / FADE_OUT_DURATION);
        drawTaskOverlay(s, bucket, 1 - t);
        break;
      }
      case 'flash_full':
        drawTaskOverlay(s, bucket, 1);
        break;
      default:
        drawIdleSlot(s);
    }
  }

  function render() {
    ctx.fillStyle = COLORS.bg;
    ctx.fillRect(0, 0, W, H);
    drawTitle();
    drawQueue();
    drawNodes();
  }

  // -------------------------------------------------------------------------
  //  Driver loop — wall-clock dt, scaled by `speed`.
  // -------------------------------------------------------------------------
  let last = performance.now();
  function frame() {
    const now = performance.now();
    const dt  = Math.min(0.1, (now - last) / 1000);
    last      = now;
    if (playing) tick(dt, dt * speed);
    render();
    requestAnimationFrame(frame);
  }

  // -------------------------------------------------------------------------
  //  Event sources
  //
  //  We support three flavors: drag-and-drop / file picker (one-shot
  //  replay from a local JSONL); SSE /replay; SSE /live.  The URL query
  //  string drives the auto-connect — `?replay=path` or `?live=path`.
  // -------------------------------------------------------------------------
  function setStatus(s) {
    connectionLabel = s;
    const el = document.getElementById('status');
    if (el) el.textContent = s;
  }

  function setMode(m) {
    mode = m;
    // Replay-only controls — hidden in live mode where they'd have no
    // effect.  Pause would stall the live feed; the file picker would
    // be confusing; the speed slider only paces a JSONL replay.
    const live      = (m === 'live');
    const speedctl  = document.getElementById('speedctl');
    const playBtn   = document.getElementById('play');
    const filePick  = document.getElementById('file');
    if (speedctl) speedctl.style.display = live ? 'none' : 'inline-flex';
    if (playBtn)  playBtn.style.display  = live ? 'none' : 'inline-block';
    if (filePick) filePick.style.display = live ? 'none' : 'inline-block';
  }

  function loadJsonlText(text) {
    // Parse all lines, sort by event_time, replay back-to-back via setInterval
    // so the simTime clock advances normally; the user can scale with `speed`.
    setMode('replay');
    const events = [];
    for (const line of text.split(/\r?\n/)) {
      const s = line.trim();
      if (!s) continue;
      try { events.push(JSON.parse(s)); } catch (_) {}
    }
    events.sort((a, b) => (a.event_time || 0) - (b.event_time || 0));
    if (!events.length) { setStatus('empty file'); return; }
    setStatus(`replaying ${events.length} events`);
    firstEventTime = null;
    simTime  = 0;
    animTime = 0;
    // Apply ResourceLayout immediately so the node grid is right from frame 0.
    for (const e of events) if (e.event_type === 'ResourceLayout') onEvent(e);

    // Re-emit in event_time order, mapped to simTime via the same clock.
    const t0 = events[0].event_time || 0;
    let idx = 0;
    function pump() {
      // Pump as many events as fit before the current simTime (rebased).
      while (idx < events.length) {
        const ev = events[idx];
        const rel = (ev.event_time || 0) - t0;
        if (rel > simTime) return;
        if (ev.event_type !== 'ResourceLayout') onEvent(ev);
        idx++;
      }
      setStatus(`replay done — ${events.length} events`);
    }
    // Hook pump into the animation loop.  Cheaper than setInterval.
    function tickAndPump() {
      pump();
      if (idx < events.length) requestAnimationFrame(tickAndPump);
    }
    requestAnimationFrame(tickAndPump);
  }

  function connectSSE(url, label) {
    setStatus(label);
    // ``/live`` = subscriber feed (no slider).  ``/replay`` = paced file
    // dump (slider shown).
    setMode(url.startsWith('/live') ? 'live' : 'replay');
    const es = new EventSource(url);
    // Default ``message`` event = live stream.  Animate normally.
    es.onmessage = (msg) => {
      try {
        const ev = JSON.parse(msg.data);
        onEvent(ev, false);
      } catch (_) { /* skip malformed line */ }
    };
    // Named ``backlog`` event = catch-up replay before live takes over.
    // Snap the slot state to its current value (no fade animation) so a
    // late-joining viewer doesn't play a misleading entry animation for
    // tasks that have been running for minutes.
    es.addEventListener('backlog', (msg) => {
      try {
        const ev = JSON.parse(msg.data);
        onEvent(ev, true);
      } catch (_) { /* skip malformed line */ }
    });
    es.addEventListener('eof', () => setStatus(label + ' — eof'));
    // Bridge is shutting down: switch the viewer to its "completed"
    // state.  Topology is preserved; the queue label flips to
    // "completed", slot tiles clear, the time counter freezes.
    // We deliberately do NOT touch the status pill here — the bridge's
    // lifecycle is an implementation detail; the user-visible cue is
    // the session-level ``session ended`` set by the SessionEnded
    // handler.  ``es.close()`` stops the auto-reconnect.
    es.addEventListener('shutdown', () => {
      markCompleted();
      es.close();
    });
    es.onerror = () => setStatus(label + ' — disconnected');
  }

  // Hook up file picker.
  const fileEl = document.getElementById('file');
  if (fileEl) {
    fileEl.addEventListener('change', (e) => {
      const f = e.target.files && e.target.files[0];
      if (!f) return;
      const r = new FileReader();
      r.onload = () => loadJsonlText(String(r.result || ''));
      r.readAsText(f);
    });
  }
  // Drag-and-drop on the canvas.
  cv.addEventListener('dragover', (e) => { e.preventDefault(); });
  cv.addEventListener('drop', (e) => {
    e.preventDefault();
    const f = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
    if (!f) return;
    const r = new FileReader();
    r.onload = () => loadJsonlText(String(r.result || ''));
    r.readAsText(f);
  });

  // Auto-connect from URL params.
  const params = new URLSearchParams(window.location.search);
  if (params.get('replay')) {
    connectSSE('/replay?file=' + encodeURIComponent(params.get('replay')),
               'replay: ' + params.get('replay'));
  } else if (params.has('live')) {
    // In-process live feed served by LiveVizBridge — no file path needed.
    connectSSE('/live', 'live');
  } else {
    setStatus('drop a JSONL on the canvas, or use ?replay= / ?live=');
  }

  // Controls.
  const $play = document.getElementById('play');
  const $sp   = document.getElementById('speed');
  const $spv  = document.getElementById('speedval');
  $play.onclick = () => {
    playing = !playing;
    $play.innerHTML = playing ? '&#10074;&#10074; pause' : '&#9654; play';
  };
  $sp.oninput = (e) => {
    speed = parseFloat(e.target.value);
    $spv.textContent = speed.toFixed(2) + '×';
  };
  $spv.textContent = speed.toFixed(2) + '×';

  // No initial topology — keep ``nodes`` empty.  drawNodes() renders a
  // single "pending" placeholder panel in the top-left ½×½ quadrant
  // until the first ResourceLayout event arrives.
  relayout();

  requestAnimationFrame(frame);

})();
