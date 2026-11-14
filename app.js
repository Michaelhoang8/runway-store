const $ = (id) => document.getElementById(id);
let state,
  busy = false,
  knownRuns = new Set();
function el(tag, text, className) {
  const n = document.createElement(tag);
  n.textContent = text;
  if (className) n.className = className;
  return n;
}
function message(text, error = false) {
  $("message").textContent = text;
  $("message").classList.toggle("error", error);
  $("write-message").textContent = text;
  $("write-message").classList.toggle("error", error);
}
async function api(path, body) {
  const response = await fetch(
    path,
    body === undefined
      ? {}
      : {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
  );
  const data = await response.json();
  if (!response.ok) throw Error(data.error || "Request failed.");
  return data;
}
function render() {
  $("stats").replaceChildren(
    ...[
      ["Sequence", state.sequence],
      ["Manifest generation", state.generation],
      ["Live runs", state.runs.length],
      ["WAL bytes", state.walBytes],
    ].map(([label, value]) => {
      const p = el("p", label);
      p.prepend(el("b", String(value)));
      return p;
    }),
  );
  $("memtable").replaceChildren(
    ...state.memtable.map((r) => {
      const n = el("div", "", `entry ${r.value === null ? "tombstone" : ""}`);
      n.append(
        el("strong", r.key),
        el(
          "p",
          `Sequence ${r.sequence} · ${r.value === null ? "tombstone" : new TextEncoder().encode(r.value).length + " value bytes"}`,
        ),
      );
      return n;
    }),
  );
  if (!state.memtable.length)
    $("memtable").append(
      el("p", "No unflushed keys. Write a record to begin.", "empty"),
    );
  $("runs").replaceChildren(
    ...state.runs.map((r, i) => {
      const n = el("div", "", `run ${knownRuns.has(r.file) ? "" : "fresh"}`);
      n.append(
        el("span", String(i + 1).padStart(2, "0"), "run-number"),
        el("strong", `${r.count} retained records`),
        el("p", r.file),
        el("p", "SHA-256 " + r.sha256),
      );
      return n;
    }),
  );
  knownRuns = new Set(state.runs.map((r) => r.file));
  if (!state.runs.length)
    $("runs").append(
      el(
        "p",
        "No live sorted runs yet. Flush the memtable to create one.",
        "empty",
      ),
    );
  $("records").replaceChildren(
    ...state.records.map((r) => {
      const tr = document.createElement("tr");
      tr.append(
        el("td", r.key),
        el("td", String(r.sequence)),
        el(
          "td",
          r.value === null ? "TOMBSTONE" : r.value,
          r.value === null ? "deleted" : "",
        ),
      );
      return tr;
    }),
  );
  $("expected").textContent = "Expected store sequence: " + state.sequence;
  $("recovery").replaceChildren(
    el(
      "p",
      `On this process startup: replayed ${state.recoveredWrites} unflushed writes; discarded ${state.discardedTailBytes} incomplete tail bytes. Manifest durable sequence: ${state.durableSequence}.`,
    ),
    el(
      "p",
      `${state.orphanFiles.length} unreferenced run files retained. ${state.totalRetainedKeys} distinct keys including tombstones.`,
    ),
  );
  if (state.orphanFiles.length)
    $("recovery").append(el("pre", state.orphanFiles.join("\n")));
  $("flush").disabled = busy || !state.memtable.length;
  $("compact").disabled =
    busy || state.runs.length + (state.memtable.length ? 1 : 0) < 2;
}
async function refresh() {
  state = await api(
    "/api/state?prefix=" + encodeURIComponent($("prefix").value),
  );
  render();
}
async function action(fn) {
  if (busy) return;
  busy = true;
  document
    .querySelectorAll("button,input,textarea")
    .forEach((b) => (b.disabled = true));
  try {
    await fn();
  } catch (error) {
    message(error.message, true);
  } finally {
    busy = false;
    document
      .querySelectorAll("button,input,textarea")
      .forEach((b) => (b.disabled = false));
    $("value").disabled = $("delete").checked;
    if (state) {
      $("flush").disabled = !state.memtable.length;
      $("compact").disabled =
        state.runs.length + (state.memtable.length ? 1 : 0) < 2;
    }
  }
}
$("write-open").onclick = () => $("write-dialog").showModal();
$("write-close").onclick = () => $("write-dialog").close();
$("refresh").onclick = () =>
  action(async () => {
    await refresh();
    message("Workspace refreshed.");
  });
$("filter").onclick = () =>
  action(async () => {
    await refresh();
    message("Prefix filter applied.");
  });
$("delete").onchange = () => {
  $("value").disabled = $("delete").checked;
};
$("write").onsubmit = (e) => {
  e.preventDefault();
  if (!state) return;
  const body = {
    key: $("key").value,
    value: $("delete").checked ? null : $("value").value,
    expectedSequence: state.sequence,
  };
  action(async () => {
    const record = await api("/api/records", body);
    await refresh();
    message(
      `Sequence ${record.sequence} appended and fsynced before updating memory.`,
    );
  });
};
$("flush").onclick = () =>
  action(async () => {
    const result = await api("/api/flush", {});
    await refresh();
    message(
      result.changed
        ? "Sorted run published in the manifest; WAL reset."
        : "Memtable was already empty.",
    );
  });
$("compact").onclick = () =>
  action(async () => {
    const result = await api("/api/compact", {});
    await refresh();
    message(
      result.changed
        ? `Merged ${result.beforeRecords} stored versions into ${result.afterRecords} latest records. Old files remain unreferenced.`
        : "Fewer than two runs need merging.",
    );
  });
$("lookup").onsubmit = (e) => {
  e.preventDefault();
  action(async () => {
    const result = await api(
      "/api/lookup?key=" + encodeURIComponent($("lookup-key").value),
    );
    $("lookup-result").textContent = JSON.stringify(result, null, 2);
    message("Lookup traced through the in-memory view of sorted runs.");
  });
};
$("export").onclick = () => {
  if (!state) return;
  const url = URL.createObjectURL(
      new Blob([JSON.stringify(state, null, 2) + "\n"], {
        type: "application/json",
      }),
    ),
    a = document.createElement("a");
  a.href = url;
  a.download = "runway-store-evidence.json";
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
};
action(async () => {
  await refresh();
  message(
    "Ready. All writes use the displayed store sequence as a concurrency check.",
  );
});
