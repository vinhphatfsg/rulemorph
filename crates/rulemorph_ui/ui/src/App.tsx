import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ReactFlow, {
  Background,
  Controls,
  Edge,
  Node,
  NodeChange,
  useEdgesState,
  useNodesState,
  ReactFlowInstance,
  applyNodeChanges
} from "reactflow";
import "reactflow/dist/style.css";
import clsx from "clsx";
import {
  __resetAuthCachesForTest as resetAuthCachesForTest,
  captureAuthSecretsFromLocation,
  getApiKey,
  getInternalKey
} from "./auth";
import { __resetTenantCachesForTest, getTenantId } from "./tenant";
import {
  API_BASE,
  INTERNAL_BASE,
  buildHeaders,
  fetchJson,
  loadFinalize,
  loadNodeChunks,
  loadRecordChunks
} from "./api_client";
import {
  applyTraceFilters,
  formatDuration,
  formatDurationParts,
  isErrorStatus,
  resolveDurationUs,
  resolveRuleLabel,
  resolveTraceStatus,
  type DurationUnit,
  type TimeRange,
  type TraceListItem
} from "./trace_list_helpers";
import {
  mergeNodesIntoRecords,
  normalizeTracePayload,
  type TraceManifest,
  type TraceNode,
  type TracePayload,
  type TraceRecord
} from "./trace_payload";
import { shouldResetInitialCenter } from "./view_mode";
import {
  DetailNode,
  buildApiDetailBundle,
  buildApiGraph,
  buildDetailBundle,
  buildMergedApiGraph,
  buildMergedGraph,
  buildOverviewGraph,
  getNodesBounds,
  type ApiDetailBundle,
  type ApiDetailEntry,
  type ApiGraphNode,
  type ApiGraphOp,
  type ApiGraphResponse,
  type DetailBundle,
  type DetailEntry
} from "./trace_graph";
import { TraceListPanel, ZipImportModal } from "./trace_list_panel";

export { getApiKey, getInternalKey } from "./auth";
export { __getTenantIdFromQueryOrStorageForTest, resolveTenantId } from "./tenant";
export type { EndpointRule, EndpointSpec, TraceNode, TracePayload, TraceRecord } from "./trace_payload";
export { buildOverviewGraph } from "./trace_graph";

export function __resetAuthCachesForTest(): void {
  resetAuthCachesForTest();
  __resetTenantCachesForTest();
}

const INITIAL_CENTER_X_RATIO = 0.45;
const INITIAL_CENTER_PADDING = 0.22;

export default function App() {
  captureAuthSecretsFromLocation();
  useEffect(() => {
    if (typeof window === "undefined") return;
    const capture = () => captureAuthSecretsFromLocation();
    window.addEventListener("hashchange", capture);
    window.addEventListener("popstate", capture);
    return () => {
      window.removeEventListener("hashchange", capture);
      window.removeEventListener("popstate", capture);
    };
  }, []);
  const [viewMode, setViewMode] = useState<"trace" | "api">("trace");
  const [durationUnit, setDurationUnit] = useState<DurationUnit>(() => {
    if (typeof window === "undefined") return "us";
    const stored = window.localStorage.getItem("traceDurationUnit");
    return stored === "ms" ? "ms" : "us";
  });
  const [traces, setTraces] = useState<TraceListItem[]>([]);
  const [traceFilterStatus, setTraceFilterStatus] = useState("all");
  const [traceFilterRule, setTraceFilterRule] = useState("all");
  const [traceFilterQuery, setTraceFilterQuery] = useState("");
  const [traceFilterRange, setTraceFilterRange] = useState<TimeRange>("all");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [trace, setTrace] = useState<TracePayload | null>(null);
  const [traceManifest, setTraceManifest] = useState<TraceManifest | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [expandedRuleIds, setExpandedRuleIds] = useState<string[]>([]);
  const [focusedRuleId, setFocusedRuleId] = useState<string | null>(null);
  const [recordIndex, setRecordIndex] = useState(0);
  const [selectedNode, setSelectedNode] = useState<TraceNode | null>(null);
  const [selectedOp, setSelectedOp] = useState<TraceNode | null>(null);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [traceListOpen, setTraceListOpen] = useState(true);
  const [zipModalOpen, setZipModalOpen] = useState(false);
  const [zipFile, setZipFile] = useState<File | null>(null);
  const [zipMessage, setZipMessage] = useState<string | null>(null);
  const [zipUploading, setZipUploading] = useState(false);
  const [traceInspectorSections, setTraceInspectorSections] = useState(() => ({
    finalize: true,
    step: true,
    opList: false,
    opResult: false
  }));
  const [apiInspectorSections, setApiInspectorSections] = useState(() => ({
    opList: false,
    memo: false
  }));
  const [apiGraph, setApiGraph] = useState<ApiGraphResponse | null>(null);
  const [selectedApiNode, setSelectedApiNode] = useState<ApiGraphNode | null>(null);
  const [selectedApiOp, setSelectedApiOp] = useState<ApiGraphOp | null>(null);
  const [apiExpandedRuleIds, setApiExpandedRuleIds] = useState<string[]>([]);
  const [apiFocusedRuleId, setApiFocusedRuleId] = useState<string | null>(null);
  const [flow, setFlow] = useState<ReactFlowInstance | null>(null);
  const initialCenterAppliedRef = useRef(false);
  const prevViewModeRef = useRef<"trace" | "api">(viewMode);
  const [pinnedPositions, setPinnedPositions] = useState<Record<string, { x: number; y: number }>>({});
  const [apiPinnedPositions, setApiPinnedPositions] = useState<Record<string, { x: number; y: number }>>({});
  const nodeTypes = useMemo(() => ({ detail: DetailNode }), []);
  const internalKey = getInternalKey();
  const tenantId = getTenantId();

  const statusOptions = useMemo(() => {
    const set = new Set<string>();
    traces.forEach((item) => {
      set.add(resolveTraceStatus(item));
    });
    return Array.from(set).sort();
  }, [traces]);

  const ruleOptions = useMemo(() => {
    const set = new Set<string>();
    traces.forEach((item) => {
      const label = resolveRuleLabel(item);
      if (label) {
        set.add(label);
      }
    });
    return Array.from(set).sort();
  }, [traces]);

  const filteredTraces = useMemo(
    () =>
      applyTraceFilters(traces, {
        status: traceFilterStatus,
        rule: traceFilterRule,
        query: traceFilterQuery,
        range: traceFilterRange
      }),
    [traces, traceFilterStatus, traceFilterRule, traceFilterQuery, traceFilterRange]
  );

  const loadTraces = useCallback(
    async (preserveSelection: boolean) => {
      const list = await fetchJson<{ traces: TraceListItem[] }>(`${API_BASE}/traces`);
      const data = list?.traces?.length ? list.traces : [];
      setTraces(data);
      setSelectedId((prev) => {
        if (preserveSelection && prev && data.some((item) => item.trace_id === prev)) {
          return prev;
        }
        return data[0]?.trace_id ?? null;
      });
    },
    []
  );

  const handleZipImport = useCallback(async () => {
    if (!zipFile) {
      setZipMessage("ZIPファイルを選択してください。");
      return;
    }
    setZipUploading(true);
    if (!internalKey) {
      setZipMessage("internal_key が未設定です。認証が必要な場合は失敗します。");
    } else {
      setZipMessage(null);
    }
    try {
      const formData = new FormData();
      formData.append("bundle", zipFile);
      const headers = { ...buildHeaders("internal"), "x-rulemorph-import": "zip" };
      const res = await fetch(`${API_BASE}/import`, {
        method: "POST",
        headers,
        body: formData
      });
      if (!res.ok) {
        const payload = await res.json().catch(() => null);
        const message = payload?.error ?? "ZIPインポートに失敗しました。";
        setZipMessage(message);
        return;
      }
      const payload = await res.json();
      const imported = typeof payload?.imported === "number" ? payload.imported : 0;
      const rulesImported = typeof payload?.rules_imported === "number" ? payload.rules_imported : 0;
      setZipMessage(`imported ${imported} traces / ${rulesImported} rules`);
      setZipFile(null);
      await loadTraces(true);
    } catch (err) {
      console.error("zip import failed", err);
      setZipMessage("ZIPインポートに失敗しました。");
    } finally {
      setZipUploading(false);
    }
  }, [zipFile, internalKey, loadTraces]);

  useEffect(() => {
    loadTraces(false);
  }, [loadTraces]);

  useEffect(() => {
    if (!selectedId) {
      if (filteredTraces.length > 0) {
        setSelectedId(filteredTraces[0].trace_id ?? null);
      }
      return;
    }
    if (!filteredTraces.some((item) => item.trace_id === selectedId)) {
      setSelectedId(filteredTraces[0]?.trace_id ?? null);
    }
  }, [filteredTraces, selectedId]);

  useEffect(() => {
    const usePolling = API_BASE.startsWith("/api");
    if (usePolling || internalKey) {
      const timer = window.setInterval(() => {
        loadTraces(true);
      }, 5000);
      return () => {
        window.clearInterval(timer);
      };
    }
    const source = new EventSource(`${INTERNAL_BASE}/stream`);
    const onUpdate = () => {
      loadTraces(true);
    };
    source.addEventListener("traces", onUpdate);
    source.onerror = () => {
      // keep EventSource alive; browser will retry automatically
    };
    return () => {
      source.removeEventListener("traces", onUpdate);
      source.close();
    };
  }, [loadTraces, internalKey]);

  useEffect(() => {
    if (viewMode !== "api") return;
    fetchJson<ApiGraphResponse>(`${API_BASE}/api-graph`).then((data) => {
      if (data) {
        setApiGraph(data);
      }
    });
  }, [viewMode]);

  useEffect(() => {
    if (viewMode !== "api") return;
    setSelectedApiNode(null);
    setSelectedApiOp(null);
    setApiExpandedRuleIds([]);
    setApiFocusedRuleId(null);
    setInspectorOpen(false);
  }, [viewMode]);

  useEffect(() => {
    if (shouldResetInitialCenter(prevViewModeRef.current, viewMode)) {
      initialCenterAppliedRef.current = false;
    }
    prevViewModeRef.current = viewMode;
  }, [viewMode]);

  useEffect(() => {
    if (!selectedId) {
      setTrace(null);
      setTraceManifest(null);
      setDetailLoading(false);
      setDetailError(null);
      initialCenterAppliedRef.current = false;
      return;
    }
    let mounted = true;
    setTrace(null);
    setTraceManifest(null);
    setDetailLoading(false);
    setDetailError(null);
    setRecordIndex(0);
    setSelectedNode(null);
    setSelectedOp(null);
    setExpandedRuleIds([]);
    setFocusedRuleId(null);
    setInspectorOpen(false);
    setPinnedPositions({});
    initialCenterAppliedRef.current = false;
    (async () => {
      const manifestResult = await fetchJson<{ manifest: TraceManifest }>(
        `${API_BASE}/traces/${selectedId}/manifest`
      );
      if (!mounted) return;
      if (!manifestResult?.manifest) {
        const result = await fetchJson<{ trace: TracePayload }>(`${API_BASE}/traces/${selectedId}`);
        if (!mounted) return;
        setTrace(normalizeTracePayload(result?.trace ?? null));
        return;
      }
      const manifest = manifestResult.manifest;
      setTraceManifest(manifest);
      const baseTrace: TracePayload = {
        trace_id: manifest.trace_id,
        timestamp: manifest.timestamp,
        status: manifest.status,
        rule: manifest.rule,
        rule_source: manifest.rule_source,
        records: [],
        finalize: undefined,
        summary: manifest.summary,
        input_format: manifest.input_format
      };
      setTrace(baseTrace);
      const detail = manifest.detail;
      if (!detail || detail.status !== "full") {
        return;
      }
      setDetailLoading(true);
      try {
        const records = await loadRecordChunks(selectedId, detail);
        if (!mounted) return;
        let nextTrace: TracePayload = { ...baseTrace, records };
        setTrace(nextTrace);
        if (detail.layout === "records_nodes_split" && (detail.nodes?.length ?? 0) > 0) {
          const nodesByRecord = await loadNodeChunks(selectedId, detail);
          if (!mounted) return;
          const mergedRecords = mergeNodesIntoRecords(records, nodesByRecord);
          nextTrace = { ...nextTrace, records: mergedRecords };
          setTrace(nextTrace);
        }
        if (detail.finalize) {
          const finalize = await loadFinalize(selectedId);
          if (!mounted) return;
          if (finalize) {
            setTrace((prev) => (prev ? { ...prev, finalize } : { ...nextTrace, finalize }));
          }
        }
      } catch (err) {
        if (!mounted) return;
        const fallback = await fetchJson<{ trace: TracePayload }>(`${API_BASE}/traces/${selectedId}`);
        if (!mounted) return;
        if (fallback?.trace) {
          setTraceManifest(null);
          setTrace(normalizeTracePayload(fallback.trace));
          setDetailError(null);
          return;
        }
        setDetailError("trace detail load failed");
      } finally {
        if (mounted) {
          setDetailLoading(false);
        }
      }
    })();
    return () => {
      mounted = false;
    };
  }, [selectedId]);

  const overviewGraph = useMemo(
    () =>
      trace
        ? buildOverviewGraph(trace)
        : {
            nodes: [],
            edges: [],
            traceMap: new Map(),
            endpointEdgeLabels: new Map(),
            edgeDurationMap: new Map(),
            errorRuleIds: new Set(),
            ruleTypeById: new Map()
          },
    [trace]
  );
  const effectiveFocusedRuleId =
    focusedRuleId ?? expandedRuleIds[expandedRuleIds.length - 1] ?? null;
  const currentTrace = effectiveFocusedRuleId
    ? overviewGraph.traceMap.get(effectiveFocusedRuleId) ?? trace
    : trace;
  const isFinalizeSelected = recordIndex < 0;
  const currentRecord = recordIndex >= 0 ? currentTrace?.records?.[recordIndex] : undefined;
  const finalizePayload = currentTrace?.finalize ?? null;
  const bundles = useMemo(() => {
    const map = new Map<string, DetailBundle>();
    expandedRuleIds.forEach((ruleId) => {
      const ruleTrace = overviewGraph.traceMap.get(ruleId);
      const record =
        ruleId === effectiveFocusedRuleId
          ? ruleTrace?.records?.[recordIndex]
          : ruleTrace?.records?.[0];
      map.set(ruleId, buildDetailBundle(record, ruleId));
    });
    return map;
  }, [expandedRuleIds, overviewGraph, recordIndex, effectiveFocusedRuleId]);
  const apiGraphLayout = useMemo(() => {
    if (!apiGraph) {
      return { nodes: [], edges: [], nodeMap: new Map<string, ApiGraphNode>(), edgeLabelMap: new Map<string, string>() };
    }
    return buildApiGraph(apiGraph);
  }, [apiGraph]);
  const apiBundles = useMemo(() => {
    const map = new Map<string, ApiDetailBundle>();
    apiExpandedRuleIds.forEach((ruleId) => {
      const rule = apiGraphLayout.nodeMap.get(ruleId);
      if (!rule) return;
      map.set(ruleId, buildApiDetailBundle(rule));
    });
    return map;
  }, [apiExpandedRuleIds, apiGraphLayout]);
  const mergedGraph = useMemo(
    () =>
      buildMergedGraph(
        overviewGraph,
        bundles,
        expandedRuleIds,
        pinnedPositions,
        overviewGraph.endpointEdgeLabels
      ),
    [overviewGraph, bundles, expandedRuleIds, pinnedPositions]
  );
  const apiMergedGraph = useMemo(
    () =>
      buildMergedApiGraph(
        { nodes: apiGraphLayout.nodes, edges: apiGraphLayout.edges },
        apiBundles,
        apiExpandedRuleIds,
        apiPinnedPositions,
        apiGraphLayout.edgeLabelMap
      ),
    [apiGraphLayout, apiBundles, apiExpandedRuleIds, apiPinnedPositions]
  );
  const activeGraph = viewMode === "api" ? apiMergedGraph : mergedGraph;

  const [nodes, setNodes] = useNodesState(activeGraph.nodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(activeGraph.edges);

  const onNodesChange = useCallback(
    (changes: NodeChange[]) => {
      let nextNodes: Node[] = [];
      setNodes((prev) => {
        nextNodes = applyNodeChanges(changes, prev);
        return nextNodes;
      });
      const settledMoves = changes.filter(
        (change) => change.type === "position" && change.dragging === false
      );
      if (settledMoves.length > 0) {
        const nextById = new Map(nextNodes.map((node) => [node.id, node]));
        if (viewMode === "api") {
          setApiPinnedPositions((prev) => {
            const next = { ...prev };
            settledMoves.forEach((change) => {
              const node = nextById.get(change.id);
              if (!node) return;
              next[change.id] = { ...node.position };
            });
            return next;
          });
        } else {
          setPinnedPositions((prev) => {
            const next = { ...prev };
            settledMoves.forEach((change) => {
              const node = nextById.get(change.id);
              if (!node) return;
              next[change.id] = { ...node.position };
            });
            return next;
          });
        }
      }
    },
    [setNodes, viewMode]
  );

  useEffect(() => {
    setNodes((prev) => {
      const prevById = new Map(prev.map((node) => [node.id, node]));
      return activeGraph.nodes.map((node) => {
        const existing = prevById.get(node.id);
        const isOverview = node.className?.includes("trace-node--overview");
        const pinned = viewMode === "api" ? apiPinnedPositions[node.id] : pinnedPositions[node.id];
        if (existing && isOverview) {
          return { ...node, position: existing.position };
        }
        if (isOverview && pinned) {
          return { ...node, position: pinned };
        }
        return node;
      });
    });
    setEdges(activeGraph.edges);
  }, [activeGraph.nodes, activeGraph.edges, pinnedPositions, apiPinnedPositions, viewMode, setNodes, setEdges]);

  useEffect(() => {
    if (!flow) return;
    if (viewMode === "api") {
      flow.fitView({ padding: INITIAL_CENTER_PADDING });
      return;
    }
    if (!trace) return;
    if (nodes.length === 0) return;
    if (initialCenterAppliedRef.current) return;
    requestAnimationFrame(() => {
      const overviewNodes = nodes.filter((node) =>
        node.className?.includes("trace-node--overview")
      );
      if (overviewNodes.length === 0) return;
      const container = document.querySelector(".trace-canvas");
      if (!container) return;
      const { width, height } = container.getBoundingClientRect();
      if (!width || !height) return;
      const bounds = getNodesBounds(overviewNodes);
      const availableWidth = width * (1 - INITIAL_CENTER_PADDING * 2);
      const availableHeight = height * (1 - INITIAL_CENTER_PADDING * 2);
      const zoom = Math.min(availableWidth / bounds.width, availableHeight / bounds.height);
      const centerX = bounds.minX + bounds.width / 2;
      const centerY = bounds.minY + bounds.height / 2;
      const desiredCenterX = width * INITIAL_CENTER_X_RATIO;
      const desiredCenterY = height * 0.5;
      const x = desiredCenterX - centerX * zoom;
      const y = desiredCenterY - centerY * zoom;
      flow.setViewport({ x, y, zoom }, { duration: 0 });
      initialCenterAppliedRef.current = true;
    });
  }, [flow, trace, viewMode, nodes.length]);
  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem("traceDurationUnit", durationUnit);
  }, [durationUnit]);
  const detailNodeMap = useMemo(() => {
    const map = new Map<string, DetailEntry>();
    bundles.forEach((bundle) => {
      bundle.map.forEach((entry, nodeId) => {
        map.set(nodeId, entry);
      });
    });
    return map;
  }, [bundles]);
  const apiDetailNodeMap = useMemo(() => {
    const map = new Map<string, ApiDetailEntry>();
    apiBundles.forEach((bundle) => {
      bundle.map.forEach((entry, nodeId) => {
        map.set(nodeId, entry);
      });
    });
    return map;
  }, [apiBundles]);
  const detailStatus = traceManifest
    ? traceManifest.detail?.status ?? "basic"
    : trace?.detail?.status;
  const detailReason = traceManifest
    ? traceManifest.detail?.reason ?? []
    : trace?.detail?.reason ?? [];
  const detailAvailable = detailStatus ? detailStatus === "full" : true;
  const hasDetail = viewMode === "trace" && expandedRuleIds.length > 0 && detailAvailable;
  const apiHasDetail = viewMode === "api" && apiExpandedRuleIds.length > 0;
  const finalizeDuration = resolveDurationUs(
    finalizePayload?.duration_us,
    finalizePayload?.duration_ms
  );
  const finalizeDurationParts = formatDurationParts(finalizeDuration, durationUnit);
  const finalizeNodes = finalizePayload?.nodes ?? [];
  const selectedMeta = (selectedNode?.meta ?? {}) as Record<string, unknown>;
  const stepRecordWhen =
    typeof selectedMeta["record_when"] === "boolean" ? selectedMeta["record_when"] : undefined;
  const stepAssertsOk =
    typeof selectedMeta["asserts_ok"] === "boolean" ? selectedMeta["asserts_ok"] : undefined;
  const stepBranchTaken =
    typeof selectedMeta["branch_taken"] === "string" ? String(selectedMeta["branch_taken"]) : undefined;
  const stepDuration = resolveDurationUs(selectedNode?.duration_us, selectedNode?.duration_ms);
  const stepDurationParts = formatDurationParts(stepDuration, durationUnit);
  const traceFinalizeOpen = traceInspectorSections.finalize;
  const traceStepOpen = traceInspectorSections.step;
  const traceOpListOpen = traceInspectorSections.opList;
  const traceOpResultOpen = traceInspectorSections.opResult;
  const apiOpListOpen = apiInspectorSections.opList;
  const apiMemoOpen = apiInspectorSections.memo;
  const recordLabel = isFinalizeSelected
    ? "finalize"
    : `record #${currentRecord?.index ?? 0}`;
  const detailLabel = detailStatus
    ? detailStatus === "full"
      ? detailLoading
        ? "loading"
        : hasDetail
          ? "detail"
          : "overview"
      : detailStatus
    : hasDetail
      ? "detail"
      : "overview";

  const renderJsonBlock = (label: string, value: unknown) => {
    const hasValue = !(value === null || value === undefined);
    const isWide = label === "input" || label === "output" || label === "error";
    return (
      <div className={clsx("inspector-block", isWide && "inspector-block--wide")} key={label}>
        <div className="inspector-block__header">
          <div className="inspector-block__title">
            <span className="inspector-block__line" />
            <span className="inspector-block__name">{label}</span>
          </div>
          <span className="inspector-block__meta">{hasValue ? "json" : "empty"}</span>
        </div>
        <pre className="inspector-block__body">{hasValue ? JSON.stringify(value, null, 2) : "なし"}</pre>
      </div>
    );
  };

  return (
    <div className="app">
      <div className="app__glow" />
      <header className="topbar">
        <div className="title-chip">
          <span className="title-chip__dot" />
          <span className="title-chip__label">
            {viewMode === "trace" ? "Rulemorph Trace" : "Rulemorph 構成図"}
          </span>
          <span className="title-chip__id">
            {viewMode === "trace"
              ? currentTrace?.rule?.path ?? currentTrace?.trace_id ?? "no-trace"
              : selectedApiNode?.path ?? "api-graph"}
          </span>
        </div>
        <div className="topbar__meta">
          <div className="meta-tabs">
            <button
              className={clsx("meta-tab", viewMode === "trace" && "is-active")}
              onClick={() => setViewMode("trace")}
            >
              Trace
            </button>
            <button
              className={clsx("meta-tab", viewMode === "api" && "is-active")}
              onClick={() => setViewMode("api")}
            >
              構成図
            </button>
          </div>
          {viewMode === "trace" ? (
            <>
              <span className="meta-pill">{detailLabel}</span>
              <span className="meta-pill">
                {filteredTraces.length} / {traces.length} traces
              </span>
              <span className="meta-pill">{recordLabel}</span>
            </>
          ) : (
            <>
              <span className="meta-pill">{apiGraph?.nodes.length ?? 0} rules</span>
              <span className="meta-pill">{apiGraph?.edges.length ?? 0} edges</span>
            </>
          )}
        </div>
      </header>

      <main className="stage">
        <div className="trace-canvas">
          <ReactFlow
            key="canvas"
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            fitViewOptions={{ padding: 0.22 }}
            nodesDraggable
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onInit={setFlow}
            onNodeClick={(_, node) => {
              if (viewMode === "api") {
                const apiNode = apiGraphLayout.nodeMap.get(node.id);
                if (apiNode) {
                  const alreadyExpanded = apiExpandedRuleIds.includes(node.id);
                  if (alreadyExpanded) {
                    setApiExpandedRuleIds((prev) => {
                      const next = prev.filter((id) => id !== node.id);
                      setApiFocusedRuleId(next[next.length - 1] ?? null);
                      return next;
                    });
                    setSelectedApiNode(null);
                    setSelectedApiOp(null);
                    setInspectorOpen(false);
                  } else {
                    setApiExpandedRuleIds((prev) => [...prev, node.id]);
                    setApiFocusedRuleId(node.id);
                    setSelectedApiNode(apiNode);
                    setSelectedApiOp(null);
                    setInspectorOpen(false);
                  }
                  return;
                }
                const apiDetail = apiDetailNodeMap.get(node.id);
                if (!apiDetail) return;
                const parent = apiGraphLayout.nodeMap.get(apiDetail.ruleId);
                if (parent) {
                  setSelectedApiNode(parent);
                  setSelectedApiOp(apiDetail.node);
                }
                setInspectorOpen(true);
                return;
              }
              const nextTrace = overviewGraph.traceMap.get(node.id);
              if (nextTrace) {
                const alreadyExpanded = expandedRuleIds.includes(node.id);
                if (alreadyExpanded) {
                  setExpandedRuleIds((prev) => {
                    const next = prev.filter((id) => id !== node.id);
                    setFocusedRuleId(next[next.length - 1] ?? null);
                    return next;
                  });
                  setRecordIndex(0);
                  setSelectedNode(null);
                  setSelectedOp(null);
                  setInspectorOpen(false);
                } else {
                  setExpandedRuleIds((prev) => [...prev, node.id]);
                  setFocusedRuleId(node.id);
                  setRecordIndex(0);
                  setSelectedNode(null);
                  setSelectedOp(null);
                  setInspectorOpen(false);
                }
                return;
              }
              const detailEntry = detailNodeMap.get(node.id);
              if (!detailEntry) return;
              setFocusedRuleId(detailEntry.ruleId);
              if (detailEntry.kind === "op") {
                setSelectedNode(detailEntry.parent ?? null);
                setSelectedOp(detailEntry.node);
                setTraceInspectorSections((prev) => ({ ...prev, opResult: true }));
              } else {
                setSelectedNode(detailEntry.node);
                setSelectedOp(detailEntry.node.children?.find((child) => child.kind === "op") ?? null);
              }
              setInspectorOpen(true);
            }}
          >
            <Background gap={hasDetail || apiHasDetail ? 28 : 32} size={1} />
            <Controls />
          </ReactFlow>
        </div>

        {viewMode === "trace" && (
          <TraceListPanel
            traceListOpen={traceListOpen}
            setTraceListOpen={setTraceListOpen}
            internalKey={internalKey}
            durationUnit={durationUnit}
            setDurationUnit={setDurationUnit}
            traceFilterQuery={traceFilterQuery}
            setTraceFilterQuery={setTraceFilterQuery}
            traceFilterStatus={traceFilterStatus}
            setTraceFilterStatus={setTraceFilterStatus}
            traceFilterRule={traceFilterRule}
            setTraceFilterRule={setTraceFilterRule}
            traceFilterRange={traceFilterRange}
            setTraceFilterRange={setTraceFilterRange}
            statusOptions={statusOptions}
            ruleOptions={ruleOptions}
            filteredTraces={filteredTraces}
            traces={traces}
            selectedId={selectedId}
            setSelectedId={setSelectedId}
            detailStatus={detailStatus}
            detailError={detailError}
            detailReason={detailReason}
            setZipMessage={setZipMessage}
            setZipModalOpen={setZipModalOpen}
          />
        )}

        {hasDetail && viewMode === "trace" && (
          <>
            <aside className="floating-panel record-panel">
              <div className="panel__header">
                <h2>Records</h2>
                <p>{currentTrace?.records?.length ?? 0} total</p>
              </div>
              <div className="record-list">
                {(currentTrace?.records ?? []).map((record, idx) => (
                  <button
                    key={record.index}
                    className={clsx("record-card", recordIndex === idx && "is-active")}
                    onClick={() => {
                      setRecordIndex(idx);
                      setSelectedNode(null);
                      setSelectedOp(null);
                      setInspectorOpen(false);
                    }}
                  >
                    <span>#{record.index}</span>
                    <span
                      className={clsx(
                        "record-status",
                        isErrorStatus(record.status) && "record-status--error"
                      )}
                    >
                      {record.status ?? "ok"}
                    </span>
                    <span>
                      {formatDuration(resolveDurationUs(record.duration_us, record.duration_ms), durationUnit)}
                    </span>
                  </button>
                ))}
                {finalizePayload && (
                  <button
                    key="finalize"
                    data-testid="record-finalize"
                    className={clsx("record-card record-card--finalize", isFinalizeSelected && "is-active")}
                    onClick={() => {
                      setRecordIndex(-1);
                      setSelectedNode(null);
                      setSelectedOp(null);
                      setInspectorOpen(true);
                    }}
                  >
                    <span>Finalize</span>
                    <span
                      className={clsx(
                        "record-status",
                        isErrorStatus(finalizePayload.status) && "record-status--error"
                      )}
                    >
                      {finalizePayload.status ?? "ok"}
                    </span>
                    <span>
                      {formatDuration(
                        resolveDurationUs(finalizePayload.duration_us, finalizePayload.duration_ms),
                        durationUnit
                      )}
                    </span>
                  </button>
                )}
              </div>
            </aside>
          </>
        )}

        <button
          className={clsx("inspector-toggle", inspectorOpen && "is-open")}
          onClick={() => setInspectorOpen((prev) => !prev)}
        >
          {inspectorOpen ? "Inspectorを閉じる" : "Inspectorを見る"}
        </button>

        <aside className={clsx("inspector-drawer", inspectorOpen && "is-open")}>
          <div className="inspector__header">
            <div>
              <h2>Inspector</h2>
              <p>
                {viewMode === "trace"
                  ? selectedNode
                    ? selectedNode.label
                    : "ノードを選択して詳細を表示"
                  : selectedApiNode
                    ? selectedApiNode.label
                    : "ノードを選択して詳細を表示"}
              </p>
            </div>
            <button className="icon-button" onClick={() => setInspectorOpen(false)}>
              ×
            </button>
          </div>

          {viewMode === "api" ? (
            <>
              <div
                className={clsx(
                  "inspector__section inspector__section--oplist",
                  !apiOpListOpen && "is-collapsed"
                )}
              >
                <button
                  className="inspector__section-toggle"
                  aria-expanded={apiOpListOpen}
                  onClick={() =>
                    setApiInspectorSections((prev) => ({ ...prev, opList: !prev.opList }))
                  }
                >
                  <h3>OP一覧</h3>
                  <span className="inspector__chevron">{apiOpListOpen ? "v" : ">"}</span>
                </button>
                {apiOpListOpen && (
                  <div className="op-list">
                    {(selectedApiNode?.ops ?? []).length === 0 && (
                      <p className="muted">このルールにOPはありません。</p>
                    )}
                    {(selectedApiNode?.ops ?? []).map((op, index) => (
                      <div
                        key={`${op.label}-${index}`}
                        className={clsx(
                          "op-item is-static",
                          selectedApiOp?.label === op.label && "is-active"
                        )}
                      >
                        <span>{op.label}</span>
                        <span className="muted">{op.detail ?? selectedApiNode?.kind}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <div className={clsx("inspector__section", !apiMemoOpen && "is-collapsed")}>
                <button
                  className="inspector__section-toggle"
                  aria-expanded={apiMemoOpen}
                  onClick={() =>
                    setApiInspectorSections((prev) => ({ ...prev, memo: !prev.memo }))
                  }
                >
                  <h3>処理メモ</h3>
                  <span className="inspector__chevron">{apiMemoOpen ? "v" : ">"}</span>
                </button>
                {apiMemoOpen && (
                  <div className="inspector__content">
                    <p className="muted">
                      実値はありません。ルールファイルに記載された処理内容のみ表示しています。
                    </p>
                  </div>
                )}
              </div>
            </>
          ) : (
            <>
              {isFinalizeSelected && (
                <div
                  className={clsx(
                    "inspector__section inspector__section--finalize",
                    !traceFinalizeOpen && "is-collapsed"
                  )}
                >
                  <button
                    className="inspector__section-toggle"
                    aria-expanded={traceFinalizeOpen}
                    onClick={() =>
                      setTraceInspectorSections((prev) => ({ ...prev, finalize: !prev.finalize }))
                    }
                  >
                    <h3>Finalize</h3>
                    <span className="inspector__chevron">{traceFinalizeOpen ? "v" : ">"}</span>
                  </button>
                  {traceFinalizeOpen && (
                    <div className="inspector__content">
                      {!finalizePayload ? (
                        <p className="muted">finalize のデータがありません。</p>
                      ) : (
                        <>
                          <div className="step-badges">
                            <span
                              className={clsx(
                                "chip",
                                isErrorStatus(finalizePayload.status) && "chip--error"
                              )}
                            >
                              {finalizePayload.status ?? "ok"}
                            </span>
                            {finalizeDurationParts && (
                              <span className="chip">
                                duration: {finalizeDurationParts.value}{" "}
                                <span className="chip__unit">{finalizeDurationParts.unit}</span>
                              </span>
                            )}
                          </div>
                          <div className="inspector-grid">
                            {renderJsonBlock("input", finalizePayload.input ?? null)}
                            {renderJsonBlock("output", finalizePayload.output ?? null)}
                          </div>
                          <div className="op-list">
                            {finalizeNodes.length === 0 ? (
                              <p className="muted">finalize ノードがありません。</p>
                            ) : (
                              finalizeNodes.map((node, index) => (
                                <div
                                  key={node.id ?? `finalize-${index}`}
                                  className="op-item is-static"
                                >
                                  <span>{node.label ?? node.kind}</span>
                                  <span className="op-item__meta">
                                    <span className="muted">{node.kind}</span>
                                    {resolveDurationUs(node.duration_us, node.duration_ms) !==
                                      undefined && (
                                      <span className="op-item__duration">
                                        {formatDuration(
                                          resolveDurationUs(node.duration_us, node.duration_ms),
                                          durationUnit
                                        )}
                                      </span>
                                    )}
                                  </span>
                                </div>
                              ))
                            )}
                          </div>
                        </>
                      )}
                    </div>
                  )}
                </div>
              )}
              {!isFinalizeSelected && (
                <>
              <div
                className={clsx(
                  "inspector__section inspector__section--opresult",
                  !traceStepOpen && "is-collapsed"
                )}
              >
                <button
                  className="inspector__section-toggle"
                  aria-expanded={traceStepOpen}
                  onClick={() =>
                    setTraceInspectorSections((prev) => ({ ...prev, step: !prev.step }))
                  }
                >
                  <h3>Step結果</h3>
                  <span className="inspector__chevron">{traceStepOpen ? "v" : ">"}</span>
                </button>
                {traceStepOpen && (
                  <div className="inspector__content">
                    {!selectedNode ? (
                      <p className="muted">ノードを選択して詳細を表示してください。</p>
                    ) : (
                      <>
                        <div className="step-badges">
                          <span
                            className={clsx(
                              "chip",
                              isErrorStatus(selectedNode.status) && "chip--error"
                            )}
                          >
                            {selectedNode.status ?? "ok"}
                          </span>
                          {stepDurationParts && (
                            <span className="chip">
                              duration: {stepDurationParts.value}{" "}
                              <span className="chip__unit">{stepDurationParts.unit}</span>
                            </span>
                          )}
                          {stepRecordWhen !== undefined && (
                            <span className="chip">record_when: {String(stepRecordWhen)}</span>
                          )}
                          {stepAssertsOk !== undefined && (
                            <span className="chip">asserts: {String(stepAssertsOk)}</span>
                          )}
                          {stepBranchTaken && <span className="chip">branch: {stepBranchTaken}</span>}
                        </div>
                        <div className="inspector-grid">
                          {renderJsonBlock("input", selectedNode.input ?? null)}
                          {renderJsonBlock("output", selectedNode.output ?? null)}
                          {selectedNode.error && renderJsonBlock("error", selectedNode.error)}
                        </div>
                      </>
                    )}
                  </div>
                )}
              </div>

              <div
                className={clsx(
                  "inspector__section inspector__section--oplist",
                  !traceOpListOpen && "is-collapsed"
                )}
              >
                <button
                  className="inspector__section-toggle"
                  aria-expanded={traceOpListOpen}
                  onClick={() =>
                    setTraceInspectorSections((prev) => ({ ...prev, opList: !prev.opList }))
                  }
                >
                  <h3>OP一覧</h3>
                  <span className="inspector__chevron">{traceOpListOpen ? "v" : ">"}</span>
                </button>
                {traceOpListOpen && (
                  <div className="op-list">
                    {(selectedNode?.children ?? []).length === 0 && (
                      <p className="muted">このノードにOPはありません。</p>
                    )}
                    {(selectedNode?.children ?? []).map((child) => (
                      <button
                        key={child.id}
                        className={clsx("op-item", selectedOp?.id === child.id && "is-active")}
                        onClick={() => setSelectedOp(child)}
                      >
                        <span>{child.label}</span>
                        <span className="op-item__meta">
                          <span className="muted">{child.meta?.op ?? "op"}</span>
                          {resolveDurationUs(child.duration_us, child.duration_ms) !== undefined && (
                            <span className="op-item__duration">
                              {formatDuration(
                                resolveDurationUs(child.duration_us, child.duration_ms),
                                durationUnit
                              )}
                            </span>
                          )}
                        </span>
                      </button>
                    ))}
                  </div>
                )}
              </div>

              <div
                className={clsx(
                  "inspector__section inspector__section--opresult",
                  !traceOpResultOpen && "is-collapsed"
                )}
              >
                <button
                  className="inspector__section-toggle"
                  aria-expanded={traceOpResultOpen}
                  onClick={() =>
                    setTraceInspectorSections((prev) => ({ ...prev, opResult: !prev.opResult }))
                  }
                >
                  <h3>OP結果</h3>
                  <span className="inspector__chevron">{traceOpResultOpen ? "v" : ">"}</span>
                </button>
                {traceOpResultOpen && (
                  <div className="inspector__content">
                    {(() => {
                      const op = selectedOp as any;
                      const input = op?.input ?? null;
                      const pipe = op?.pipe_value ?? null;
                      const args = op?.args ?? null;
                      const output = op?.output ?? null;
                      const opDuration = resolveDurationUs(op?.duration_us, op?.duration_ms);
                      const opDurationParts = formatDurationParts(opDuration, durationUnit);
                      const pipeSteps = (op?.pipe_steps ?? []) as {
                        index: number;
                        label: string;
                        input?: unknown;
                        output?: unknown;
                      }[];
                      return (
                        <>
                          {opDurationParts && (
                            <div className="step-badges">
                              <span className="chip">
                                duration: {opDurationParts.value}{" "}
                                <span className="chip__unit">{opDurationParts.unit}</span>
                              </span>
                            </div>
                          )}
                          <div className="inspector-grid">
                            {renderJsonBlock("input", input)}
                            {renderJsonBlock("pipe", pipe)}
                            {renderJsonBlock("args", args)}
                            {renderJsonBlock("output", output)}
                          </div>
                          <div className="pipe-steps">
                            <div className="pipe-steps__header">
                              <h4>ステップ推移</h4>
                              <span className="muted">{pipeSteps.length} steps</span>
                            </div>
                            {pipeSteps.length === 0 ? (
                              <p className="muted">ステップがありません。</p>
                            ) : (
                              <div className="pipe-steps__list">
                                {pipeSteps.map((step) => (
                                  <div className="pipe-step" key={step.index}>
                                    <div className="pipe-step__title">
                                      <span className="pipe-step__index">#{step.index}</span>
                                      <span className="pipe-step__label">{step.label}</span>
                                    </div>
                                    <div className="pipe-step__io">
                                      <div className="pipe-step__cell">
                                        <span className="pipe-step__name">input</span>
                                        <pre>
                                          {step.input !== undefined
                                            ? JSON.stringify(step.input)
                                            : "なし"}
                                        </pre>
                                      </div>
                                      <div className="pipe-step__cell">
                                        <span className="pipe-step__name">output</span>
                                        <pre>
                                          {step.output !== undefined
                                            ? JSON.stringify(step.output)
                                            : "なし"}
                                        </pre>
                                      </div>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            )}
                          </div>
                        </>
                      );
                    })()}
                  </div>
                )}
              </div>
                </>
              )}
            </>
          )}
        </aside>
        {zipModalOpen && (
          <ZipImportModal
            tenantId={tenantId}
            zipMessage={zipMessage}
            zipUploading={zipUploading}
            zipFile={zipFile}
            setZipFile={setZipFile}
            setZipMessage={setZipMessage}
            setZipModalOpen={setZipModalOpen}
            handleZipImport={handleZipImport}
          />
        )}
      </main>
    </div>
  );
}
