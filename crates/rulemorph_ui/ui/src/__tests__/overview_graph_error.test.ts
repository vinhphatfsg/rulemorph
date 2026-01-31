import { describe, expect, it } from "vitest";
import { buildOverviewGraph, type TracePayload } from "../App";

describe("buildOverviewGraph", () => {
  it("marks child rule when record status is error even with nodes", () => {
    const trace: TracePayload = {
      rule: {
        name: "endpoint",
        path: "rules/endpoint.yaml",
        type: "endpoint",
        version: 2
      },
      rule_source: {
        version: 2,
        type: "endpoint",
        endpoints: [
          {
            method: "GET",
            path: "/api/hello/a",
            steps: [{ rule: "./hello.yaml" }]
          }
        ]
      },
      records: [
        {
          index: 0,
          status: "ok",
          nodes: [
            {
              id: "step-0",
              kind: "endpoint",
              label: "hello",
              status: "ok",
              child_trace: {
                rule: {
                  name: "hello",
                  path: "rules/hello.yaml",
                  type: "normal",
                  version: 2
                },
                records: [
                  {
                    index: 0,
                    status: "error",
                    nodes: [
                      {
                        id: "step-0",
                        kind: "map",
                        label: "step-1",
                        status: "ok"
                      }
                    ]
                  }
                ]
              }
            }
          ]
        }
      ]
    };

    const graph = buildOverviewGraph(trace);

    expect(graph.errorRuleIds.has("rules/hello.yaml")).toBe(true);
  });
});
