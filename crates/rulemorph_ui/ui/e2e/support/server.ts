import { spawn } from "child_process";
import { existsSync } from "fs";
import net from "net";
import path from "path";
import { fileURLToPath } from "url";

const repoRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../../.."
);
const uiDist = path.resolve(repoRoot, "crates/rulemorph_ui/ui/dist");
const apiRulesDir = path.resolve(repoRoot, "assets/api_rules");

export function getAvailablePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("failed to acquire port")));
        return;
      }
      const { port } = address;
      server.close(() => resolve(port));
    });
  });
}

export async function startServer(dataDir: string, port: number) {
  return startServerWithOptions(dataDir, port, {
    allowUnauthInternal: true
  });
}

export type ServerOptions = {
  apiKey?: string;
  tenantId?: string;
  internalApiKey?: string;
  allowUnauthInternal?: boolean;
};

export async function startServerWithOptions(
  dataDir: string,
  port: number,
  options: ServerOptions
) {
  if (!existsSync(uiDist)) {
    throw new Error(
      `UI dist not found at ${uiDist}. Run npm --prefix crates/rulemorph_ui/ui run build first.`
    );
  }
  if (!existsSync(apiRulesDir)) {
    throw new Error(`api rules not found at ${apiRulesDir}.`);
  }
  const args = [
    "run",
    "-p",
    "rulemorph_server",
    "--",
    "--api-mode",
    "rules",
    "--rules-dir",
    apiRulesDir,
    "--data-dir",
    dataDir,
    "--ui-dir",
    uiDist,
    "--ssrf-allow-private",
    "--port",
    String(port)
  ];
  if (options.allowUnauthInternal ?? false) {
    args.push("--allow-unauth-internal");
  }
  if (options.apiKey) {
    args.push("--api-key", options.apiKey);
  }
  if (options.tenantId) {
    args.push("--tenant-id", options.tenantId);
  }
  if (options.internalApiKey) {
    args.push("--internal-api-key", options.internalApiKey);
  }
  if (options.apiKey) {
    args.push("--ssrf-allow-any");
  }

  const child = spawn("cargo", args, {
    cwd: repoRoot,
    env: { ...process.env, RUST_LOG: "info" },
    stdio: ["ignore", "pipe", "pipe"]
  });

  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const timeout = setTimeout(() => {
      settled = true;
      reject(new Error("server did not start in time"));
    }, 20_000);
    const cleanup = () => {
      clearTimeout(timeout);
      child.stdout?.off("data", onData);
      child.stderr?.off("data", onData);
    };
    const onData = (data: Buffer) => {
      const text = data.toString();
      if (text.includes("listening on 127.0.0.1")) {
        settled = true;
        cleanup();
        resolve();
      }
    };
    child.stdout?.on("data", onData);
    child.stderr?.on("data", onData);
    child.on("exit", (code) => {
      if (!settled) {
        settled = true;
        cleanup();
        reject(new Error(`server exited early: ${code}`));
      }
    });
  });

  return child;
}
