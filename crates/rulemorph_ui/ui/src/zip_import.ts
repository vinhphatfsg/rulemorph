import { API_BASE, buildHeaders } from "./api_client";

type RunZipImportArgs = {
  zipFile: File | null;
  internalKey: string | null;
  setZipMessage: (message: string | null) => void;
  setZipUploading: (uploading: boolean) => void;
  setZipFile: (file: File | null) => void;
  loadTraces: (preserveSelection: boolean) => Promise<void>;
};

export async function runZipImport({
  zipFile,
  internalKey,
  setZipMessage,
  setZipUploading,
  setZipFile,
  loadTraces
}: RunZipImportArgs): Promise<void> {
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
}
