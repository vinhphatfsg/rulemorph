type ZipImportModalProps = {
  tenantId: string | null;
  zipMessage: string | null;
  zipUploading: boolean;
  zipFile: File | null;
  setZipFile: (file: File | null) => void;
  setZipMessage: (value: string | null) => void;
  setZipModalOpen: (open: boolean) => void;
  handleZipImport: () => void;
};

export function ZipImportModal({
  tenantId,
  zipMessage,
  zipUploading,
  zipFile,
  setZipFile,
  setZipMessage,
  setZipModalOpen,
  handleZipImport
}: ZipImportModalProps) {
  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal">
        <div className="modal__header">
          <div>
            <h3>ZIPインポート</h3>
            <p className="muted">traces/ と rules/ を含むZIPをアップロードしてください。</p>
          </div>
          <button
            className="icon-button"
            onClick={() => {
              setZipModalOpen(false);
              setZipFile(null);
            }}
          >
            ×
          </button>
        </div>
        <div className="modal__body">
          {tenantId && <p className="muted">tenant: {tenantId}</p>}
          <input
            className="modal__file"
            data-testid="zip-import-file"
            type="file"
            accept=".zip"
            onChange={(event) => {
              const file = event.currentTarget.files?.[0] ?? null;
              setZipFile(file);
              setZipMessage(null);
            }}
          />
          {zipMessage && (
            <div className="modal__message" data-testid="zip-import-message">
              {zipMessage}
            </div>
          )}
        </div>
        <div className="modal__actions">
          <button
            className="modal__button"
            type="button"
            onClick={() => {
              setZipModalOpen(false);
              setZipFile(null);
            }}
          >
            閉じる
          </button>
          <button
            className="modal__button modal__button--primary"
            data-testid="zip-import-submit"
            type="button"
            disabled={zipUploading || !zipFile}
            onClick={handleZipImport}
          >
            {zipUploading ? "アップロード中..." : "インポート"}
          </button>
        </div>
      </div>
    </div>
  );
}
