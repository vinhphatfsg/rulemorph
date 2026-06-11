import { useCallback, useState } from "react";
import { runZipImport } from "../import/zip_import";

type ZipImportStateArgs = {
  internalKey: string | null;
  loadTraces: (preserveSelection: boolean) => Promise<void>;
};

export function useZipImportState({ internalKey, loadTraces }: ZipImportStateArgs) {
  const [zipModalOpen, setZipModalOpen] = useState(false);
  const [zipFile, setZipFile] = useState<File | null>(null);
  const [zipMessage, setZipMessage] = useState<string | null>(null);
  const [zipUploading, setZipUploading] = useState(false);

  const handleZipImport = useCallback(async () => {
    await runZipImport({
      zipFile,
      internalKey,
      setZipMessage,
      setZipUploading,
      setZipFile,
      loadTraces
    });
  }, [zipFile, internalKey, loadTraces]);

  return {
    zipModalOpen,
    setZipModalOpen,
    zipFile,
    setZipFile,
    zipMessage,
    setZipMessage,
    zipUploading,
    handleZipImport
  };
}
