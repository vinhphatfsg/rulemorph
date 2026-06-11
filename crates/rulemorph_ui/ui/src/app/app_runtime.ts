import { type Dispatch, type SetStateAction, useEffect, useState } from "react";
import { captureAuthSecretsFromLocation } from "../api/auth";
import type { DurationUnit } from "../trace_list/trace_list_helpers";

const TRACE_DURATION_UNIT_STORAGE_KEY = "traceDurationUnit";

function resolveStorage(storage?: Storage): Storage | null {
  if (storage) return storage;
  if (typeof window === "undefined") return null;
  return window.localStorage;
}

export function resolveStoredDurationUnit(storage?: Storage): DurationUnit {
  const stored = resolveStorage(storage)?.getItem(TRACE_DURATION_UNIT_STORAGE_KEY);
  return stored === "ms" ? "ms" : "us";
}

export function persistDurationUnit(unit: DurationUnit, storage?: Storage): void {
  resolveStorage(storage)?.setItem(TRACE_DURATION_UNIT_STORAGE_KEY, unit);
}

export function useAuthSecretCapture(): void {
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
}

export function useStoredDurationUnit(): [
  DurationUnit,
  Dispatch<SetStateAction<DurationUnit>>
] {
  const [durationUnit, setDurationUnit] = useState<DurationUnit>(() => resolveStoredDurationUnit());
  useEffect(() => {
    persistDurationUnit(durationUnit);
  }, [durationUnit]);
  return [durationUnit, setDurationUnit];
}
