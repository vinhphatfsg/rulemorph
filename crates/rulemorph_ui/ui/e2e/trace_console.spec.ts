import { test } from "@playwright/test";
import { registerTraceConsoleDetailTests } from "./trace_console/detail";
import {
  registerTraceConsoleBasicFallbackTest,
  registerTraceConsoleBrokenFallbackTest
} from "./trace_console/fallback";
import { registerTraceConsoleImportTests } from "./trace_console/import";
import { registerTraceConsoleNormalizationTests } from "./trace_console/normalization";

test.describe.configure({ mode: "serial" });

registerTraceConsoleDetailTests();
registerTraceConsoleImportTests();
registerTraceConsoleBasicFallbackTest();
registerTraceConsoleNormalizationTests();
registerTraceConsoleBrokenFallbackTest();
