import clsx from "clsx";

export function renderJsonBlock(label: string, value: unknown) {
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
      <pre className="inspector-block__body">
        {hasValue ? JSON.stringify(value, null, 2) : "なし"}
      </pre>
    </div>
  );
}
