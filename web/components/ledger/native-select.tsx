/**
 * A plain <select>, not the Radix Select: every filter bar in this dashboard
 * is a native GET form so search works with zero client JS, and Radix's
 * Select renders no real <select> for the browser to submit.
 */
export function NativeSelect({
  id,
  name,
  defaultValue,
  options,
  placeholder = "Any",
}: {
  id: string;
  name: string;
  defaultValue: string;
  options: string[];
  placeholder?: string;
}) {
  return (
    <select
      id={id}
      name={name}
      defaultValue={defaultValue}
      className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
    >
      {options.map((o) => (
        <option key={o || "any"} value={o}>
          {o || placeholder}
        </option>
      ))}
    </select>
  );
}
