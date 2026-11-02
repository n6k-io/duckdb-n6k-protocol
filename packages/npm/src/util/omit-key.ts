export function omitKey<V>(
  obj: Record<string, V>,
  key: string,
): Record<string, V> {
  if (!(key in obj)) return obj;
  const next = { ...obj };
  delete next[key];
  return next;
}
