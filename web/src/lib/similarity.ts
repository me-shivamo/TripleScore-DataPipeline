export function cosineSimilarity(a: number[], b: number[]): number {
  let dot = 0, magA = 0, magB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    magA += a[i] * a[i];
    magB += b[i] * b[i];
  }
  return dot / (Math.sqrt(magA) * Math.sqrt(magB));
}

export function findNearest(
  targetEmbedding: number[],
  questions: { id: number; embedding: number[] }[],
  targetId: number,
  n: number = 5
): number[] {
  const scores = questions
    .filter((q) => q.id !== targetId)
    .map((q) => ({ id: q.id, score: cosineSimilarity(targetEmbedding, q.embedding) }))
    .sort((a, b) => b.score - a.score);
  return scores.slice(0, n).map((s) => s.id);
}
