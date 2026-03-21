export interface Question {
  id: number;
  question: string;
  options: string[];
  type: string;
  correct_answer: string;
  explanation: string;
  subject: string;
  topic: string;
  chapter: string;
  image_url?: string;
  x: number;
  y: number;
  embedding: number[];
}

export const SUBJECT_COLORS: Record<string, string> = {
  Physics: "#3b82f6",
  Chemistry: "#22c55e",
  Maths: "#f97316",
};
