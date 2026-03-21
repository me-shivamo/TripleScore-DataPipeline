"use client";

import { Question, SUBJECT_COLORS } from "@/lib/types";

interface Props {
  questions: Question[];
  onSelect: (id: number) => void;
}

export default function NearbyQuestions({ questions, onSelect }: Props) {
  return (
    <div className="space-y-2">
      {questions.map((q) => (
        <button
          key={q.id}
          onClick={() => onSelect(q.id)}
          className="w-full text-left flex items-start gap-2 p-2 rounded hover:bg-[#1a1a1a] transition-colors"
        >
          <span
            className="w-2 h-2 rounded-full mt-1.5 shrink-0"
            style={{ backgroundColor: SUBJECT_COLORS[q.subject] }}
          />
          <div className="min-w-0">
            <span className="text-xs text-gray-500">Q{q.id}</span>
            <p className="text-xs text-gray-400 truncate">
              {q.question.replace(/\$[^$]*\$/g, "[math]").replace(/\n/g, " ").slice(0, 80)}...
            </p>
          </div>
        </button>
      ))}
    </div>
  );
}
