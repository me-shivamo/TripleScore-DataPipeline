"use client";

import { useState, useMemo } from "react";
import { Question, SUBJECT_COLORS } from "@/lib/types";
import { renderLatex } from "@/lib/latex";
import NearbyQuestions from "./NearbyQuestions";
import { findNearest } from "@/lib/similarity";

interface Props {
  question: Question;
  allQuestions: Question[];
  onSelect: (id: number) => void;
  onClose: () => void;
}

export default function QuestionPanel({ question, allQuestions, onSelect, onClose }: Props) {
  const [showAnswer, setShowAnswer] = useState(false);

  const nearbyIds = useMemo(
    () => findNearest(question.embedding, allQuestions, question.id, 5),
    [question, allQuestions]
  );

  const nearbyQuestions = useMemo(
    () => nearbyIds.map((id) => allQuestions.find((q) => q.id === id)!),
    [nearbyIds, allQuestions]
  );

  return (
    <div className="h-full overflow-y-auto p-5 bg-[#0f0f0f] border-l border-[#222]">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-white">Q{question.id}</h2>
        <button onClick={onClose} className="text-gray-500 hover:text-white text-xl leading-none">&times;</button>
      </div>

      {/* Tags */}
      <div className="flex flex-wrap gap-2 mb-4">
        <span
          className="px-2 py-0.5 rounded text-xs font-medium"
          style={{ backgroundColor: SUBJECT_COLORS[question.subject] + "22", color: SUBJECT_COLORS[question.subject] }}
        >
          {question.subject}
        </span>
        <span className="px-2 py-0.5 rounded text-xs bg-[#1a1a1a] text-gray-400">{question.topic}</span>
        <span className="px-2 py-0.5 rounded text-xs bg-[#1a1a1a] text-gray-400">{question.chapter}</span>
        <span className="px-2 py-0.5 rounded text-xs bg-[#1a1a1a] text-gray-500">{question.type}</span>
      </div>

      {/* Question */}
      <div
        className="text-gray-200 text-sm leading-relaxed mb-4 latex-content"
        dangerouslySetInnerHTML={{ __html: renderLatex(question.question) }}
      />

      {/* Options */}
      {question.options.length > 0 && (
        <div className="space-y-2 mb-4">
          {question.options.map((opt, i) => (
            <div key={i} className="flex gap-2 text-sm text-gray-300 bg-[#1a1a1a] rounded px-3 py-2">
              <span className="text-gray-500 font-medium">{String.fromCharCode(65 + i)}.</span>
              <span className="latex-content" dangerouslySetInnerHTML={{ __html: renderLatex(opt) }} />
            </div>
          ))}
        </div>
      )}

      {/* Answer */}
      {!showAnswer ? (
        <button
          onClick={() => setShowAnswer(true)}
          className="px-4 py-2 bg-[#1a1a1a] hover:bg-[#222] text-gray-300 text-sm rounded border border-[#333] mb-4"
        >
          Show Answer
        </button>
      ) : (
        <div className="mb-4 p-3 bg-[#111] border border-[#2a2a2a] rounded">
          <div className="text-xs text-gray-500 mb-1">Answer</div>
          <div className="text-green-400 text-sm font-medium mb-2 latex-content" dangerouslySetInnerHTML={{ __html: renderLatex(question.correct_answer) }} />
          <div className="text-xs text-gray-500 mb-1">Explanation</div>
          <div
            className="text-gray-300 text-sm leading-relaxed latex-content"
            dangerouslySetInnerHTML={{ __html: renderLatex(question.explanation) }}
          />
        </div>
      )}

      {/* Nearby Questions */}
      <div className="mt-6 pt-4 border-t border-[#222]">
        <h3 className="text-sm font-medium text-gray-400 mb-3">Similar Questions</h3>
        <NearbyQuestions questions={nearbyQuestions} onSelect={onSelect} />
      </div>
    </div>
  );
}
