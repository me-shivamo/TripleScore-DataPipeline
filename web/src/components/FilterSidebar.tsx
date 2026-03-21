"use client";

import { Question, SUBJECT_COLORS } from "@/lib/types";
import { useMemo } from "react";

interface Props {
  questions: Question[];
  subjects: Set<string>;
  toggleSubject: (s: string) => void;
  selectedTopic: string | null;
  setSelectedTopic: (t: string | null) => void;
  selectedChapter: string | null;
  setSelectedChapter: (c: string | null) => void;
}

export default function FilterSidebar({
  questions,
  subjects,
  toggleSubject,
  selectedTopic,
  setSelectedTopic,
  selectedChapter,
  setSelectedChapter,
}: Props) {
  const topics = useMemo(() => [...new Set(questions.map((q) => q.topic))].sort(), [questions]);
  const chapters = useMemo(() => [...new Set(questions.map((q) => q.chapter))].sort(), [questions]);

  return (
    <div className="w-56 shrink-0 p-4 bg-[#0f0f0f] border-r border-[#222] overflow-y-auto">
      <h2 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">Subjects</h2>
      <div className="space-y-2 mb-6">
        {Object.keys(SUBJECT_COLORS).map((s) => (
          <label key={s} className="flex items-center gap-2 cursor-pointer text-sm">
            <input
              type="checkbox"
              checked={subjects.has(s)}
              onChange={() => toggleSubject(s)}
              className="accent-current"
              style={{ accentColor: SUBJECT_COLORS[s] }}
            />
            <span className="w-2 h-2 rounded-full" style={{ backgroundColor: SUBJECT_COLORS[s] }} />
            <span className="text-gray-300">{s}</span>
          </label>
        ))}
      </div>

      <h2 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Topic</h2>
      <select
        value={selectedTopic || ""}
        onChange={(e) => setSelectedTopic(e.target.value || null)}
        className="w-full mb-6 bg-[#1a1a1a] border border-[#333] text-gray-300 text-xs rounded px-2 py-1.5"
      >
        <option value="">All Topics</option>
        {topics.map((t) => (
          <option key={t} value={t}>{t}</option>
        ))}
      </select>

      <h2 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Chapter</h2>
      <select
        value={selectedChapter || ""}
        onChange={(e) => setSelectedChapter(e.target.value || null)}
        className="w-full bg-[#1a1a1a] border border-[#333] text-gray-300 text-xs rounded px-2 py-1.5"
      >
        <option value="">All Chapters</option>
        {chapters.map((c) => (
          <option key={c} value={c}>{c}</option>
        ))}
      </select>
    </div>
  );
}
