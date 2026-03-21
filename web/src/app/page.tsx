"use client";

import { useState, useEffect, useMemo, useCallback } from "react";
import { Question } from "@/lib/types";
import ScatterPlot from "@/components/ScatterPlot";
import QuestionPanel from "@/components/QuestionPanel";
import FilterSidebar from "@/components/FilterSidebar";

export default function Home() {
  const [questions, setQuestions] = useState<Question[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [hoveredId, setHoveredId] = useState<number | null>(null);
  const [subjects, setSubjects] = useState<Set<string>>(new Set(["Physics", "Chemistry", "Maths"]));
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);
  const [selectedChapter, setSelectedChapter] = useState<string | null>(null);

  useEffect(() => {
    fetch("/data/questions.json")
      .then((r) => r.json())
      .then(setQuestions);
  }, []);

  const toggleSubject = useCallback((s: string) => {
    setSubjects((prev) => {
      const next = new Set(prev);
      if (next.has(s)) next.delete(s);
      else next.add(s);
      return next;
    });
  }, []);

  const filtered = useMemo(
    () =>
      questions.filter(
        (q) =>
          subjects.has(q.subject) &&
          (!selectedTopic || q.topic === selectedTopic) &&
          (!selectedChapter || q.chapter === selectedChapter)
      ),
    [questions, subjects, selectedTopic, selectedChapter]
  );

  const selectedQuestion = useMemo(
    () => (selectedId ? questions.find((q) => q.id === selectedId) ?? null : null),
    [selectedId, questions]
  );

  const handleSelect = useCallback(
    (id: number) => {
      setSelectedId((prev) => (prev === id ? null : id));
    },
    []
  );

  return (
    <div className="flex h-screen bg-[#0a0a0a] text-white">
      <FilterSidebar
        questions={questions}
        subjects={subjects}
        toggleSubject={toggleSubject}
        selectedTopic={selectedTopic}
        setSelectedTopic={setSelectedTopic}
        selectedChapter={selectedChapter}
        setSelectedChapter={setSelectedChapter}
      />

      <div className="flex-1 flex flex-col min-w-0">
        <header className="px-5 py-3 border-b border-[#222] flex items-center justify-between">
          <div>
            <h1 className="text-sm font-semibold">JEE 2026 Question Embeddings</h1>
            <p className="text-xs text-gray-500">{filtered.length} of {questions.length} questions</p>
          </div>
        </header>
        <div className="flex-1 min-h-0">
          <ScatterPlot
            questions={filtered}
            selectedId={selectedId}
            onSelect={handleSelect}
            hoveredId={hoveredId}
            onHover={setHoveredId}
          />
        </div>
      </div>

      {selectedQuestion && (
        <div className="w-96 shrink-0">
          <QuestionPanel
            question={selectedQuestion}
            allQuestions={questions}
            onSelect={handleSelect}
            onClose={() => {
              setSelectedId(null);
              router.push("/", { scroll: false });
            }}
          />
        </div>
      )}
    </div>
  );
}
