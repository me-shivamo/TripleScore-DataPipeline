"use client";

import { useEffect, useState, useMemo, use } from "react";
import { useRouter } from "next/navigation";
import { Question } from "@/lib/types";
import QuestionPanel from "@/components/QuestionPanel";

export default function QuestionPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const router = useRouter();
  const [questions, setQuestions] = useState<Question[]>([]);

  useEffect(() => {
    fetch("/data/questions.json")
      .then((r) => r.json())
      .then(setQuestions);
  }, []);

  const question = useMemo(
    () => questions.find((q) => q.id === Number(id)) ?? null,
    [questions, id]
  );

  if (!questions.length) return <div className="flex h-screen items-center justify-center bg-[#0a0a0a] text-gray-500">Loading...</div>;
  if (!question) return <div className="flex h-screen items-center justify-center bg-[#0a0a0a] text-gray-500">Question not found</div>;

  return (
    <div className="flex h-screen bg-[#0a0a0a]">
      <div className="flex-1 flex items-center justify-center">
        <button
          onClick={() => router.push("/")}
          className="text-gray-500 hover:text-white text-sm"
        >
          ← Back to visualization
        </button>
      </div>
      <div className="w-[480px] shrink-0">
        <QuestionPanel
          question={question}
          allQuestions={questions}
          onSelect={(qid) => router.push(`/q/${qid}`)}
          onClose={() => router.push("/")}
        />
      </div>
    </div>
  );
}
