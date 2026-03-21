"use client";

import { useRef, useEffect, useCallback } from "react";
import { Question, SUBJECT_COLORS } from "@/lib/types";
import { zoom as d3Zoom, zoomIdentity, ZoomTransform } from "d3-zoom";
import { select } from "d3-selection";

interface Props {
  questions: Question[];
  selectedId: number | null;
  onSelect: (id: number) => void;
  hoveredId: number | null;
  onHover: (id: number | null) => void;
}

const PADDING = 40;
const DOT_RADIUS = 6;
const SELECTED_RADIUS = 10;

export default function ScatterPlot({ questions, selectedId, onSelect, hoveredId, onHover }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const transformRef = useRef<ZoomTransform>(zoomIdentity);

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);

    const w = rect.width;
    const h = rect.height;
    const t = transformRef.current;

    ctx.clearRect(0, 0, w, h);
    ctx.save();
    ctx.translate(t.x, t.y);
    ctx.scale(t.k, t.k);

    for (const q of questions) {
      const cx = PADDING + q.x * (w - 2 * PADDING);
      const cy = PADDING + q.y * (h - 2 * PADDING);
      const isSelected = q.id === selectedId;
      const isHovered = q.id === hoveredId;
      const r = (isSelected ? SELECTED_RADIUS : isHovered ? DOT_RADIUS + 2 : DOT_RADIUS) / t.k;

      ctx.beginPath();
      ctx.arc(cx, cy, r, 0, Math.PI * 2);
      ctx.fillStyle = SUBJECT_COLORS[q.subject] || "#888";
      ctx.globalAlpha = isSelected || isHovered ? 1 : 0.7;
      ctx.fill();

      if (isSelected) {
        ctx.strokeStyle = "#fff";
        ctx.lineWidth = 2 / t.k;
        ctx.stroke();
      }
      ctx.globalAlpha = 1;
    }

    ctx.restore();

    // Tooltip in screen space
    if (hoveredId) {
      const q = questions.find((q) => q.id === hoveredId);
      if (q) {
        const cx = t.applyX(PADDING + q.x * (w - 2 * PADDING));
        const cy = t.applyY(PADDING + q.y * (h - 2 * PADDING));
        const label = `Q${q.id}: ${q.subject} - ${q.topic}`;
        ctx.font = "12px Inter, sans-serif";
        const metrics = ctx.measureText(label);
        const tw = metrics.width + 16;
        const th = 28;
        let tx = cx - tw / 2;
        let ty = cy - SELECTED_RADIUS - th - 8;
        if (tx < 0) tx = 4;
        if (tx + tw > w) tx = w - tw - 4;
        if (ty < 0) ty = cy + SELECTED_RADIUS + 8;

        ctx.fillStyle = "#1a1a1a";
        ctx.strokeStyle = "#333";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.roundRect(tx, ty, tw, th, 6);
        ctx.fill();
        ctx.stroke();

        ctx.fillStyle = "#fff";
        ctx.fillText(label, tx + 8, ty + 18);
      }
    }
  }, [questions, selectedId, hoveredId]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const zoomBehavior = d3Zoom<HTMLCanvasElement, unknown>()
      .scaleExtent([0.3, 20])
      .on("zoom", (event) => {
        transformRef.current = event.transform;
        draw();
      });

    const sel = select(canvas);
    sel.call(zoomBehavior);
    sel.on("dblclick.zoom", null);

    draw();
    const onResize = () => draw();
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      sel.on(".zoom", null);
    };
  }, [draw]);

  const getQuestionAt = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas) return null;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const w = rect.width;
    const h = rect.height;
    const t = transformRef.current;

    let closest: Question | null = null;
    let closestDist = Infinity;

    for (const q of questions) {
      const cx = t.applyX(PADDING + q.x * (w - 2 * PADDING));
      const cy = t.applyY(PADDING + q.y * (h - 2 * PADDING));
      const dist = Math.sqrt((mx - cx) ** 2 + (my - cy) ** 2);
      if (dist < 20 && dist < closestDist) {
        closest = q;
        closestDist = dist;
      }
    }
    return closest;
  };

  return (
    <div ref={containerRef} className="w-full h-full relative">
      <canvas
        ref={canvasRef}
        className="w-full h-full cursor-crosshair"
        onClick={(e) => {
          const q = getQuestionAt(e);
          if (q) onSelect(q.id);
        }}
        onMouseMove={(e) => {
          const q = getQuestionAt(e);
          onHover(q?.id ?? null);
        }}
        onMouseLeave={() => onHover(null)}
      />
      {/* Legend */}
      <div className="absolute top-3 right-3 bg-[#111] border border-[#333] rounded-lg px-3 py-2 text-xs flex flex-col gap-1">
        {Object.entries(SUBJECT_COLORS).map(([subject, color]) => (
          <div key={subject} className="flex items-center gap-2">
            <span className="w-2.5 h-2.5 rounded-full inline-block" style={{ backgroundColor: color }} />
            <span className="text-gray-300">{subject}</span>
          </div>
        ))}
      </div>
      <div className="absolute bottom-3 left-3 text-[10px] text-gray-600">
        Scroll to zoom · Drag to pan
      </div>
    </div>
  );
}
