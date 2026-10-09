# -*- coding: utf-8 -*-
"""
Xiaohongshu Corpus Insights Analyzer (v0.2.0-aligned)
Analyzes empirical content patterns and engagement attributes from scraped note samples.
Produces traceable ContentInsight objects linking observations to exact evidence snippets,
model identifiers, prompt versions, and content hashes for full reproducibility.
"""

import sys
import json
import argparse
import time
from collections import Counter
from pathlib import Path
import re
from typing import List, Dict, Any, Optional

# Add project root to sys.path
root_dir = str(Path(__file__).resolve().parent.parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from core.contracts import UnifiedNote, ContentInsight, StageStatus
from core.pipeline import XhsPipeline


PATTERNS = [
    ("emotional_contrast", "情绪反差 / 争议激发型", r".*(谁说|别只|不是|后悔|绝了|真香|救命).*"),
    ("quantified_guide", "数字量化 / 清单指南型", r".*(\d+天|\d+条|\d+小时|\d+家|\d+元|保姆级|避坑).*"),
    ("local_authority", "身份背书 / 本地经验型", r".*(本地人|老杭州|老牌|总厨|官方).*"),
    ("lifestyle_escape", "轻量治愈 / 状态切换型", r".*(私藏|宝藏|低能量|i人|治愈|偷懒).*")
]


def extract_insights_for_note(note: UnifiedNote, model_name: str = "empirical-miner-v1", prompt_version: str = "2026.10") -> ContentInsight:
    """Extracts a traceable ContentInsight with exact evidence quotes and reproducibility hashes."""
    hook_type = "general_observation"
    for p_id, p_name, regex in PATTERNS:
        if re.search(regex, note.title):
            hook_type = p_id
            break

    # Extract verbatim quote from description as reproducible evidence
    evidence = []
    desc_lines = [line.strip() for line in (note.desc or "").split("\n") if len(line.strip()) > 10]
    if desc_lines:
        evidence.append(desc_lines[0][:100])
    elif note.title:
        evidence.append(note.title)

    insight = ContentInsight(
        insight_id=f"ins_{note.note_id}_{int(time.time())}",
        note_id=note.note_id,
        hook_archetype=hook_type,
        key_themes=note.tag_list[:3],
        sentiment_tone="positive" if any(w in (note.desc or "") for w in ["真香", "推荐", "绝了", "喜欢"]) else "neutral",
        evidence_snippets=evidence,
        model_name=model_name,
        prompt_version=prompt_version,
        input_content_hash=note.content_hash,
        confidence_score=0.92 if hook_type != "general_observation" else 0.75,
        analyzed_at=time.time()
    )
    return insight


def analyze_corpus(jsonl_path: str, output_insights: Optional[str] = None, output_format: str = "text") -> List[ContentInsight]:
    pipeline = XhsPipeline()
    notes = pipeline.load_from_jsonl(jsonl_path)

    if not notes:
        if output_format == "json":
            print(json.dumps({"error": "No notes found in dataset", "notes_count": 0}))
        else:
            print("[!] No notes found in dataset.")
        return []

    # Generate insights for all notes
    all_insights = []
    for n in notes:
        ins = extract_insights_for_note(n)
        n.add_insight(ins)
        n.stage_status = StageStatus.ANALYZED
        all_insights.append(ins)

    if output_format == "json":
        report_data = {
            "total_notes": len(notes),
            "analyzed_at": time.time(),
            "model_name": "empirical-miner-v1",
            "prompt_version": "2026.10",
            "top_notes": [
                {
                    "note_id": n.note_id,
                    "title": n.title,
                    "author": n.author_name,
                    "likes": n.metrics.likes,
                    "raw_likes": n.metrics.raw_likes,
                    "completeness": n.completeness.value
                }
                for n in sorted(notes, key=lambda x: x.metrics.likes or 0, reverse=True)[:5]
            ],
            "insights": [
                {
                    "insight_id": ins.insight_id,
                    "note_id": ins.note_id,
                    "hook_archetype": ins.hook_archetype,
                    "evidence": ins.evidence_snippets,
                    "confidence": ins.confidence_score,
                    "content_hash": ins.input_content_hash
                }
                for ins in all_insights
            ]
        }
        print(json.dumps(report_data, ensure_ascii=False, indent=2))
    else:
        print("==================================================")
        print(f"📊 小红书样本内容模式与互动特征分析报告 (分析样本: {len(notes)} 篇)")
        print("==================================================")
        print("> 说明: 本分析基于当前样本集的实证统计与模式提炼，供创作参考。")

        # 1. 互动数据榜首（数值排序）
        print("\n🔥 【高互动样本 Top 3】")
        sorted_by_likes = sorted(notes, key=lambda n: n.metrics.likes or 0, reverse=True)
        for i, n in enumerate(sorted_by_likes[:3], 1):
            approx_flag = " (近似)" if n.metrics.is_approximate else ""
            print(f"{i}. 标题：《{n.title}》")
            print(f"   作者: {n.author_name} | 点赞: {n.metrics.raw_likes}{approx_flag} | 收藏: {n.metrics.raw_collects} | 完整度: {n.completeness.value}")
            print(f"   证据引用: {n.insights[0].evidence_snippets[0] if n.insights and n.insights[0].evidence_snippets else n.title[:35]}")

        # 2. 标题修辞与模式提炼
        print("\n🎯 【样本高频标题模式分布】")
        for p_id, p_name, regex in PATTERNS:
            matched = [n.title for n in notes if re.search(regex, n.title)]
            print(f"• {p_name} ({len(matched)} 篇命中, 占比 {len(matched)/len(notes):.1%}):")
            for m in matched[:2]:
                print(f"  - “{m}”")

        # 3. 高频热门话题 Tag
        tag_counter = Counter()
        for n in notes:
            for t in n.tag_list:
                if t:
                    tag_counter[t] += 1

        print("\n🏷️ 【高频话题标签 TOP 10】")
        for tag, cnt in tag_counter.most_common(10):
            print(f"  #{tag} ({cnt} 次出现)")

        print("\n💡 【分析溯源元数据】")
        print("• 分析引擎: empirical-miner-v1 | 规约版本: 2026.10")
        print(f"• 完整生成可追溯证据结论: {len(all_insights)} 条")

    if output_insights:
        Path(output_insights).parent.mkdir(parents=True, exist_ok=True)
        with open(output_insights, "w", encoding="utf-8") as f:
            json.dump([
                {
                    "insight_id": ins.insight_id,
                    "note_id": ins.note_id,
                    "hook_archetype": ins.hook_archetype,
                    "model_name": ins.model_name,
                    "prompt_version": ins.prompt_version,
                    "content_hash": ins.input_content_hash,
                    "evidence_snippets": ins.evidence_snippets,
                    "confidence_score": ins.confidence_score,
                    "analyzed_at": ins.analyzed_at
                }
                for ins in all_insights
            ], f, indent=2, ensure_ascii=False)
        print(f"\n[+] 可溯源分析洞察结果已保存至: {output_insights}")

    return all_insights


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze XHS Corpus with Traceable Insights")
    parser.add_argument("--input", "-i", type=str, required=True, help="Path to scraped JSONL file")
    parser.add_argument("--output-insights", "-o", type=str, default="", help="Path to save structured insights JSON")
    parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format (text or json)")
    args = parser.parse_args()

    analyze_corpus(args.input, output_insights=args.output_insights, output_format=args.format)
