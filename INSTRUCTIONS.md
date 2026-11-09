# Instructions

Guidelines for contributing to this repo. We'll iterate on these as the repo grows. Think of preparing for a System Design Interview (including HLD, LLD/Machine Coding rounds) for product-based companies.

## Writing Style

- Keep explanations simple and conversational but do not loose any information/facts. Avoid jargon unless it's being defined.
- Use real-world engineering/industry standard examples to motivate concepts before diving into the how.
- Use diagrams (Mermaid) when they make the explanation clearer. Keep diagrams detail oriented and not just arrows and boxes.
- try to include the excalidraw based diagrams for better understanding
- Use index/tables for definitions and quick-reference summaries.
- No em dashes.
- Check out for the #TODO and complete that assigned task.
- Structure each concept file with: **What it is -> Why it matters -> How it works -> Summary**.
- **Do not loose any information written by the author, feel free to try to improve on it and add your own real-world engineering knowledge.** Do not generate new topics or sections that haven't been provided. Keep it to what's there.

## File Organization

- Each concept gets its own folder under `concepts/` (e.g., `concepts/kafka/`).
- Split content into sequential numbered files (e.g., `01-the-problem.md`, `02-deep-dive.md`, `03-qna.md`) to keep each file focused and digestible.
- Each concept folder has a `README.md` that indexes its files.
- Implementation projects go in `implementations/`, each in its own subfolder.
- Case studies (full system design breakdowns) go in `case-studies/`.

## Concept File Template

```markdown
# [Topic Name]

1-2 liner description.

---

## Contents

- [Why Topic?](#why-topic)
  - [Sub-section](#sub-section)
- [How Topic Works](#how-topic-works)
  - [Core Concepts](#core-concepts)
  - [Deeper Section](#deeper-section)
- [Summary](#summary)

---

## Why [Topic]?

Motivate the problem first. Use a relatable example.

## How [Topic] Works

### Core Concepts

| Term | What it is |
|------|------------|
| **Term 1** | Definition. |
| **Term 2** | Definition. |

### [Key Properties / Trade-offs / Patterns]

Go deeper where needed. Use diagrams, tables, and examples.

---

## Summary

| Concept | One-liner |
|---------|-----------|
| **Concept 1** | Quick summary. |
| **Concept 2** | Quick summary. |
```

## Updating the Index

When adding a new file, add a row to the table in `README.md`.

## Commits

- Use the format `[type-scope]: message`
  - `[chore-docs]: update instructions template`
  - `[feat-kafka]: add async processing options`
  - `[fix-kafka]: fix mermaid diagram syntax`
- Keep commits incremental and focused. One logical change per commit.
- Always ask before committing or pushing.
