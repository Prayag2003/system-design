# Instructions

Guidelines for contributing to this repo. We'll iterate on these as the repo grows.

## Writing Style

- Keep explanations simple and conversational. Avoid jargon unless it's being defined.
- Use real-world examples to motivate concepts before diving into the how.
- Use diagrams (Mermaid) when they make the explanation clearer.
- No em dashes.
- Structure each concept file with: **What it is -> Why it matters -> How it works**.

## File Organization

- One concept per file in `concepts/`.
- Implementation projects go in `implementations/`, each in its own subfolder.
- Case studies (full system design breakdowns) go in `case-studies/`.

## Concept File Template

```markdown
# [Topic Name]

One-liner description.

## Why [Topic]?

Motivate the problem first. Use a relatable example.

## How [Topic] Works

### Core Concepts

Explain the building blocks.

### [Key Properties / Trade-offs / Patterns]

Go deeper where needed.
```

## Updating the Index

When adding a new file, add a row to the table in `README.md`.

