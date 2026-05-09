---
name: Prose questions over picker UI
description: User dislikes the AskUserQuestion picker — default to plain prose questions in chat for any interactive elicitation
type: feedback
originSessionId: f6606859-6c67-4751-991f-4abbbcaf1584
---
User dislikes the `AskUserQuestion` picker UI ("i hate the question prompt"). Default to plain prose questions in chat for any interactive elicitation — brainstorming, scope clarification, approach choice, requirements gathering.

**Why:** They find the picker friction-heavy and prefer free-flowing chat; they explicitly asked to drop out of it during ce-brainstorm on issue 226 (2026-05-09).

**How to apply:** When a skill defaults to a blocking question tool (ce-brainstorm, ce-plan, etc.), override and use prose. Still follow the underlying discipline: one question per turn, offer 2-4 distinct options when narrowing, present them as a numbered list in markdown. The user can answer with a number, label, or free text. Don't add "should I switch to prose?" confirmation steps — just do it.
