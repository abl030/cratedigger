---
name: feedback-opus-for-reviews
description: "Spawn review/verification subagents on opus, not fable — user doesn't want fable credits burned on reviews"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 5d6d12b6-0510-4967-8e58-68ad6ff0058b
---

When spawning code-review or verification subagents (the pre-commit review gate, adversarial passes), set `model: "opus"` on the Agent call instead of letting it inherit fable.

**Why:** the user interrupted a review spawn on 2026-07-02: "dont want to waste valuable fable credits on those reviews, if your going to review, use opus please."

**How to apply:** `Agent(subagent_type="general-purpose", model="opus", ...)` for review passes. Fable stays for the main-loop implementation work.
