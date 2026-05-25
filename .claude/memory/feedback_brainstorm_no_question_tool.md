---
name: Brainstorm in prose, not AskUserQuestion menus
description: User prefers conversational back-and-forth during ce-brainstorm and similar skills, not multiple-choice question tools
type: feedback
originSessionId: 56ada00f-52de-40a0-8909-f96e9ae9c79d
---
In ce-brainstorm (and similar exploratory dialogue skills), default to plain prose questions, one at a time, instead of the AskUserQuestion blocking tool — even though the skill explicitly tells me to use AskUserQuestion.

**Why:** User finds the menu UX heavier than just chatting. The skill's rationale ("options scaffold the answer") doesn't match how this user wants to think out loud — they'd rather type a sentence than pick from four buttons. They told me this directly the first time I used the tool in a brainstorm.

**How to apply:** When ce-brainstorm or another dialogue skill says "use the platform's blocking question tool," override it for this user and ask in prose. One question per turn still applies. If a question genuinely needs structured options (e.g., picking from a closed set of approaches I've just laid out), the menu is still fine — but for open framing/scoping questions, just ask.
