# Kaiiron marketing strategy

Revised 2026-09-08 to reflect the product owner's direction: sell ambitious project delegation, longer runs, and affordable scale to nontechnical users of agents. The audience choice is a strategic direction, not a measured claim about current market demographics. Customer motivations and launch priorities below are hypotheses to validate. Technical capability claims remain grounded in the implementation and sources listed below.

## Positioning

**Stop managing tasks. Start delegating projects.**

Kaiiron makes longer agent runs more affordable, giving people room to delegate whole projects, pursue more alternatives, and put more agents to work on ambitious goals.

The customer wants to stop being the bottleneck between every small task. They want to set a worthwhile destination, give agents time to pursue it, and spend their own attention on decisions. Lead with that change in how they work. Make affordable longer runs the reason bigger asks become practical. Technical simplicity alone is not a selling point; replacing jargon with generic phrases such as “explore an idea” loses the ambition.

Long-horizon agents remain the product thesis. Translate that into the customer's experience: work that takes more than one answer, involves several steps, and may take time. The value is a larger practical scope for delegation, including more thorough work and more frequent use. Lower processing costs support that outcome.

The current implementation is a local inference proxy for existing agents. That definition belongs in technical documentation. Kaiiron does not itself conduct research, plan a project, supervise an agent, or restore a stopped agent's tool state. Attribute task capabilities to the user's agent, with Kaiiron helping make its AI work more affordable.

## Audience and customer evaluation

The primary marketing audience is **people who use agents to accomplish work**, including people with no programming background. Recognizing the name Codex, OpenCode, or Pi does not imply that a reader understands APIs, tokens, command lines, or databases.

| Priority | Audience | Desired outcome | Why Kaiiron could matter | Main adoption barrier | First experience |
|---|---|---|---|---|---|
| 1 | Individuals and small teams already asking agents for help with projects | Delegate a longer task they would otherwise postpone or do themselves | More of their work becomes affordable to hand over | Understanding the wait, separate AI charges, and obtaining setup help | One useful task with a clear result and flexible deadline |
| 1 | Researchers, consultants, and operations staff using agents with their own materials | Compare information, develop a plan, or organize a substantial body of work | Room for more thorough exploration and repeated refinement | Trust in the output and clarity about which materials the agent can use | A comparison or draft based on material they provide |
| 2 | Creators and small business owners developing ideas with an agent | Explore alternatives, refine materials, and move a project forward | More ideas become worth trying | Setup difficulty and uncertainty about the eventual cost | One reviewable piece of an existing project |
| Enabler | Technical colleagues, agent builders, and implementers | Help another person get connected and use the tool successfully | Existing-agent compatibility and inspectable operational behavior | Installation, client limitations, and continuation requirements | Accurate setup and troubleshooting documentation |

These use cases rely on capabilities already available in the user's agent. Do not imply that Kaiiron adds browsing, a particular business integration, research verification, or document editing tools. Tailor a pilot to the tools and materials the participant actually has.

The current release requires technical installation. This limits immediate adoption but should not determine the language of the marketing page. Treat the person who understands the value and the person who performs setup as potentially different people. Clearly acknowledge the setup requirement, then provide a guide suitable for a helper. Do not imply a one-click consumer installer, managed service, or support team exists.

## Customer values and supporting evidence

| Customer value | Plain-language expression | Supporting basis | Boundary |
|---|---|---|---|
| More possibilities within reach | Stop managing tasks. Start delegating projects. | Lower eligible processing rates can make previously uneconomical tasks worthwhile | Validate with actual new work attempted and completed |
| More thorough work | Give big asks the time they deserve | A lower cost per inference can support additional steps | More time or more steps does not guarantee a better result |
| More frequent use | More agents. Longer runs. Bigger asks. | Better task economics can lower the threshold for delegation | Do not promise a fixed number of extra tasks or unlimited use |
| Familiarity and control | Keep the agent you know | Codex, OpenCode, and Pi integrations exist | Version and setup details belong in the guide |
| Affordable ambition | Shoot big. Don’t spend big. | OpenAI offers discounted asynchronous processing | Savings vary; work takes longer; completion time is not guaranteed |

Principles: make ambition accessible, respect the reader's time, preserve their control, and state material limitations plainly. The page should let someone decide whether the idea suits their work without learning the implementation.

## Public messaging

- **Headline:** Stop managing tasks. Start delegating projects.
- **Supporting copy:** Give your agents ambitious goals and the time to pursue them. Kaiiron makes longer runs more affordable, so more of the projects you’ve been putting off become worth taking on.
- **Time:** Give big asks the time they deserve.
- **Economics:** Shoot big. Don’t spend big.
- **Attention:** Why spend your evening switching between agents?
- **Scale:** More agents. Longer runs. Bigger asks.
- **Primary action:** Get started → practical setup expectations → technical guide for the user or their helper.
- **Secondary action:** Think bigger → examples of project-sized asks.
- **Message order:** A change in how the customer works; ambitious asks; affordable scale; overnight delegation habits; practical questions; start with the postponed project.

Use complete asks rather than generic categories: “Compare these proposals and recommend a plan,” “Develop three ways to launch this idea,” and “Turn these scattered materials into a complete guide.” Show an outcome someone can hand over and judge. The chosen agent must supply the tools to carry out each project.

“Stop managing tasks” expresses the delegation approach: define the project and reserve attention for judgment. Kaiiron currently supplies more economical inference, not full task supervision. The page attributes project execution to the user's existing agent and explains that it can still require input or approval.

Overnight is a useful way to frame flexible time, not a completion deadline. “Give it the night” and “Review progress when you return” fit the current product better than a promise of finished projects every morning. Avoid claiming unlimited runtime or that interrupted agents automatically restore themselves.

The owner's five-agents-to-twenty example identifies the desired value: more agent work for the same budget with less context switching. Do not publish that numerical comparison as a measured capability. It implies four times the work per dollar, which does not follow from a 50% provider processing discount. A defensible comparison needs matched work, actual bills, completion results, and a defined time window. Until measured, express the value through affordable scale without inventing a multiplier.

GitHub is available in the footer for readers who want the project. Source code is not the primary or secondary sales action. The technical guide retains accurate installation instructions and operational limits.

Do not invent pricing plans, a waitlist, customer logos, testimonials, measured savings, or a managed product. “Shoot big. Don’t spend big.” is supported by an explanation of lower-priced AI work and a short statement that savings vary; it is not a hard budget guarantee.

## Editorial rules

Write for an intelligent person who uses an agent but has no interest in its implementation. Technical accuracy is necessary; technical vocabulary is not a substitute for an explanation.

Keep the landing page free of commands, source-install instructions, database names, protocol names, request identities, token accounting, routing tables, transport behavior, job APIs, test matrices, and recovery contracts. Keep those details in the technical guide and repository.

Use direct, memorable language about ambition, attention, time, and money. Every section should give the reader a reason to change what they delegate. Show whole outcomes and meaningful choices, not a list of minor assistant chores. Avoid assuming the reader has a repository, knows what a migration is, or thinks in terms of inference budgets. Keep the claims precise without draining the headline of its point.

Retain facts that change the visitor's decision, stated simply:

- Kaiiron currently works with Codex, OpenCode, and Pi; Claude Code is not yet supported.
- This is for work that can wait. Tasks may take hours or days.
- Keep the agent running; closing it can interrupt the work.
- Kaiiron is free to use; the AI service charges separately. A ChatGPT subscription does not cover this usage.
- Installation currently requires technical experience, so some users will need help.

Do not turn these qualifications into a technical lecture. Avoid claiming automatic unattended operation, guaranteed deadlines, unlimited agents, fixed savings, or a hard spending cap. Preserve truth through the promise itself and brief practical answers.

## Alternatives and differentiation

The customer's most relevant alternatives are doing the work themselves, postponing it, asking the agent for a smaller task, or paying the normal rate for a faster result. Kaiiron earns adoption when a useful task becomes worth delegating despite the additional wait and setup effort.

For technical evaluators, native OpenAI Batch offers the same advertised processing discount; Kaiiron adds integration and durable inference behavior. Client-specific batch tools and custom agent runners are adjacent alternatives. These comparisons belong in the repository or technical discussions. A consumer landing page should not require understanding them.

## Adoption plan

### Validate comprehension with the intended audience

Show the page to nontechnical people who already use agents, including those whose setup is managed by someone else. Without explaining the product first, ask:

1. What would Kaiiron help you do?
2. Which project would you now consider delegating, and what would you let the agent do before checking in?
3. What would you expect to pay for, and how quickly would you expect the result?
4. What do you think you need to get started?

Listen for a clear understanding of more affordable longer tasks, the wait, and the current setup requirement. If people describe a fully autonomous employee, free AI usage, or an instant consumer app, fix the message.

### Establish one useful result

Recruit a small group with real tasks and flexible deadlines. Offer a bounded pilot using an existing supported agent and whatever setup help is actually available. No outreach or support service has been created as part of this work.

Use materials the participant can provide and a result they can judge: a comparison, an improved draft, or an organized guide. Record where setup required help, whether the wait was acceptable, the result's usefulness, actual AI charges, and whether the person would delegate another task.

### Publish understandable evidence

Build a case study around the person's goal, what the agent produced, elapsed time, actual cost, and what they chose to do next. Include the amount of human help required. Technical reproduction details can be linked separately.

Reach people through communities discussing practical agent use, research, creative projects, and small-team work. Develop examples around tasks people recognize; do not assume programming expertise in agent-user communities. No outreach has been sent.

### Remove observed adoption friction

If people understand the value but cannot get started, prioritize guided installation and clearer account/billing setup. If tasks fail when the agent stops, prioritize supported continuation. If charges are hard to understand, prioritize useful spending visibility and budget controls. These are product priorities to assess, not advertised current features.

## Measures of success

Primary outcome: **more useful work delegated and completed**. Track tasks previously left undone, broader task scope, and repeat agent use. Cost is part of that value, alongside waiting time, result quality, and human effort.

| Stage | Measurement | Decision |
|---|---|---|
| Comprehension | Can a nontechnical reader explain the value, wait, charges, and setup requirement? | Does the page communicate the product accurately? |
| Relevance | Can the person name a whole project they would hand over, with its desired outcome? | Does the message expand their ambition beyond small tasks? |
| Activation | A useful first result; amount of setup help and time required | Can the intended audience actually adopt the release? |
| Value | Newly attempted and completed work, actual AI charges, waiting time, usefulness | Does affordability expand worthwhile delegation? |
| Retention | A second independent task within two weeks, and why they returned or stopped | Is there recurring value? |

Proposed initial learning target: five people from the intended audience complete a useful task, and at least three choose a second task within two weeks. Record whether setup assistance was needed. These are proposed targets, not current results or established benchmarks.

The site has no analytics collector. Begin with opt-in interviews, pilot notes, issue reports, and aggregate repository traffic. Do not report website conversion rates without appropriate instrumentation. No telemetry or automatic outreach is introduced.

## Evidence and technical reference

Reviewed 2026-09-05; retained for claim verification and implementers:

- [OpenAI Batch API](https://developers.openai.com/api/docs/guides/batch): advertised discount, supported endpoints, completion window, expiry, and rate limits. Supports the economic mechanism, not measured Kaiiron customer outcomes.
- [Repository README](../README.md), [client configuration](../src/configure.rs), [real-client scenarios](../tests/scenarios/client_compatibility.rs), and [CI](../.github/workflows/ci.yml): current implementation and compatibility-test scope. Mock-provider tests do not prove live-provider uptime or customer savings.
- [Architecture and roadmap](long-horizon-workflows.md): durable inference versus full agent execution, plus unimplemented capabilities that must remain out of present claims.
- [Technical setup guide](../site/docs/index.html): installation, billing prerequisites, supported versions, and operational limitations.

Use **Kaiiron** as the public name. Preserve existing repository/package/configuration names in technical instructions. Revisit both marketing and onboarding when easier installation, additional agents, supported continuation, or measured customer outcomes become available.
