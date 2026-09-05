# Kaiiron marketing strategy

Revised 2026-09-05 to reflect the product owner's direction: market to nontechnical users of agents. The audience choice is a strategic direction, not a measured claim about current market demographics. Customer motivations and launch priorities below are hypotheses to validate. Technical capability claims remain grounded in the implementation and sources listed below.

## Positioning

**Put agents to work on bigger things.**

Kaiiron makes longer AI tasks more affordable, so people can give their existing agents more of the work worth doing.

The customer wants an idea explored, a decision researched, or a project moved forward. They do not need to understand how AI requests are submitted or stored to understand that value. Lead with what becomes worth doing, explain the tradeoff in everyday language, and offer a clear next step.

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
| More possibilities within reach | Put agents to work on bigger things | Lower eligible processing rates can make previously uneconomical tasks worthwhile | Validate with actual new work attempted and completed |
| More thorough work | Some worthwhile work takes more than one answer | A lower cost per inference can support additional steps | More time or more steps does not guarantee a better result |
| More frequent use | Give your agent more of the work worth doing | Better task economics can lower the threshold for delegation | Do not promise a fixed number of extra tasks or unlimited use |
| Familiarity and control | Keep the agent you know | Codex, OpenCode, and Pi integrations exist | Version and setup details belong in the guide |
| An understandable tradeoff | When the work can wait, your budget can go further | OpenAI offers discounted asynchronous processing | Savings vary; work takes longer; completion time is not guaranteed |

Principles: make ambition accessible, respect the reader's time, preserve their control, and state material limitations plainly. The page should let someone decide whether the idea suits their work without learning the implementation.

## Public messaging

- **Headline:** Put agents to work on bigger things.
- **Supporting copy:** Explore an idea. Research a decision. Work through a bigger project. Kaiiron makes longer AI tasks more affordable, so you can give your agent more of the work worth doing.
- **Examples:** Understand your options; develop an idea; move a project forward.
- **Economic explanation:** Kaiiron helps your agent use lower-priced AI processing for work that does not need an immediate answer.
- **Primary action:** Get started → plain-language setup expectations → technical guide for the user or their helper.
- **Secondary action:** See what's possible → recognizable uses on the page.
- **Message order:** Desired outcome; familiar examples; affordability and waiting; how the person uses it; practical questions; next step.

GitHub is available in the footer for readers who want the project. Source code is not the primary or secondary sales action. The technical guide is explicitly labeled and retains accurate installation instructions and operational limits.

The landing page does not need a numerical discount to establish the promise. A broad percentage next to consumer benefits can be mistaken for a reduction in the whole bill. Explain lower processing rates in plain language and keep provider-specific pricing detail in the technical reference. Do not invent pricing plans, a waitlist, customer logos, testimonials, measured savings, or a managed product.

## Editorial rules

Write for an intelligent person who uses an agent but has no interest in its implementation. Technical accuracy is necessary; technical vocabulary is not a substitute for an explanation.

Keep the landing page free of commands, source-install instructions, database names, protocol names, request identities, token accounting, routing tables, transport behavior, job APIs, test matrices, and recovery contracts. Keep those details in the technical guide and repository.

Use ordinary task language: compare proposals, work through background reading, shape a plan, refine a draft, organize notes, update materials. Avoid assuming the reader has a repository, knows what a migration is, or thinks in terms of inference budgets.

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
2. Which task of yours would you try?
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
| Relevance | Can the person name a real task they would hand over? | Do the examples connect to actual work? |
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
