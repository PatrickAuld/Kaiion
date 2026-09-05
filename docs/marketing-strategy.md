# Kaiiron marketing strategy

Prepared 2026-09-05 against repository commit `00ce1dc`. This is a reasoned launch strategy based on the implementation and public provider documentation, not validated customer research. Customer priorities, objections, and channel choices below are hypotheses to test.

## Positioning decision

**Put agents to work on bigger things.**

Kaiiron makes long-horizon agent work more economical, so developers can delegate deeper investigations, broader changes, and more of their backlog. Its current implementation is an open-source local inference proxy connecting existing clients to OpenAI Batch, with durable inference state and explicit routing controls.

The primary selling point is expanding what people can afford to ask agents to do. Long-horizon work involves repeated investigation, action, verification, and reasoning; lowering the cost of those steps can make more ambitious tasks worthwhile. Batch pricing is the enabling mechanism and evidence for this promise. Existing-client compatibility makes the change accessible.

The underlying customer value is **a larger practical scope for delegation**: go deeper on a problem, cover more of a codebase, and use agents on work that would otherwise stay in the backlog. Evaluate success by the work newly attempted and completed, as well as its cost. The page leads with these outcomes, followed by economics and implementation proof. A bounded first run remains the adoption path into that larger ambition.

This positioning describes the economic foundation for long-horizon agents. It does not claim that Kaiiron itself plans tasks, supervises agents, schedules recurring work, restores tool state, or guarantees unattended completion over days. The harness owns those capabilities; current recovery boundaries remain explicit in the page and setup guide.

## Customer evaluation

| Priority | Segment and decision maker | Job to be done | Why Kaiiron could win | Main barrier | Recommended first offer |
|---|---|---|---|---|---|
| 1 | Developers and small engineering teams already delegating work to agents; developer pays or controls API spend | Expand from small tasks into deeper investigations, broader migrations, and more backlog work | More economical repeated reasoning without changing the agent or model | Batch waits, source installation, client timeouts, separate API billing | One bounded step from a larger real project, followed by a measured expansion |
| 2 | Long-horizon agent and developer infrastructure builders; technical lead owns integration | Make sustained agent workflows economically viable while owning their continuation | Durable job API, reusable Responses contract, inspectable routing | Must own tool-state checkpoints and continuation; no shared-database scaling | A detached inference example and documented recovery contract |
| 3 | Evaluation/research practitioners already using Responses-based harnesses | Run independent inference across a repeatable workload | CLI plus stored results without writing submission/polling from scratch | Native Batch is a strong alternative; one-entry batches limit throughput efficiency | A small evaluation pilot using the job API, with provider receipts |
| Later | Enterprise platform and FinOps buyers | Govern large agent fleets and cumulative spend | Potential future reuse across harnesses | No hard workflow budgets, central fleet management, rotation across jobs, or proven enterprise scale | Defer broad enterprise claims until the product supplies evidence |

Do not spend launch effort targeting people who need interactive latency, users who only want to use a ChatGPT subscription allowance, Claude Code-only users, or buyers expecting fully managed unattended agents. These are qualification boundaries, not failings to hide in a footnote.

## Value hierarchy and proof

| Value to the customer | Site message | Available evidence | Claim boundary |
|---|---|---|---|
| Expand the work worth delegating | Put agents to work on bigger things | Lower eligible inference rates provide an economic basis for repeated reasoning | A value hypothesis to validate with completed customer work; no guaranteed increase in quality, capacity, or autonomy |
| Make repeated reasoning more affordable | Lower the cost of taking the next step | OpenAI documents a 50% Batch discount against synchronous pricing | Token pricing for eligible models; not a measured 50% reduction in total workflow cost |
| Preserve investment in tools and habits | Keep your agent | Configure implementation plus real Codex, OpenCode, and Pi CLI tests in CI | Responses integrations and pinned versions; not every harness, release, or endpoint |
| Avoid losing track of paid inference | Keep inference recoverable | Durable SQLite state, replay, detached jobs, ambiguous-create reconciliation | Does not restore tool execution, workspace state, or a stopped agent process |
| Deliberate control over latency costs | Choose when to wait | Batch/direct/auto with explicit per-call price policy and route explanation | Estimates, not cumulative reservations or billing guarantees |
| Own the deployment | Your API key. Your machine. | Rust binary, local SQLite, MIT license, credentials not persisted | Prompts and results still go to the configured inference provider and persist locally |

Product principles implied by this positioning: enable ambition; make sustained agent use accessible; preserve existing tools and user control; state recovery semantics honestly. Keep these visible through the product and documentation rather than adding a generic values page.

## Alternatives and differentiation

1. **Continue using synchronous inference.** The strongest competitor is doing nothing. It has zero setup cost and avoids waiting. Kaiiron earns adoption only when recurring eligible spend outweighs installation, operational effort, and delay. Avoid implying batch is the best default for all work.
2. **Use OpenAI Batch directly.** For independent bulk jobs, this is already a good solution with the same advertised pricing advantage. Kaiiron adds client configuration, protocol compatibility, stored state, and a job API. It does not create an extra provider discount. Large-volume pure batch users may prefer native submission while Kaiiron lacks pooling.
3. **Use a client-specific batch tool.** For example, the public `claude-batch-toolkit` project targets non-urgent Anthropic batch work from Claude Code. This is adjacent evidence of the category, not proof of Kaiiron demand. Kaiiron's implemented distinction is a Responses proxy with three client integrations and durable jobs; it does not support that toolkit's Anthropic use case.
4. **Build a custom agent runner.** Custom runners can own full continuation and tool state, but require more integration. Kaiiron should remain an inference component a runner can adopt. Avoid a claim of equivalent end-to-end workflow durability.

Do not claim uniqueness, best-in-class reliability, universal compatibility, market size, or customer savings without evidence.

## Messaging and conversion

- **Category:** Economical inference for long-horizon agents.
- **Headline:** Put agents to work on bigger things.
- **Explanation:** Deeper investigations. Broader migrations. More of the backlog. Kaiiron makes work that takes many rounds of reasoning more economical, so you can give your existing agents more to do.
- **Value sequence:** Larger practical scope for delegation → more sustained agent use → lower cost of repeated inference → batch, routing, and recovery as supporting mechanisms.
- **Primary action:** Get started → client-specific setup → a small successful batch-backed task.
- **Secondary action:** Explore the source, for technical evaluation and trust.
- **Proof order:** ambitious but recognizable work; provider economics and wait tradeoff; concrete implementation; verified compatibility; operational boundaries.

The page deliberately has no pricing tiers, sales form, fabricated testimonials, customer logos, vanity adoption counts, or unverified savings calculator. It is a self-hosted open-source tool with a developer-led adoption path. A white/navy layout, one blue accent, strong typography, and source-level examples suit that decision. Plain HTML/CSS keeps maintenance and delivery simple.

Use **Kaiiron** in marketing and the `kaiiron` command in examples. Preserve the existing `Kaiion` repository/package/configuration names and `KAIION_*` variables; explain the spelling once in the guide rather than silently breaking compatibility.

## Adoption plan

### First: establish one repeatable success

Recruit a small handful of developers already paying for supported API inference. Ask each to bring a real maintenance or code-analysis task with a flexible deadline. Start with read-only output and a bounded scope; a client-backed run still needs to stay alive. Work with them until they can explain both the value and the limitations without help.

Collect: previous workflow, eligible spend, acceptable wait, model/client version, installation friction, time to first completed inference, useful output, provider charges, errors, and willingness to repeat. Ask which task was previously too expensive to delegate, which additional steps became worthwhile, and whether they would now use agents more often. Separate genuinely new work from existing work made cheaper. Do not assume every completed trial represents demand.

### Then: publish evidence through existing communities

Create a reproducible example with a public repository and exact client version. Publish the input task, resulting artifact, wall time, direct/batch usage, and provider-reported cost. Share it where Codex, OpenCode, and Pi users discuss real automation. Lead with the task and result, link to the guide, and invite concrete compatibility reports. No messages or outreach have been sent as part of building this site.

Target search intent such as “Codex batch API,” “OpenCode batch inference,” and “Pi OpenAI batch.” The page title, description, body, and setup documentation explain the actual integration without keyword stuffing. Do not pursue Claude Code traffic until its protocol is supported.

### Expand after repeat usage

If developers return for another workflow, turn their strongest task into a case study and improve the largest observed onboarding or completion bottleneck. If interest comes mainly from builders needing continuation, prioritize a supported harness adapter. If independent jobs dominate, assess pooling and rate-limit handling. Let observed usage decide before adding an enterprise sales motion or paid hosting.

## Success criteria and measurements

The primary outcome is **more useful work delegated and completed**: tasks previously left undone, broader task scope, and repeat agent use. Track **useful completed workflows per dollar**, reasoning steps, wall time, and human setup/recovery effort alongside it. Longer runs are valuable only when they produce useful outcomes; maximizing tokens or agent runtime is not the objective. Page views, stars, and downloads are acquisition signals, not proof of value.

| Stage | Measurement | Decision it informs |
|---|---|---|
| Interest | Qualified developers proceeding to the setup guide; which use case brought them | Whether positioning attracts the intended user |
| Activation | First successful batch-backed task; elapsed setup time; failures by client/version | Whether adoption is feasible without live assistance |
| Value | Newly attempted and completed work, scope relative to prior tasks, useful output, provider-reported charges, wall time | Whether better economics actually expand worthwhile delegation |
| Reliability | Finished/incomplete tasks, recovery success, uncertain submissions, manual interventions | Whether a broader launch would set expectations the product can meet |
| Retention | A second independent workflow within two weeks, with the reason for using or abandoning it | Whether there is recurring value |

Proposed initial gate: at least five external developers complete a representative task, at least three run a second workflow within two weeks, and all failures have understood causes. These are learning targets, not established benchmarks or current results. Do not broaden claims while a core client path remains unreliable.

The site has no analytics collector. Begin with opt-in pilot notes, issue reports, and GitHub's aggregate repository traffic. Website conversion rates require adding appropriate instrumentation later; do not report them as currently measurable. No automatic telemetry or outreach is introduced by this work.

For a cost comparison, use the same task, model, settings, and acceptance rubric. Report actual token usage and provider billing, cached versus uncached input where available, direct calls, retries, and wall time. A nominal illustration with eligible uncached token spend `S`, batch share `b`, and a 50% batch discount gives `S × (1 − 0.5b)`. It excludes differences in execution, caching, tools, and infrastructure; it is not a savings claim. Repeated runs are needed because model outputs and token counts vary.

## Sources and evidence boundaries

Reviewed 2026-09-05:

- [OpenAI Batch API](https://developers.openai.com/api/docs/guides/batch): advertised discount, supported endpoints, completion window, expiry, and rate limits. Supports economics and use-case suitability, not Kaiiron performance.
- [Anthropic batch processing](https://platform.claude.com/docs/en/build-with-claude/batch-processing): a separate batch protocol for non-urgent work. Market context only; Kaiiron does not implement it.
- [Claude batch toolkit](https://github.com/s2-streamstore/claude-batch-toolkit): an adjacent client-specific approach. No head-to-head evaluation was performed.
- [Repository README](../README.md), [client configuration](../src/configure.rs), [real-client scenarios](../tests/scenarios/client_compatibility.rs), and [CI](../.github/workflows/ci.yml): current implementation and test scope. Mock-provider tests are not evidence of live-provider uptime or real savings.
- [Architecture and roadmap](long-horizon-workflows.md): distinction between durable inference and full execution, and the unimplemented capabilities that must remain out of current marketing claims.

Revisit positioning when native Anthropic support, an actual continuation adapter, workflow budgets, or measured customer outcomes ship. Update public claims only with corresponding evidence.
