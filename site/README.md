# Kaiiron marketing site

Plain HTML and CSS, served directly by GitHub Pages. No build step, JavaScript, external fonts, analytics, cookies, or runtime dependencies.

- `index.html`: nontechnical marketing, recognizable outcomes, plain-language cost/time expectations, and a setup entry point.
- `docs/index.html`: explicitly labeled technical setup guide and operational reference, for the user or someone helping them install.
- `styles.css`: shared responsive design and accessibility styles.
- `../docs/marketing-strategy.md`: customer evaluation, positioning rationale, evidence, and launch plan.

The Pages workflow in `../.github/workflows/pages.yml` uploads `site/` on changes to this directory on `main`, or on manual dispatch. Repository Pages settings must use GitHub Actions. Relative internal links work under the repository's `/Kaiion/` path and on a custom domain.

For local inspection, run `python3 -m http.server 8000 --directory site` from the repository root, then open `http://localhost:8000`.

Keep public product naming as **Kaiiron**. The repository, package, configuration paths, and environment variable prefix retain **Kaiion** for compatibility. Verify commands and supported versions against the implementation before changing them. Keep discounts scoped to provider token pricing; do not imply measured whole-workflow savings or automatic agent continuation.

The marketing audience uses agents to get work done and may have no programming background. Lead with worthwhile tasks and more affordable use. Keep implementation terminology, commands, databases, protocol names, routing tables, and test details out of the landing page. Include only facts that affect a visitor's decision: supported agents, longer waits, separate AI charges, and today's need for technical setup. Keep the technical reference accessible through the setup guide.
