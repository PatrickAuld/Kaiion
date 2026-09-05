# Kaiiron marketing site

Plain HTML and CSS, served directly by GitHub Pages. No build step, JavaScript, external fonts, analytics, cookies, or runtime dependencies.

- `index.html`: positioning, use cases, economics, compatibility, and setup entry point.
- `docs/index.html`: practical onboarding and the recovery contract.
- `styles.css`: shared responsive design and accessibility styles.
- `../docs/marketing-strategy.md`: customer evaluation, positioning rationale, evidence, and launch plan.

The Pages workflow in `../.github/workflows/pages.yml` uploads `site/` on changes to this directory on `main`, or on manual dispatch. Repository Pages settings must use GitHub Actions. Relative internal links work under the repository's `/Kaiion/` path and on a custom domain.

For local inspection, run `python3 -m http.server 8000 --directory site` from the repository root, then open `http://localhost:8000`.

Keep public product naming as **Kaiiron**. The repository, package, configuration paths, and environment variable prefix retain **Kaiion** for compatibility. Verify commands and supported versions against the implementation before changing them. Keep discounts scoped to provider token pricing; do not imply measured whole-workflow savings or automatic agent continuation.
