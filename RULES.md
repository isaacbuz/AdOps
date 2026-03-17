# RULES.md — AdOps-Repo Guardrails

## 🏰 Disney Development Standards

**All development MUST follow Disney enterprise standards.**

### Blueprints (AI Coding Standards)
- **URL:** <https://blueprints.internal.disneystudiostech.com/catalog>
- Install: `curl -fsSL https://blueprints.internal.disneystudiostech.com/install | bash`
- Generates `.claude/rules/` and `.cursor/rules/` for AI compliance

### DevX CLI
- Binary: `~/devx` — `devx ai` (65+ MCPs), `devx cloud` (AWS), `devx locksmith` (Vault), `devx mariner` (K8s)
- Key MCPs: `disney_dev_mcp`, `jira-mcp-server`, `knowhere-mcp`, `yellowpages-mcp`, `weaponx-mcp`

### Norfolk/Mariner (CI/CD)
- GitHub Actions → Norfolk v2 Docker → Spinnaker → Kubernetes
- Support: `#build-deploy-support` Teams channel

### AI Policy
- **Microsoft Copilot** (Disney SSO) = ONLY authorized AI for proprietary data
- DisneyGPT at ai.disney.com | Standards: <https://developer.disneystudiostech.com/using-ai>

### Key Resources
Dev Portal: <https://portal.twdcgrid.net> | GitHub: <https://github.twdcgrid.net> | GitLab: <https://gitlab.disney.com> | Confluence: <https://confluence.disney.com> | Blueprints: <https://blueprints.internal.disneystudiostech.com>

---

## 📝 Walkthrough Vault Directive

All walkthrough guides and session summaries must be committed to the project vault —
not just left in the AI brain/artifacts directory. At session end:

1. **Create a walkthrough** summarizing what was done, what was tested, and results.
2. **Save it to the project vault** (e.g., `knowledge-vault/`, `vault/sessions/`, or `GoldenSignals-Brain/`).
3. **Include key decisions**, learnings, and any open questions for the next session.
4. **File naming**: `YYYY-MM-DD-walkthrough-<topic>.md`

This ensures persistent, project-local documentation that survives across AI sessions.

---

## 🧠 Knowledge Base — Auto-Save Directive

**IMPORTANT: At the end of every session, you MUST save new learnings to the knowledge base.**

Use the `mcp_memory_add_observations` tool to persist anything new you learned. This includes:
- New discoveries about the codebase, APIs, or configurations
- Bugs found and fixes applied
- Architecture decisions, workarounds, contacts, tools, or setup gotchas

### How to Save

```javascript
mcp_memory_add_observations({
  observations: [{
    entityName: "AdOps-Repo",
    contents: ["<concise learning 1>", "<concise learning 2>"]
  }]
})
```

If a learning relates to a different entity, create or update that entity instead. Always prefer updating existing entities over creating duplicates.

**Do NOT skip this step.** The knowledge base is shared across all AI sessions and tools.
