---
description: Search with RAG toolkit
---
Search the centralized knowledge base (ChromaDB + FTS5) for any query.
// turbo
1. Ask the user for their search query using the `notify_user` tool.
2. Execute the semantic search script with the user's query: `~/.gemini/chief-of-staff/rag-venv/bin/python3 ~/.gemini/chief-of-staff/vault-hybrid-search.py "$QUERY"`
3. Read the search results and summarize them concisely for the user.
