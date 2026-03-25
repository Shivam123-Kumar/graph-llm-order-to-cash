# AI Coding Logs (Cursor)

## Session 1 - Graph Query Design
Prompt:
"How to design a graph query system using Neo4j and LLM?"

Response Summary:
- Suggested Cypher queries
- Suggested schema: Nodes (Entity), Edges (Relation)

---

## Session 2 - Neo4j Integration
Prompt:
"How to connect Python backend with Neo4j Aura?"

Response Summary:
- Used neo4j-driver
- Created session-based query execution

---

## Session 3 - Graph Highlight Fix
Prompt:
"Why are nodes not highlighting in Streamlit graph?"

Response Summary:
- Issue: highlight_ids not passed correctly
- Fix: send node IDs from backend → frontend

---

## Session 4 - Deployment Issue
Prompt:
"Graph not loading on Render deployment"

Response Summary:
- Problem: Neo4j Aura connection / env vars
- Fix: add environment variables in Render dashboard

---

## Session 5 - Streamlit UI Debugging
Prompt:
"Streamlit graph not rendering after code changes"

Response Summary:
- Issue: Missing graph_data in session state
- Fix: Add graph_data to session state after query execution