from openai import OpenAI
from dotenv import load_dotenv
import os

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# OpenAI Client (OpenRouter)
# -----------------------------
client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url="https://openrouter.ai/api/v1"
)

# -----------------------------
# Graph Schema (IMPORTANT)
# -----------------------------
GRAPH_SCHEMA = """
Nodes:
- Customer(id)
- Invoice(id)
- Payment(id)
- JournalEntry(id)

Relationships:
- (:Invoice)-[:BILLED_TO]->(:Customer)
- (:Invoice)-[:RECORDED_AS]->(:JournalEntry)
- (:Payment)-[:RECORDED_AS]->(:JournalEntry)
- (:Payment)-[:PAYS]->(:Invoice)
"""

# -----------------------------
# Clean Cypher Output
# -----------------------------
def clean_cypher(query: str) -> str:
    if not query:
        return ""

    query = query.strip()

    # Remove markdown formatting
    if "```" in query:
        query = query.replace("```cypher", "")
        query = query.replace("```", "")

    # Remove extra explanation lines
    lines = query.split("\n")
    cypher_lines = [
        line for line in lines
        if not line.lower().startswith("here")
        and not line.lower().startswith("this")
    ]

    return "\n".join(cypher_lines).strip()


# -----------------------------
# Generate Cypher Query
# -----------------------------
def generate_cypher(question: str) -> str:
    prompt = f"""
You are an expert Neo4j Cypher query generator.

Your job is to convert a natural language question into a VALID Cypher query.

Use ONLY the schema below:

{GRAPH_SCHEMA}

IMPORTANT RULES:
- Only use the given node labels and relationships
- Do NOT invent properties or labels
- Always use proper Cypher syntax
- Use MATCH and RETURN appropriately
- If ID is numeric, DO NOT use quotes
- Most IDs in this dataset are numeric → use: {{id: 91150187}}

AGGREGATION RULES:
- Use COUNT() for "how many" or "number of"
- Use ORDER BY for ranking queries (highest, most)
- Use DESC for highest values
- Use LIMIT for top results

RETURN RULES:
- Return meaningful fields like i.id, COUNT(p)
- Use aliases like payment_count

STRICT:
- Return ONLY the Cypher query
- No explanations
- No markdown

Examples:

Q: Find journal entry for invoice 123
A:
MATCH (i:Invoice {{id: 123}})-[:RECORDED_AS]->(j:JournalEntry)
RETURN j

Q: Find customer for invoice 456
A:
MATCH (i:Invoice {{id: 456}})-[:BILLED_TO]->(c:Customer)
RETURN c

Q: Find payments for invoice 789
A:
MATCH (p:Payment)-[:PAYS]->(i:Invoice {{id: 789}})
RETURN p

Q: How many payments does invoice 123 have?
A:
MATCH (p:Payment)-[:PAYS]->(i:Invoice {{id: 123}})
RETURN COUNT(p) as payment_count

Q: Which invoices have the most payments?
A:
MATCH (p:Payment)-[:PAYS]->(i:Invoice)
RETURN i.id, COUNT(p) as payment_count
ORDER BY payment_count DESC
LIMIT 5

Now generate query for:

Question:
{question}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )

        raw_query = response.choices[0].message.content.strip()
        cleaned_query = clean_cypher(raw_query)

        # Debug print
        print("\n🧠 Generated Cypher Query:")
        print(cleaned_query)
        print("-" * 50)

        return cleaned_query

    except Exception as e:
        print("❌ Error generating Cypher:", str(e))
        return ""


# -----------------------------
# Test (Optional)
# -----------------------------
if __name__ == "__main__":
    test_question = "How many payments does invoice 91150187 have?"
    query = generate_cypher(test_question)
    print("\nFinal Query:")
    print(query)