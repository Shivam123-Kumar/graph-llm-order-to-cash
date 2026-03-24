import sys
import os
import html

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

import streamlit.components.v1 as components

from backend.graph.graph_builder import fetch_graph_data
from backend.graph.visualize import build_graph_html
from backend.llm.cypher_generator import generate_cypher
from backend.graph.query_executor import run_query
from backend.utils.helpers import is_valid_question

# -----------------------------
# Page Config
# -----------------------------
st.set_page_config(layout="wide")
st.title("📊 Graph + LLM Query System")

# -----------------------------
# Layout
# -----------------------------
col1, col2 = st.columns([3, 1])

# -----------------------------
# Initialize highlight IDs
# -----------------------------
if "highlight_ids" not in st.session_state:
    st.session_state.highlight_ids = set()

# -----------------------------
# RIGHT: Chat Interface
# -----------------------------
with col2:
    st.subheader("💬 Chat with Graph")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    question = st.text_input("Ask something about your data")

    if st.button("Send"):

        if not is_valid_question(question):
            st.warning("This system is designed for dataset-related queries only.")
            st.stop()

        cypher = generate_cypher(question)
        result = run_query(cypher)

        highlight_ids = set()

        # -----------------------------
        # Extract IDs for highlighting
        # -----------------------------
        if result:
            for r in result:
                for key in r:
                    value = r[key]

                    if isinstance(value, dict) and "id" in value:
                        highlight_ids.add(value["id"])

                    elif isinstance(value, int):
                        highlight_ids.add(value)

        # Save highlight IDs
        st.session_state.highlight_ids = highlight_ids

        # -----------------------------
        # Format response
        # -----------------------------
        if result:
            try:
                record = result[0]

                if "j" in record:
                    answer = f"Journal Entry ID: {record['j']['id']}"

                elif "i" in record and isinstance(record["i"], dict):
                    answer = f"Invoice ID: {record['i']['id']}"

                elif "p" in record:
                    answer = f"Payment ID: {record['p']['id']}"

                elif "c" in record:
                    answer = f"Customer ID: {record['c']['id']}"

                elif "payment_count" in record:
                    if "i.id" in record:
                        answer = "📊 Top invoices by payments:\n"
                        for r in result:
                            answer += f"• Invoice {r['i.id']} → {r['payment_count']} payments\n"
                    else:
                        answer = f"Total Payments: {record['payment_count']}"

                elif "sales_orders" in record:
                    so_list = record["sales_orders"]

                    preview = ", ".join(map(str, so_list[:5]))
                    total = len(so_list)

                    answer = (
                        "🔄 Full Flow Found:\n\n"
                        f"• Total Sales Orders: {total}\n"
                        f"• Sample: {preview}...\n"
                        f"• Invoice ID: {record.get('i.id')}\n"
                        f"• Journal Entry ID: {record.get('j.id')}"
                    )

                elif "i.id" in record:
                    answer = "⚠️ Invoices with issues:\n"
                    for r in result[:10]:
                        answer += f"• Invoice {r['i.id']}\n"

                else:
                    answer = str(result)

            except Exception as e:
                answer = f"Error parsing result: {str(e)}"

        else:
            answer = "No data found."

        # Save chat
        st.session_state.chat_history.append(("You", question))
        st.session_state.chat_history.append(("AI", answer))

        st.rerun()  # 🔥 IMPORTANT (refresh UI)

    # -----------------------------
    # Display Chat
    # -----------------------------
    for role, msg in st.session_state.chat_history:

        safe_msg = html.escape(msg).replace("\n", "<br>")

        if role == "You":
            st.markdown(
                f"""
                <div style="background:#1e293b;padding:12px;border-radius:10px;margin-bottom:10px;color:white">
                🧑 <b>You:</b><br>{safe_msg}
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                f"""
                <div style="background:#064e3b;padding:12px;border-radius:10px;margin-bottom:12px;color:white">
                🤖 <b>AI:</b><br>{safe_msg}
                </div>
                """,
                unsafe_allow_html=True
            )

# -----------------------------
# LEFT: Graph Visualization (UPDATED)
# -----------------------------
with col1:
    st.subheader("📌 Graph View")

    try:
        nodes, edges = fetch_graph_data(st.session_state.highlight_ids)

        html_file = build_graph_html(nodes, edges)

        with open(html_file, "r", encoding="utf-8") as f:
            html_content = f.read()

        components.html(html_content, height=600)

    except Exception as e:
        st.error(f"Graph loading failed: {str(e)}")