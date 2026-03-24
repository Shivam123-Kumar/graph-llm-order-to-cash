from backend.db.neo4j_driver import get_session

def run_query(query):
    try:
        with get_session() as session:
            result = session.run(query)

            records = [record.data() for record in result]

            return records

    except Exception as e:
        print("❌ Query Execution Error:", e)
        return []