from backend.db.neo4j_driver import get_driver

def run_query(query):
    driver = get_driver()

    with driver.session() as session:
        result = session.run(query)

        records = []
        for record in result:
            records.append(record.data())

        return records