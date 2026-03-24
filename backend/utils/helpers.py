def is_valid_question(q):
    keywords = ["invoice", "payment", "customer", "order", "journal"]
    return any(k in q.lower() for k in keywords)