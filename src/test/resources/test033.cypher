MATCH (n:EEZ {code: {code}})-[EEZ_CONTAINING_PORTS]->(p:PORT) RETURN count(p)

