MATCH (n:EEZ {code: {code}})-[EEZ_CONTAINING_PORTS]->(p:PORT) RETURN collect(p) as ports

