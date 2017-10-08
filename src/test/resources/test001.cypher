MATCH (n:EEZ {eez_id: {eez_id} })-[r:EEZ_CONTAINING_PORTS]->(p:PORT) RETURN collect(p) as ports


