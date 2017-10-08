MATCH (n:EEZ {eez_id: {eez_id} })-[r:EEZ_CONTAINING_PORTS]->(p:PORT) RETURN p.geometry as geometry, p.bbox as bbox, p.name as name


