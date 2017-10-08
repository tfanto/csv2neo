MATCH (n:EEZ {code: {code}})-[EEZ_CONTAINING_PORTS]->(p:PORT) RETURN  p.geometry as geometry, p.bbox as bbox, p.name as name


