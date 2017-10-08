MATCH (pa:PORT_AREA)-[r:PORTAREA_CONTAINING_PORTS]->(p:PORT) where pa.name={name} RETURN p.geometry as geometry


