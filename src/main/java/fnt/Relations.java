package fnt;

import org.neo4j.graphdb.RelationshipType;

public enum Relations implements RelationshipType{
	
	BELONGS_TO_EEZ,
	PORTAREA_CONTAINING_PORTS,
	PORTS_IN_PORTAREA, PORTS_IN_EEZ, EEZ_CONTAINING_PORTS;

}
