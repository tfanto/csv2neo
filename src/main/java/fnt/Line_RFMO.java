package fnt;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;

public class Line_RFMO {

	public String SRID = null;
	public String geometry = null;
	public String name = null;
	public String code = null;
	public String tuna = null;
	public Boolean enabled = null;
	
	public SpatialDatabaseRecord spatialDatabaseRecord = null;


	@Override
	public String toString() {
		return "Line_RFMO [SRID=" + SRID + ", geometry=" + geometry + ", name=" + name + ", code=" + code + ", tuna="
				+ tuna + ", enabled=" + enabled + "]";
	}

}
