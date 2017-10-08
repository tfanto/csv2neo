package fnt;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;

public class Line_PortArea {
	
	public String SRID = null;
	public String geometry = null;
	public String code = null;
	public String name = null;
	public Boolean enabled = null;
	
	
	public SpatialDatabaseRecord spatialDatabaseRecord = null;

	
	
	@Override
	public String toString() {
		return "Line_PortArea [SRID=" + SRID + ", geometry=" + geometry + ", code=" + code + ", name=" + name
				+ ", enabled=" + enabled + "]";
	}
	
	
	
	
	
}
