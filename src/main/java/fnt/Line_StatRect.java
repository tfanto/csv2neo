package fnt;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;

public class Line_StatRect {
	
	public String SRID = null;
	public String geometry = null;
	public Double north = null;
	public Double south = null;
	public Double east = null;
	public Double west = null;
	public String code = null;
	public String name = null;
	public Boolean enabled = null;
	
	public SpatialDatabaseRecord spatialDatabaseRecord = null;

	
	
	
	@Override
	public String toString() {
		return "Line_StatRect [SRID=" + SRID + ", geometry=" + geometry + ", north=" + north + ", south=" + south
				+ ", east=" + east + ", west=" + west + ", code=" + code + ", name=" + name + ", enabled=" + enabled
				+ "]";
	}
	
	
	
	
	
	
}
