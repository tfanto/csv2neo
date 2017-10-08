package fnt;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;

public class Line_Port {

	public String SRID = null;
	public String geometry = null;
	public String country_code = null;
	public String code = null;
	public String name = null;
	public String fishing_port = null;
	public String landing_place = null;
	public String commercial_port = null;
	public Boolean enabled = null;
	
	
	public SpatialDatabaseRecord spatialDatabaseRecord = null;


	@Override
	public String toString() {
		return "Line_Port [SRID=" + SRID + ", geometry=" + geometry + ", country_code=" + country_code + ", code="
				+ code + ", name=" + name + ", fishing_port=" + fishing_port + ", landing_place=" + landing_place
				+ ", commercial_port=" + commercial_port + ", enabled=" + enabled + "]";
	}

}
