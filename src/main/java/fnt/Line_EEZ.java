package fnt;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;

public class Line_EEZ {
		
	public String SRID = null;
	public String geometry = null;
	public String name = null;
	public String country = null;
	public String sovereign = null;
	public String remarks = null;
	public Integer sov_id = null;
	public Integer eez_id = null;
	public String code = null;
	public Double mrgid = null;
	public String date_chang = null;
	public Double area_m2 = null;
	public Integer mrgid_eez = null;
	public Boolean enabled = null;
	
	public SpatialDatabaseRecord spatialDatabaseRecord = null;
	
	// enables on 
	// longitude
	// latitude
	
	
	
	
	
	
	@Override
	public String toString() {
		return "Line_EEZ [SRID=" + SRID + ", geometry=" + geometry + ", name=" + name + ", country=" + country
				+ ", sovereign=" + sovereign + ", remarks=" + remarks + ", sov_id=" + sov_id + ", eez_id=" + eez_id
				+ ", code=" + code + ", mrgid=" + mrgid + ", date_chang=" + date_chang + ", area_m2=" + area_m2
				+ ", mrgid_eez=" + mrgid_eez + ", enabled=" + enabled + "]";
	}
	
	
	
	
	
}
