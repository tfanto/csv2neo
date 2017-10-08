package fnt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;

public class BaseClass {

	protected static final Logger LOGGER = LoggerFactory.getLogger(BaseClass.class);

	protected File DB_PATH = null;
	protected GraphDatabaseService db = null;
	protected SpatialDatabaseService spatialService = null;

	protected Layer layerEEZ = null;
	protected Layer layerFAO = null;
	protected Layer layerGFCM = null;
	protected Layer layerPORT = null;
	protected Layer layerPORTAREA = null;
	protected Layer layerRFMO = null;
	protected Layer layerSTATRECT = null;

	protected List<Line_EEZ> EEZ;
	protected List<Line_FAO> FAO;
	protected List<Line_StatRect> STAT_RECT;
	protected List<Line_Port> PORT;
	protected List<Line_GFCM> GFCM;
	protected List<Line_RFMO> RFMO;
	protected List<Line_PortArea> PORT_AREA;

	public void openNeo() {
		DB_PATH = new File("neo4j-db-" + UUID.randomUUID().toString());
		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		spatialService = new SpatialDatabaseService(db);
		layerEEZ = spatialService.getOrCreateEditableLayer("EEZ","geometry");
		layerFAO = spatialService.getOrCreateEditableLayer("FAO","geometry");
		layerGFCM = spatialService.getOrCreateEditableLayer("GFCM","geometry");
		layerPORT = spatialService.getOrCreateEditableLayer("PORT","geometry");
		layerPORTAREA = spatialService.getOrCreateEditableLayer("PORT_AREA","geometry");
		layerRFMO = spatialService.getOrCreateEditableLayer("RFMO","geometry");
		layerSTATRECT = spatialService.getOrCreateEditableLayer("STATRECT","geometry");
		
		
	}

	public void closeNeo() {
		if (db != null) {
			db.shutdown();
		}
	}

	private String getAbsolutePath(String filename) {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		URL url = loader.getResource(filename);
		return url.getPath();
	}

	private MemoryFile load(String fileName) throws Exception {
		String csvFile = getAbsolutePath(fileName);
		MemoryFile memFile = new MemoryFile();
		CSVReader reader = null;
		try {
			
			BufferedReader br = new BufferedReader(new InputStreamReader(
				    new FileInputStream(csvFile), "UTF-8"));
			reader = new CSVReader(br);
			// reader = new CSVReader(new FileReader(csvFile));
			String[] line;
			String[] headers;
			headers = reader.readNext();
			int nHeaderCols = headers.length;
			for (int i = 0; i < nHeaderCols; i++) {
				memFile.header.add(headers[i]);
			}

			while ((line = reader.readNext()) != null) {
				int nLineCols = headers.length;

				// sanitycheck
				if (nHeaderCols != nLineCols) {
					throw new Exception(fileName);
				}

				List<String> row = new ArrayList<>();
				for (int i = 0; i < nLineCols; i++) {
					row.add(line[i]);
				}
				memFile.data.add(row);
			}
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}
		return memFile;
	}

	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	private List<Line_EEZ> loadEEZ() throws Exception {

		List<Line_EEZ> ret = new ArrayList<>();

		MemoryFile memoryFile = load("eez.csv");
		List<String> header = memoryFile.header;
		int columns = header.size();
		List<List<String>> data = memoryFile.data;

		for (List<String> row : data) {
			boolean addThisNode = true;
			Line_EEZ node = new Line_EEZ();

			for (int column = 0; column < columns; column++) {
				String key = header.get(column);
				String dta = (row.get(column) == null ? "" : row.get(column).trim());
				// no empty columns
				if (dta.length() < 1) {
					continue;
				}
				switch (key) {
				case "geom":
					int pos = dta.indexOf(';');
					String geom = dta.substring(pos + 1);
					node.geometry = geom;
					String srid = dta.substring(5, pos); // ??
					node.SRID = srid;
					break;
				case "name":
					node.name = dta;
					break;
				case "country":
					node.country = dta;
					break;
				case "sovereign":
					node.sovereign = dta;
					break;
				case "remarks":
					node.remarks = dta;
					break;
				case "sov_id":
					try {
						node.sov_id = Integer.parseInt(dta);
					} catch (NumberFormatException e) {
						node.sov_id = null;
					}
					break;
				case "eez_id":
					try {
						node.eez_id = Integer.parseInt(dta);
					} catch (NumberFormatException e) {
						node.eez_id = null;
					}
					break;
				case "code":
					node.code = dta;
					break;
				case "mrgid":
					try {
						node.mrgid = Double.parseDouble(dta);
					} catch (NumberFormatException e) {
						node.mrgid = null;
					}
					break;
				case "date_chang":
					node.date_chang = dta;
					break;
				case "area_m2":
					try {
						node.area_m2 = Double.parseDouble(dta);
					} catch (NumberFormatException e) {
						node.area_m2 = null;
					}
					break;
				case "mrgid_eez":
					try {
						node.mrgid_eez = Integer.parseInt(dta);
					} catch (NumberFormatException e) {
						node.mrgid_eez = null;
					}
					break;
				case "enabled":
					try {
						node.enabled = dta.toUpperCase().equals("Y");
					} catch (RuntimeException e) {
						node.enabled = false;
					}
					break;
				}
			}
			if (addThisNode) {
				ret.add(node);
			}
		}
		memoryFile.data.clear();
		memoryFile.data = null;
		memoryFile.header.clear();
		memoryFile.header = null;
		memoryFile = null;
		return ret;
	}

	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	private List<Line_FAO> loadFAO() throws Exception {

		List<Line_FAO> ret = new ArrayList<>();

		MemoryFile memoryFile = load("fao.csv");
		List<String> header = memoryFile.header;
		int columns = header.size();
		List<List<String>> data = memoryFile.data;

		for (List<String> row : data) {
			boolean addThisNode = true;
			Line_FAO node = new Line_FAO();

			for (int column = 0; column < columns; column++) {
				String key = header.get(column);
				String dta = (row.get(column) == null ? "" : row.get(column).trim());
				// no empty columns
				if (dta.length() < 1) {
					continue;
				}
				switch (key) {
				case "geom":
					int pos = dta.indexOf(';');
					String geom = dta.substring(pos + 1);
					node.geometry = geom;
					String srid = dta.substring(5, pos); // ??
					node.SRID = srid;
					break;
				case "name":
					node.name = dta;
					break;
				case "enabled":
					try {
						node.enabled = dta.toUpperCase().equals("Y");
					} catch (RuntimeException e) {
						node.enabled = false;
					}
					break;

				case "ocean":
					node.ocean = dta;
					break;
				case "subocean":
					node.subocean = dta;
					break;
				case "f_area":
					node.f_area = dta;
					break;
				case "area_l":
					node.area_l = dta;
					break;
				case "f_subarea":
					node.f_subarea = dta;
					break;
				case "subarea_n":
					node.subarea_n = dta;
					break;
				case "subarea_l":
					node.subarea_l = dta;
					break;
				case "f_division":
					node.f_division = dta;
					break;
				case "division_n":
					node.division_n = dta;
					break;
				case "division_l":
					node.division_l = dta;
					break;
				case "f_subdivis":
					node.f_subdivis = dta;
					break;
				case "subdivis_n":
					node.subdivis_n = dta;
					break;
				case "subdivis_l":
					node.subdivis_l = dta;
					break;
				case "f_subunit":
					node.f_subunit = dta;
					break;
				case "subunit_n":
					node.subunit_n = dta;
					break;
				case "subunit_l":
					node.subunit_l = dta;
					break;
				case "ele_name":
					node.ele_name = dta;
					break;
				case "ele_label":
					node.ele_label = dta;
					break;
				case "ele_type":
					node.ele_type = dta;
					break;
				case "f_label":
					node.f_label = dta;
					break;
				case "code":
					node.code = dta;
					break;
				}
			}
			if (addThisNode) {
				ret.add(node);
			}
		}
		memoryFile.data.clear();
		memoryFile.data = null;
		memoryFile.header.clear();
		memoryFile.header = null;
		memoryFile = null;
		return ret;
	}

	/**
	 * 
	 * @return
	 * @throws Exception
	 *             ,north,south,east,west,,,
	 */
	private List<Line_StatRect> loadStatRect() throws Exception {

		List<Line_StatRect> ret = new ArrayList<>();

		MemoryFile memoryFile = load("stat_rect.csv");
		List<String> header = memoryFile.header;
		int columns = header.size();
		List<List<String>> data = memoryFile.data;

		for (List<String> row : data) {
			boolean addThisNode = true;
			Line_StatRect node = new Line_StatRect();

			for (int column = 0; column < columns; column++) {
				String key = header.get(column);
				String dta = (row.get(column) == null ? "" : row.get(column).trim());
				// no empty columns
				if (dta.length() < 1) {
					continue;
				}
				switch (key) {
				case "geom":
					int pos = dta.indexOf(';');
					String geom = dta.substring(pos + 1);
					node.geometry = geom;
					String srid = dta.substring(5, pos); // ??
					node.SRID = srid;
					break;
				case "name":
					node.name = dta;
					break;
				case "enabled":
					try {
						node.enabled = dta.toUpperCase().equals("Y");
					} catch (RuntimeException e) {
						node.enabled = false;
					}
					break;
				case "code":
					node.code = dta;
					break;
				case "north":
					try {
						node.north = Double.parseDouble(dta);
					} catch (NumberFormatException e) {
						node.north = null;
					}
					break;
				case "south":
					try {
						node.south = Double.parseDouble(dta);
					} catch (NumberFormatException e) {
						node.south = null;
					}
					break;
				case "east":
					try {
						node.east = Double.parseDouble(dta);
					} catch (NumberFormatException e) {
						node.east = null;
					}
					break;
				case "west":
					try {
						node.west = Double.parseDouble(dta);
					} catch (NumberFormatException e) {
						node.west = null;
					}
					break;

				}
			}
			if (addThisNode) {
				ret.add(node);
			}
		}
		memoryFile.data.clear();
		memoryFile.data = null;
		memoryFile.header.clear();
		memoryFile.header = null;
		memoryFile = null;
		return ret;
	}

	/**
	 * 
	 * @return
	 * @throws Exception
	 */
	private List<Line_Port> loadPort() throws Exception {

		List<Line_Port> ret = new ArrayList<>();

		MemoryFile memoryFile = load("port.csv");
		List<String> header = memoryFile.header;
		int columns = header.size();
		List<List<String>> data = memoryFile.data;

		for (List<String> row : data) {
			boolean addThisNode = true;
			Line_Port node = new Line_Port();

			for (int column = 0; column < columns; column++) {
				String key = header.get(column);
				String dta = (row.get(column) == null ? "" : row.get(column).trim());
				// no empty columns
				if (dta.length() < 1) {
					continue;
				}
				switch (key) {
				case "geom":

					if (dta.contains("EMPTY")) {
						addThisNode = false;
						break;
					}

					int pos = dta.indexOf(';');
					String geom = dta.substring(pos + 1);
					node.geometry = geom;
					String srid = dta.substring(5, pos); // ??
					node.SRID = srid;
					break;
				case "name":
					node.name = dta;
					break;
				case "enabled":
					try {
						node.enabled = dta.toUpperCase().equals("Y");
					} catch (RuntimeException e) {
						node.enabled = false;
					}
					break;
				case "code":
					node.code = dta;
					break;
				case "country_code":
					node.country_code = dta;
					break;
				case "fishing_port":
					node.fishing_port = dta;
					break;
				case "landing_place":
					node.landing_place = dta;
					break;
				case "commercial_port":
					node.commercial_port = dta;
					break;

				}
			}
			if (addThisNode) {
				ret.add(node);
			}
		}
		memoryFile.data.clear();
		memoryFile.data = null;
		memoryFile.header.clear();
		memoryFile.header = null;
		memoryFile = null;
		return ret;
	}

	private List<Line_GFCM> loadGFCM() throws Exception {

		List<Line_GFCM> ret = new ArrayList<>();

		MemoryFile memoryFile = load("gfcm.csv");
		List<String> header = memoryFile.header;
		int columns = header.size();
		List<List<String>> data = memoryFile.data;

		for (List<String> row : data) {
			boolean addThisNode = true;
			Line_GFCM node = new Line_GFCM();

			for (int column = 0; column < columns; column++) {
				String key = header.get(column);
				String dta = (row.get(column) == null ? "" : row.get(column).trim());
				// no empty columns
				if (dta.length() < 1) {
					continue;
				}
				switch (key) {
				case "geom":
					int pos = dta.indexOf(';');
					String geom = dta.substring(pos + 1);
					node.geometry = geom;
					String srid = dta.substring(5, pos); // ??
					node.SRID = srid;
					break;
				case "name":
					node.name = dta;
					break;
				case "enabled":
					try {
						node.enabled = dta.toUpperCase().equals("Y");
					} catch (RuntimeException e) {
						node.enabled = false;
					}
					break;
				case "code":
					node.code = dta;
					break;
				}
			}
			if (addThisNode) {
				ret.add(node);
			}
		}
		memoryFile.data.clear();
		memoryFile.data = null;
		memoryFile.header.clear();
		memoryFile.header = null;
		memoryFile = null;
		return ret;
	}

	private List<Line_RFMO> loadRFMO() throws Exception {

		List<Line_RFMO> ret = new ArrayList<>();

		MemoryFile memoryFile = load("rfmo.csv");
		List<String> header = memoryFile.header;
		int columns = header.size();
		List<List<String>> data = memoryFile.data;

		for (List<String> row : data) {
			boolean addThisNode = true;
			Line_RFMO node = new Line_RFMO();

			for (int column = 0; column < columns; column++) {
				String key = header.get(column);
				String dta = (row.get(column) == null ? "" : row.get(column).trim());
				// no empty columns
				if (dta.length() < 1) {
					continue;
				}
				switch (key) {
				case "geom":
					int pos = dta.indexOf(';');
					String geom = dta.substring(pos + 1);
					node.geometry = geom;
					String srid = dta.substring(5, pos); // ??
					node.SRID = srid;
					break;
				case "tuna":
					node.tuna = dta;
					break;
				case "name":
					node.name = dta;
					break;
				case "enabled":
					try {
						node.enabled = dta.toUpperCase().equals("Y");
					} catch (RuntimeException e) {
						node.enabled = false;
					}
					break;
				case "code":
					node.code = dta;
					break;
				}
			}
			if (addThisNode) {
				ret.add(node);
			}
		}
		memoryFile.data.clear();
		memoryFile.data = null;
		memoryFile.header.clear();
		memoryFile.header = null;
		memoryFile = null;
		return ret;
	}

	private List<Line_PortArea> loadPortArea() throws Exception {

		List<Line_PortArea> ret = new ArrayList<>();

		MemoryFile memoryFile = load("port_area.csv");
		List<String> header = memoryFile.header;
		int columns = header.size();
		List<List<String>> data = memoryFile.data;

		for (List<String> row : data) {
			boolean addThisNode = true;
			Line_PortArea node = new Line_PortArea();

			for (int column = 0; column < columns; column++) {
				String key = header.get(column);
				String dta = (row.get(column) == null ? "" : row.get(column).trim());
				// no empty columns
				if (dta.length() < 1) {
					continue;
				}
				switch (key) {
				case "geom":
					int pos = dta.indexOf(';');
					String geom = dta.substring(pos + 1);
					node.geometry = geom;
					String srid = dta.substring(5, pos); // ??
					node.SRID = srid;
					break;
				case "name":
					node.name = dta;
					break;
				case "enabled":
					try {
						node.enabled = dta.toUpperCase().equals("Y");
					} catch (RuntimeException e) {
						node.enabled = false;
					}
					break;
				case "code":
					node.code = dta;
					break;
				}
			}
			if (addThisNode) {
				ret.add(node);
			}
		}
		memoryFile.data.clear();
		memoryFile.data = null;
		memoryFile.header.clear();
		memoryFile.header = null;
		memoryFile = null;
		return ret;
	}

	protected void loadFilesToMemory() throws Exception {

		try {

			EEZ = loadEEZ();
			FAO = loadFAO();
			STAT_RECT = loadStatRect();
			PORT = loadPort();
			GFCM = loadGFCM();
			RFMO = loadRFMO();
			PORT_AREA = loadPortArea();

			LOGGER.info("EEZ\t" + EEZ.size());
			LOGGER.info("FAO\t" + FAO.size());
			LOGGER.info("STAT_RECT\t" + STAT_RECT.size());
			LOGGER.info("PORT\t" + PORT.size());
			LOGGER.info("GFCM\t" + GFCM.size());
			LOGGER.info("RFMO\t" + RFMO.size());
			LOGGER.info("PORT_AREA\t" + PORT_AREA.size());
		} catch (Exception e) {
			throw e;
		}
	}

	
	// bara för att visa hur man gör detta i batchmode
	private  void importNodesSAMPLE() throws IOException {
	  
	  String suffix = UUID.randomUUID().toString();  
	  //BatchInserter inserter = BatchInserters.inserter(new File("neo4j-db/")); 
	  BatchInserter inserter = BatchInserters.inserter(new File("neo4j-db_" + suffix + "/"));
	  
	  // inject some data 
	  Map<String, Object> properties = new HashMap<String,Object>();
	  
	  properties.put("name", "Mr. Andersson"); properties.put("age", 29); long
	  node1 = inserter.createNode(properties, Labels.EEZ);
	  
	  properties.put("name", "Trinity"); properties.remove("age"); long node2 =
	  inserter.createNode(properties, Labels.EEZ);
	  
	  inserter.createRelationship(node1, node2, RelationshipType.withName("KNOWS"),
	  null);
	  
	  // shutdown, makes sure all changes are written to disk 
	  inserter.shutdown();
	  
	  }

}
