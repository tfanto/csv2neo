package fnt;

import java.io.IOException;

import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class Importer extends BaseClass {

	public void importEEZ() throws IOException {

		try (Transaction tx = db.beginTx()) {

			for (Line_EEZ line : EEZ) {

				Node node = db.createNode(Labels.EEZ);

				node.setProperty("entitytype", "EEZ");

				if (line.SRID != null)
					node.setProperty("SRID", line.SRID);
				if (line.geometry != null)
					node.setProperty("geometry", line.geometry);
				if (line.name != null)
					node.setProperty("name", line.name);
				if (line.country != null)
					node.setProperty("country", line.country);
				if (line.sovereign != null)
					node.setProperty("sovereign", line.sovereign);
				if (line.remarks != null)
					node.setProperty("remarks", line.remarks);
				if (line.sov_id != null)
					node.setProperty("sov_id", line.sov_id);
				if (line.eez_id != null)
					node.setProperty("eez_id", line.eez_id);
				if (line.code != null)
					node.setProperty("code", line.code);
				if (line.mrgid != null)
					node.setProperty("mrgid", line.mrgid);
				if (line.date_chang != null)
					node.setProperty("date_chang", line.date_chang);
				if (line.area_m2 != null)
					node.setProperty("area_m2", line.area_m2);
				if (line.mrgid_eez != null)
					node.setProperty("mrgid_eez", line.mrgid_eez);
				if (line.enabled != null)
					node.setProperty("enabled", line.enabled);

				SpatialDatabaseRecord spatialDatabaseRecord = layerEEZ.add(node);

				line.spatialDatabaseRecord = spatialDatabaseRecord;

			}
			tx.success();
		}

		// inserter.createRelationship(node1, node2,RelationshipType.withName("KNOWS"),
		// // null);

	}

	public void importFAO() throws IOException {

		try (Transaction tx = db.beginTx()) {

			for (Line_FAO line : FAO) {

				Node node = db.createNode(Labels.FAO);
				
				node.setProperty("entitytype", "FAO");


				if (line.SRID != null)
					node.setProperty("SRID", line.SRID);
				if (line.geometry != null)
					node.setProperty("geometry", line.geometry);
				if (line.name != null)
					node.setProperty("name", line.name);
				if (line.enabled != null)
					node.setProperty("enabled", line.enabled);
				if (line.ocean != null)
					node.setProperty("ocean", line.ocean);
				if (line.subocean != null)
					node.setProperty("subocean", line.subocean);
				if (line.f_area != null)
					node.setProperty("f_area", line.f_area);
				if (line.area_l != null)
					node.setProperty("area_l", line.area_l);
				if (line.f_subarea != null)
					node.setProperty("f_subarea", line.f_subarea);
				if (line.subarea_n != null)
					node.setProperty("subarea_n", line.subarea_n);
				if (line.subarea_l != null)
					node.setProperty("subarea_l", line.subarea_l);
				if (line.f_division != null)
					node.setProperty("f_division", line.f_division);
				if (line.division_n != null)
					node.setProperty("division_n", line.division_n);
				if (line.division_l != null)
					node.setProperty("division_l", line.division_l);
				if (line.f_subdivis != null)
					node.setProperty("f_subdivis", line.f_subdivis);
				if (line.subdivis_n != null)
					node.setProperty("subdivis_n", line.subdivis_n);
				if (line.subdivis_l != null)
					node.setProperty("subdivis_l", line.subdivis_l);
				if (line.f_subunit != null)
					node.setProperty("f_subunit", line.f_subunit);
				if (line.subunit_n != null)
					node.setProperty("subunit_n", line.subunit_n);
				if (line.subunit_l != null)
					node.setProperty("subunit_l", line.subunit_l);
				if (line.ele_name != null)
					node.setProperty("ele_name", line.ele_name);
				if (line.ele_label != null)
					node.setProperty("ele_label", line.ele_label);
				if (line.ele_type != null)
					node.setProperty("ele_type", line.ele_type);
				if (line.f_label != null)
					node.setProperty("f_label", line.f_label);
				if (line.code != null)
					node.setProperty("code", line.code);

				SpatialDatabaseRecord spatialDatabaseRecord = layerFAO.add(node);
				line.spatialDatabaseRecord = spatialDatabaseRecord;

			}
			tx.success();
		}
	}

	public void importPORT() throws IOException {

		try (Transaction tx = db.beginTx()) {

			for (Line_Port line : PORT) {

				Node node = db.createNode(Labels.PORT);
				node.setProperty("entitytype", "PORT");

				if (line.SRID != null)
					node.setProperty("SRID", line.SRID);
				if (line.geometry != null)
					node.setProperty("geometry", line.geometry);
				if (line.name != null)
					node.setProperty("name", line.name);
				if (line.code != null)
					node.setProperty("code", line.code);
				if (line.country_code != null)
					node.setProperty("country_code", line.country_code);
				if (line.fishing_port != null)
					node.setProperty("fishing_port", line.fishing_port);
				if (line.landing_place != null)
					node.setProperty("landing_place", line.landing_place);
				if (line.commercial_port != null)
					node.setProperty("commercial_port", line.commercial_port);
				if (line.enabled != null)
					node.setProperty("enabled", line.enabled);

				SpatialDatabaseRecord spatialDatabaseRecord = layerPORT.add(node);

				line.spatialDatabaseRecord = spatialDatabaseRecord;

			}
			tx.success();
		}

		// inserter.createRelationship(node1, node2,RelationshipType.withName("KNOWS"),
		// // null);

	}

	public void importGFCM() throws IOException {

		try (Transaction tx = db.beginTx()) {

			for (Line_GFCM line : GFCM) {

				Node node = db.createNode(Labels.GFCM);
				node.setProperty("entitytype", "GFCM");

				if (line.SRID != null)
					node.setProperty("SRID", line.SRID);
				if (line.geometry != null)
					node.setProperty("geometry", line.geometry);
				if (line.name != null)
					node.setProperty("name", line.name);
				if (line.code != null)
					node.setProperty("code", line.code);
				if (line.enabled != null)
					node.setProperty("enabled", line.enabled);

				SpatialDatabaseRecord spatialDatabaseRecord = layerGFCM.add(node);

				line.spatialDatabaseRecord = spatialDatabaseRecord;

			}
			tx.success();
		}

		// inserter.createRelationship(node1, node2,RelationshipType.withName("KNOWS"),
		// // null);

	}

	public void importPortArea() throws IOException {

		try (Transaction tx = db.beginTx()) {

			for (Line_PortArea line : PORT_AREA) {

				Node node = db.createNode(Labels.PORT_AREA);
				node.setProperty("entitytype", "PORT_AREA");

				if (line.SRID != null)
					node.setProperty("SRID", line.SRID);
				if (line.geometry != null)
					node.setProperty("geometry", line.geometry);
				if (line.name != null)
					node.setProperty("name", line.name);
				if (line.code != null)
					node.setProperty("code", line.code);
				if (line.enabled != null)
					node.setProperty("enabled", line.enabled);

				SpatialDatabaseRecord spatialDatabaseRecord = layerPORTAREA.add(node);

				line.spatialDatabaseRecord = spatialDatabaseRecord;

			}
			tx.success();
		}

		// inserter.createRelationship(node1, node2,RelationshipType.withName("KNOWS"),
		// // null);

	}

	public void importRFMO() throws IOException {

		try (Transaction tx = db.beginTx()) {

			for (Line_RFMO line : RFMO) {

				Node node = db.createNode(Labels.RFMO);
				node.setProperty("entitytype", "RFMO");

				if (line.SRID != null)
					node.setProperty("SRID", line.SRID);
				if (line.geometry != null)
					node.setProperty("geometry", line.geometry);
				if (line.name != null)
					node.setProperty("name", line.name);
				if (line.code != null)
					node.setProperty("code", line.code);
				if (line.enabled != null)
					node.setProperty("enabled", line.enabled);
				if (line.tuna != null)
					node.setProperty("tuna", line.tuna);

				SpatialDatabaseRecord spatialDatabaseRecord = layerRFMO.add(node);

				line.spatialDatabaseRecord = spatialDatabaseRecord;

			}
			tx.success();
		}

		// inserter.createRelationship(node1, node2,RelationshipType.withName("KNOWS"),
		// // null);

	}

	public void importStatRect() throws IOException {

		try (Transaction tx = db.beginTx()) {

			for (Line_StatRect line : STAT_RECT) {

				Node node = db.createNode(Labels.STAT_RECT);
				node.setProperty("entitytype", "STAT_RECT");

				if (line.SRID != null)
					node.setProperty("SRID", line.SRID);
				if (line.geometry != null)
					node.setProperty("geometry", line.geometry);
				if (line.name != null)
					node.setProperty("name", line.name);
				if (line.code != null)
					node.setProperty("code", line.code);
				if (line.enabled != null)
					node.setProperty("enabled", line.enabled);
				if (line.north != null)
					node.setProperty("north", line.north);
				if (line.south != null)
					node.setProperty("south", line.south);
				if (line.east != null)
					node.setProperty("east", line.east);
				if (line.west != null)
					node.setProperty("west", line.west);

				SpatialDatabaseRecord spatialDatabaseRecord = layerSTATRECT.add(node);

				line.spatialDatabaseRecord = spatialDatabaseRecord;

			}
			tx.success();
		}

		// inserter.createRelationship(node1, node2,RelationshipType.withName("KNOWS"),
		// // null);
		/*
		 * select * from spatial.port_area as pa join spatial.port as p on p.code =
		 * pa.code
		 */

	}

	private void relations_Port_PortArea() {

		// one way
		try (Transaction tx = db.beginTx()) {
			for (Line_PortArea paLine : PORT_AREA) {
				if (paLine.code == null || paLine.code.trim().length() < 1) {
					continue;
				}
				for (Line_Port pLine : PORT) {
					if (pLine.code == null || pLine.code.trim().length() < 1) {
						continue;
					}
					if (paLine.code.trim().equals(pLine.code.trim())) {
						Long paId = paLine.spatialDatabaseRecord.getNodeId();
						Long pId = pLine.spatialDatabaseRecord.getNodeId();
						Node portAreaNode = db.getNodeById(paId);
						Node portNode = db.getNodeById(pId);
						portAreaNode.createRelationshipTo(portNode, Relations.PORTAREA_CONTAINING_PORTS);
					}
				}
			}
			tx.success();
		}
		
		// the other way
		try (Transaction tx = db.beginTx()) {
			for (Line_Port pLine : PORT) {
				if (pLine.code == null || pLine.code.trim().length() < 1) {
					continue;
				}
				for (Line_PortArea paLine : PORT_AREA) {
					if (paLine.code == null || paLine.code.trim().length() < 1) {
						continue;
					}
					if (paLine.code.trim().equals(pLine.code.trim())) {
						Long paId = paLine.spatialDatabaseRecord.getNodeId();
						Long pId = pLine.spatialDatabaseRecord.getNodeId();
						Node portAreaNode = db.getNodeById(paId);
						Node portNode = db.getNodeById(pId);
						portNode.createRelationshipTo(portAreaNode, Relations.PORTS_IN_PORTAREA);
					}
				}
			}
			tx.success();
		}

		
	}
	
	
	private void relations_Port_EEZ() {

		
		// one way
		try (Transaction tx = db.beginTx()) {
			for (Line_Port pLine : PORT) {
				if (pLine.country_code == null || pLine.country_code.trim().length() < 1) {
					continue;
				}
				for (Line_EEZ eezLine : EEZ) {
					if (eezLine.code == null || eezLine.code.trim().length() < 1) {
						continue;
					}
					if (eezLine.code.trim().equals(pLine.country_code.trim())) {
						Long eezId = eezLine.spatialDatabaseRecord.getNodeId();
						Long pId = pLine.spatialDatabaseRecord.getNodeId();
						Node eezNode = db.getNodeById(eezId);
						Node portNode = db.getNodeById(pId);
						portNode.createRelationshipTo(eezNode, Relations.PORTS_IN_EEZ);
					}
				}
			}
			tx.success();
		}
		
		// the other way
		try (Transaction tx = db.beginTx()) {
			for (Line_EEZ eezLine : EEZ) {
				if (eezLine.code == null || eezLine.code.trim().length() < 1) {
					continue;
				}
				for (Line_Port pLine : PORT) {
					if (pLine.country_code == null || pLine.country_code.trim().length() < 1) {
						continue;
					}
					if (eezLine.code.trim().equals(pLine.country_code.trim())) {
						Long eezId = eezLine.spatialDatabaseRecord.getNodeId();
						Long pId = pLine.spatialDatabaseRecord.getNodeId();
						Node eezNode = db.getNodeById(eezId);
						Node portNode = db.getNodeById(pId);
						eezNode.createRelationshipTo(portNode, Relations.EEZ_CONTAINING_PORTS);
					}
				}
			}
			tx.success();
		}

		
	}

	
	

	private void populateNeo() throws IOException {

		openNeo();

		importEEZ();
		importFAO();
		importPORT();
		importGFCM();
		importPortArea();
		importRFMO();
		importStatRect();

		relations_Port_PortArea();
		relations_Port_EEZ();

		closeNeo();

	}

	private void main() {
		try {
			loadFilesToMemory();
			populateNeo();
		} catch (Exception e) {
			LOGGER.error(e.toString(), e);
		}

	}

	public static void main(String[] args) throws IOException {
		new Importer().main();
	}

}
