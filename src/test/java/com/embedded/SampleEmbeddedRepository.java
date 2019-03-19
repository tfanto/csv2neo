package com.embedded;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import com.CypherLoader;

public class SampleEmbeddedRepository {

	protected static File DB_PATH = null;
	protected static GraphDatabaseService db = null;
	protected static SpatialDatabaseService spatialService = null;

	protected static Layer layerEEZ = null;
	protected static Layer layerFAO = null;
	protected static Layer layerGFCM = null;
	protected static Layer layerPORT = null;
	protected static Layer layerPORTAREA = null;
	protected static Layer layerRFMO = null;
	protected static Layer layerSTATRECT = null;

	@BeforeClass
	public static void beforeClass() throws IllegalArgumentException, KernelException {
		DB_PATH = new File("E:\\Neo4j\\neo4j-db-2");
		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);

		spatialService = new SpatialDatabaseService(db);
		layerEEZ = spatialService.getOrCreateEditableLayer("EEZ", "geometry");
		layerFAO = spatialService.getOrCreateEditableLayer("FAO", "geometry");
		layerGFCM = spatialService.getOrCreateEditableLayer("GFCM", "geometry");
		layerPORT = spatialService.getOrCreateEditableLayer("PORT", "geometry");
		layerPORTAREA = spatialService.getOrCreateEditableLayer("PORT_AREA", "geometry");
		layerRFMO = spatialService.getOrCreateEditableLayer("RFMO", "geometry");
		layerSTATRECT = spatialService.getOrCreateEditableLayer("STATRECT", "geometry");
	}

	@AfterClass
	public static void afterClass() {
		if (db != null) {
			db.shutdown();
		}
	}

	@Test
	public void test01() throws IOException {

		String query = CypherLoader.get("test011.cypher");
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("eez_id", 4);

		try {

			Result result = null;
			int NUMBER_OF_CALLS = 5;

			long then = System.currentTimeMillis();
			for (int i = 0; i < NUMBER_OF_CALLS; i++) {
				result = db.execute(query, parameters);
			}
			long now = System.currentTimeMillis();

			int recCount = 0;
			while (result.hasNext()) {
				recCount++;
				Map<String, Object> map = result.next();
				String name = String.valueOf(map.get("name"));
				String geometry = String.valueOf(map.get("geometry"));

				double ar[] = (double[]) map.get("bbox");
				List<Double> bbox = new ArrayList<>();
				for (int i = 0; i < ar.length; i++) {
					bbox.add(ar[i]);
				}

				System.out.println("---------------------------------------------------------");
				System.out.println("name    " + name);
				System.out.println("geometry    " + geometry);
				System.out.println("bbox    " + bbox);
			}

			double tot = (double) now - (double) then;
			System.out.println((tot / (double) NUMBER_OF_CALLS) + " ms " + recCount + " in rs");
		} catch (RuntimeException e) {
			e.printStackTrace();
		}

	}

}
