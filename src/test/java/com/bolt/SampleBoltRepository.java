package com.bolt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;

import com.CypherLoader;

public class SampleBoltRepository {

	private static Driver driver;

	// CREATE INDEX ON:PORT(entitytype)

	private static String uri = "bolt://localhost";
	private static String uid = "neo4j";
	private static String pwd = "neo4j";

	@BeforeClass
	public static void beforeClass() {
		driver = GraphDatabase.driver(uri, AuthTokens.basic(uid, pwd));
	}

	@AfterClass
	public static void afterClass() {
		if (driver != null) {
			driver.close();
		}
	}

	@Before
	public void before() {
		// driver = GraphDatabase.driver(uri, AuthTokens.basic(uid, pwd));
	}

	@After
	public void after() {
		if (driver != null) {
			// driver.close();
		}
	}

	public List<Map<String, Object>> spatExec(String query, Map<String, Object> params,
			Consumer<StatementResult> resultConsumer) {

		try (Session session = driver.session()) {
			resultConsumer.accept(session.run(query, params));
		}
		return null;
	}

	public List<Map<String, Object>> execQuery(String query) {
		return execQuery(query, null);
	}

	public List<Map<String, Object>> execQuery(String query, Map<String, Object> parameters) {

		List<Map<String, Object>> resultSet = new ArrayList<>();

		try (Session session = driver.session()) {
			StatementResult result = null;
			if (parameters != null) {
				result = session.run(query, parameters);
			} else {
				result = session.run(query);
			}
			boolean headersRetrieved = false;
			List<String> keys = new ArrayList<>();
			while (result.hasNext()) {
				Record record = result.next();
				if (!headersRetrieved) {
					keys.addAll(record.keys());
					headersRetrieved = true;
				}
				Map<String, Object> row = new HashMap<>();
				for (String key : keys) {
					Value value = record.get(key);
					row.put(key, value);
				}
				resultSet.add(row);
			}
		}
		return resultSet;
	}

	@Test
	public void test01() throws IOException {

		int NUMBER_OF_CALLS = 5;

		String query = CypherLoader.get("test011.cypher");
		Map<String, Object> prm = new HashMap<>();
		prm.put("eez_id", 4);

		List<Map<String, Object>> rs = null;
		long then = System.currentTimeMillis();
		for (int i = 0; i < NUMBER_OF_CALLS; i++) {
			rs = execQuery(query, prm);
		}
		long now = System.currentTimeMillis();

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}

		double tot = (double) now - (double) then;
		System.out.println((tot / (double) NUMBER_OF_CALLS) + " ms " + rs.size() + " in rs");

	}

	@Test
	public void test02() throws IOException {

		String query = CypherLoader.get("test022.cypher");
		Map<String, Object> prm = new HashMap<>();
		prm.put("code", "SWE");
		List<Map<String, Object>> rs = execQuery(query, prm);

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}
	}

	@Test
	public void test03() throws IOException {

		String query = CypherLoader.get("test033.cypher");
		Map<String, Object> prm = new HashMap<>();
		prm.put("code", "SWE");
		List<Map<String, Object>> rs = execQuery(query, prm);

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}
	}

	@Test
	public void test04() throws IOException {

		String query = CypherLoader.get("test044.cypher");
		Map<String, Object> prm = new HashMap<>();
		prm.put("name", "Antwerpen");
		List<Map<String, Object>> rs = execQuery(query, prm);

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}
	}

	@Test
	public void test05() throws IOException {
		String query = CypherLoader.get("test100.cypher");
		Map<String, Object> prm = new HashMap<>();
		prm.put("layer", "PORT");

		long then = System.currentTimeMillis();
		List<Map<String, Object>> rs = execQuery(query, prm);
		long now = System.currentTimeMillis();

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}
		System.out.println(now - then + " ms");

	}

	@Test
	public void test07() throws IOException {

		String query = CypherLoader.get("test101.cypher");

		Map<String, Object> prm = new HashMap<>();
		prm.put("latitude", 57.71604);
		prm.put("longitude", 11.971407);
		prm.put("distanceInKm", 1.5);
		prm.put("layer", "PORT_AREA");

		long then = System.currentTimeMillis();
		List<Map<String, Object>> rs = execQuery(query, prm);
		long now = System.currentTimeMillis();

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}
		System.out.println(now - then + " ms");
	}

	@Test
	public void test08() throws IOException {

		String query = CypherLoader.get("test101.cypher");

		Map<String, Object> prm = new HashMap<>();
		prm.put("latitude", 51.124799);
		prm.put("longitude", 1.339989);
		prm.put("distanceInKm", 2);
		prm.put("layer", "PORT");

		long then = System.currentTimeMillis();
		List<Map<String, Object>> rs = execQuery(query, prm);
		long now = System.currentTimeMillis();

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}
		System.out.println(now - then + " ms");
	}

	@Test
	public void test09() throws IOException {

		String query = CypherLoader.get("test101.cypher");

		Map<String, Object> prm = new HashMap<>();
		prm.put("latitude", 51.124799);
		prm.put("longitude", 1.339989);
		prm.put("distanceInKm", 2);
		prm.put("layer", "PORT_AREA");

		long then = System.currentTimeMillis();
		List<Map<String, Object>> rs = execQuery(query, prm);
		long now = System.currentTimeMillis();

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}
		System.out.println(now - then + " ms");

	}

	@Test
	public void test10() throws IOException {

		String query = CypherLoader.get("test101.cypher");

		Map<String, Object> prm = new HashMap<>();
		prm.put("latitude", 51.124799);
		prm.put("longitude", 1.339989);
		prm.put("distanceInKm", 10);
		prm.put("layer", "EEZ");

		long then = System.currentTimeMillis();
		List<Map<String, Object>> rs = execQuery(query, prm);
		long now = System.currentTimeMillis();
		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}
		System.out.println(now - then + " ms");
	}

	@Test
	public void test11() throws IOException {

		String query = CypherLoader.get("test101.cypher");

		Map<String, Object> prm = new HashMap<>();
		prm.put("latitude", 57.71604);
		prm.put("longitude", 11.971407);
		prm.put("distanceInKm", 10);
		prm.put("layer", "EEZ");

		long then = System.currentTimeMillis();
		List<Map<String, Object>> rs = execQuery(query, prm);
		long now = System.currentTimeMillis();

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}

		System.out.println(now - then + " ms");

	}

	@Test
	public void test12() throws IOException {

		String query = CypherLoader.get("test102_distance.cypher");

		long then = System.currentTimeMillis();
		List<Map<String, Object>> rs = execQuery(query);
		long now = System.currentTimeMillis();

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}

		System.out.println(now - then + " ms");

	}

	@Test
	public void test13() throws IOException {

		String query = CypherLoader.get("test103_distance.cypher");
		Map<String, Object> prm = new HashMap<>();
		//
		prm.put("from_latitude", 57.71604);
		prm.put("from_longitude", 11.971407);
		//
		prm.put("to_latitude", 57.743564);
		prm.put("to_longitude", 14.166870);

		long then = System.currentTimeMillis();
		List<Map<String, Object>> rs = execQuery(query, prm);
		long now = System.currentTimeMillis();

		for (Map<String, Object> rec : rs) {
			System.out.println("---------------------------------------------------------");
			for (Map.Entry<String, Object> o : rec.entrySet()) {
				System.out.println(o.getKey() + "    " + o.getValue());
			}
		}

		System.out.println(now - then + " ms");

	}

	@Test
	public void test14() throws IOException {

		String query = CypherLoader.get("test104_closest.cypher");
		Map<String, Object> prm = new HashMap<>();
		//
		prm.put("from_latitude", 57.71604);
		prm.put("from_longitude", 11.971407);
		//
		prm.put("to_latitude", 57.743564);
		prm.put("to_longitude", 14.166870);

		long then = System.currentTimeMillis();
		List<Map<String, Object>> rs = spatExec(query, null, res -> {

			while (res.hasNext()) {
				Record rec = res.next();
				System.out.println(rec.toString());
			}

		});
		long now = System.currentTimeMillis();

		System.out.println(now - then + " ms");

	}

}
