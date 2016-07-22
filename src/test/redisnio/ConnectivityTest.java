package redisnio;

import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test if we can use an environment variable to establish connectivity to redis.
 * @author mshankar
 *
 */
public class ConnectivityTest {
	String redisUrl;

	@Before
	public void setUp() throws Exception {
		redisUrl = System.getenv("ARCHAPPL_PERSISTENCE_LAYER_REDISURL");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConnections() {
		Path p = Paths.get(redisUrl + "/");
		assertTrue("Count not get a connection to the redis server at " + redisUrl, p != null);
	}

}
