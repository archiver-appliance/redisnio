package redisnio;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.spi.FileSystemProvider;
import java.util.List;

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
		List<FileSystemProvider> providers =  FileSystemProvider.installedProviders();
		for(FileSystemProvider provider : providers) { 
			System.out.println("Found provider: " + provider.getScheme());
		}
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConnections() throws IOException, URISyntaxException {
		assertTrue("Please define the environemnt variable ARCHAPPL_PERSISTENCE_LAYER_REDISURL", redisUrl != null);
		assertTrue("ARCHAPPL_PERSISTENCE_LAYER_REDISURL should start with redis://", redisUrl.startsWith("redis://"));
		Path p = Paths.get(new URI(redisUrl + "/"));
		assertTrue("Count not get a connection to the redis server at " + redisUrl, p != null);
		List<String> allLines = Files.readAllLines(p);
		assertTrue("We should have some data in the root contect", allLines.size() > 0);
	}

}
