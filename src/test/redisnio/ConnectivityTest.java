package redisnio;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test if we can use an environment variable to establish connectivity to redis.
 * @author mshankar
 *
 */
public class ConnectivityTest {
	private static final Logger logger = LoggerFactory.getLogger(ConnectivityTest.class);
	String redisUrl;

	@Before
	public void setUp() throws Exception {
		redisUrl = System.getenv("ARCHAPPL_PERSISTENCE_LAYER_REDISURL");
		List<FileSystemProvider> providers =  FileSystemProvider.installedProviders();
		for(FileSystemProvider provider : providers) { 
			logger.info("Found provider: " + provider.getScheme());
		}
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConnections() throws IOException, URISyntaxException {
		assertTrue("Please define the environemnt variable ARCHAPPL_PERSISTENCE_LAYER_REDISURL", redisUrl != null);
		assertTrue("ARCHAPPL_PERSISTENCE_LAYER_REDISURL should start with redis://", redisUrl.startsWith("redis://"));
		assertTrue("ARCHAPPL_PERSISTENCE_LAYER_REDISURL should end with /", redisUrl.endsWith("/"));
		Path p = Paths.get(new URI(redisUrl + "music/cc"));
		Files.deleteIfExists(p);
		List<String> srcLines = Arrays.asList(new String[] {"Time won't give me time", "Church of the poisoned mind", "Culture club"});
		Files.write(p, srcLines);
		assertTrue("Count not get a connection to the redis server at " + redisUrl, p != null);
		List<String> allLines = Files.readAllLines(p);
		assertTrue("We should have some data in the root contect", allLines.size() > 0);
		assertTrue("We did not get back what we wrote", allLines.equals(srcLines));
	}

}
