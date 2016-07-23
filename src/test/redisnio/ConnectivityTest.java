package redisnio;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
	public void testReadWrite() throws IOException, URISyntaxException {
		assertTrue("Please define the environemnt variable ARCHAPPL_PERSISTENCE_LAYER_REDISURL", redisUrl != null);
		assertTrue("ARCHAPPL_PERSISTENCE_LAYER_REDISURL should start with redis://", redisUrl.startsWith("redis://"));
		assertTrue("ARCHAPPL_PERSISTENCE_LAYER_REDISURL should end with /", redisUrl.endsWith("/"));
		
		Set<Path> keysInData = new TreeSet<Path>();
		for(TestData t : testData) { 
			Files.deleteIfExists(Paths.get(new URI(redisUrl + t.key)));
			keysInData.add(Paths.get(new URI(redisUrl + t.key)));
		}

		
		for(TestData t : testData) {
			Files.write(Paths.get(new URI(redisUrl + t.key)), t.content);
			List<String> allLines = Files.readAllLines(Paths.get(new URI(redisUrl + t.key)));
			assertTrue("We should have some data at the key " + t.key, allLines.size() > 0);
			assertTrue("We did not get back what we wrote", allLines.equals(t.content));
		}
		
		Set<Path> keysFromDStream = new TreeSet<Path>();
		try(DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(new URI(redisUrl + "music")))) {
			for(Path p : ds) { 
				keysFromDStream.add(p);
			}
		}
		
		assertTrue("Directory streams did not match", keysFromDStream.equals(keysInData));
				
		for(TestData t : testData) { 
			Files.delete(Paths.get(new URI(redisUrl + t.key)));
		}
	}

	private final class TestData {
		String key;
		List<String> content;
		TestData(String key, String[] content) { 
			this.key = key;
			this.content = Arrays.asList(content);
		}
	}
	TestData[] testData = new TestData[] {
		new TestData("music/pink_floyd/wish_you_were_here", new String[] {"Shine on you crazy diamond", "Welcome to the machine", "Have a cigar", "Wish you were here"}),
		new TestData("music/pink_floyd/dark_side_of_the_moon", new String[] {"Speak to Me", "Breathe", "On the Run", "Time", "The Great Gig in the Sky"})
	};
}
