package redisnio;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Test if we can use an environment variable to establish connectivity to redis.
 * @author mshankar
 *
 */
public class ConnectivityTest {
	private static final Logger logger = Logger.getLogger(ConnectivityTest.class.getName());
	String redisUrl;

	@Before
	public void setUp() throws Exception {
		redisUrl = System.getenv("ARCHAPPL_TEST_REDISURL");
		if(redisUrl == null) { 
			redisUrl = "redis://localhost:6379/";
		} else { 
			assertTrue("ARCHAPPL_TEST_REDISURL should start with redis://", redisUrl.startsWith("redis://"));
			assertTrue("ARCHAPPL_TEST_REDISURL should end with /", redisUrl.endsWith("/"));
		}
		
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
		
		Set<Path> keysInData = new TreeSet<Path>();
		for(TestData t : testData) { 
			Files.deleteIfExists(Paths.get(new URI(redisUrl + t.key)));
			keysInData.add(Paths.get(new URI(redisUrl + t.key)));
		}
		
		try(DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(new URI(redisUrl + "music")))) {
			for(Path p : ds) {
				logger.info("Cleaning up leftovers from previous test runs.");
				Files.delete(p);
			}
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
		
		// Test move and append
		List<String> srcAllLines = new LinkedList<String>(); 
		srcAllLines.addAll(testData[0].content);
		srcAllLines.addAll(testData[1].content);
		{
			Path srcPath = Paths.get(new URI(redisUrl + "music/moveSrc"));
			Files.deleteIfExists(srcPath);
			Path destPath = Paths.get(new URI(redisUrl + "music/moveDest"));
			Files.deleteIfExists(destPath);
			
			Files.write(srcPath, testData[0].content);
			Files.write(srcPath, testData[1].content, StandardOpenOption.APPEND);
			
			{ 
				List<String> allLines = Files.readAllLines(srcPath);
				assertTrue("Append did not work correctly", allLines.equals(srcAllLines));
			}
			
			Files.move(srcPath, destPath);

			{
				assertTrue("Move did not create new file", Files.exists(destPath));
				assertTrue("Move did not remove old file", !Files.exists(srcPath));
				List<String> allLines = Files.readAllLines(destPath);
				assertTrue("Append did not work correctly", allLines.equals(srcAllLines));
			}

			Files.deleteIfExists(srcPath);
			Files.deleteIfExists(destPath);		
		}
		
		// Test copy 
		{
			Path srcPath = Paths.get(new URI(redisUrl + "music/copySrc"));
			Files.deleteIfExists(srcPath);
			Path destPath = Paths.get(new URI(redisUrl + "music/copyDest"));
			Files.deleteIfExists(destPath);
			
			Files.write(srcPath, testData[0].content);
			Files.write(srcPath, testData[1].content, StandardOpenOption.APPEND);
			
			{ 
				List<String> allLines = Files.readAllLines(srcPath);
				assertTrue("Move did not move content correctly", allLines.equals(srcAllLines));
			}
			
			Files.copy(srcPath, destPath);			
			{
				assertTrue("Copy did not create new file", Files.exists(destPath));
				assertTrue("Copy removed old file", Files.exists(srcPath));
				List<String> allLines = Files.readAllLines(destPath);
				assertTrue("Copy did not copy content correctly", allLines.equals(srcAllLines));
			}

			Files.deleteIfExists(srcPath);
			Files.deleteIfExists(destPath);		
		}
		
		// Make sure everything is clean
		URI redisURI = new URI(redisUrl);
		JedisPool jedisPool = new JedisPool(redisURI.getHost(), redisURI.getPort());
		try(Jedis jedis = jedisPool.getResource()) {
			assertTrue("Still some keys remaining", jedis.keys("music/*").isEmpty());
			assertTrue("Still some attributes remaining", jedis.keys("Attrsmusic/*").isEmpty());
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
