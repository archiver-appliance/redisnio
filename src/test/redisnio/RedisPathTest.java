package redisnio;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.stanford.slac.archiverappliance.PlainPB.fs.redis.RedisFileSystemProvider;
import edu.stanford.slac.archiverappliance.PlainPB.fs.redis.RedisPath;

/**
 * Tests to make sure various RedisPath methods satisfy the requirements of the EPICS archiver appliance.
 * @author mshankar
 *
 */
public class RedisPathTest {
	RedisFileSystemProvider provider = new RedisFileSystemProvider();

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRedisPath() {
		RedisPath p1 = new RedisPath(provider, "localhost:6379", "/folder1/folder2/fileName:1234.pb");
		RedisPath absolute = new RedisPath(provider, "localhost:6379", "/folder1/folder2/fileName:1234.pb", true);
		assertTrue("FileName should only return the final name component fileName:1234.pb; instead we got " + p1.getFileName(), p1.getFileName().toString().equals("fileName:1234.pb"));
		assertTrue("FileName should only return the final name component; instead we got " + absolute.getFileName(), absolute.getFileName().toString().equals("fileName:1234.pb"));
		assertTrue("absolutePath.toString should return URI; instead we got " + p1.toAbsolutePath().toString(), p1.toAbsolutePath().toString().equals("redis://localhost:6379/folder1/folder2/fileName:1234.pb"));
		assertTrue("absolutePath.toString should return URI; instead we got " + absolute.toAbsolutePath().toString(), absolute.toAbsolutePath().toString().equals("redis://localhost:6379/folder1/folder2/fileName:1234.pb"));
	}

}
