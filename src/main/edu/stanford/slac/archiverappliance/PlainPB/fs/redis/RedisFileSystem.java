package edu.stanford.slac.archiverappliance.PlainPB.fs.redis;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPool;

/**
 * A redis file system encapsulates a jedis connection. 
 * We create a JedisPool when the filesystem is created.
 * 
 * @author mshankar
 *
 */
public class RedisFileSystem extends FileSystem {
	private static final Logger logger = LoggerFactory.getLogger(RedisFileSystem.class);
	private RedisFileSystemProvider theProvider;
	JedisPool jedisPool = null;
	private String pathPrefix;
	private String connectionName;
	
	public RedisFileSystem(RedisFileSystemProvider theProvider, String server, int port, String pathPrefix, Map<String, ?> env) {
		this.theProvider = theProvider;
		this.connectionName = server + ":" + port;
		this.jedisPool = new JedisPool(server, port);
		this.pathPrefix = pathPrefix;
	}

	@Override
	public FileSystemProvider provider() {
		return theProvider;
	}

	@Override
	public void close() throws IOException {
		// For now, we don't do anything here..
		// Should we close the jedis pool?
		logger.debug("Close called on RedisFileSytem; should we close the connection pool?");
	}

	@Override
	public boolean isOpen() {
		return !jedisPool.isClosed();
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public String getSeparator() {
		return "/";
	}
	
	@Override
	public Path getPath(String first, String... more) {
		return new RedisPath(this, first + String.join("/", more));
	}

	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<String> supportedFileAttributeViews() {
		throw new UnsupportedOperationException();
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		throw new UnsupportedOperationException();
	}

	@Override
	public WatchService newWatchService() throws IOException {
		throw new UnsupportedOperationException();
	}

	/**
	 * This is the path portion of the URL that was used to create this fileSystem.
	 * This could be null.
	 * @return
	 */
	public String getPathPrefix() {
		return pathPrefix;
	}
	
	public String toString() { 
		return "redis://" + this.connectionName + "/" + this.getPathPrefix();
	}
}
