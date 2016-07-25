package edu.stanford.slac.archiverappliance.PlainPB.fs.redis;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
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
	private String connectionName;
	
	public RedisFileSystem(RedisFileSystemProvider theProvider, String server, int port, Map<String, ?> env) {
		this.theProvider = theProvider;
		this.connectionName = server + ":" + port;
		this.jedisPool = new JedisPool(server, port);
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
		return new RedisPath(theProvider, this.connectionName, first + String.join("/", more));
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

	public String toString() { 
		return "redis://" + this.connectionName;
	}

	public void deleteKey(RedisPath redisPath) {
		try(Jedis jedis = this.jedisPool.getResource()) {
			jedis.del(redisPath.getRedisKey().getBytes());
		}		
	}

	public DirectoryStream<Path> getMatchingKeys(String redisKey, Filter<? super Path> filter) {
		try(Jedis jedis = this.jedisPool.getResource()) {
			Set<Path> matchingPaths = new TreeSet<Path>();
			for(String matchingKey :jedis.keys(redisKey + "/*")) { 
				RedisPath mathingRedisPath = new RedisPath(this.theProvider, this.connectionName, matchingKey);
				if(filter != null) {
					try { 
						if(filter.accept(mathingRedisPath)) { 
							matchingPaths.add(mathingRedisPath);
						}
					} catch(IOException ex) { 
						logger.error("Exception from filter when matching " + matchingKey);
					}
				} else { 
					matchingPaths.add(mathingRedisPath);
				}
			}
			return new RedisPathDirectoryStream(matchingPaths);
		}
	}
	
	private final class RedisPathDirectoryStream implements DirectoryStream<Path> {
		private Set<Path> paths;
		RedisPathDirectoryStream(Set<Path> paths) { 
			this.paths = paths;
		}
		
		@Override
		public void close() throws IOException {
		}

		@Override
		public Iterator<Path> iterator() {
			return paths.iterator();
		}
	}

	/**
	 * Rename the key redisSrcKey to redisTargetKey
	 * @param redisSrcKey
	 * @param redisTargetKey
	 */
	public void rename(String redisSrcKey, String redisTargetKey) throws IOException {
		try(Jedis jedis = this.jedisPool.getResource()) {
			jedis.rename(redisSrcKey, redisTargetKey);
		}
	}

	public boolean exists(RedisPath redisPath) throws IOException {
		try(Jedis jedis = this.jedisPool.getResource()) {
			return jedis.exists(redisPath.getRedisKey());
		}
	}
}
