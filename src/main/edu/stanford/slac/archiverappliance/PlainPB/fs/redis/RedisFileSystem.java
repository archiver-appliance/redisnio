package edu.stanford.slac.archiverappliance.PlainPB.fs.redis;

import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	private static final Logger logger = Logger.getLogger(RedisFileSystem.class.getName());
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
		logger.info("Close called on RedisFileSytem");
		this.jedisPool.close();
		this.jedisPool = null;
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
		String fullName = first + String.join("/", more);
		// If you get keys in redis with the scheme and host etc, the caller of this method is probably to blame...
		logger.fine("Creating a path for full name " + fullName);
		return new RedisPath(theProvider, this.connectionName, fullName);
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
	
	
	public RedisKeyAttributes readAttributes(RedisPath redisPath, LinkOption[] options) {
		return new RedisKeyAttributes(this, redisPath);
	}

	/**
	 * 
	 * @return
	 */
	public FileStore getFileStore() { 
		return new FileStore() {
			@Override
			public String type() {
				return "redis";
			}
			
			@Override
			public boolean supportsFileAttributeView(String name) {
				return false;
			}
			
			@Override
			public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
				return false;
			}
			
			@Override
			public String name() {
				return "redis";
			}
			
			@Override
			public boolean isReadOnly() {
				return false;
			}
			
			@Override
			public long getUsableSpace() throws IOException {
				return 0;
			}
			
			@Override
			public long getUnallocatedSpace() throws IOException {
				return 0;
			}
			
			@Override
			public long getTotalSpace() throws IOException {
				return 0;
			}
			
			@Override
			public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Object getAttribute(String attribute) throws IOException {
				// TODO Auto-generated method stub
				return null;
			}
		};
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
			jedis.del(redisPath.getRedisKey());
			jedis.del("Attrs" + redisPath.getRedisKey());
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
						logger.log(Level.SEVERE, "Exception from filter when matching " + matchingKey, ex);
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
	 * Copy the contents of the key redisSrcKey to redisTargetKey
	 * @param redisSrcKey
	 * @param redisTargetKey
	 * @param options - For now, this is largely ignored.
	 */
	public void copy(String redisSrcKey, String redisTargetKey, CopyOption... options) throws IOException {
		try(Jedis jedis = this.jedisPool.getResource()) {
			// No support for copy in redis; use a LUA script instead.
			// If this seems to be used often, we can compile this script and use the SHA instead..
			jedis.eval("redis.call('SET', KEYS[2], redis.call('GET', KEYS[1])); return 1;", 2, redisSrcKey, redisTargetKey);
		}
	}

	/**
	 * Rename the key redisSrcKey to redisTargetKey
	 * @param redisSrcKey
	 * @param redisTargetKey
	 * @param options - For now, this is largely ignored.
	 */
	public void rename(String redisSrcKey, String redisTargetKey, CopyOption... options) throws IOException {
		try(Jedis jedis = this.jedisPool.getResource()) {
			jedis.rename(redisSrcKey, redisTargetKey);
		}
	}

	public boolean exists(RedisPath redisPath) throws IOException {
		try(Jedis jedis = this.jedisPool.getResource()) {
			return jedis.exists(redisPath.getRedisKey());
		}
	}
	
	
	class RedisKeyAttributes implements BasicFileAttributes {
		boolean keyExists = false;
		RedisPath redisPath;
		long size;
		long lastModifiedTime;
		long lastAccessedTime;
		long keyCreationTime;
		
		public RedisKeyAttributes(RedisFileSystem fs, RedisPath redisPath) {
			this.redisPath = redisPath;
			try(Jedis jedis = fs.jedisPool.getResource()) {
				if(jedis.exists(redisPath.getRedisKey())) { 
					keyExists = true;
					size = jedis.strlen(redisPath.getRedisKey());
					String attrKey = "Attrs" + redisPath.getRedisKey();
					if(jedis.hexists(attrKey, "lastModifiedTime")) { lastModifiedTime = Long.parseLong(jedis.hget(attrKey, "lastModifiedTime")); } 
					if(jedis.hexists(attrKey, "lastAccessedTime")) { lastAccessedTime = Long.parseLong(jedis.hget(attrKey, "lastAccessedTime")); } 
					if(jedis.hexists(attrKey, "keyCreationTime")) { keyCreationTime = Long.parseLong(jedis.hget(attrKey, "keyCreationTime")); } 
				}
			}
		}

		@Override
		public FileTime lastModifiedTime() {
			return FileTime.fromMillis(lastModifiedTime);
		}

		@Override
		public FileTime lastAccessTime() {
			return FileTime.fromMillis(lastAccessedTime);
		}

		@Override
		public FileTime creationTime() {
			return FileTime.fromMillis(keyCreationTime);
		}

		@Override
		public boolean isRegularFile() {
			return keyExists;
		}

		@Override
		public boolean isDirectory() {
			return false;
		}

		@Override
		public boolean isSymbolicLink() {
			return false;
		}

		@Override
		public boolean isOther() {
			return false;
		}

		@Override
		public long size() {
			return size;
		}

		@Override
		public Object fileKey() {
			return redisPath.toString();
		}		
	}
}
