package edu.stanford.slac.archiverappliance.PlainPB.fs.redis;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A NIO.2 file system provider meant to be used against a redis backend.
 * To connect, use URI's like so - redis://localhost:port/keyPrefix
 * The "path" is appended to the keyPrefix to determine the redis key.
 * 
 * We reuse Jedis connections to the same server:port; the filesystem's returned by this provider are mapped to server:port combinations.
 * The Jedis connection itself is in the FileSystem.
 * @author mshankar
 *
 */
public class RedisFileSystemProvider extends FileSystemProvider {
	private static final Logger logger = LoggerFactory.getLogger(RedisFileSystemProvider.class);
	
	private ConcurrentHashMap<String, RedisFileSystem> createdFileSystems = new ConcurrentHashMap<String, RedisFileSystem>();
	
	public RedisFileSystemProvider() { 
	}

	@Override
	public String getScheme() {
		return "redis";
	}

	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
		String scheme = uri.getScheme();
		if(!scheme.equals("redis")) { 
			throw new IllegalArgumentException("The redis file system can only understand URI's with the redis scheme; for example, redis://localhost:port");
		}
		String server = uri.getHost();
		int port = uri.getPort();
		String connectionName = server + ":" + port;
		RedisFileSystem fs = createFileSystem(connectionName, server, port, env);
		return fs;
	}

	private RedisFileSystem createFileSystem(String connectionName, String server, int port, Map<String, ?> env) {
		if(createdFileSystems.containsKey(connectionName)) { 
			throw new FileSystemAlreadyExistsException("A redis file system that connects to " + connectionName + " was already created");
		}
		logger.debug("Creating a new redis file system connecting to " + connectionName);
		RedisFileSystem fs = new RedisFileSystem(this, server, port, env);
		createdFileSystems.put(connectionName, fs);
		return fs;
	}

	@Override
	public FileSystem getFileSystem(URI uri) {
		String server = uri.getHost();
		int port = uri.getPort();
		String connectionName = server + ":" + port;
		RedisFileSystem fs = createdFileSystems.get(connectionName);
		if(fs == null) { 
			throw new FileSystemNotFoundException("Pre-existing file system for " + connectionName + " not found");
		}
		return fs;
	}
	
	public RedisFileSystem getFileSystem(String connectionName) { 
		RedisFileSystem fs = createdFileSystems.get(connectionName);
		String[] parts = connectionName.split(":");
		String server = parts[0];
		int port = Integer.parseInt(parts[1]);
		if(fs == null) { 
			fs = createFileSystem(connectionName, server, port, null);
		}
		return fs;
	}

	@Override
	public Path getPath(URI uri) {
		String server = uri.getHost();
		int port = uri.getPort();
		String connectionName = server + ":" + port;
		return new RedisPath(this, connectionName, uri.getPath());
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		RedisPath redisPath = (RedisPath) path;
		RedisFileSystem fs = createdFileSystems.get(redisPath.getConnectionName());
		return new RedisSeekableByteChannel(fs, redisPath, options);
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
		RedisPath redisPath = (RedisPath) dir;
		RedisFileSystem fs = createdFileSystems.get(redisPath.getConnectionName());
		return fs.getMatchingKeys(redisPath.getRedisKey(), filter);
	}

	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(Path path) throws IOException {
		RedisPath redisPath = (RedisPath) path;
		RedisFileSystem fs = createdFileSystems.get(redisPath.getConnectionName());
		fs.deleteKey(redisPath);
	}

	@Override
	public void copy(Path source, Path target, CopyOption... options) throws IOException {
		RedisPath redisSrcPath = (RedisPath) source;
		RedisPath redisTargetPath = (RedisPath) target;
		if(!redisSrcPath.getConnectionName().equals(redisTargetPath.getConnectionName())) { 
			throw new IOException("Cannot move data between different redis instances yet Src: " + redisSrcPath.getConnectionName() + " Dest: " + redisTargetPath.getConnectionName());
		}
		RedisFileSystem fs = createdFileSystems.get(redisSrcPath.getConnectionName());
		fs.copy(redisSrcPath.getRedisKey(), redisTargetPath.getRedisKey(), options);
	}

	@Override
	public void move(Path source, Path target, CopyOption... options) throws IOException {
		RedisPath redisSrcPath = (RedisPath) source;
		RedisPath redisTargetPath = (RedisPath) target;
		if(!redisSrcPath.getConnectionName().equals(redisTargetPath.getConnectionName())) { 
			throw new IOException("Cannot move data between different redis instances yet Src: " + redisSrcPath.getConnectionName() + " Dest: " + redisTargetPath.getConnectionName());
		}
		RedisFileSystem fs = createdFileSystems.get(redisSrcPath.getConnectionName());
		fs.rename(redisSrcPath.getRedisKey(), redisTargetPath.getRedisKey(), options);
	}

	@Override
	public boolean isSameFile(Path path, Path path2) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isHidden(Path path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
		RedisPath redisPath = (RedisPath) path;
		RedisFileSystem fs = createdFileSystems.get(redisPath.getConnectionName());
		if(!fs.exists(redisPath)) { 
			throw new NoSuchFileException (redisPath.getRedisKey() + " does not exist on the server");
		}
	}

	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}
}
