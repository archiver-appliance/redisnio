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
		if(createdFileSystems.containsKey(connectionName)) { 
			throw new FileSystemAlreadyExistsException("A redis file system that connects to " + connectionName + " was already created");
		}
		logger.debug("Creating a new redis file system connecting to " + connectionName);
		RedisFileSystem fs = new RedisFileSystem(this, server, port, uri.getPath(), env);
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

	@Override
	public Path getPath(URI uri) {
		String server = uri.getHost();
		int port = uri.getPort();
		String connectionName = server + ":" + port;
		RedisFileSystem fs = createdFileSystems.get(connectionName);
		return fs.getPath(uri.getPath());
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void delete(Path path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void copy(Path source, Path target, CopyOption... options) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void move(Path source, Path target, CopyOption... options) throws IOException {
		throw new UnsupportedOperationException();
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
		throw new UnsupportedOperationException();
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
