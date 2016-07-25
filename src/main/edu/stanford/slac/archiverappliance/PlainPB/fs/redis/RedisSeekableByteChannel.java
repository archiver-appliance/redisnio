package edu.stanford.slac.archiverappliance.PlainPB.fs.redis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;

import edu.stanford.slac.archiverappliance.PlainPB.fs.redis.RedisFileSystem;
import edu.stanford.slac.archiverappliance.PlainPB.fs.redis.RedisPath;
import redis.clients.jedis.Jedis;

/**
 * Given a redisPath, this gets you a SeekableByteChannel that can be used to read and write data into the value of the key.
 * @author mshankar
 *
 */
public class RedisSeekableByteChannel implements SeekableByteChannel {
	private RedisFileSystem fs;
	private RedisPath path;
	private long currentPosition = 0;
	
	public RedisSeekableByteChannel(RedisFileSystem theFileSystem, RedisPath path, Set<? extends OpenOption> options) throws IOException { 
		this.fs = theFileSystem;
		this.path = path;
		if(options.contains(StandardOpenOption.APPEND)) { 
			this.currentPosition = this.size();
		}
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		try(Jedis jedis = this.fs.jedisPool.getResource()) { 
			byte[] redisData = jedis.getrange(this.path.getRedisKey().getBytes(), this.currentPosition, dst.limit());
			if(redisData.length <= 0) { 
				return -1;
			}
			dst.put(redisData);
			currentPosition = currentPosition + redisData.length;
			return redisData.length;
		}	
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		try(Jedis jedis = this.fs.jedisPool.getResource()) {
			long previousCurrent = currentPosition;
			byte[] buf = new byte[src.remaining()];
			src.get(buf);
			currentPosition = jedis.setrange(this.path.getRedisKey().getBytes(), this.currentPosition, buf);
			return (int) (currentPosition - previousCurrent);
		}	
	}

	@Override
	public long position() throws IOException {
		return currentPosition;
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		this.currentPosition = newPosition;
		return this;
	}

	@Override
	public long size() throws IOException {
		try(Jedis jedis = this.fs.jedisPool.getResource()) {
			return jedis.strlen(this.path.getRedisKey());
		}	
	}

	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		throw new UnsupportedOperationException();
	}

}
