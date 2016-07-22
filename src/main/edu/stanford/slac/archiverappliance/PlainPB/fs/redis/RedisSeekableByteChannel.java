package edu.stanford.slac.archiverappliance.PlainPB.fs.redis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

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
	
	public RedisSeekableByteChannel(RedisFileSystem theFileSystem, RedisPath path) { 
		this.fs = theFileSystem;
		this.path = path;
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
			dst.put(redisData);
			currentPosition = currentPosition + redisData.length;
			return redisData.length;
		}	
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long position() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long size() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
