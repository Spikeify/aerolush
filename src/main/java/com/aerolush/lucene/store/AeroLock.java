package com.aerolush.lucene.store;

import com.aerospike.client.AerospikeException;
import com.spikeify.Spikeify;
import org.apache.lucene.store.Lock;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class AeroLock extends Lock {

	private final Spikeify sfy;
	private final String name;
	private final String lockHash;

	public AeroLock(Spikeify spikeify, String lockName) {
		sfy = spikeify;
		name = lockName;
		lockHash = UUID.randomUUID().toString();
	}

	public AeroLock obtain() throws IOException {
		FileLock lock = new FileLock(name);
		lock.lockHash = lockHash;
		try {
			sfy.create(lock).now();
		} catch (AerospikeException e) {
			throw new IOException();
		}
		return this;
	}

	@Override
	public void close() throws IOException {

		FileLock found = sfy.get(FileLock.class).key(name).now();
		if (found != null) {
			sfy.delete(found).now();
		}
	}

	@Override
	public void ensureValid() throws IOException {
		FileLock found = sfy.get(FileLock.class).key(name).now();
		if(found == null) {
			FileLock lock = new FileLock(name);
			try {
				sfy.create(lock).now();
			} catch (AerospikeException e) {
				throw new IOException();
			}
		} else {
			if (!found.lockHash.equals(lockHash)) {
				throw new IOException();
			}
		}
	}
}
