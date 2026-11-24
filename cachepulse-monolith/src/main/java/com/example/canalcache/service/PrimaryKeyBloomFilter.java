package com.example.canalcache.service;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 主键布隆过滤器。没有假阴性：加入过的主键一定判定为“可能存在”；误判率由容量决定，超出预计数量后升高。
 * 位图用 AtomicLongArray，CDC 消费线程加入与查询线程判断可以并发。删除的主键无法移除，只会多一些误判。
 */
public final class PrimaryKeyBloomFilter {
    private final AtomicLongArray words;
    private final long bitCount;
    private final int hashCount;
    private final AtomicLong added = new AtomicLong();

    public PrimaryKeyBloomFilter(long expectedInsertions, double falsePositiveRate) {
        if (expectedInsertions < 1 || !(falsePositiveRate > 0 && falsePositiveRate < 1)) {
            throw new IllegalArgumentException("Bloom filter needs expected-insertions >= 1 and 0 < false-positive-rate < 1");
        }
        double ln2 = Math.log(2);
        long bits = (long) Math.ceil(-expectedInsertions * Math.log(falsePositiveRate) / (ln2 * ln2));
        int wordCount = (int) Math.min(Integer.MAX_VALUE - 8, Math.max(1, (bits + 63) / 64));
        this.words = new AtomicLongArray(wordCount);
        this.bitCount = (long) wordCount * 64;
        this.hashCount = Math.max(1, (int) Math.round((double) bitCount / expectedInsertions * ln2));
    }

    public void add(long id) {
        long hash = mix(id);
        boolean changed = false;
        for (int i = 1; i <= hashCount; i++) {
            long bit = index(hash, i);
            long mask = 1L << bit;
            long previous = words.getAndAccumulate((int) (bit >>> 6), mask, (a, b) -> a | b);
            changed |= (previous & mask) == 0;
        }
        if (changed) {
            added.incrementAndGet();
        }
    }

    public boolean mightContain(long id) {
        long hash = mix(id);
        for (int i = 1; i <= hashCount; i++) {
            long bit = index(hash, i);
            if ((words.get((int) (bit >>> 6)) & (1L << bit)) == 0) {
                return false;
            }
        }
        return true;
    }

    /** 加入时置位了新比特的次数，约等于不同主键数；重复加入不计。 */
    public long approximateCount() {
        return added.get();
    }

    public long bitCount() {
        return bitCount;
    }

    public int hashCount() {
        return hashCount;
    }

    /** 双重哈希：第 i 个位置为 h1 + i * h2，两个哈希取自 64 位混合结果的高低两半。 */
    private long index(long hash, int i) {
        long combined = (int) hash + (long) i * (int) (hash >>> 32);
        return (combined < 0 ? ~combined : combined) % bitCount;
    }

    /** MurmurHash3 的 64 位收尾混合，让相邻主键分散到整个位图。 */
    private static long mix(long value) {
        long k = value;
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }
}
