package io.galaxycache.examples.sizing;

import io.galaxycache.common.codec.BinaryCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

/**
 * Speed of the binary codec on BaseTransaction objects, single-threaded and on 8 threads, plus a checksum of the
 * encoded bytes: the checksum must not change when the codec is tuned, or data already on disk would become unreadable.
 * <pre>
 * java -cp galaxy-examples.jar io.galaxycache.examples.sizing.CodecBench
 * </pre>
 */
public final class CodecBench {

    private CodecBench() {
    }

    public static void main(String[] args) throws Exception {
        SizingRun gen = new SizingRun(2026);
        List<BaseTransaction> objects = new ArrayList<>();
        for (int i = 0; i < 2_000; i++) {
            BaseTransaction t = gen.build(String.format("TX%010d", i));
            t.tradeDate = java.time.LocalDate.of(2026, 11, 1 + i % 28); // generator uses today: keep the checksum stable
            objects.add(t);
        }
        BinaryCodec codec = new BinaryCodec();
        List<byte[]> encoded = new ArrayList<>();
        for (BaseTransaction t : objects) {
            encoded.add(codec.encode(t));
        }
        CRC32 crc = new CRC32();
        long bytes = 0;
        for (byte[] b : encoded) {
            crc.update(b);
            bytes += b.length;
        }
        System.out.printf("encoded %,d objects, avg %,d bytes, checksum %08x%n", encoded.size(), bytes / encoded.size(), crc.getValue());

        for (int warm = 0; warm < 15; warm++) { // let the JIT settle
            encoded.forEach(b -> codec.decode(b, 1, BaseTransaction.class));
            objects.forEach(codec::encode);
        }
        for (int round = 1; round <= 3; round++) {
            long t0 = System.nanoTime();
            for (byte[] b : encoded) {
                codec.decode(b, 1, BaseTransaction.class);
            }
            double decodeUs = (System.nanoTime() - t0) / 1e3 / encoded.size();
            t0 = System.nanoTime();
            for (BaseTransaction t : objects) {
                codec.encode(t);
            }
            double encodeUs = (System.nanoTime() - t0) / 1e3 / objects.size();
            ExecutorService pool = Executors.newFixedThreadPool(8);
            t0 = System.nanoTime();
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                fs.add(pool.submit(() -> {
                    for (int r = 0; r < 3; r++) {
                        encoded.forEach(b -> codec.decode(b, 1, BaseTransaction.class));
                    }
                }));
            }
            for (Future<?> f : fs) {
                f.get();
            }
            double perSec = 8 * 3 * encoded.size() / ((System.nanoTime() - t0) / 1e9);
            pool.shutdown();
            System.out.printf("round %d: decode %.0f us/object, encode %.0f us/object, decode on 8 threads %,.0f objects/s%n",
                    round, decodeUs, encodeUs, perSec);
        }
    }
}
