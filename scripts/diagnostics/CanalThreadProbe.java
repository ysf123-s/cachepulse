import com.sun.tools.attach.VirtualMachine;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Bounded, read-only thread snapshots through the existing HotSpot attach listener. */
public final class CanalThreadProbe {
    public static void main(String[] args) throws Exception {
        String pid = args[0];
        long durationMillis = Long.parseLong(args[2]);
        long intervalMillis = Long.parseLong(args[3]);
        VirtualMachine vm = VirtualMachine.attach(pid);
        Method command = Class.forName("sun.tools.attach.HotSpotVirtualMachine")
                .getMethod("executeJCmd", String.class);
        try (BufferedWriter out = Files.newBufferedWriter(Paths.get(args[1]), StandardCharsets.UTF_8)) {
            long deadline = System.nanoTime() + durationMillis * 1000000L;
            int sample = 0;
            while (System.nanoTime() < deadline) {
                long epochMillis = System.currentTimeMillis();
                long begin = System.nanoTime();
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                try (InputStream in = (InputStream) command.invoke(vm, "Thread.print")) {
                    byte[] buffer = new byte[8192];
                    int count;
                    while ((count = in.read(buffer)) != -1) bytes.write(buffer, 0, count);
                }
                out.write("SAMPLE " + (++sample) + " epoch_ms=" + epochMillis
                        + " capture_ms=" + ((System.nanoTime() - begin) / 1000000.0) + "\n");
                out.write(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
                out.write("\nEND_SAMPLE\n");
                out.flush();
                Thread.sleep(intervalMillis);
            }
        } finally {
            vm.detach();
        }
    }
}
