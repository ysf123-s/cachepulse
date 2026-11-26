FROM canal/canal-server:v1.1.8

COPY patch/CanalMQStarter.java /tmp/cachepulse-canal/CanalMQStarter.java
RUN javac -source 8 -target 8 -encoding UTF-8 \
        -cp '/home/admin/canal-server/lib/*' -d /tmp/cachepulse-canal \
        /tmp/cachepulse-canal/CanalMQStarter.java \
    && jar uf /home/admin/canal-server/lib/canal.server-1.1.8.jar -C /tmp/cachepulse-canal com \
    && rm -rf /tmp/cachepulse-canal
