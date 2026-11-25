package com.cachepulse.delivery;

import com.cachepulse.delivery.replay.ArchiveClient;
import com.cachepulse.delivery.replay.ReplayJob;
import com.cachepulse.delivery.replay.ReplayService;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import org.junit.jupiter.api.*;
import org.mockito.InOrder;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static com.cachepulse.delivery.DeliveryTestFixture.inserted;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

class ReplayRepairTest {
    private DeliveryTestFixture f;
    private EventSubscription sub;
    private List<String> ids;
    @BeforeEach void setup() throws Exception {
        f=new DeliveryTestFixture();
        f.target("hook","http://original/hook");
        sub=f.subscribe("sub","hook","INSERT",true);
        List<String> consumed=new ArrayList<>();
        for(int i=1;i<=2;i++) {
            var record=f.record(i,f.rows(EventType.INSERT,inserted(""+i,"A")));
            f.publish(record);
            consumed.addAll(f.eventIds(record));
        }
        ids=consumed.stream().sorted().toList();
    }
    private ReplayJob pending() {
        var job=new ReplayJob("job","request",f.properties.getSourceId(),f.properties.getDatabase(),"inventory","sub","PENDING",2,0,0,0,
                ids,f.store.encode(sub),"http://original/hook",null,Instant.now().toString(),Instant.now().toString());
        f.store.saveReplayJob(job);return job;
    }
    @Test void failedRoundRollsBackAllTasksThenRestartUsesFrozenRuleAndTarget() {
        pending();
        var spyStore=spy(f.store);
        AtomicInteger calls=new AtomicInteger();
        doAnswer(call->{if(calls.incrementAndGet()==2) throw new org.springframework.dao.DataAccessResourceFailureException("injected interruption");return call.callRealMethod();})
                .when(spyStore).createDeliveryTask(any(DeliveryTask.class));
        assertThatThrownBy(()->new ReplayService(f.properties,spyStore,f.archive).processJob("job")).hasMessageContaining("injected interruption");
        assertThat(f.jdbc.queryForObject("SELECT COUNT(*) FROM delivery_task WHERE round_id='job'",Long.class)).isZero();
        assertThat(f.store.findReplayJob("job").status()).isEqualTo("PENDING");
        f.properties.getTargets().get("hook").setUrl("http://new/hook");
        f.store.updateSubscription(new EventSubscription("sub",sub.sourceId(),sub.database(),sub.table(),"hook","DELETE",null,null,null,false,1,null,null));
        new ReplayService(f.properties,f.store,f.archive).resumePending();
        assertThat(f.store.findReplayJob("job").status()).isEqualTo("COMPLETED");
        assertThat(f.store.findReplayJob("job").matchedEvents()).isEqualTo(2);
        assertThat(f.store.listDeliveryTasks(null,"sub",null,10).stream().filter(t->t.roundId().equals("job")))
                .hasSize(2).allSatisfy(t->assertThat(t.targetUrl()).isEqualTo("http://original/hook"));
    }
    @Test void duplicateRequestCannotChangeItsMeaning() {
        var service=new ReplayService(f.properties,f.store,f.archive);
        var original=service.createReplayJob(new ReplayJob.Request("same","sub",ids));
        assertThat(service.createReplayJob(new ReplayJob.Request("same","sub",ids)).jobId()).isEqualTo(original.jobId());
        assertThatThrownBy(()->service.createReplayJob(new ReplayJob.Request("same","sub",List.of(ids.get(0)))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("different replay request");
    }

    /** Replaces "replayDoesNotReadRecoveryMetadata": the delivery store has no recovery metadata at all now. */
    @Test void archivedEventsAreFetchedBeforeTheReplayRowLockIsTaken() {
        pending();
        var spyStore = spy(f.store);
        ArchiveClient archive = spy(new ArchiveClient() {
            @Override public Optional<com.cachepulse.common.model.BinlogEvent> event(String eventId) { return f.archive.event(eventId); }
        });
        var job = new ReplayService(f.properties, spyStore, archive).processJob("job");
        assertThat(job.status()).isEqualTo("COMPLETED");
        assertThat(job.matchedEvents()).isEqualTo(2);
        assertThat(f.store.replayDeliveryStats("job")).containsEntry("PENDING", 2L);
        InOrder order = inOrder(archive, spyStore);
        order.verify(archive, times(2)).event(anyString());
        order.verify(spyStore).withReplayLock(eq("job"), any());
        verify(archive, times(2)).event(anyString());
    }
}
