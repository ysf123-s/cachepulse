package com.cachepulse.archive;

import com.cachepulse.framework.GlobalExceptionHandler;
import com.cachepulse.framework.TraceIds;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static com.cachepulse.archive.ArchiveTestFixture.inserted;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** The only cross-service endpoint: delivery's ArchiveClient reads archived events through it (design 9.3). */
class InternalEventControllerTest {
    private ArchiveTestFixture fixture;
    private MockMvc mvc;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        fixture = new ArchiveTestFixture();
        mvc = MockMvcBuilders.standaloneSetup(new InternalEventController(fixture.store, fixture.properties))
                .setControllerAdvice(new GlobalExceptionHandler(new TraceIds(mock(ObjectProvider.class)))).build();
    }

    @Test
    void rejectsIdsThatAreNotLowercaseSha256() throws Exception {
        for (String id : new String[]{"abc", "A".repeat(64), "g".repeat(64), "a".repeat(65)}) {
            mvc.perform(get("/internal/events/{id}", id)).andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
        }
    }

    @Test
    void missingEventIs404SoTheFeignClientCanReturnEmpty() throws Exception {
        mvc.perform(get("/internal/events/{id}", "f".repeat(64))).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("NOT_FOUND"));
    }

    @Test
    void archivedEventIsReturnedAsTheOriginalBinlogEvent() throws Exception {
        fixture.service.archive(fixture.record(7, fixture.rows(CanalEntry.EventType.INSERT, inserted("42", "internal"))));
        String eventId = fixture.store.search(fixture.properties,
                new ArchivedChange.Filter(null, null, null, null, null, null, 1)).items().get(0).eventId();
        mvc.perform(get("/internal/events/{id}", eventId)).andExpect(status().isOk())
                .andExpect(jsonPath("$.tableName").value("inventory"))
                .andExpect(jsonPath("$.eventType").value("INSERT"))
                .andExpect(jsonPath("$.afterData.id").value("42"))
                .andExpect(jsonPath("$.afterData.name").value("internal"));
    }
}
