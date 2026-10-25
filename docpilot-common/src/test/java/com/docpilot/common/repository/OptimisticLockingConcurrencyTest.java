package com.docpilot.common.repository;

import com.docpilot.common.AbstractPostgresRepositoryTest;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OptimisticLockingConcurrencyTest extends AbstractPostgresRepositoryTest {

    @Autowired
    private DocumentRepository documentRepository;

    @Autowired
    private CommunicationRepository communicationRepository;

    @Test
    @DisplayName("DOD Optimistic Locking: Concurrent update on Document throws ObjectOptimisticLockingFailureException and prevents lost updates")
    void shouldPreventLostUpdateOnDocumentViaOptimisticLocking() {
        Document doc = Document.builder()
                .filename("concurrency-spec.pdf")
                .storageKey("documents/spec.pdf")
                .contentType("application/pdf")
                .fileSize(2048L)
                .status(DocumentStatus.UPLOADED)
                .build();
        doc = documentRepository.saveAndFlush(doc);
        UUID docId = doc.getId();
        assertThat(doc.getVersion()).isEqualTo(0L);

        // Simulate two separate transactions reading the same Document at version 0
        Document tx1Doc = documentRepository.findById(docId).orElseThrow();
        Document tx2Doc = documentRepository.findById(docId).orElseThrow();

        assertThat(tx1Doc.getVersion()).isEqualTo(0L);
        assertThat(tx2Doc.getVersion()).isEqualTo(0L);

        // Transaction 1 updates and commits first -> version increments to 1
        tx1Doc.setStatus(DocumentStatus.ANALYZED);
        documentRepository.saveAndFlush(tx1Doc);

        Document committed = documentRepository.findById(docId).orElseThrow();
        assertThat(committed.getVersion()).isEqualTo(1L);
        assertThat(committed.getStatus()).isEqualTo(DocumentStatus.ANALYZED);

        // Transaction 2 attempts to update with stale version 0 -> MUST fail
        tx2Doc.setStatus(DocumentStatus.FAILED);
        assertThatThrownBy(() -> documentRepository.saveAndFlush(tx2Doc))
                .isInstanceOf(ObjectOptimisticLockingFailureException.class);

        // Verify that Transaction 1's update was NOT lost or overwritten
        Document finalDoc = documentRepository.findById(docId).orElseThrow();
        assertThat(finalDoc.getStatus()).isEqualTo(DocumentStatus.ANALYZED);
        assertThat(finalDoc.getVersion()).isEqualTo(1L);
    }

    @Test
    @DisplayName("DOD Optimistic Locking: Concurrent update on Communication throws ObjectOptimisticLockingFailureException and prevents lost updates")
    void shouldPreventLostUpdateOnCommunicationViaOptimisticLocking() {
        Document doc = Document.builder()
                .filename("policy.pdf")
                .storageKey("documents/policy.pdf")
                .contentType("application/pdf")
                .fileSize(1024L)
                .status(DocumentStatus.ANALYZED)
                .build();
        doc = documentRepository.saveAndFlush(doc);

        Communication comm = Communication.builder()
                .document(doc)
                .type(CommunicationType.EMAIL)
                .generatedContent("Original AI Content")
                .status(CommunicationStatus.DRAFT)
                .build();
        comm = communicationRepository.saveAndFlush(comm);
        UUID commId = comm.getId();
        assertThat(comm.getVersion()).isEqualTo(0L);

        // Two transactions read stale version 0
        Communication tx1Comm = communicationRepository.findById(commId).orElseThrow();
        Communication tx2Comm = communicationRepository.findById(commId).orElseThrow();

        // Transaction 1 edits and commits
        tx1Comm.setEditedContent("User A Edit");
        tx1Comm.setStatus(CommunicationStatus.EDITED);
        communicationRepository.saveAndFlush(tx1Comm);

        Communication afterTx1 = communicationRepository.findById(commId).orElseThrow();
        assertThat(afterTx1.getVersion()).isEqualTo(1L);
        assertThat(afterTx1.getEditedContent()).isEqualTo("User A Edit");

        // Transaction 2 tries to commit stale modification -> MUST fail
        tx2Comm.setEditedContent("User B Overwrite Attempt");
        assertThatThrownBy(() -> communicationRepository.saveAndFlush(tx2Comm))
                .isInstanceOf(ObjectOptimisticLockingFailureException.class);

        // Confirm User A's content was preserved
        Communication finalComm = communicationRepository.findById(commId).orElseThrow();
        assertThat(finalComm.getEditedContent()).isEqualTo("User A Edit");
        assertThat(finalComm.getVersion()).isEqualTo(1L);
    }

    @Test
    @DisplayName("DOD Optimistic Locking: Multi-threaded simultaneous update produces exactly 1 winner and 1 optimistic lock conflict")
    void shouldHandleMultiThreadedConcurrentUpdatesSafely() throws InterruptedException {
        Document doc = Document.builder()
                .filename("multithread-test.pdf")
                .storageKey("documents/mt.pdf")
                .contentType("application/pdf")
                .fileSize(4096L)
                .status(DocumentStatus.UPLOADED)
                .build();
        Document savedDoc = documentRepository.saveAndFlush(doc);
        UUID docId = savedDoc.getId();

        int threadCount = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger conflictCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    // Both threads load the initial entity at version 0
                    Document myDoc = documentRepository.findById(docId).orElseThrow();
                    readyLatch.countDown(); // signal entity loaded
                    startLatch.await(); // Wait for simultaneous release

                    if (threadId == 0) {
                        myDoc.setStatus(DocumentStatus.ANALYZED);
                    } else {
                        myDoc.setStatus(DocumentStatus.GENERATING);
                    }
                    documentRepository.saveAndFlush(myDoc);
                    successCount.incrementAndGet();
                } catch (ObjectOptimisticLockingFailureException ex) {
                    conflictCount.incrementAndGet();
                } catch (Exception e) {
                    // unexpected
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await(); // Ensure both threads loaded version 0
        startLatch.countDown(); // Fire both threads concurrently
        doneLatch.await();
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(1);
        assertThat(conflictCount.get()).isEqualTo(1);

        Document finalDoc = documentRepository.findById(docId).orElseThrow();
        assertThat(finalDoc.getVersion()).isEqualTo(1L);
    }
}
