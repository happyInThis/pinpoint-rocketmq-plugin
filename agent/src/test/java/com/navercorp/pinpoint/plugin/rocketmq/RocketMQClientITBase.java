/*
 * Copyright 2018 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.plugin.rocketmq;

import com.aliyun.openservices.ons.api.Message;
import com.navercorp.pinpoint.bootstrap.plugin.test.ExpectedTrace;
import com.navercorp.pinpoint.bootstrap.plugin.test.PluginTestVerifier;
import com.navercorp.pinpoint.bootstrap.plugin.test.PluginTestVerifierHolder;
import com.navercorp.pinpoint.plugin.rocketmq.client.RocketMqClientConstants;
import com.navercorp.pinpoint.plugin.rocketmq.rocketmq.MessageReceiver;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.navercorp.pinpoint.bootstrap.plugin.test.Expectations.annotation;
import static com.navercorp.pinpoint.bootstrap.plugin.test.Expectations.event;
import static com.navercorp.pinpoint.bootstrap.plugin.test.Expectations.root;

/**
 * @author 微风
 */

public abstract class RocketMQClientITBase {
    @Test
    public void testQueuePull() throws Exception {
        // Given
        final String testMessage = "Hello World for Queue!";
        final CountDownLatch consumerLatch = new CountDownLatch(1);
        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        // create and start message receiver
        final MessageReceiver messageReceiver = new MessageReceiver();

        final Thread consumerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("subscribe");
                    messageReceiver.subscribe();
                } catch (Exception e) {
                    exception.set(e);
                } finally {
                    consumerLatch.countDown();
                }
            }
        });
        // create producer
        Message message = new Message();
//        System.out.println("send message");
        consumerThread.start();
        consumerLatch.await(10L, TimeUnit.SECONDS);

        // Then
        assertNoConsumerError(exception.get());

        PluginTestVerifier verifier = PluginTestVerifierHolder.getInstance();
        verifier.printCache();
        // Wait till all traces are recorded (consumer traces are recorded from another thread)
        awaitAndVerifyTraceCount(verifier, 1, 5000L);

//        verifyProducerSendEvent(verifier, message);
        verifyConsumerConsumeEvent(verifier, message);

        // Separate transaction for the consumer's request to receive the message

        verifier.verifyTraceCount(0);
    }

//    @Test
//    public void testQueuePush() throws Exception {
//        // Given
//        final String testQueueName = "TestPushQueue";
//        final ActiveMQQueue testQueue = new ActiveMQQueue(testQueueName);
//        final String testMessage = "Hello World for Queue!";
//        final MessagePrinter messagePrinter = new MessagePrinter();
//        final CountDownLatch consumerLatch = new CountDownLatch(1);
//        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
//        // create producer
//        final ActiveMQSession producerSession = ActiveMQClientITHelper.createSession(getProducerBrokerName(), getProducerBrokerUrl());
//        final MessageProducer producer = new MessageProducerBuilder(producerSession, testQueue).waitTillStarted().build();
//        final TextMessage expectedTextMessage = producerSession.createTextMessage(testMessage);
//        // create consumer
//        final ActiveMQSession consumerSession = ActiveMQClientITHelper.createSession(getConsumerBrokerName(), getConsumerBrokerUrl());
//        new MessageConsumerBuilder(consumerSession, testQueue)
//                .waitTillStarted()
//                .withMessageListener(new MessageListener() {
//                    @Override
//                    public void onMessage(Message message) {
//                        try {
//                            messagePrinter.printMessage(message);
//                        } catch (Exception e) {
//                            exception.set(e);
//                        } finally {
//                            consumerLatch.countDown();
//                        }
//                    }
//                })
//                .build();
//
//        // When
//        producer.send(expectedTextMessage);
//        consumerLatch.await(1L, TimeUnit.SECONDS);
//
//        // Then
//        assertNoConsumerError(exception.get());
//
//        PluginTestVerifier verifier = PluginTestVerifierHolder.getInstance();
//        verifier.printCache();
//        // Wait till all traces are recorded (consumer traces are recorded from another thread)
//        awaitAndVerifyTraceCount(verifier, 4, 5000L);
//
//        verifyProducerSendEvent(verifier, testQueue, producerSession);
//        verifyConsumerConsumeEvent(verifier, testQueue, consumerSession);
//
//        Method printMessageMethod = MessagePrinter.class.getDeclaredMethod("printMessage", Message.class);
//        verifier.verifyTrace(event(ServiceType.INTERNAL_METHOD.getName(), printMessageMethod));
//
//        verifier.verifyTraceCount(0);
//    }

//    @Test
//    public void testQueuePoll() throws Exception {
//        // Given
//        final String testQueueName = "TestPollQueue";
//        final ActiveMQQueue testQueue = new ActiveMQQueue(testQueueName);
//        final String testMessage = "Hello World for Queue!";
//        // create producer
//        final ActiveMQSession producerSession = ActiveMQClientITHelper.createSession(getProducerBrokerName(), getProducerBrokerUrl());
//        final MessageProducer producer = new MessageProducerBuilder(producerSession, testQueue).waitTillStarted().build();
//        final TextMessage expectedTextMessage = producerSession.createTextMessage(testMessage);
//        // create consumer
//        final ActiveMQSession consumerSession = ActiveMQClientITHelper.createSession(getConsumerBrokerName(), getConsumerBrokerUrl());
//        final MessageConsumer consumer = new MessageConsumerBuilder(consumerSession, testQueue).waitTillStarted().build();
//        final MessageReceiveHandler messageReceiveHandler = new MessageReceiveHandler();
//        final PollingMessageReceiver pollingMessageReceiver = new PollingMessageReceiver(consumer, messageReceiveHandler);
//
//        // When
//        pollingMessageReceiver.start();
//        producer.send(expectedTextMessage);
//        messageReceiveHandler.await(5000L);
//        pollingMessageReceiver.stop();
//
//        // Then
//        assertNoConsumerError(pollingMessageReceiver.getException());
//
//        PluginTestVerifier verifier = PluginTestVerifierHolder.getInstance();
//        verifier.printCache();
//
//        // Wait till all traces are recorded (consumer traces are recorded from another thread)
//        awaitAndVerifyTraceCount(verifier, 6, 5000L);
//
//        verifyProducerSendEvent(verifier, testQueue, producerSession);
//        verifyConsumerConsumeEvent(verifier, testQueue, consumerSession);
//
//        verifier.verifyTrace(event(ServiceType.ASYNC.getName(), "Asynchronous Invocation"));
//        Method handleMessageMethod = MessageReceiveHandler.class.getDeclaredMethod("handleMessage", Message.class);
//        verifier.verifyTrace(event(ServiceType.INTERNAL_METHOD.getName(), handleMessageMethod));
//        Method printMessageMethod = MessagePrinter.class.getDeclaredMethod("printMessage", Message.class);
//        verifier.verifyTrace(event(ServiceType.INTERNAL_METHOD.getName(), printMessageMethod));
//
//        verifier.verifyTraceCount(0);
//    }

//    @Test
//    public void testTopicPull() throws Exception {
//        // Given
//        final String testTopicName = "TestPullTopic";
//        final ActiveMQTopic testTopic = new ActiveMQTopic(testTopicName);
//        final String testMessage = "Hello World for Topic!";
//        final CountDownLatch consumerLatch = new CountDownLatch(2);
//        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
//        // create producer
//        final ActiveMQSession producerSession = ActiveMQClientITHelper.createSession(getProducerBrokerName(), getProducerBrokerUrl());
//        final MessageProducer producer = new MessageProducerBuilder(producerSession, testTopic).waitTillStarted().build();
//        final TextMessage expectedTextMessage = producerSession.createTextMessage(testMessage);
//        // create 2 consumers
//        final ActiveMQSession consumer1Session = ActiveMQClientITHelper.createSession(getConsumerBrokerName(), getConsumerBrokerUrl());
//        final MessageConsumer consumer1 = new MessageConsumerBuilder(consumer1Session, testTopic).waitTillStarted().build();
//        final MessageReceiver messageReceiver1 = new MessageReceiver(consumer1);
//        final Thread consumer1Thread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    messageReceiver1.receiveMessage(1000L);
//                } catch (Exception e) {
//                    exception.set(e);
//                } finally {
//                    consumerLatch.countDown();
//                }
//            }
//        });
//        final ActiveMQSession consumer2Session = ActiveMQClientITHelper.createSession(getConsumerBrokerName(), getConsumerBrokerUrl());
//        final MessageConsumer consumer2 = new MessageConsumerBuilder(consumer2Session, testTopic).waitTillStarted().build();
//        final MessageReceiver messageReceiver2 = new MessageReceiver(consumer2);
//        final Thread consumer2Thread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    messageReceiver2.receiveMessage(1000L);
//                } catch (Exception e) {
//                    exception.set(e);
//                } finally {
//                    consumerLatch.countDown();
//                }
//            }
//        });
//
//        // When
//        producer.send(expectedTextMessage);
//        consumer1Thread.start();
//        consumer2Thread.start();
//        consumerLatch.await(1L, TimeUnit.SECONDS);
//
//        // Then
//        assertNoConsumerError(exception.get());
//
//        PluginTestVerifier verifier = PluginTestVerifierHolder.getInstance();
//        verifier.printCache();
//        // Wait till all traces are recorded (consumer traces are recorded from another thread)
//        awaitAndVerifyTraceCount(verifier, 13, 5000L);
//
//        verifyProducerSendEvent(verifier, testTopic, producerSession);
//        verifyConsumerConsumeEvent(verifier, testTopic, consumer1Session);
//        verifyConsumerConsumeEvent(verifier, testTopic, consumer2Session);
//
//        // Separate transaction for the consumer's request to receive the message
//        Method receiveMessageMethod = MessageReceiver.class.getDeclaredMethod("receiveMessage", long.class);
//        Method receiveMethod = ActiveMQMessageConsumer.class.getDeclaredMethod("receive", long.class);
//        Method printMessageMethod = MessagePrinter.class.getDeclaredMethod("printMessage", Message.class);
//        for (int i = 0; i < 2; ++i) {
//            verifier.verifyDiscreteTrace(
//                    root(ServiceType.STAND_ALONE.getName(), "Entry Point Process", null, null, null, null),
//                    event(ServiceType.INTERNAL_METHOD.getName(), receiveMessageMethod),
//                    event(ACTIVEMQ_CLIENT_INTERNAL, receiveMethod, annotation("activemq.message", getMessageAsString(expectedTextMessage))),
//                    event(ServiceType.INTERNAL_METHOD.getName(), printMessageMethod));
//        }
//
//        verifier.verifyTraceCount(0);
//    }

//    @Test
//    public void testTopicPush() throws Exception {
//        // Given
//        final String testTopicName = "TestPushTopic";
//        final ActiveMQTopic testTopic = new ActiveMQTopic(testTopicName);
//        final String testMessage = "Hello World for Topic!";
//        final MessagePrinter messagePrinter = new MessagePrinter();
//        final CountDownLatch consumerLatch = new CountDownLatch(2);
//        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
//        // create producer
//        final ActiveMQSession producerSession = ActiveMQClientITHelper.createSession(getProducerBrokerName(), getProducerBrokerUrl());
//        final MessageProducer producer = new MessageProducerBuilder(producerSession, testTopic).waitTillStarted().build();
//        final TextMessage expectedTextMessage = producerSession.createTextMessage(testMessage);
//        // create 2 consumers
//        final MessageListener messageListener = new MessageListener() {
//            @Override
//            public void onMessage(Message message) {
//                try {
//                    messagePrinter.printMessage(message);
//                } catch (Exception e) {
//                    exception.set(e);
//                } finally {
//                    consumerLatch.countDown();
//                }
//            }
//        };
//        final ActiveMQSession consumer1Session = ActiveMQClientITHelper.createSession(getConsumerBrokerName(), getConsumerBrokerUrl());
//        new MessageConsumerBuilder(consumer1Session, testTopic)
//                .withMessageListener(messageListener)
//                .waitTillStarted()
//                .build();
//        ActiveMQSession consumer2Session = ActiveMQClientITHelper.createSession(getConsumerBrokerName(), getConsumerBrokerUrl());
//        new MessageConsumerBuilder(consumer2Session, testTopic)
//                .withMessageListener(messageListener)
//                .waitTillStarted()
//                .build();
//
//        // When
//        producer.send(expectedTextMessage);
//        consumerLatch.await(1L, TimeUnit.SECONDS);
//
//        // Then
//        assertNoConsumerError(exception.get());
//
//        PluginTestVerifier verifier = PluginTestVerifierHolder.getInstance();
//        verifier.printCache();
//        // Wait till all traces are recorded (consumer traces are recorded from another thread)
//        awaitAndVerifyTraceCount(verifier, 7, 5000L);
//
//        verifyProducerSendEvent(verifier, testTopic, producerSession);
//        verifyConsumerConsumeEvent(verifier, testTopic, consumer1Session);
//        verifyConsumerConsumeEvent(verifier, testTopic, consumer2Session);
//
//        Method printMessageMethod = MessagePrinter.class.getDeclaredMethod("printMessage", Message.class);
//        for (int i = 0; i < 2; ++i) {
//            verifier.verifyTrace(event(ServiceType.INTERNAL_METHOD.getName(), printMessageMethod));
//        }
//
//        verifier.verifyTraceCount(0);
//    }

//    @Test
//    public void testTopicPoll() throws Exception {
//        // Given
//        final String testTopicName = "TestPollTopic";
//        final ActiveMQTopic testTopic = new ActiveMQTopic(testTopicName);
//        final String testMessage = "Hello World for Topic!";
//        // create producer
//        final ActiveMQSession producerSession = ActiveMQClientITHelper.createSession(getProducerBrokerName(), getProducerBrokerUrl());
//        final MessageProducer producer = new MessageProducerBuilder(producerSession, testTopic).waitTillStarted().build();
//        final TextMessage expectedTextMessage = producerSession.createTextMessage(testMessage);
//        // create 2 consumers
//        final ActiveMQSession consumer1Session = ActiveMQClientITHelper.createSession(getConsumerBrokerName(), getConsumerBrokerUrl());
//        final MessageConsumer consumer1 = new MessageConsumerBuilder(consumer1Session, testTopic).waitTillStarted().build();
//        final MessageReceiveHandler messageReceiveHandler1 = new MessageReceiveHandler();
//        final PollingMessageReceiver pollingMessageReceiver1 = new PollingMessageReceiver(consumer1, messageReceiveHandler1);
//
//        final ActiveMQSession consumer2Session = ActiveMQClientITHelper.createSession(getConsumerBrokerName(), getConsumerBrokerUrl());
//        final MessageConsumer consumer2 = new MessageConsumerBuilder(consumer2Session, testTopic).waitTillStarted().build();
//        final MessageReceiveHandler messageReceiveHandler2 = new MessageReceiveHandler();
//        final PollingMessageReceiver pollingMessageReceiver2 = new PollingMessageReceiver(consumer2, messageReceiveHandler2);
//
//        // When
//        pollingMessageReceiver1.start();
//        pollingMessageReceiver2.start();
//        producer.send(expectedTextMessage);
//        messageReceiveHandler1.await(5000L);
//        messageReceiveHandler2.await(5000L);
//        pollingMessageReceiver1.stop();
//        pollingMessageReceiver2.stop();
//
//        // Then
//        assertNoConsumerError(pollingMessageReceiver1.getException());
//        assertNoConsumerError(pollingMessageReceiver2.getException());
//
//        PluginTestVerifier verifier = PluginTestVerifierHolder.getInstance();
//        verifier.printCache();
//
//        // Wait till all traces are recorded (consumer traces are recorded from another thread)
//        awaitAndVerifyTraceCount(verifier, 11, 5000L);
//
//        verifyProducerSendEvent(verifier, testTopic, producerSession);
//        verifyConsumerConsumeEvent(verifier, testTopic, consumer1Session);
//        verifyConsumerConsumeEvent(verifier, testTopic, consumer2Session);
//
//        ExpectedTrace asyncTrace = event(ServiceType.ASYNC.getName(), "Asynchronous Invocation");
//        Method handleMessageMethod = MessageReceiveHandler.class.getDeclaredMethod("handleMessage", Message.class);
//        ExpectedTrace handleMessageTrace = event(ServiceType.INTERNAL_METHOD.getName(), handleMessageMethod);
//        Method printMessageMethod = MessagePrinter.class.getDeclaredMethod("printMessage", Message.class);
//        ExpectedTrace printMessageTrace = event(ServiceType.INTERNAL_METHOD.getName(), printMessageMethod);
//        for (int i = 0; i < 2; ++i) {
//            verifier.verifyDiscreteTrace(asyncTrace, handleMessageTrace, printMessageTrace);
//        }
//
//        verifier.verifyTraceCount(0);
//    }

    /**
     * Verifies traces for when {@link com.aliyun.openservices.ons.api.impl.rocketmq.ProducerImpl ProducerImpl}
     * sends the message.
     *
     * @param verifier verifier used to verify traces
     * @param message the destination to which the producer is sending the message
     * @throws Exception
     */
    private void verifyProducerSendEvent(PluginTestVerifier verifier, Message message) throws Exception {
        Class<?> messageProducerClass = Class.forName("com.aliyun.openservices.ons.api.impl.rocketmq.ProducerImpl");
        Method send = messageProducerClass.getDeclaredMethod("send",  Message.class);
        verifier.verifyDiscreteTrace(event(
                // serviceType
                RocketMqClientConstants.ROCKETMQ_CLIENT.getName(),
                // method
                send,
                // rpc
                null,
                null,
                // endPoint
                message.getTopic(),
                // destinationId
                RocketMqClientConstants.ROCKETMQ_CLIENT_SCOPE,
                annotation("rocketmq.message", new String(message.getBody()))
        ));
    }

    /**
     * Verifies traces for when  consumes the message and dispatches for
     * further processing (for example, calling  attached to the consumer directly, or adding
     * the message to .
     *
     * @param verifier verifier used to verify traces
     * @param message the destination from which the consumer is receiving the message
     * @throws Exception
     */
    private void verifyConsumerConsumeEvent(PluginTestVerifier verifier, Message message) {
        ExpectedTrace activeMQConsumerInvocationTrace = root(
                // serviceType
                RocketMqClientConstants.ROCKETMQ_CLIENT.getName(),
                // method
                "RocketMQ Consumer Invocation",
                // rpc
                message.getTag(),
                // endPoint (collected but there's no easy way to retrieve local address)
                message.getTopic(),
                // remotAddress
                message.getBornHost(),
                annotation("rocketmq.message", new String(message.getBody())));
        verifier.verifyDiscreteTrace(activeMQConsumerInvocationTrace);
    }

    private String getMessageAsString(Message message) {
        StringBuilder messageStringBuilder = new StringBuilder(message.getClass().getSimpleName());
        return new String(message.getBody());
    }

    protected final void assertNoConsumerError(Exception consumerException) {
        Assert.assertNull("Failed with exception : " + consumerException, consumerException);
    }

    protected final void awaitAndVerifyTraceCount(PluginTestVerifier verifier, int expectedTraceCount, long maxWaitMs) throws InterruptedException {
        final long waitIntervalMs = 100L;
        long maxWaitTime = maxWaitMs;
        if (maxWaitMs < waitIntervalMs) {
            maxWaitTime = waitIntervalMs;
        }
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < maxWaitTime) {
            try {
                verifier.verifyTraceCount(expectedTraceCount);
                return;
            } catch (AssertionError e) {
                // ignore and retry
                Thread.sleep(waitIntervalMs);
            }
        }
        verifier.verifyTraceCount(expectedTraceCount);
    }
}
