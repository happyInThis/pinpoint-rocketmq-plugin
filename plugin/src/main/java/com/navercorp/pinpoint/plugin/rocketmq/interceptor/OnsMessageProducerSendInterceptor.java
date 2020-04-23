/*
 * Copyright 2016 Naver Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.plugin.rocketmq.interceptor;

import com.aliyun.openservices.ons.api.Message;
import com.navercorp.pinpoint.bootstrap.config.Filter;
import com.navercorp.pinpoint.bootstrap.context.MethodDescriptor;
import com.navercorp.pinpoint.bootstrap.context.SpanEventRecorder;
import com.navercorp.pinpoint.bootstrap.context.Trace;
import com.navercorp.pinpoint.bootstrap.context.TraceContext;
import com.navercorp.pinpoint.bootstrap.context.TraceId;
import com.navercorp.pinpoint.bootstrap.interceptor.AroundInterceptor;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.plugin.rocketmq.OnsClientHeader;
import com.navercorp.pinpoint.plugin.rocketmq.RocketMQClientConstants;
import com.navercorp.pinpoint.plugin.rocketmq.descriptor.RocketMQProducerEntryMethodDescriptor;


/**
 * @author 微风
 */
public class OnsMessageProducerSendInterceptor implements AroundInterceptor {

    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());
    private final boolean isDebug = logger.isDebugEnabled();
    private static final RocketMQProducerEntryMethodDescriptor PRODUCER_ENTRY_METHOD_DESCRIPTOR = new RocketMQProducerEntryMethodDescriptor();

    private final TraceContext traceContext;
    private final MethodDescriptor descriptor;

    public OnsMessageProducerSendInterceptor(TraceContext traceContext, MethodDescriptor descriptor, Filter<String> excludeDestinationFilter) {
        this.traceContext = traceContext;
        this.descriptor = descriptor;
        traceContext.cacheApi(PRODUCER_ENTRY_METHOD_DESCRIPTOR);
    }

    @Override
    public void before(Object target, Object[] args) {
        if (isDebug) {
            logger.beforeInterceptor(target, args);
        }
        try {

            Message message = (Message) args[0];

            final Trace trace = traceContext.currentTraceObject();
            if (trace == null) {
                logger.info("RocketMQMessageProducerSendInterceptor before trade is null");
                return;
            }
            if (trace.canSampled()) {
                logger.info("RocketMQMessageProducerSendInterceptor before canSampled");

                SpanEventRecorder spanEventRecorder = trace.traceBlockBegin();
                spanEventRecorder.recordServiceType(RocketMQClientConstants.ROCKETMQ_PRODUCER);
                spanEventRecorder.recordAttribute(RocketMQClientConstants.ROCKETMQ_MESSAGE, new String(message.getBody()));
                TraceId nextId = trace.getTraceId().getNextTraceId();
                spanEventRecorder.recordNextSpanId(nextId.getSpanId());

                OnsClientHeader.setTraceId(message, nextId.getTransactionId());
                OnsClientHeader.setSpanId(message, nextId.getSpanId());
                OnsClientHeader.setParentSpanId(message, nextId.getParentSpanId());
                OnsClientHeader.setFlags(message, nextId.getFlags());
                OnsClientHeader.setParentApplicationName(message, traceContext.getApplicationName());
                OnsClientHeader.setParentApplicationType(message, traceContext.getServerTypeCode());

            } else {
                OnsClientHeader.setSampled(message, false);
            }
        } catch (Throwable t) {
            logger.warn("BEFORE. Cause:{}", t.getMessage(), t);
        }
    }

    @Override
    public void after(Object target, Object[] args, Object result, Throwable throwable) {
        if (isDebug) {
            logger.afterInterceptor(target, args);
        }
        Trace trace = traceContext.currentTraceObject();
        if (trace == null) {
            logger.info("RocketMQMessageProducerSendInterceptor trade is null ");
            return;
        }

        try {
            SpanEventRecorder recorder = trace.currentSpanEventRecorder();
            recorder.recordApi(descriptor, args);
            recorder.recordServiceType(RocketMQClientConstants.ROCKETMQ_PRODUCER);
            if (throwable != null) {
                recorder.recordException(throwable);
            }
        } catch (Throwable t) {
            logger.warn("AFTER error. Cause:{}", t.getMessage(), t);
        } finally {
            trace.traceBlockEnd();
        }

    }

}
