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

package com.navercorp.pinpoint.plugin.rocketmq;

import com.navercorp.pinpoint.bootstrap.config.Filter;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentClass;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentException;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentMethod;
import com.navercorp.pinpoint.bootstrap.instrument.Instrumentor;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformCallback;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplate;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplateAware;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPlugin;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPluginSetupContext;

import java.security.ProtectionDomain;

import static com.navercorp.pinpoint.common.util.VarArgs.va;

/**
 * @author 微风
 */
public class RocketMQClientPlugin implements ProfilerPlugin, TransformTemplateAware {

    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());

    private TransformTemplate transformTemplate;

    @Override
    public void setup(ProfilerPluginSetupContext context) {
        logger.info("load rocketmq plugin");
        RocketMQClientPluginConfig config = new RocketMQClientPluginConfig(context.getConfig());
        if (!config.isTraceRocketMQClient()) {
            logger.info("load rocketmq plugin return");
            return;
        }
        if (config.isTraceRocketMQClientConsumer() || config.isTraceRocketMQClientProducer()) {
            Filter<String> excludeDestinationFilter = config.getExcludeDestinationFilter();
            if (config.isTraceRocketMQClientProducer()) {
                logger.info("load rocketmq plugin producer");
                this.addProducerEditor(excludeDestinationFilter);
            }
            if (config.isTraceRocketMQClientConsumer()) {
                logger.info("load rocketmq plugin consumer");
                this.addConsumerEditor();
            }
        }
    }

    private void addProducerEditor(final Filter<String> excludeDestinationFilter) {

        transformTemplate.transform("com.aliyun.openservices.ons.api.impl.rocketmq.ProducerImpl", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);
                logger.info("find Interceptor com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageProducerSendInterceptor");
                InstrumentMethod method = target.getDeclaredMethod("send", "com.aliyun.openservices.ons.api.Message");
                try {
                    method.addScopedInterceptor("com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageProducerSendInterceptor", va(excludeDestinationFilter), RocketMQClientConstants.ROCKETMQ_CLIENT_SCOPE);
                } catch (Exception e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unsupported method " + method, e);
                    }
                }
                method = target.getDeclaredMethod("sendOneway", "com.aliyun.openservices.ons.api.Message");
                try {
                    method.addScopedInterceptor("com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageProducerSendInterceptor", va(excludeDestinationFilter), RocketMQClientConstants.ROCKETMQ_CLIENT_SCOPE);
                } catch (Exception e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unsupported method " + method, e);
                    }
                }

                method = target.getDeclaredMethod("sendAsync", "com.aliyun.openservices.ons.api.Message", "com.aliyun.openservices.ons.api.SendCallback");
                try {
                    method.addScopedInterceptor("com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageProducerSendInterceptor", va(excludeDestinationFilter), RocketMQClientConstants.ROCKETMQ_CLIENT_SCOPE);
                } catch (Exception e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unsupported method " + method, e);
                    }
                }
                logger.info("add Interceptor com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageProducerSendInterceptor");

                return target.toBytecode();
            }
        });

    }

    private void addConsumerEditor() {
        transformTemplate.transform("com.aliyun.openservices.ons.api.impl.rocketmq.ConsumerImpl$MessageListenerImpl", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                logger.info("find Interceptor com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageConsumerReceiveInterceptor");

                final InstrumentMethod consumeMessageMethod = target.getDeclaredMethod("consumeMessage", "java.util.List","com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext");
                if (consumeMessageMethod != null) {
                    consumeMessageMethod.addScopedInterceptor("com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageConsumerReceiveInterceptor", RocketMQClientConstants.ROCKETMQ_CLIENT_SCOPE);
                    logger.info("add Interceptor com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageConsumerReceiveInterceptor");
                }

                return target.toBytecode();
            }
        });
        transformTemplate.transform("com.aliyun.openservices.ons.api.impl.rocketmq.OrderConsumerImpl$MessageListenerOrderlyImpl", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                logger.info("find Interceptor com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageConsumerReceiveInterceptor");

                final InstrumentMethod consumeMessageMethod = target.getDeclaredMethod("consumeMessage", "java.util.List","com.aliyun.openservices.shade.com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext");
                if (consumeMessageMethod != null) {
                    consumeMessageMethod.addScopedInterceptor("com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageConsumerReceiveInterceptor", RocketMQClientConstants.ROCKETMQ_CLIENT_SCOPE);
                    logger.info("add Interceptor com.navercorp.pinpoint.plugin.rocketmq.interceptor.RocketMQMessageConsumerReceiveInterceptor");
                }

                return target.toBytecode();
            }
        });
        transformTemplate.transform("org.apache.rocketmq.spring.support.DefaultRocketMQListenerContainer$DefaultMessageListenerConcurrently", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                logger.info("find Interceptor com.navercorp.pinpoint.plugin.rocketmq.interceptor.OpenRocketMQMessageConsumerReceiveInterceptor");

                final InstrumentMethod consumeMessageMethod = target.getDeclaredMethod("consumeMessage", "java.util.List","org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext");
                if (consumeMessageMethod != null) {
                    consumeMessageMethod.addScopedInterceptor("com.navercorp.pinpoint.plugin.rocketmq.interceptor.OpenRocketMQMessageConsumerReceiveInterceptor", RocketMQClientConstants.ROCKETMQ_CLIENT_SCOPE);
                    logger.info("add Interceptor com.navercorp.pinpoint.plugin.rocketmq.interceptor.OpenRocketMQMessageConsumerReceiveInterceptor");
                }

                return target.toBytecode();
            }
        });
        transformTemplate.transform("org.apache.rocketmq.spring.support.DefaultRocketMQListenerContainer$DefaultMessageListenerOrderly", new TransformCallback() {

            @Override
            public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
                InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

                logger.info("find Interceptor com.navercorp.pinpoint.plugin.rocketmq.interceptor.OpenRocketMQMessageConsumerReceiveInterceptor");

                final InstrumentMethod consumeMessageMethod = target.getDeclaredMethod("consumeMessage", "java.util.List","org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext");
                if (consumeMessageMethod != null) {
                    consumeMessageMethod.addScopedInterceptor("com.navercorp.pinpoint.plugin.rocketmq.interceptor.OpenRocketMQMessageConsumerReceiveInterceptor", RocketMQClientConstants.ROCKETMQ_CLIENT_SCOPE);
                    logger.info("add Interceptor com.navercorp.pinpoint.plugin.rocketmq.interceptor.OpenRocketMQMessageConsumerReceiveInterceptor");
                }

                return target.toBytecode();
            }
        });
    }

    @Override
    public void setTransformTemplate(TransformTemplate transformTemplate) {
        this.transformTemplate = transformTemplate;
    }
}
