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

import com.navercorp.pinpoint.plugin.rocketmq.util.AgentPath;
import com.navercorp.pinpoint.test.plugin.Dependency;
import com.navercorp.pinpoint.test.plugin.JvmVersion;
import com.navercorp.pinpoint.test.plugin.PinpointAgent;
import com.navercorp.pinpoint.test.plugin.PinpointConfig;
import com.navercorp.pinpoint.test.plugin.PinpointPluginTestSuite;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

/**
 * @author 微风
 */
@RunWith(PinpointPluginTestSuite.class)
@PinpointAgent(AgentPath.PATH)
@PinpointConfig("rocketmq/pinpoint-rocketmq-client.config")
// 5.4.1 bug creates activemq-data directory even if persistence is set to false - skip it
// 5.5.x activemq-all missing slf4j binder - just skip instead of supplying one
@Dependency({"com.aliyun.openservices:ons-client:1.8.4.Final"})
@JvmVersion(8)
public class RocketMQClientSingleBrokerIT extends RocketMQClientITBase {

    @BeforeClass
    public static void setUpBeforeClass() {
    }

    @AfterClass
    public static void tearDownAfterClass() {
    }

}
