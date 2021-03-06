/**
 * Copyright 2014 NAVER Corp.
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
package com.navercorp.plugin.rocketmq.target;


/**
 * @author Jongho Moon
 *
 */
public class TargetClass12_AsyncInitiator {
    
    public TargetClass12_Future asyncHello(String name) {
        TargetClass12_Future future = new TargetClass12_Future();
        TargetClass12_Worker worker = new TargetClass12_Worker(name, future);
        
        new Thread(worker).start();
        
        return future;
    }
}
