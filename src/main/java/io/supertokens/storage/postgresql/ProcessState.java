/*
 *    Copyright (c) 2020, VRAI Labs and/or its affiliates. All rights reserved.
 *
 *    This software is licensed under the Apache License, Version 2.0 (the
 *    "License") as published by the Apache Software Foundation.
 *
 *    You may not use this file except in compliance with the License. You may
 *    obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *    License for the specific language governing permissions and limitations
 *    under the License.
 *
 */

package io.supertokens.storage.postgresql;

import java.util.ArrayList;
import java.util.List;

public class ProcessState extends ResourceDistributor.SingletonResource {

    private static final String RESOURCE_KEY = "io.supertokens.storage.postgresql.ProcessState";
    private final List<EventAndException> history = new ArrayList<>();

    private ProcessState() {

    }

    public static ProcessState getInstance(Start main) {
        ResourceDistributor.SingletonResource instance = main.getResourceDistributor().getResource(RESOURCE_KEY);
        if (instance == null) {
            instance = main.getResourceDistributor().setResource(RESOURCE_KEY, new ProcessState());
        }
        return (ProcessState) instance;
    }

    public synchronized EventAndException getLastEventByName(PROCESS_STATE processState) {
        for (int i = history.size() - 1; i >= 0; i--) {
            if (history.get(i).state == processState) {
                return history.get(i);
            }
        }
        return null;
    }

    public synchronized void addState(PROCESS_STATE processState, Exception e) {
        if (Start.isTesting) {
            history.add(new EventAndException(processState, e));
        }
    }

    public synchronized void clear() {
        history.clear();
    }

    /**
     * CREATING_NEW_TABLE: When the program is attempting to create new tables.
     * DEADLOCK_FOUND: In case of a deadlock situation, we put this event
     */
    public enum PROCESS_STATE {
        CREATING_NEW_TABLE, DEADLOCK_FOUND, DEADLOCK_NOT_RESOLVED
    }

    public static class EventAndException {
        public Exception exception;
        PROCESS_STATE state;

        public EventAndException(PROCESS_STATE state, Exception e) {
            this.state = state;
            this.exception = e;
        }
    }

}
