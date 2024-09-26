/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.connect.mirror.admin.offsetinspector;

import org.apache.kafka.common.ConsumerGroupState;

import java.util.Objects;

public final class GroupAndState {

    private final String id;
    private final ConsumerGroupState state;

    public GroupAndState(final String id, final ConsumerGroupState state) {
        this.id = id;
        this.state = state;
    }

    public String id() {
        return id;
    }

    public ConsumerGroupState state() {
        return state;
    }

    public boolean active() {
        switch (state) {
            case STABLE:
            case ASSIGNING:
            case RECONCILING:
            case PREPARING_REBALANCE:
            case COMPLETING_REBALANCE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupAndState that = (GroupAndState) o;
        return Objects.equals(id, that.id) && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, state);
    }

    @Override
    public String toString() {
        return "GroupAndState{" +
                "id='" + id + '\'' +
                ", state=" + state +
                '}';
    }
}