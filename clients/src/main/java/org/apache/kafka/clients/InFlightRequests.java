/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The set of requests which have been sent or are being sent but haven't yet received a response
 */
final class InFlightRequests {

    private final int maxInFlightRequestsPerConnection;
    private final Map<String, Deque<ClientRequest>> requests = new HashMap<>();

    public InFlightRequests(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    /**
     * Add the given request to the queue for the connection it was directed to
     * 表示已经发送，或正在发送。并且还没有收到响应的（客户端）请求。请求首先加入到（目标节点对应的）队列头部
     * 注意：由于这个方法调用之前会先使用canSendRequest()，因此新的请求能加进来的条件是上一个请求必须已经发送成功！（这就避免了因为网络阻塞，请求一直堆积在某个节点上。）
     */
    public void add(ClientRequest request) {
        Deque<ClientRequest> reqs = this.requests.get(request.request().destination());
        if (reqs == null) {
            reqs = new ArrayDeque<>();
            this.requests.put(request.request().destination(), reqs);
        }
        // reqs队列首部添加
        reqs.addFirst(request);
    }

    /**
     * Get the request queue for the given node
     */
    private Deque<ClientRequest> requestQueue(String node) {
        Deque<ClientRequest> reqs = requests.get(node);
        if (reqs == null || reqs.isEmpty())
            throw new IllegalStateException("Response from server for which there are no in-flight requests.");
        return reqs;
    }

    /**
     * Get the oldest request (the one that that will be completed next) for the given node
     */
    public ClientRequest completeNext(String node) {
        //取出队尾元素（队尾的元素是时间最久的，也是应该先处理的）
        return requestQueue(node).pollLast();
    }

    /**
     * Get the last request we sent to the given node (but don't remove it from the queue)
     * @param node The node id
     */
    public ClientRequest lastSent(String node) {
        return requestQueue(node).peekFirst();
    }

    /**
     * Complete the last request that was sent to a particular node.
     * 取出该连接，最新的请求
     * @param node The node the request was sent to
     * @return The request
     */
    public ClientRequest completeLastSent(String node) {
        return requestQueue(node).pollFirst();
    }

    /**
     * Can we send more requests to this node?
     * 使用队列虽然可以存储多个请求，但是新的请求能加进来的条件是上一个请求必须已经发送成功。
     * @param node Node in question
     * @return true iff we have no requests still being sent to the given node
     */
    public boolean canSendMore(String node) {
        Deque<ClientRequest> queue = requests.get(node);
        //判断条件：队列为空
        return queue == null || queue.isEmpty() ||
                //或者队列头请求已经发送完成且队列中没有堆积过多请求
                //如果队头的请求迟迟发送不出去，可能是网络的原因，则不能继续向此Node发送请求;如果Node已经堆积了很多未响应的请求，说明这个节点的负载或网络连接有问题，继续发送请求，则可能会超时
               (queue.peekFirst().request().completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }

    /**
     * Return the number of inflight requests directed at the given node
     * @param node The node
     * @return The request count.
     */
    public int inFlightRequestCount(String node) {
        Deque<ClientRequest> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Count all in-flight requests for all nodes
     */
    public int inFlightRequestCount() {
        int total = 0;
        for (Deque<ClientRequest> deque : this.requests.values())
            total += deque.size();
        return total;
    }

    /**
     * Clear out all the in-flight requests for the given node and return them
     * 
     * @param node The node
     * @return All the in-flight requests for that node that have been removed
     */
    public Iterable<ClientRequest> clearAll(String node) {
        Deque<ClientRequest> reqs = requests.get(node);
        if (reqs == null) {
            return Collections.emptyList();
        } else {
            return requests.remove(node);
        }
    }

    /**
     * Returns a list of nodes with pending inflight request, that need to be timed out
     *
     * @param now current time in milliseconds
     * @param requestTimeout max time to wait for the request to be completed
     * @return list of nodes
     */
    public List<String> getNodesWithTimedOutRequests(long now, int requestTimeout) {
        List<String> nodeIds = new LinkedList<>();
        for (Map.Entry<String, Deque<ClientRequest>> requestEntry : requests.entrySet()) {
            String nodeId = requestEntry.getKey();
            Deque<ClientRequest> deque = requestEntry.getValue();

            if (!deque.isEmpty()) {
                ClientRequest request = deque.peekLast();
                long timeSinceSend = now - request.sendTimeMs();
                if (timeSinceSend > requestTimeout)
                    nodeIds.add(nodeId);
            }
        }

        return nodeIds;
    }
}
