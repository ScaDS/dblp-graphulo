package de.alkern.graphulo.connected_components.data;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Main memory implementation of {@see VisitQueue} using a PriorityQueue
 */
public class VisitQueueImpl implements VisitQueue {

    private Queue<String> queue;

    public VisitQueueImpl() {
        queue = new PriorityQueue<>();
    }

    @Override
    public void add(String node) {
        queue.add(node);
    }

    @Override
    public void addAll(Collection<String> nodes) {
        queue.addAll(nodes);
    }

    @Nullable
    @Override
    public String poll() {
        return queue.poll();
    }

    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
