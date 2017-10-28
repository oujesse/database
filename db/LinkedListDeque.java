package db;
public class LinkedListDeque<Item> implements Deque<Item>{
    private IntNode sentinel;
    private int size;

    private class IntNode {
        private Item item;
        private IntNode next;
        private IntNode last;
        private IntNode(IntNode l, Item i, IntNode n) {
            item = i;
            next = n;
            last = l;
        }
        private IntNode(IntNode i) {
            last = i.last;
            next = i.next;
            item = i.item;
        }
    }


    public LinkedListDeque() {
        size = 0;
        sentinel = new IntNode(null, null, null);
        sentinel.next = sentinel;
        sentinel.last = sentinel;
    }
    @Override
    public void addFirst(Item x) {
        sentinel.next = new IntNode(sentinel, x, sentinel.next);
        sentinel.next.next.last = sentinel.next;
        size += 1;
    }
    @Override
    public void addLast(Item x) {
        sentinel.last = new IntNode(sentinel.last, x, sentinel);
        sentinel.last.last.next = sentinel.last;
        size += 1;
    }
    @Override
    public boolean isEmpty() {
        if (size == 0) {
            return true;
        }
        return false;
    }
    @Override
    public int size() {
        return size;
    }
    @Override
    public void printDeque() {
        if (size == 0) {
            return;
        }
        IntNode p = sentinel.next;
        int count = 0;
        while (count < size) {
            System.out.print(p.item + " ");
            p = p.next;
            count += 1;
        }
    }
    @Override
    public Item removeFirst() {

        Item removed = sentinel.next.item;
        sentinel.next = sentinel.next.next;
        sentinel.next.last = sentinel;
        size -= 1;
        return removed;
    }
    @Override
    public Item removeLast() {

        Item removed = sentinel.last.item;
        sentinel.last = sentinel.last.last;
        sentinel.last.next = sentinel;
        size -= 1;
        return removed;
    }
    @Override
    public Item get(int index) {
        if (index >= size) {
            return null;
        }
        int count = 0;
        if (size == 0) {
            return null;
        }
        IntNode node = new IntNode(sentinel.next);
        while (count < index) {
            node = node.next;
            count += 1;
        }
        return node.item;
    }

    private IntNode getNode(int count, IntNode node) {
        if (size <= 0) {
            return null;
        }
        if (count <= 0) {
            return node;
        } else {
            count -= 1;
            node = node.next;
        }
        return getNode(count, node);
    }
    public Item getRecursive(int index) {
        if (index >= size || size == 0) {
            return null;
        }
        return getNode(index, new IntNode(sentinel.next)).item;
    }
}
