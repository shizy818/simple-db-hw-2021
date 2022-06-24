package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;

    // Another implementation
    private Map<Field, Tuple> tupMap;

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUPING_STRING_FIELD = new IntField(Integer.MIN_VALUE);

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT)
            throw new IllegalArgumentException("Not COUNT op");

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.tupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        TupleDesc td = this.getTupleDesc();

        if (gbfield != NO_GROUPING) {
            // insert tuple into corresponding list.
            Field key = tup.getField(gbfield);
            if (!tupMap.containsKey(key)) {
                Tuple newTup = new Tuple(td);
                newTup.setField(0, key);
                newTup.setField(1, new IntField(0));
                tupMap.put(key, newTup);
            }

            // Calculate aggregated(count) result
            Tuple t = tupMap.get(key);
            int newVal = ((IntField)t.getField(1)).getValue() + 1;
            t.setField(1, new IntField(newVal));
        } else {
            Field key = NO_GROUPING_STRING_FIELD;
            if (!tupMap.containsKey(key)) {
                Tuple newTup = new Tuple(td);
                newTup.setField(0, new IntField(0));
                tupMap.put(key, newTup);
            }

            // Calculate aggregated(count) result
            Tuple t = tupMap.get(key);
            int newVal = ((IntField)t.getField(0)).getValue() + 1;
            t.setField(0, new IntField(newVal));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // TupleDesc td = this.getTupleDesc();
        // return new TupleIterator(td, tupMap.values());

        return new OpIterator() {
            private static final long serialVersionUID = 1L;

            private int pos = 0;
            private boolean isOpen = false;
            private Tuple[] allTuples = tupMap.values().toArray(new Tuple[tupMap.size()]);

            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
                pos = 0;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!isOpen)
                    throw new IllegalStateException("OpIterator is not open");
                return pos >= 0 && pos < allTuples.length;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext())
                    throw new NoSuchElementException("Out of bound");
                return allTuples[pos++];
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pos = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return getTupleDesc();
            }

            @Override
            public void close() {
                isOpen = false;
            }
        };
    }

    // helper function to get tupledesc of this aggregator. It only support COUNT aggregator.
    private TupleDesc getTupleDesc() {
        if (this.gbfield == NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            return new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        }
    }
}
