package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;

    private List<Tuple> all; // aggregate for NO_GROUPING
    private Map<Field, List<Tuple>> groups;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.all = new ArrayList<>();
        this.groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        all.add(tup);
        if (gbfield != NO_GROUPING) {
            // insert tuple into corresponding list.
            Field key = tup.getField(gbfield);
            if (!groups.containsKey(key)) {
                groups.put(key, new ArrayList<>());
            }
            List<Tuple> tuples = groups.get(key);
            tuples.add(tup);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td = this.getTupleDesc();
        List<Tuple> tuples = new ArrayList<>();
        if (gbfield != NO_GROUPING) {
            groups.forEach((k, v) -> {
                Tuple t = new Tuple(td);
                t.setField(0, k);
                t.setField(1, new IntField(getAggregateResult(v, what)));
                tuples.add(t);
            });
        } else {
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(getAggregateResult(all, what)));
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

    // helper function to get tupledesc of this aggregator
    private TupleDesc getTupleDesc() {
        if (this.gbfield == NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            return new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        }
    }

    // calculate aggregate result
    private int getAggregateResult(List<Tuple> list, Aggregator.Op op) {
        switch (op) {
            case COUNT:
                return list.size();
            case SUM:
                return list.stream().mapToInt(k -> ((IntField)k.getField(afield)).getValue()).sum();
            case AVG:
                return (int)(list.stream().mapToInt(k -> ((IntField)k.getField(afield)).getValue()).sum() * 1.0 / list.size());
            case MAX:
                return list.stream().mapToInt(k -> ((IntField)k.getField(afield)).getValue()).max().getAsInt();
            case MIN:
                return list.stream().mapToInt(k -> ((IntField)k.getField(afield)).getValue()).min().getAsInt();
            default:
                return -1;
        }
    }
}
