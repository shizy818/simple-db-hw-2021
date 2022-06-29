package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int buckets;
    private int min, max;

    private int nTups;
    private int[] h_b;
    private double w_b;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;

        this.nTups = 0;
        this.h_b = new int[buckets];
        // 加1保证插入max的时候不会越界
        this.w_b = (max - min + 1) / (1.0 * buckets);
    }

    // helper func to get indx of a value v
    private int getIndex(int v) {
        if (v < min) return -1;
        if (v > max) return buckets;

        return (int) Math.floor((v - min) / w_b);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = getIndex(v);
        if (index < 0 || index > buckets)
            return;

        this.h_b[index]++;
        this.nTups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        double cost = 0;
        double b_f = 0.0, b_r = 0.0;
        int idx = getIndex(v);
        switch (op) {
            case EQUALS:
                cost = idx < 0 || idx >= buckets ? 0.0 : h_b[idx] / (nTups * w_b);
                break;
            case NOT_EQUALS:
                cost = 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            case LESS_THAN:
                if (idx < 0) return 0.0;
                if (idx >= buckets) return 1.0;
                for(int i = 0; i < idx; i++) {
                    cost += h_b[i] / (nTups * 1.0);
                }
                b_f = h_b[idx] / (nTups * 1.0);
                // 多出来一小部分和w_b的比例
                b_r = (v - min - w_b * idx) / w_b;
                cost += b_f * b_r;
                break;
            case LESS_THAN_OR_EQ:
                cost += estimateSelectivity(Predicate.Op.LESS_THAN, v);
                cost += estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            case GREATER_THAN:
                if (idx < 0) return 1.0;
                if (idx >= buckets) return 0.0;
                for(int i = idx + 1; i < buckets; i++) {
                    cost += h_b[i] / (nTups * 1.0);
                }
                b_f = h_b[idx] / (nTups * 1.0);
                // 多出来一小部分和w_b的比例
                b_r = (min + w_b * (idx + 1) - v) / w_b;
                cost += b_f * b_r;
                break;
            case GREATER_THAN_OR_EQ:
                cost += estimateSelectivity(Predicate.Op.GREATER_THAN, v);
                cost += estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            default:
                throw new RuntimeException("Unsupported Op");
        }

        if (cost > 1.0) return 1.0;
        if (cost < 0.0) return 0.0;

        return cost;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "IntHistogram{" + "buckets=" + buckets +
                ", min=" + min + ", max=" + max + ", nTups=" + nTups +
                ", w_b=" + w_b + ", h_b=" + Arrays.toString(h_b) + '}';
    }
}
