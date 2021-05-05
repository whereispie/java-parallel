package edu.coursera.parallel;

import edu.rice.pcdp.PCDP;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import static edu.rice.pcdp.PCDP.*;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    private static double sum;
    private static double sum1;
    private static double sum2;
    static int SEQUENTIAL_THRESHOLD = 1000;

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks   The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk     The chunk to compute the start of
     * @param nChunks   The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     * nElements
     */
    private static int getChunkStartInclusive(final int chunk,
                                              final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk     The chunk to compute the end of
     * @param nChunks   The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
                                            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        int lo;
        int hi;
        double[] arr;
        double answer = 0;

        public ReciprocalArraySumTask(double[] arr, int lo, int hi) {
            this.lo = lo;
            this.hi = hi;
            this.arr = arr;
        }


        @Override
        protected void compute() {
            if (hi - lo <= SEQUENTIAL_THRESHOLD) {
                for (int i = lo; i < hi; ++i)
                    answer += 1 / arr[i];
            } else {
                ReciprocalArraySumTask left = new ReciprocalArraySumTask(arr, lo, (hi + lo) / 2);
                ReciprocalArraySumTask right = new ReciprocalArraySumTask(arr, (hi + lo) / 2, lo);
                left.fork(); // async
                right.compute();
                left.join();

                answer = left.answer + right.answer;
            }
        }
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param X Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(double[] X) {
        long startTime = System.nanoTime();
        assert X.length % 2 == 0;
        sum1 = 0;
        sum2 = 0;

//        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "4");
//        ForkJoinPool pool = new ForkJoinPool(4);

        finish(() -> {
            async(() -> {
                for (int i = 0; i < X.length / 2; i++) {
                    sum1 += 1 / X[i];
                }
            });
            for (int i = X.length / 2; i < X.length; i++) {
                sum2 += 1 / X[i];
            }
        });

        return sum1 + sum2;
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input    Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
                                                final int numTasks) {
        sum = 0;

//        final int chunk = 0;
//        final int nChunks = 0;
//        final int nElements = 0;
        int chunkSize = getChunkSize(SEQUENTIAL_THRESHOLD, input.length);


        ForkJoinPool pool = new ForkJoinPool(numTasks);
        // Compute sum of reciprocals of array elements
        pool.submit(() -> {
            for (int i = 0; i < input.length; i++) {
                sum += 1 / input[i];
            }
        });
        return sum;
    }
}
