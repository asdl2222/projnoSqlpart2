package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            // Hint: you may find the helper methods getTransaction() and getRecordIterator(tableName)
            // in JoinOperator useful here.
            String sortedRighttemp = new SortOperator(getTransaction(),getRightTableName(),new RightRecordComparator()).sort();
            String sortedLefttemp =  new SortOperator(getTransaction(),getLeftTableName(),new LeftRecordComparator()).sort();
            rightIterator = SortMergeOperator.this.getRecordIterator(sortedRighttemp);
            leftIterator = SortMergeOperator.this.getRecordIterator(sortedLefttemp);
            rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
            leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            // We mark the first record so we can reset to it when we advance the left record.
            if (rightRecord != null) {
                rightIterator.markPrev();
            } else { return; }

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }

        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement
            return this.nextRecord != null;
            //return false;
        }

        // Added my own function
        private void fetchNextRecord() {
            if (leftRecord == null) {
                throw new NoSuchElementException("There is no record to fetch");
            }
            this.nextRecord = null;

            do {
                if (rightRecord != null && !marked) {
                    while (compareToLeftRight() > 0) {
                        rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    }

                    while (compareToLeftRight() < 0) {
                        leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                        if (leftRecord == null) {
                            //throw new NoSuchElementException("There is no record to fetch");
                            break;
                        }
                    }

                    marked = true;
                    rightIterator.markPrev();
                }
                if (rightRecord != null && compareToLeftRight() == 0) {
                    nextRecord = joinRecords(leftRecord, rightRecord);
                    rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                }
                else {
                    this.rightIterator.reset();
                    assert(rightIterator.hasNext());
                    rightRecord = rightIterator.next();
                    leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                    if (leftRecord == null) {
                        break;
                    }
                    marked = false;
                }
            } while (!hasNext());
        }

        // Added from BNLJOperator.java
        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        // Added my own function
        private int compareToLeftRight() {
            DataBox leftTemp = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
            DataBox rightTemp = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());

            return leftTemp.compareTo(rightTemp);
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
            //throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
