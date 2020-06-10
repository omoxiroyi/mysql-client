package com.fan.mysql.position;


import org.apache.commons.lang3.StringUtils;

import java.util.*;

@SuppressWarnings("unused")
public class GtidSetPosition extends LogPosition {

    public final static String GTID_START = "GTID_START";
    private final Map<String, UUIDSet> map = new LinkedHashMap<>();

    public GtidSetPosition(String gtidSet) {
        if (gtidSet.equals(GTID_START)) {
            gtidSet = "";
        }
        gtidSet = gtidSet.replaceAll("\\s", "");
        String[] uuidSets = gtidSet.isEmpty() ? new String[0] : gtidSet.split(",");
        for (String uuidSet : uuidSets) {
            int uuidSeparatorIndex = uuidSet.indexOf(":");
            String sourceId = uuidSet.substring(0, uuidSeparatorIndex);
            List<Interval> intervals = new LinkedList<>();
            String[] rawIntervals = uuidSet.substring(uuidSeparatorIndex + 1).split(":");
            for (String interval : rawIntervals) {
                String[] is = interval.split("-");
                long[] split = new long[is.length];
                for (int i = 0, e = is.length; i < e; i++) {
                    split[i] = Long.parseLong(is[i]);
                }
                if (split.length == 1) {
                    split = new long[]{split[0], split[0]};
                }
                intervals.add(new Interval(split[0], split[1]));
            }
            map.put(sourceId, new UUIDSet(sourceId, intervals));
        }
    }

    /**
     * @param gtid GTID("source_id:transaction_id")
     * @return whether or not gtid was added to the set (false if it was already
     * there)
     */
    public boolean add(String gtid) {
        String[] split = gtid.split(":");
        String sourceId = split[0];
        long transactionId = Long.parseLong(split[1]);
        UUIDSet uuidSet = map.get(sourceId);

        // corner case: gtid is not in uuidSet
        if (uuidSet == null) {
            map.put(sourceId, new UUIDSet(sourceId,
                    new ArrayList<>(Collections.singletonList(new Interval(transactionId, transactionId)))));
            return true;
        }

        List<Interval> intervals = uuidSet.intervals;
        int index = findInterval(intervals, transactionId);
        boolean addedToExisting = false;
        if (index < intervals.size()) {
            Interval interval = intervals.get(index);
            if (interval.getStart() == transactionId + 1) {
                interval.start = transactionId;
                addedToExisting = true;
            } else if (interval.getEnd() == transactionId - 1) {
                interval.end = transactionId;// add is open.
                addedToExisting = true;
            } else if (interval.getStart() <= transactionId && transactionId <= interval.getEnd()) {
                return false;
            }
        }
        if (!addedToExisting) {
            intervals.add(index, new Interval(transactionId, transactionId));// 保证interval列表是按照时间排序的
        }
        if (intervals.size() > 1) {
            joinAdjacentIntervals(intervals, index);
        }
        return true;
    }

    public void add(GtidSetPosition otherPos) {
        for (String uuid : otherPos.map.keySet()) {
            if (!map.containsKey(uuid)) {
                map.put(uuid, otherPos.map.get(uuid));
            } else {
                UUIDSet uuidSet = map.get(uuid);
                uuidSet.merge(otherPos.map.get(uuid));
                joinIntervals(uuidSet.intervals);
            }
        }
    }

    public boolean contains(String gtid) {
        String[] split = gtid.split(":");
        String sourceId = split[0];
        long transactionId = Long.parseLong(split[1]);
        UUIDSet selfUuidSet = map.get(sourceId);
        if (selfUuidSet == null) {
            return false;
        }
        for (Interval selfInterval : selfUuidSet.intervals) {
            if (selfInterval.start <= transactionId && selfInterval.end >= transactionId) {
                return true;
            }
        }
        return false;
    }

    public boolean contains(GtidSetPosition subset) {
        for (String uuid : subset.map.keySet()) {
            UUIDSet uuidSet = subset.map.get(uuid);
            UUIDSet selfUuidSet = map.get(uuid);
            if (selfUuidSet == null) {
                continue;
            }
            for (Interval interval : uuidSet.intervals) {
                // check contains
                boolean findContain = false;
                for (Interval selfInterval : selfUuidSet.intervals) {
                    if (selfInterval.start <= interval.start && selfInterval.end >= interval.end) {
                        findContain = true;
                        break;
                    }
                }
                if (!findContain) {
                    return false;
                }
            }
        }
        return true;
    }

    public long getNextInterval(String uuid) {
        long maxInterval = 1;
        UUIDSet uuidSet = map.get(uuid);
        if (uuidSet == null) {
            return maxInterval;
        }
        for (Interval intvl : uuidSet.intervals) {
            maxInterval = Math.max(intvl.end, maxInterval);
        }
        return maxInterval + 1;
    }

    /**
     * l1,l2,...,index-1, index, index+1, r1,r2,... Collapses intervals like a-b:b-c
     * into a-c (only in index+-1 range)
     */
    private void joinAdjacentIntervals(List<Interval> intervals, int index) {
        for (int i = Math.min(index + 1, intervals.size() - 1), e = Math.max(index - 1, 0); i > e; i--) {
            Interval a = intervals.get(i - 1), b = intervals.get(i);
            if (a.getEnd() + 1 == b.getStart()) {
                a.end = b.end;
                intervals.remove(i);
            }
        }
    }

    private void joinIntervals(List<Interval> intervals) {
        List<Interval> removeInt = new ArrayList<>();
        for (int i = 0; i < intervals.size() - 1; i++) {
            Interval checkingInt = intervals.get(i);
            for (int j = i + 1; j < intervals.size(); j++) {
                Interval nextInt = intervals.get(j);
                if (checkingInt.end != nextInt.start - 1) {
                    break;
                }
                checkingInt.end = nextInt.end;
                removeInt.add(nextInt);
                i = j;
            }
        }
        intervals.removeAll(removeInt);
    }

    /**
     * @return index which is either a pointer to the interval containing v or a
     * position at which v can be added
     */
    private int findInterval(List<Interval> intervals, long transactionId) {/* 可以优化，如果已有，则直接返回-1 */
        if (intervals.isEmpty())
            return 0;

        int l = 0, p = 0, r = intervals.size();
        while (l < r) {
            p = (l + r) / 2;
            Interval i = intervals.get(p);
            if (i.getEnd() < transactionId - 1) {
                l = p + 1;
            } else if (transactionId < i.getStart() - 1) {
                r = p;
            } else {// i.getStart() <= transactionID <= i.getEnd()
                return p;
            }
        }
        /* return intervals.size() if not found */
        if (!intervals.isEmpty() && intervals.get(p).getEnd() < transactionId) {
            p++;
        }
        return p;
    }

    public Collection<UUIDSet> getUUIDSets() {
        return map.values();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (UUIDSet uuidSet : map.values()) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(",");
            }
            sb.append(uuidSet.getUUID());
            for (Interval interval : uuidSet.intervals) {
                sb.append(":").append(interval.toClosedString());
            }
        }
        return sb.toString();
    }

    public static boolean isEquals(String masterGtidSet, String slaveGtidSet) {
        masterGtidSet = StringUtils.strip(masterGtidSet);
        slaveGtidSet = StringUtils.strip(slaveGtidSet);
        if (StringUtils.isEmpty(masterGtidSet) && StringUtils.isEmpty(slaveGtidSet))
            return true;
        if (!StringUtils.isEmpty(masterGtidSet) && !StringUtils.isEmpty(slaveGtidSet)) {
            return toSortString(masterGtidSet).equals(toSortString(slaveGtidSet));
        }
        return false;
    }

    public static String toSortString(String gtidSet) {
        gtidSet = StringUtils.strip(gtidSet);
        gtidSet = StringUtils.chomp(gtidSet);
        if (StringUtils.isEmpty(gtidSet))
            return "";
        gtidSet = gtidSet.replaceAll("\\s", "");
        String[] splited = gtidSet.split(",");
        Arrays.sort(splited);
        return StringUtils.join(splited, ",");
    }

    public static class UUIDSet {

        private final String uuid;
        private List<Interval> intervals;

        public UUIDSet(String uuid, List<Interval> intervals) {
            this.uuid = uuid;
            this.intervals = intervals;
        }

        public String getUUID() {
            return uuid;
        }

        public Collection<Interval> getIntervals() {
            return intervals;
        }

        public long getMaxIntervalPos() {
            long max = 1;
            for (Interval interval : intervals) {
                max = Math.max(max, interval.end);
            }
            return max;
        }

        private void merge(UUIDSet other) {
            // build merge position list
            List<MergePosition> mergePosList = new ArrayList<>();
            for (Interval interval : intervals) {
                mergePosList.add(new MergePosition(interval.start, true));
                mergePosList.add(new MergePosition(interval.end, false));
            }
            for (Interval interval : other.intervals) {
                mergePosList.add(new MergePosition(interval.start, true));
                mergePosList.add(new MergePosition(interval.end, false));
            }
            mergePosList.sort((mp1, mp2) -> {
                long rs = mp1.pos - mp2.pos;
                if (rs != 0) {
                    return rs > 0 ? 1 : -1;
                }
                // define in less than out
                if (mp1.in && !mp2.in) {
                    return -1;
                } else if (!mp1.in && mp2.in) {
                    return 1;
                }
                return 0;
            });
            // generate new intervals
            Interval currentInterval = null;
            int depth = 0;
            this.intervals = new ArrayList<>();
            for (MergePosition mp : mergePosList) {
                if (depth == 0) {
                    currentInterval = new Interval();
                    currentInterval.start = mp.pos;
                    depth = 1;
                } else if (mp.in) {
                    depth++;
                } else if (--depth == 0) {
                    currentInterval.end = mp.pos;
                    intervals.add(currentInterval);
                }
            }
        }

        private static class MergePosition {

            private final long pos;
            private final boolean in;

            private MergePosition(long pos, boolean in) {
                this.pos = pos;
                this.in = in;
            }

        }

    }

    public static class Interval {

        private long start;
        private long end;

        public Interval() {
        }

        public Interval(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        @Override
        public String toString() {
            return start + "-" + end;
        }

        public String toClosedString() {
            if (start == end) {
                return start + "";
            }
            return start + "-" + end;
        }

    }

}
