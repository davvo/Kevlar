package kevlar;

class IndexEntry {

    private long timestamp;
    private long offset;

    public IndexEntry(long timestamp, long offset) {
        this.timestamp = timestamp;
        this.offset = offset;
    }

    /**
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * @return the offset
     */
    public long getOffset() {
        return offset;
    }

}
