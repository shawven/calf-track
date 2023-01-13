package com.github.shawven.calf.track.datasource.mongo;

/**
 * @author xw
 * @date 2023-01-05
 */
public class OpLogEventFormatterFactory {

    private final String namespace;
    private final String dsName;
    private final String destQueue;

    private final OpLogEventFormatter updateEventFormatter;

    private final OpLogEventFormatter writeEventFormatter;

    private final OpLogEventFormatter deleteEventFormatter;

    private final OpLogEventFormatter defaultEventFormatter;

    public OpLogEventFormatterFactory(String namespace, String dsName, String destQueue) {
        this.namespace = namespace;
        this.dsName = dsName;
        this.destQueue = destQueue;
        this.writeEventFormatter = new OpLogEventFormatter.Write(namespace, dsName, destQueue);
        this.updateEventFormatter = new OpLogEventFormatter.Update(namespace, dsName, destQueue);
        this.deleteEventFormatter = new OpLogEventFormatter.Delete(namespace, dsName, destQueue);

        this.defaultEventFormatter = event -> null;
    }

    public OpLogEventFormatter getFormatter(String eventType) {
        switch (eventType) {
            case "u":
                return updateEventFormatter;
            case "i":
                return writeEventFormatter;
            case "d":
                return deleteEventFormatter;
            default:
                return defaultEventFormatter;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OpLogEventFormatterFactory{");
        sb.append("namespace='").append(namespace).append('\'');
        sb.append(", dsName='").append(dsName).append('\'');
        sb.append(", destQueue='").append(destQueue).append('\'');
        sb.append(", updateEventFormatter=").append(updateEventFormatter);
        sb.append(", writeEventFormatter=").append(writeEventFormatter);
        sb.append(", deleteEventFormatter=").append(deleteEventFormatter);
        sb.append(", defaultEventFormatter=").append(defaultEventFormatter);
        sb.append('}');
        return sb.toString();
    }
}
