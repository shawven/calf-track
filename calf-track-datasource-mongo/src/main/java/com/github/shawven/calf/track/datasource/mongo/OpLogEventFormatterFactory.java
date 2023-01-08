package com.github.shawven.calf.track.datasource.mongo;

/**
 * @author xw
 * @date 2023-01-05
 */
public class OpLogEventFormatterFactory {

    private final OpLogEventFormatter updateEventFormatter;

    private final OpLogEventFormatter writeEventFormatter;

    private final OpLogEventFormatter deleteEventFormatter;

    private final OpLogEventFormatter defaultEventFormatter;

    public OpLogEventFormatterFactory(String name) {
        this.writeEventFormatter = new OpLogEventFormatter.Write(name);
        this.updateEventFormatter = new OpLogEventFormatter.Update(name);
        this.deleteEventFormatter = new OpLogEventFormatter.Delete(name);

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
}
