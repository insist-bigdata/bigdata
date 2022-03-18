package top.damoncai.top.bean;

import com.sun.jmx.snmp.Timestamp;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/18 10:31
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;
    public Event() {
    }
    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }
    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
