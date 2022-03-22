package top.damoncai.top.bean;

import java.sql.Timestamp;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/22 17:34
 */
public class UrlViewCount {

    public String url;
    public Long count;
    public Long windowStart;
    public Long windowEnd;
    public UrlViewCount() {
    }
    public UrlViewCount(String url, Long count, Long windowStart, Long windowEnd)
    {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }
    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
