package top.damoncai.top.bean;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/25 18:15
 */
public class Pattern {

    public String pattern1;
    public String pattern2;

    public Pattern() {

    }

    public Pattern(String pattern1, String pattern2) {
        this.pattern1 = pattern1;
        this.pattern2 = pattern2;
    }

    @Override
    public String toString() {
        return pattern1 + " - " + pattern2;
    }
}
