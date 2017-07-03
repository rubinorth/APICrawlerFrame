package util;

/**
 * Created by nizeyang on 2017/6/3.
 * 过滤emoji表情
 * 数据库默认情况下是utf8_general_ci,这种字符集下，默认是支持1-3字节的编码，而Emoji表情是4个字节的。
 */
public class EmojiFilter {
    /**
     * emoji表情替换
     *
     * @param source 原字符串
     * @param slipStr emoji表情替换成的字符串
     * @return 过滤后的字符串
     */
    public static String filterEmoji(String source,String slipStr) {
        return source.replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", slipStr);
    }
}
