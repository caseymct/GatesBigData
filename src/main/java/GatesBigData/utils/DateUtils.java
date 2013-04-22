package GatesBigData.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.schema.DateField;
import org.apache.solr.util.DateMathParser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateUtils {

    private static TimeZone UTC = TimeZone.getTimeZone("UTC");

    private static DateMathParser p = new DateMathParser(UTC, Locale.US);
    //private static SimpleDateFormat solrDateFormatNoTimeZone = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
    private static Pattern genericDatePattern = Pattern.compile("^(\\d{2}|\\d{4}|\\w{3})(-|\\s)(\\w{3}|\\d{1,2})(-|\\s)(\\d{1,2}|\\d{4})(T|\\s){0,1}(\\d{2}:\\d{2}){0,1}(:\\d{2}){0,1}(\\.\\d{3}){0,1}(Z){0,1}$");
    private static List<String> monthAbbrevs = Arrays.asList("JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                                                             "JUL", "AUG", "SEP", "OCT", "NOV", "DEC");

    public static int SOLR_DATE_FORMAT       = 0;
    public static int SHORT_DATE_FORMAT      = 1;
    public static int MONTH_DATE_FORMAT      = 2;
    public static int DAY_DATE_FORMAT        = 3;
    public static int FULL_MONTH_DATE_FORMAT = 4;
    public static int LONG_DATE_FORMAT       = 5;

    public static HashMap<Integer, SimpleDateFormat> formatMap = new HashMap<Integer, SimpleDateFormat>() {{
        put(SHORT_DATE_FORMAT,      new SimpleDateFormat("MM-dd-yyyy HH:mm z", Locale.US));
        put(MONTH_DATE_FORMAT,      new SimpleDateFormat("MMM yyyy"));
        put(DAY_DATE_FORMAT,        new SimpleDateFormat("dd-MMM-yy"));
        put(FULL_MONTH_DATE_FORMAT, new SimpleDateFormat("MMM-dd-yyyy"));
        put(LONG_DATE_FORMAT,       new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy"));
    }};

    private static final Logger logger = Logger.getLogger(DateUtils.class);

    private static boolean isMonthAbbrev(String s) {
        return monthAbbrevs.contains(s.toUpperCase());
    }

    private static String getDateGapString(Long ms) {
        long sec = ms/1000, min = sec/60, hrs = min/60, days = hrs/24, months = days/30, years = months/12;

        if (years != 0)  return "+" + years + "YEARS";
        if (months != 0) return "+" + months + "MONTHS";
        if (days != 0)   return "+" + days + "DAYS";
        if (hrs != 0)    return "+" + hrs + "HOURS";
        if (min != 0)    return "+" + min + "MINUTES";
        if (sec != 0)    return "+" + sec + "SECONDS";
        return "+" + ms + "MILLISECONDS";
    }

    public static String getDateGapString(Date startDate, Date endDate, int buckets) {
        if (startDate == null || endDate == null) return null;
        return getDateGapString((endDate.getTime() - startDate.getTime())/buckets);
    }

    /* Possible date combinations                           Possible time additions
       MM-dd-yyyy           : 08-01-2012                    HH:mm:ss             : 00:00:00
       dd-MMM-yy            : 01-Aug-12                     'T'HH:mm:ss.SSS      : T00:00:00.000
       dd-MMM-yyyy          : 01-Aug-2012                   'T'HH:mm:ss'Z'       : T00:00:00Z
       yyyy-MM-dd           : 2012-08-01                    'T'HH:mm:ss.SSS'Z'   : T00:00:00.000Z
     */
    public static Date getDateFromDateString(String dateString) {
        if (Utils.nullOrEmpty(dateString)) {
            return null;
        }

        Matcher m = genericDatePattern.matcher(dateString);
        if (m.matches()) {
            boolean hasTime = m.group(6) != null && m.group(7) != null;
            SimpleDateFormat format = getSimpleDateFormat(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5),
                                                          tFormatString(hasTime, m.group(6)),
                                                          secFormatString(m.group(8)), msFormatString(m.group(9)),
                                                          zFormatString(m.group(10)));
            return formatDateString(format, dateString);
        }

        return checkAllDateFormats(dateString);
    }

    private static String yearFormatString(int n)   { return StringUtils.repeat("y", n); }
    private static String monthFormatString(int n)  { return StringUtils.repeat("M", n); }
    private static String dayFormatString(int n)    { return StringUtils.repeat("d", n); }
    private static String hrMinFormatString()       { return "hh:mm"; }
    private static String secFormatString(String s) { return s == null ? "" : ":ss"; }
    private static String msFormatString(String s)  { return s == null ? "" : ".SSS"; }
    private static String zFormatString(String s)   { return s == null ? "" : "'Z'"; }
    private static String tFormatString(boolean hasTime, String s) {
        return hasTime ? s.replaceAll("T", "'T'") : null;
    }

    private static SimpleDateFormat getSimpleDateFormat(String f1, String div1, String f2, String div2, String f3, String t,
                                              String secStr, String msStr, String z) {
        String format;
        boolean f1isYear  = f1.length() == 4, f2isMonth = isMonthAbbrev(f2);

        if (f1isYear) {             // valid: 2012-08-01, 2012-8-1, 2012-Aug-1, 2012-Aug-01
            format = yearFormatString(4) + div1 + monthFormatString(f2.length()) + div2 + dayFormatString(f3.length());
        } else if (f2isMonth) {     // valid: 01-JUN-2012, 01-JUN-12, 1-JUN-12
            format = dayFormatString(f1.length()) + div1 + monthFormatString(f2.length()) + div2 + yearFormatString(f3.length());
        } else {                    // valid: JUN-1-2012, JUN-1-12, 06-10-2012, 6-1-2012
            format = monthFormatString(f1.length()) + div1 + dayFormatString(f2.length()) + div2 + yearFormatString(f3.length());
        }

        if (t != null) {
            format += t + hrMinFormatString() + secStr + msStr + z;
        }

        return new SimpleDateFormat(format);
    }

    public static Date checkAllDateFormats(String dateString) {
        for(SimpleDateFormat sdf : formatMap.values()) {
            Date d = formatDateString(sdf, dateString);
            if (d != null) {
                return d;
            }
        }
        return null;
    }

    public static Date formatDateString(SimpleDateFormat sdf, String dateString) {
        try {
            return sdf.parse(dateString);
        } catch (ParseException e) {
            return null;
        }
    }

    public static Date addGapToDate(Date d, String gap) {
        DateMathParser p = new DateMathParser(UTC, Locale.US);
        p.setNow(d);
        try {
            return p.parseMath(gap);
        } catch (ParseException e) {
            return null;
        }
    }

    public static String getFormattedDateString(String dateString, int format) {
        return getFormattedDateString(getDateFromDateString(dateString), format);
    }

    public static String getSolrDate(String dateString) {
        return getFormattedDateString(dateString, SOLR_DATE_FORMAT);
    }

    public static String getFormattedDateString(Date d, int format) {
        if (d == null) {
            return null;
        }

        return formatMap.containsKey(format) ? formatMap.get(format).format(d) : DateField.formatExternal(d);
    }


}
