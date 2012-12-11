package LucidWorksApp.utils;

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
    private static Pattern dateStringNoMilliseconds = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z");
    private static Pattern shortDatePattern = Pattern.compile("\\d{2}-\\d{2}-\\d{4} \\d{2}:\\d{2} [A-Z]{3}");
    private static Pattern shortDate2Pattern = Pattern.compile("\\d{2}-[A-Z]{3}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
    private static Pattern solrDateString = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z");
    private static Pattern onlyDatePattern = Pattern.compile("^(\\d{4})-(\\d{2})-(\\d{2})$");

    private static SimpleDateFormat shortDateFormat          = new SimpleDateFormat("MM-dd-yyyy HH:mm z", Locale.US);
    private static SimpleDateFormat shortDateFormat2         = new SimpleDateFormat("dd-MMM-yy HH:mm:ss", Locale.US);
    private static SimpleDateFormat solrDateFormatNoTimeZone = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
    private static SimpleDateFormat solrDateFormat           = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
    private static SimpleDateFormat longDateFormat           = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
    private static List<SimpleDateFormat> formatOptions      = Arrays.asList(shortDateFormat, shortDateFormat2, longDateFormat,
                                                                             solrDateFormat, solrDateFormatNoTimeZone);

    public static String SOLR_DATE  = "solrdate";
    public static String SHORT_DATE = "shortdate";
    public static String LONG_DATE  = "longdate";

    private static final Logger logger = Logger.getLogger(DateUtils.class);

    public static String getDateGapString(Long ms) {
        Long sec = ms/1000;
        Long min = sec/60;
        Long hrs = min/60;
        Long days = hrs/24;
        Long months = days/30;
        Long years = months/12;

        if (years != 0) {
            return "+" + years + "YEARS";
        } else if (months != 0) {
            return "+" + months + "MONTHS";
        } else if (days != 0) {
            return "+" + days + "DAYS";
        } else if (hrs != 0) {
            return "+" + hrs + "HOURS";
        } else if (min != 0) {
            return "+" + min + "MINUTES";
        } else if (sec != 0) {
            return "+" + sec + "SECONDS";
        }
        return "+" + ms + "MILLISECONDS";
    }

    private static String addDateMillisecondsIfMissing(String date) {
        Matcher m = dateStringNoMilliseconds.matcher(date);
        if (m.matches()) {
            date = date.replaceAll("(\\d{2})Z", "$1\\.000Z");
        }
        return date;
    }

    public static String formatToSolr(Date date) {
        return solrDateFormat.format(date);
    }

    public static Date solrDateMath(String solrDate, String gap) {
        solrDateFormatNoTimeZone.setTimeZone(UTC);

        try {
            solrDate = addDateMillisecondsIfMissing(solrDate);
            p.setNow(solrDateFormatNoTimeZone.parse(solrDate));
            return p.parseMath(gap);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }
        return null;
    }

    private static int getCalendarMonth(int month) {
        switch(month) {
            case 1: return Calendar.JANUARY; 
            case 2: return Calendar.FEBRUARY;
            case 3: return Calendar.MARCH; 
            case 4: return Calendar.APRIL; 
            case 5: return Calendar.MAY; 
            case 6: return Calendar.JUNE; 
            case 7: return Calendar.JULY; 
            case 8: return Calendar.AUGUST; 
            case 9: return Calendar.SEPTEMBER; 
            case 10: return Calendar.OCTOBER; 
            case 11: return Calendar.NOVEMBER; 
            case 12: return Calendar.DECEMBER; 
        }
        return 0;
    }
    public static String getSolrDate(String dateString) {
        if (!Utils.stringIsNullOrEmpty(dateString)) {
            dateString = addDateMillisecondsIfMissing(dateString);

            // If it's already a solr date string, just return
            Matcher m = solrDateString.matcher(dateString);
            if (m.matches()) {
                return dateString;
            }

            // If it's in the format yyyy-mm-dd
            m = onlyDatePattern.matcher(dateString);
            if (m.matches()) {
                Calendar calendar = Calendar.getInstance();
                calendar.set(Integer.parseInt(m.group(1)), getCalendarMonth(Integer.parseInt(m.group(2))),
                        Integer.parseInt(m.group(3)), 0, 0, 0);
                return DateField.formatExternal(calendar.getTime());
            }

            for(SimpleDateFormat format : formatOptions) {
                try {
                    Date date = format.parse(dateString);
                    return DateField.formatExternal(date);
                } catch (ParseException e) {
                    //logger.error(e.getMessage());
                }
            }
        }
        return dateString;
    }

    public static String getFormattedDateStringFromSolrDate(String solrDate, String format) {
        if (format.equals(DateUtils.SOLR_DATE)) {
            return solrDate;
        }
        if (format.equals(DateUtils.SHORT_DATE)) {
            return getShortDateStringFromSolrDate(solrDate);
        }
        if (format.equals(DateUtils.LONG_DATE)) {
            return getDateStringFromSolrDate(solrDate);
        }
        return solrDate;
    }

    public static Date getDateFromSolrDate(String solrDate) {
        try {
            return DateField.parseDate(solrDate);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }
        return null;
    }

    public static String getDateStringFromSolrDate(String solrDate) {
        try {
            return DateField.parseDate(solrDate).toString();
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }
        return null;
    }

    public static String getShortDateStringFromSolrDate(String solrDate) {
        try {
            return shortDateFormat.format(DateField.parseDate(solrDate));
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }
        return null;
    }

    /*public static String getSolrDateFromDateString(String dateString) {
        try {
            return DateField.formatExternal(new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy").parse(dateString));
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }
        return null;
    }  */

}
