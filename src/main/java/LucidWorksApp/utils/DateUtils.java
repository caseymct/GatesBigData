package LucidWorksApp.utils;

import org.apache.solr.schema.DateField;
import org.apache.solr.util.DateMathParser;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class DateUtils {

    private static TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static DateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
    private static DateMathParser p = new DateMathParser(UTC, Locale.US);

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


    public static Date solrDateMath(String solrDate, String gap) {
        parser.setTimeZone(UTC);

        try {
            p.setNow(parser.parse(solrDate));
            return p.parseMath(gap);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }
        return null;
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

    public static String getSolrDateFromDateString(String dateString) {
        try {
            return DateField.formatExternal(new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy").parse(dateString));
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }
        return null;
    }

}
