var DATEPICK = {};
DATEPICK.ui = {};
DATEPICK.util = {};

(function() {
    var Connect         = YAHOO.util.Connect,
        Event           = YAHOO.util.Event,
        Dom             = YAHOO.util.Dom;
    
    var dateBeginInputElName            = "date_begin",
        dateEndInputElName              = "date_end",
        dateConstraintTextElName        = "date_constraint_text",
        dateConstraintRangeTextElName   = "date_constraint_range",
        dateRangeTextStyle              = "font-size:12px; padding-top:3px",
        dateTextInputSize               = 10;

    var monthAbbrev = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];

    var dateField = "",
        dateRangeUrl = "";

    DATEPICK.ui.initDatePickerVars = function(field, url) {
        dateField = field;
        dateRangeUrl = url;
    };

    function formatDateString(day, month, year) {
        return year + "-" + (((month < 10) ? "0" : "") + month) + "-" + (((day < 10) ? "0" : "") + day);
    }

    function getDateConstraint(inputName, datePicker) {
        if (datePicker == false) return "*";

        //first check the input
        var input = Dom.get(inputName).value.trim();
        if (input.match(/^[\s]*[\\*]*$/) != null) return "*";

        var datePickerRep = formatDateString(parseInt(datePicker.day), parseInt(datePicker.month), parseInt(datePicker.year));
        var match = input.match(/^([a-zA-Z]{3})-([0-9]{2})-([0-9]{4})$/);
        if (match != null) {
            var inputMonth = monthAbbrev.indexOf(match[1]) + 1;
            var inputDay = parseInt(match[2].replace(/^0/,""));
            var inputYear = parseInt(match[3]);
            if (inputMonth != parseInt(datePicker.month) || inputDay != parseInt(datePicker.day) || inputYear != datePicker.year) {
                return formatDateString(inputDay, inputMonth, inputYear);
            }
        }
        return datePickerRep;
    }

    function createJsDatePick(targetElName) {
        return new JsDatePick({
            useMode     : 2,
            dateFormat  : "%M-%d-%Y",
            imgPath     : "../static/images/jsdatepick/",
            target      : targetElName
        });
    }

    Event.onContentReady(dateConstraintRangeTextElName, function() {
        var beginDatePick = createJsDatePick(dateBeginInputElName),
              endDatePick = createJsDatePick(dateEndInputElName);

        Dom.get(dateConstraintTextElName).innerHTML = dateField;

        Connect.asyncRequest('GET', dateRangeUrl, {
            success : function(o) {
                var dateStr = o.responseText.split(" to "),
                      start = dateStr[0].match(/([0-9]+)-([0-9]+)-([0-9]+)/),
                        end = dateStr[1].match(/([0-9]+)-([0-9]+)-([0-9]+)/);

                Dom.get(dateConstraintRangeTextElName).innerHTML = dateField + " range is: " +
                    "<i>" + dateStr[0] + "</i> to <i>" + dateStr[1] + "</i>";

                beginDatePick.setSelectedDay({ day:start[2], month:start[1], year:start[3] });
                endDatePick.setSelectedDay({ day:end[2], month:end[1], year:end[3] });
            }
        });

        DATEPICK.util.getDateConstraintFilterQueryString = function() {
            var startDate = getDateConstraint(dateBeginInputElName, beginDatePick.getSelectedDay());
            var endDate   = getDateConstraint(dateEndInputElName, endDatePick.getSelectedDay());

            return (startDate == "*" && endDate == "*") ? "" :
                "%2B" + dateField + ":[" + startDate + " TO " + endDate + "] ";
        };
    });

    DATEPICK.ui.buildDatePickHTML = function(datePickEl) {
        var datePickDiv = Dom.get(datePickEl);
        datePickDiv.innerHTML = 
            "<label for='" + dateBeginInputElName + "'>Constrain by <span id='" + dateConstraintTextElName + "'></span>: </label>" +
            "<input type='text' size='" + dateTextInputSize + "' id='" + dateBeginInputElName + "' value='*'/>" +
            "<label for='" + dateEndInputElName + "'> to </label>" +
            "<input type='text' size='" + dateTextInputSize + "' id='" + dateEndInputElName + "' value='*'/>" +
            "<br>" +
            "<span style='" + dateRangeTextStyle + "'>In this index, field <span id = '" + dateConstraintRangeTextElName + "'></span></span>";
    };
})();
