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

    DATEPICK.ui.init = function(names) {
        dateField = names['dateField'];
        dateRangeUrl = names['dateRangeUrl'];
        buildHTML(names['datePickElName']);
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

    function buildHTML(datePickElName) {
        var datePickDiv = Dom.get(datePickElName);

        var l = UI.addDomElementChild('label', datePickDiv, { for: dateBeginInputElName, innerHTML : "Constrain by "});
        UI.addDomElementChild('span', l, { id : dateConstraintTextElName });
        UI.addDomElementChild('input', datePickDiv, { type : 'text', size: dateTextInputSize, id : dateBeginInputElName, value: '*'});
        UI.addDomElementChild('label', datePickDiv, { for : dateEndInputElName, innerHTML: " to "});
        UI.addDomElementChild('input', datePickDiv, { type : 'text', size: dateTextInputSize, id : dateEndInputElName, value: '*'});
        UI.addDomElementChild('br', datePickDiv);
        var s = UI.addDomElementChild('span', datePickDiv, { innerHTML: "In this index, field " }, { "font-size" : "12px", "padding-top" : "3px" });
        UI.addDomElementChild('span', s, { id : dateConstraintRangeTextElName });

        createDatePickers();
    }

    function createJsDatePick(targetElName) {
        return new JsDatePick({
            useMode     : 2,
            dateFormat  : "%M-%d-%Y",
            imgPath     : "../static/images/jsdatepick/",
            target      : targetElName
        });
    }

    function createDatePickers() {
        var beginDatePick = createJsDatePick(dateBeginInputElName),
              endDatePick = createJsDatePick(dateEndInputElName);

        Dom.get(dateConstraintTextElName).innerHTML = dateField + ": ";

        Connect.asyncRequest('GET', dateRangeUrl, {
            success : function(o) {
                var dateStr = o.responseText.split(" to "),
                      start = dateStr[0].match(/([0-9]+)-([0-9]+)-([0-9]+)/),
                        end = dateStr[1].match(/([0-9]+)-([0-9]+)-([0-9]+)/);

                Dom.get(dateConstraintRangeTextElName).innerHTML = dateField + " has range: " +
                    "<i>" + dateStr[0] + "</i> to <i>" + dateStr[1] + "</i>";

                if (start != null) beginDatePick.setSelectedDay({ day:start[2], month:start[1], year:start[3] });
                if (end != null) endDatePick.setSelectedDay({ day:end[2], month:end[1], year:end[3] });
            }
        });

        DATEPICK.util.getDateConstraintFilterQueryString = function() {
            var startDate = getDateConstraint(dateBeginInputElName, beginDatePick.getSelectedDay());
            var endDate   = getDateConstraint(dateEndInputElName, endDatePick.getSelectedDay());

            return (startDate == "*" && endDate == "*") ? "" :
                "%2B" + dateField + ":[" + startDate + " TO " + endDate + "] ";
        };
    }


})();
