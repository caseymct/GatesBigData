var DATEPICK = {};
DATEPICK.ui = {};
DATEPICK.util = {};

(function() {
    var Connect         = YAHOO.util.Connect,
        Dom             = YAHOO.util.Dom;
    
    var dateBeginInputElName            = "date_begin",
        dateEndInputElName              = "date_end",
        dateConstraintTextElName        = "date_constraint_text",
        dateConstraintRangeTextElName   = "date_constraint_range",
        dateRangeTextCSS                = { 'font-size' : '12px', 'padding-top' : '3px' },
        dateTextInputSize               = 10,
        defaultDateSearchValue          = '*';

    var dateField = "",
        dateRangeUrl = "";

    DATEPICK.ui.init = function(names) {
        dateField    = names[UI.DATEPICK.DATE_FIELD_KEY];
        dateRangeUrl = names[UI.DATE_PICKER_URL_KEY];
        buildHTML(names[UI.DATEPICK.DATE_PICK_EL_NAME_KEY]);
    };

    function getDateConstraint(inputName, picker) {
        var input = Dom.get(inputName).value.trim(),
            match = input.match(/^([a-zA-Z]{3}|[0-9]{1,2})-([0-9]{1,2})-([0-9]{4})$/);

        if (input.match(/\*/) != null || (match == null && picker == false)) return defaultDateSearchValue;

        var dateString = match != null ? UI.date.getDateString(match[1], match[2], match[3]) :
                                         UI.date.getDateString(picker.month, picker.day, picker.year);

        return dateString != null ? dateString : defaultDateSearchValue;
    }

    function buildHTML(datePickElName) {
        var datePickDiv = Dom.get(datePickElName);

        var l = UI.addDomElementChild('label', datePickDiv, { "for" : dateBeginInputElName, innerHTML : "Constrain by "});
        UI.addDomElementChild('span', l, { id : dateConstraintTextElName });
        UI.addDomElementChild('input', datePickDiv, { type : 'text', size: dateTextInputSize, id : dateBeginInputElName, value: defaultDateSearchValue});
        UI.addDomElementChild('label', datePickDiv, { "for" : dateEndInputElName, innerHTML: " to "});
        UI.addDomElementChild('input', datePickDiv, { type : 'text', size: dateTextInputSize, id : dateEndInputElName, value: defaultDateSearchValue});
        UI.addDomElementChild('br', datePickDiv);
        var s = UI.addDomElementChild('span', datePickDiv, { innerHTML: "In this index, field " }, dateRangeTextCSS);
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
                if (end != null)   endDatePick.setSelectedDay({ day:end[2], month:end[1], year:end[3] });
            }
        });


        DATEPICK.util.getDateConstraintFilterQueryString = function() {
            var startDate = getDateConstraint(dateBeginInputElName, beginDatePick.getSelectedDay()),
                endDate   = getDateConstraint(dateEndInputElName, endDatePick.getSelectedDay()),
                startDateSpecified = (startDate != defaultDateSearchValue),
                endDateSpecified   = (endDate != defaultDateSearchValue);

            if (!startDateSpecified && !endDateSpecified) return '';
            return UI.FACET_FIELDS_DELIMITER_KEY + dateField + UI.FACET_VALUES_DELIMITER_KEY +
                   startDate + UI.FACET_DATE_RANGE_DELIMITER_KEY + endDate;
        };
    }


})();
