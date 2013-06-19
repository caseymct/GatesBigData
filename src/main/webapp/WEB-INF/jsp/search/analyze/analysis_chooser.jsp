<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page pageEncoding="UTF-8" %>
<%@ page contentType="text/html" %>

<layout:main>

    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/analysis_chooser.css"/>" />

    <div id = "info"></div>
    <div class="clearboth"></div>

    <script type="text/javascript">
    (function() {
        /* Variables */
        var Dom          = YAHOO.util.Dom,            Button          = YAHOO.widget.Button,
            Connect      = YAHOO.util.Connect,        Event           = YAHOO.util.Event,
            AutoComplete = YAHOO.widget.AutoComplete, LocalDataSource = YAHOO.util.LocalDataSource,
            Json         = YAHOO.lang.JSON;

        var infoElId             = 'info',              wordTreeElId              = 'wordtree',
            wordTreeSelectId     = 'wordtree_select',   wordTreeSelectContainerId = 'wordtree_select_container',
            xAxisInputElId       = 'x_axis_field',      xAxisACElId               = 'x_axis_autocomplete',
            xAxisToggleElId      = 'x_axis_toggle',     xAxisContainerElId        = 'x_axis_container',
            yAxisInputElId       = 'y_axis_field',      yAxisACElId               = 'y_axis_autocomplete',
            yAxisToggleElId      = 'y_axis_toggle',     yAxisContainerElId        = 'y_axis_container',
            seriesInputElId      = 'series_field',      seriesACElId              = 'series_autocomplete',
            seriesToggleElId     = 'series_toggle',     seriesContainerElId       = 'series_container',
            plotOptionsDivElId   = 'plot_options',      useDateRangeLimitElId     = 'use_date_range_limit',
            maxPlotPointsElId    = 'max_plot_points',   useMaxPlotPointsElId      = 'use_max_plot_points',
            infoEl               = Dom.get(infoElId),   noMaxPlotPointsElId       = 'no_max_plot_points',
            plotElId             = 'plot',              dateRangeLimitSelectElId  = 'date_range_limit_select',
            maxPlotPoints        = 5000,                dateRangeLimitNElId       = 'date_range_limit_n',
            warnDialogElName     = 'warn_dialog',       warnDialogCSSClass        = 'yui-pe-content',
            toggleCSSClass       = 'toggle',            toggleLabelCSSClass       = 'toggle_label';

        var wordTreeSelected = null,                warnDlg                   = null,
            xAxisSelected    = {},                  yAxisSelected             = {},
            seriesSelected   = {};

        var requestParams = UI.util.getRequestParameters(),
            plotRequestParams = [UI.util.REQUEST_CORE_KEY, UI.util.REQUEST_QUERY_KEY, UI.util.REQUEST_FQ_KEY, UI.util.REQUEST_NUM_FOUND_KEY];

        var href                = window.location.href,
            urlRequestStr       = href.substring(href.indexOf("?")),
            baseUrl             = '<c:url value="/" />',
            infoFieldsUrl       = baseUrl + "search/infofields",
            wordTreeUrl         = baseUrl + "search/analyze/wordtree" + urlRequestStr,
            plotUrl             = baseUrl + "search/analyze/plot" + UI.util.constructRequestString(requestParams, plotRequestParams);

        /* Code to execute */
        UI.buildInfoHtml(infoEl, requestParams);
        buildWordTreeHTML();
        createSelect(wordTreeSelectContainerId, wordTreeSelectId);

        if (requestParams[UI.STRUCTURED_DATA_EL_ID_KEY] == "true") {
            buildPlotSelectHTML();
            initWarnDlg();

            createAutoComplete(UI.INFO_FIELDS_X_AXIS_FIELDS_KEY,  xAxisInputElId,  xAxisContainerElId,  xAxisToggleElId);
            createAutoComplete(UI.INFO_FIELDS_Y_AXIS_FIELDS_KEY,  yAxisInputElId,  yAxisContainerElId,  yAxisToggleElId);
            createAutoComplete(UI.INFO_FIELDS_SERIES_FIELDS_KEY, seriesInputElId, seriesContainerElId, seriesToggleElId);
        }

        /* Helper functions */
        function initAutoComplete(data, acInputElName, acContainerElName, acToggleElId) {
            var acConfigs = {
                prehighlightClassName: "yui-ac-prehighlight",
                useShadow: true,            queryDelay: 0,
                minQueryLength: 0,          animVert: .01,
                maxResultsDisplayed: 50
            };

            var ds = new LocalDataSource(data);
            var ac = new AutoComplete(acInputElName, acContainerElName, ds, acConfigs);

            var toggleEl = Dom.get(acToggleElId);
            var button = new Button(toggleEl);

            var toggle = function(e) {
                if (!Dom.hasClass(toggleEl, "open")) Dom.addClass(toggleEl, "open");

                if (ac.isContainerOpen()) {
                    ac.collapseContainer();
                } else {
                    ac.getInputEl().focus(); // Needed to keep widget active
                    setTimeout(function() { // For IE
                        ac.sendQuery("");
                    }, 0);
                }
            };
            button.on("click", toggle);
            ac.containerCollapseEvent.subscribe(function(){ Dom.removeClass(toggleEl, "open")});
            return ac;
        }

        function adjustContentHeight(nFields) {
            var newH = nFields * 25;
            var h = UI.util.getNPixels(Dom.getStyle(UI.CONTENT_EL_NAME, "height"));
            if (newH > h) {
                Dom.setStyle(Dom.get(UI.CONTENT_EL_NAME), "height", newH + "px");
            }
        }

        function createAutoComplete(field, acInputElName, acContainerElName, acToggleElId) {
            var countsKey = (field == UI.INFO_FIELDS_SERIES_FIELDS_KEY) ? 'separatefieldcounts' : 'nonnullcounts';
            var url = infoFieldsUrl + UI.util.constructRequestString(requestParams,
                    [UI.util.REQUEST_CORE_KEY, UI.util.REQUEST_QUERY_KEY, UI.util.REQUEST_FQ_KEY],
                    [ { key : UI.util.REQUEST_INFO_FIELD_KEY, value : field }, { key : countsKey, value : true }]);

            Connect.asyncRequest('GET', url, {
                success: function(o) {
                    var response = Json.parse(o.responseText),
                        fields   = response[field];
                    adjustContentHeight(fields.length);

                    return initAutoComplete(response[field], acInputElName, acContainerElName, acToggleElId);
                }
            });
        }

        function createSelect(selectContainerId, selectId) {
            var onMenuRender = function (type, args, button) {
                button.set("selectedMenuItem", this.getItem(0));
            };

            var onSelectedMenuItemChange = function (e) {
                wordTreeSelected = e.newValue.cfg.getProperty("text");
                this.set("label", ("<em class=\"yui-button-label\">" + wordTreeSelected + "</em>"));
            };

            var url = infoFieldsUrl + UI.util.constructRequestString(requestParams, [UI.util.REQUEST_CORE_KEY],
                        [{ key : UI.util.REQUEST_INFO_FIELD_KEY, value : UI.INFO_FIELDS_WORD_TREE_FIELDS_KEY }]);
            Connect.asyncRequest('GET', url, {
                success: function(o) {
                    var response = Json.parse(o.responseText);
                    var fields = response[UI.INFO_FIELDS_WORD_TREE_FIELDS_KEY];
                    var data = [];
                    for(var i = 0; i < fields.length; i++) {
                        data.push({ text: fields[i], value: fields[i] });
                    }

                    var select = new Button({
                        id : selectContainerId,         name : selectContainerId,
                        type      : "menu",     lazyloadmenu : false,
                        container : selectId,           menu : data });

                    select.on("selectedMenuItemChange", onSelectedMenuItemChange);
                    select.getMenu().subscribe("render", onMenuRender, select);

                    return select;
                }
            });
        }

        function buildWordTreeHTML() {
            var div = UI.addDomElementChild('div', infoEl, {}, { "padding-top" : "20px"});
            var fieldset = UI.addDomElementChild('fieldset', div);
            UI.addDomElementChild('legend', fieldset, { innerHTML: "Word Tree"});
            div = UI.addDomElementChild('div', fieldset);
            UI.addDomElementChild('span', div,  { innerHTML : "Field: "}, { "vertical-align" : "4px" });
            UI.addDomElementChild('label', div, { id : wordTreeSelectId });
            UI.addClearBothDiv(div);
            UI.addDomElementChild('a', fieldset, { id: wordTreeElId, innerHTML: "Show word tree" }, { 'class' : 'button small' });

            Event.addListener(wordTreeElId, "click", function(o) {
                Event.stopEvent(o);
                window.open(wordTreeUrl + '&analysisfield=' + wordTreeSelected, '_blank');
            });
        }

        function buildACContainer(parentDiv, inputElId, axisACElId, toggleElId, containerElId, labelText) {

            var acDiv = UI.addDomElementChild('div', parentDiv, { id : axisACElId });
            UI.addDomElementChild('label', acDiv, { htmlFor : inputElId, innerHTML: labelText }, { 'class' : toggleLabelCSSClass });
            UI.addDomElementChild('input', acDiv, { id : inputElId,  type : "text"});
            UI.addDomElementChild('input', acDiv, { id : toggleElId, type : "button", name : toggleElId, value: "Show" }, { 'class' : toggleCSSClass });
            UI.addDomElementChild('div',   acDiv, { id : containerElId });
        }

        function populateDateSelect(sel) {
            var ops = ["DAYS", "MONTHS", "YEARS"];
            for(var i = 0; i < ops.length; i++) {
                sel.options[sel.options.length] = new Option(ops[i], ops[i]);
            }
            sel.selectedIndex = 1;
        }

        function buildPlotOptions(parentDiv) {
            var nfound = requestParams[UI.util.REQUEST_NUM_FOUND_KEY], largeDataSet = nfound > maxPlotPoints;

            var d = UI.addDomElementChild('div', parentDiv, {id : plotOptionsDivElId });
            var limitDiv = UI.addDomElementChild('div', d);
            UI.addDomElementChild('input', limitDiv, { id : useMaxPlotPointsElId, type : 'checkbox' });
            UI.addDomElementChild('label', limitDiv, { for: useMaxPlotPointsElId, innerHTML : 'Limit # of plotted points to: '});
            UI.addDomElementChild('input', limitDiv, { id : maxPlotPointsElId, type : 'text', value : maxPlotPoints });

            var s = 'Plot all ' + nfound + ' points' + (largeDataSet ? ' (not recommended)' : ' ');

            var nolimitDiv = UI.addDomElementChild('div', d);
            UI.addDomElementChild('input', nolimitDiv, { id : noMaxPlotPointsElId, type : 'checkbox' });
            UI.addDomElementChild('label', nolimitDiv, { for: noMaxPlotPointsElId, innerHTML : s}, { width : '400px'});

            if (largeDataSet) {
                Dom.get(useMaxPlotPointsElId).checked = true;
            } else {
                Dom.get(noMaxPlotPointsElId).checked = true;
            }

            var dateLimitDiv = UI.addDomElementChild('div', d);
            UI.addDomElementChild('input', dateLimitDiv, { id : useDateRangeLimitElId, type : 'checkbox' });
            UI.addDomElementChild('label', dateLimitDiv, { for: useDateRangeLimitElId, innerHTML : 'Limit dates to bucket size: '});
            UI.addDomElementChild('input', dateLimitDiv, { id : dateRangeLimitNElId, type : 'text', value : 1, size: 3});

            var sel = UI.addDomElementChild('select', dateLimitDiv, { id : dateRangeLimitSelectElId });
            populateDateSelect(sel);
        }

        function initWarnDlg() {
            warnDlg = UI.createSimpleDlg(warnDialogElName, "Are you sure?", "", openPlotWindow, "Yes");
            warnDlg.render(document.body);
        }

        function buildWarnDlgHTML(siblingDiv) {
            var div = UI.insertDomElementAfter('div', siblingDiv, {id : warnDialogElName}, { "class" : warnDialogCSSClass});
            UI.addDomElementChild('div', div, null, { "class" : "hd"});
            var bd = UI.addDomElementChild('div', div, null, { "class" : "bd"});
        }

        function addExplanatoryPlotHTML(fieldset) {

            UI.addDomElementChild('div', fieldset, { innerHTML :
                    "The X-Axis and Y-Axis fields are followed by the number of valid values they contain <i>for this search</i>.<br>" +
                    "The Series fields are followed by the number of different categories exist <i>for this search</i>. <br>E.g.<br><br>" +
                    "<span class='explain_span'><b>X Axis field</b> : ACCOUNT_DATE (420)</span>" +
                            "420 records have valid ACCOUNT_DATE values.<br>" +
                    "<span class='explain_span'><b>Y Axis field</b> : ITEM_COST (0)</span>" +
                            " A value of 0 means all ITEM_COST values for this search are undefined.<br>" +
                    "<span class='explain_span explain_span_last'><b>Series field</b> : CUSTOMER_NAME (10)</span>" +
                            " This search can be split into 10 different series per customer name."});
        }

        function buildPlotSelectHTML() {
            var div = UI.addDomElementChild('div', infoEl, {}, { "padding-top" : "10px"} );
            var fieldset = UI.addDomElementChild('fieldset', div);
            UI.addDomElementChild('legend', fieldset, { innerHTML: "Plot"});

            addExplanatoryPlotHTML(fieldset);

            div = UI.addDomElementChild('div', fieldset);

            buildACContainer(div, xAxisInputElId, xAxisACElId, xAxisToggleElId, xAxisContainerElId, "X axis field: ");
            buildACContainer(div, yAxisInputElId, yAxisACElId, yAxisToggleElId, yAxisContainerElId, "Y axis field: ");
            buildACContainer(div, seriesInputElId, seriesACElId, seriesToggleElId, seriesContainerElId, "Series field: ");
            buildWarnDlgHTML(div);

            UI.addClearBothDiv(div);
            buildPlotOptions(div);

            UI.addClearBothDiv(div);
            UI.addDomElementChild('a', fieldset, { id: plotElId, innerHTML: "Plot" }, { "class" :  "button small" });

            Event.addListener(plotElId, "click", function(o) {
                Event.stopEvent(o);

                xAxisSelected  = getInputValueAndCount(xAxisInputElId);
                yAxisSelected  = getInputValueAndCount(yAxisInputElId);
                seriesSelected = getInputValueAndCount(seriesInputElId);

                if (xAxisSelected.count == 0 || yAxisSelected.count == 0) {
                    var xAxisStr = "field " + xAxisSelected.name + " has " + xAxisSelected.count + " valid values",
                        yAxisStr = "field " + yAxisSelected.name + " has " + yAxisSelected.count + " valid values",
                        justXis0 = xAxisSelected.count == 0 && yAxisSelected.count != 0,
                        joinStr  = justXis0 || yAxisSelected.count == 0 && xAxisSelected.count != 0 ? ' BUT ' : ' AND ',
                        s = justXis0 ? [yAxisStr, xAxisStr] : [xAxisStr, yAxisStr];

                    warnDlg.setBody(s[0] + joinStr + s[1] + ". The plot will not render.");
                    warnDlg.show();
                } else {
                    openPlotWindow();
                }
            });
        }

        function openPlotWindow() {
            var plotParams = "&xaxis=" + xAxisSelected.name + "&yaxis=" + yAxisSelected.name +
                    (seriesSelected.name != null ? "&series=" + seriesSelected.name : "");
            window.open(plotUrl + plotParams, "_blank");
        }

        function getInputValueAndCount(el) {
            var input = Dom.get(el).value;
            var ret = { name : null, count : 0};

            if (input != "") {
                var match = input.match(/(.*?) \((\d+)\)/);
                if (match.length == 3) {
                    ret.name = match[1];
                    ret.count = match[2];
                }
            }
            return ret;
        }

    })();
    </script>
</layout:main>