<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page pageEncoding="UTF-8" %>
<%@ page contentType="text/html" %>

<layout:main>

    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/reports.css"/>" />

    <div id = "info"></div>

    <script type="text/javascript">
    (function() {
        /* Variables */
        var Dom          = YAHOO.util.Dom,            Button          = YAHOO.widget.Button,
            Connect      = YAHOO.util.Connect,        Event           = YAHOO.util.Event,
            AutoComplete = YAHOO.widget.AutoComplete, LocalDataSource = YAHOO.util.LocalDataSource,
            Json         = YAHOO.lang.JSON;

        var allRequestParams       = UI.util.getRequestParameters();
            allRequestParams[UI.util.REQUEST_COLLECTION_KEY] = allRequestParams[UI.util.REQUEST_CORE_KEY];
        var infoFieldRequestParams = [UI.util.REQUEST_COLLECTION_KEY],
            infoFieldRequestString = UI.util.constructRequestString(allRequestParams, infoFieldRequestParams);

        var href                = window.location.href,
            urlRequestStr       = href.substring(href.indexOf("?")),
            baseUrl             = '<c:url value="/" />',
            infoFieldsUrl       = baseUrl + "report/titles" + infoFieldRequestString,
            generateReportUrl   = baseUrl + "search/reports/generate" + urlRequestStr;

        var infoElId    = 'info',       infoEl = Dom.get(infoElId),
            buttonsElId = 'buttons';

        UI.buildInfoHtml(infoEl, allRequestParams);

        Connect.asyncRequest('GET', infoFieldsUrl, {
            success: function(o) {
                var reportNames = Json.parse(o.responseText);
                var reportData = [];
                for(var i = 0; i < reportNames.length; i++) {
                    reportData.push({ buttonElId : 'report' + i, name : reportNames[i]});
                }

                buildHtml(reportData);
            }
        });

        function buildHtml(reportData) {
            UI.insertDomElementAfter('div', infoEl, null, { 'clear' : 'both'});
            var el = UI.insertDomElementAfter('div', infoEl, { id : buttonsElId });

            for(var i = 0; i < reportData.length; i++) {
                var elId = reportData[i].buttonElId;
                var d = UI.addDomElementChild('div', el);
                UI.addDomElementChild('a', d, { id: elId, innerHTML: reportData[i].name, href: '#'}, {'class':'button small'});

                Event.addListener(elId, "click", function (e) {
                    Event.stopEvent(e);
                    window.open(generateReportUrl + "&" + UI.util.REQUEST_TITLE_KEY + "=" + this.innerHTML, "_self");
                });
            }
        }
    })();
    </script>
    
</layout:main>
