<!--< %@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
< %@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
< %@ taglib uri="http://java.sun.com/portlet" prefix="portlet"%>
< %@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
-->
<%@ page session="false" contentType="text/html" pageEncoding="ISO-8859-1" import="java.util.*,javax.portlet.*" %>

<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib prefix="spring" uri="http://www.springframework.org/tags" %>
<%@ taglib prefix="form" uri="http://www.springframework.org/tags/form" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/export.css"/>" />

    <h1 id="search_header">Export Search Results</h1>
    <div id="search_info">
        <div id="core_info_container">
            <div class="info_label">Core:</div>
            <div class="info_value" id="core_info"></div>
        </div>
        <div id="query_info_container">
            <div class="info_label">Query:</div>
            <div class="info_value" id="query_info"></div>
        </div>
        <div id="fq_info_container">
            <div class="info_label">Filter query:</div>
            <div class="info_value" id="fq_info"></div>
        </div>
        <div id="sort_info_container">
            <div class="info_label">Sort by:</div>
            <div class="info_value" id="sort_info"></div>
        </div>
        <div id="numfound_info_container">
            <div class="info_label"># Results:</div>
            <div class="info_value" id="numfound_info"></div>
        </div>
        <div class="clearboth"></div>
    </div>

    <c:url value="/export" var="postURL" />

    <!--<form :form commandName="command" action="${postURL}" method="POST">-->

        <div class="buttons">
            <a href="#" class="button small" id="checkall">Check all</a>
            <a href="#" class="button small" id="checknone">Check none</a>
        </div>

        <div class="clearboth"></div>

        <div id="export_options">
        </div>

        <div id="export_file_div">
            <label for= "export_file_name">Exported file name: </label>
            <input id = "export_file_name" type="text" value="export.csv"/>
        </div>
        <div class="buttons">
            <a href="#" class="button small" id="export">Export</a>
        </div>
        <div class = "row"></div>
    <!--</form :form>-->

    <script type="text/javascript">
        (function() {
            var Connect = YAHOO.util.Connect,   Json = YAHOO.lang.JSON,
                Dom = YAHOO.util.Dom,           Overlay = YAHOO.widget.Overlay,
                Event = YAHOO.util.Event;

            // TODO: put query information on page
            var coreName = YAHOO.deconcept.util.getRequestParameter("core");
            var query    = YAHOO.deconcept.util.getRequestParameter("query");
            var fq       = YAHOO.deconcept.util.getRequestParameter("fq");
            var sort     = YAHOO.deconcept.util.getRequestParameter("sort");
            var order    = YAHOO.deconcept.util.getRequestParameter("order");
            var numFound = YAHOO.deconcept.util.getRequestParameter("numfound");

            Dom.get("query_info").innerHTML    = "<b>" + query + "</b>";
            Dom.get("sort_info").innerHTML     = "<b>" + sort + ", " + order + "</b>";
            Dom.get("core_info").innerHTML     = "<b>" + coreName + "</b>";
            Dom.get("fq_info").innerHTML       = "<b>" + decodeURIComponent(fq) + "</b>";
            Dom.get("numfound_info").innerHTML = "<b>" + numFound + "</b>";

            var urlStr = window.location.href.substring(window.location.href.indexOf("?") + 1);

            var checkboxParentNode = Dom.get("export_options");
            var checkboxGrpName = "export_fields";

            var skipName = function(name) {
                return name.match(/^_|suggest|^HDFS|digest|boost|host|segment|preview|Suggest$|Prefix$/) != null;
            };

            Connect.asyncRequest('GET', '<c:url value="/core/fieldnames" />' + "?core=" + coreName, {
                success : function(o) {
                    var names = Json.parse(o.responseText).names, i;
                    var currOverlay, currCheckboxParent;

                    for(i = 0; i < names.length; i++) {
                        if (skipName(names[i])) continue;

                        var namesArr = names[i].split("."), parent = namesArr[0];
                        var child = (namesArr.length > 1) ? namesArr[1] : parent;

                        if (names[i].indexOf(".") > -1) {
                            var overlayEl = "overlay_" + parent;
                            var overlayLink = overlayEl + "_expand";
                            var overlayInnerEl = overlayEl + "_child";

                            if (Dom.get(overlayEl) == null) {
                                var outerDiv = UI.addDomElementChild('div', checkboxParentNode, { id : overlayEl }, { class : "overlay_el" });

                                UI.addDomElementChild('a', outerDiv, { id: overlayLink},
                                        { "margin-right" : "2px", float : "left", class: "button show_overlay"});
                                UI.addDomElementChild('div', outerDiv, { innerHTML: parent });

                                var div = UI.addDomElementChild('div', outerDiv, { id: parent }, {class: "overlay_div" });

                                currCheckboxParent = UI.addDomElementChild('div', div, {id: overlayInnerEl }, {class: "overlay_inner_el" });

                                currOverlay = new Overlay(parent, {
                                    context: [overlayEl, "tl","bl", ["beforeShow", "windowResize"]],
                                    visible: false
                                });

                                currOverlay.render(checkboxParentNode);

                                Event.addListener(overlayEl, "click", function(e) {
                                    var l = "overlay_" + this.id + "_expand";
                                    if (Dom.hasClass(l, "button show_overlay")) {
                                        Dom.removeClass(l, "button show_overlay");
                                        Dom.addClass(l, "button collapse");
                                        this.show();
                                    } else {
                                        Dom.addClass(l, "button show_overlay");
                                        Dom.removeClass(l, "button collapse");
                                        this.hide();
                                    }
                                }, currOverlay, true);
                            }
                        } else {
                            currCheckboxParent = checkboxParentNode;
                        }

                        var d = UI.addDomElementChild('div', currCheckboxParent, null, { class: "overlay_el" });

                        var c = UI.addDomElementChild('input', d, {type: "checkbox", name: checkboxGrpName, value: i, id: names[i]});
                        if (child.match(/^[A-Z]/) != null) c.checked = true;

                        UI.addDomElementChild('label', d, { htmlFor: names[i], innerHTML : child }, {padding: "5px"});
                        UI.addDomElementChild('br', d);
                    }

                    UI.addDomElementChild('div', checkboxParentNode, null, { clear: "both" });
                }
            });

            var changeAllCheckedStatus = function(status) {
                var checkboxes = document.getElementsByName(checkboxGrpName);
                for (var i = 0; i < checkboxes.length; i++) {
                    checkboxes[i].checked = status;
                }
            };

            var getAllCheckedFields = function() {
                var checked = [], unchecked = [], checkboxes = document.getElementsByName(checkboxGrpName);

                for (var i = 0; i < checkboxes.length; i++) {
                    if (checkboxes[i].checked) {
                        checked.push(checkboxes[i].value);
                    } else {
                        unchecked.push(checkboxes[i].value);
                    }
                }

                return (checked.length > unchecked.length) ? "-" + unchecked.join(",") : "%2B" + checked.join(",");
            };

            Event.addListener("checkall", "click", function(e) {
                Event.stopEvent(e);
                changeAllCheckedStatus(true);
            });

            Event.addListener("checknone", "click", function(e) {
                Event.stopEvent(e);
                changeAllCheckedStatus(false);
            });

            Event.addListener("export", "click", function(e) {
                Event.stopEvent(e);

                var urlParams = "?type=csv&file=" + Dom.get("export_file_name").value + "&" + urlStr;
                var exportFields = getAllCheckedFields();
                if (exportFields != "") {
                    urlParams += "&fields=" + exportFields;
                }

                window.open('<c:url value="/export" />' + urlParams);
                window.focus();
            })
        })();
    </script>
</layout:main>