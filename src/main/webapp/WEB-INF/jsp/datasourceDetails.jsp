<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/datasource.css"/>" />

    <h2 id = "datasourceName">Datasource details</h2>
    <div class="buttons">
        <a href="#" class="button small" id="delete">Delete</a>
        <a href="#" class="button small" id="empty">Empty</a>
    </div>

    <div style="clear:both"></div>

    <div id="results_table"></div>

    <script type="text/javascript">
        (function() {
            var Dom     = YAHOO.util.Dom,
                    Event   = YAHOO.util.Event,
                    Connect = YAHOO.util.Connect,
                    Json    = YAHOO.lang.JSON,
                    Dialog  = YAHOO.widget.SimpleDialog;

            var url = window.location.href.split('/');
            var datasourceId = url[url.indexOf("datasource") + 1];
            var collectionName = url[url.indexOf("collection") + 1];
            var urlParams = "?collection=" + collectionName + "&datasourceId=" + datasourceId;

            var handleYes = function() {
                Connect.asyncRequest('DELETE', '<c:url value="/datasource/delete" />' + urlParams, {
                    success: function (o) {
                        if (o.responseText == "") {
                            window.location = '<c:url value="/collection/" />' + collectionName;
                        } else {
                            LWA.ui.alertErrors(o);
                        }
                    },
                    failure: function (e) {
                        alert("Could not delete");
                    }
                });
                this.hide();
            };

            LWA.ui.confirmDelete.cfg.queueProperty("buttons", [
                { text: "Yes", handler: handleYes },
                { text: "Cancel", handler: LWA.ui.confirmDeleteHandleNo, isDefault:true}
            ]);

            LWA.ui.confirmDelete.render(Dom.get("content"));

            var buildTable = function(result) {
                var keys = Object.keys(result);
                var containerDiv, childDiv, resultsTable = Dom.get("results_table");

                for(var i = 0; i < keys.length; i++) {
                    containerDiv = LWA.ui.createDomElement("div", resultsTable, [ { key : "class", value : "datasource_results_div" }]);
                    childDiv = LWA.ui.createDomElement("div", containerDiv, [ { key : "class", value : "datasource_child_div" },
                        { key: "text", value: "<b>" + keys[i] + "</b>: " + result[keys[i]] }]);

                    if (i < keys.length) {
                        childDiv = LWA.ui.createDomElement("div", containerDiv, [ { key : "class", value : "datasource_child_div" },
                            { key: "text", value: "<b>" + keys[++i] + "</b>: " + result[keys[i]] }]);
                    }

                    LWA.ui.createDomElement("div", containerDiv, [ { key: "style", value : "clear:both" } ]);
                }
            };


            Connect.asyncRequest('GET', '<c:url value="/datasource/datasourcedetails" />' + urlParams, {
                success: function (o) {
                    var result = Json.parse(o.responseText);
                    if (result.hasOwnProperty("errors")) {
                        var errmsg = "Error message : " + result.errors[0].message + "\n" +
                                "Error key : " + result.errors[0].key + "\n" +
                                "Error code : " + result.errors[0].code;
                        alert(errmsg);
                    } else {

                        buildTable(result);
                    }
                },
                failure: function (e) {
                    alert("Could not get collection details");
                    console.log(e);
                }
            });

            Event.addListener("delete", "click", function(e) {
                Event.stopEvent(e);
                confirmDelete.show();
            });

        })();
    </script>
</layout:main>