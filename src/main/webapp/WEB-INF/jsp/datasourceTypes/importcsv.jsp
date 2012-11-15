<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/datasource.css"/>" />

    <h2 id = "collection_header"></h2>

    <div class = "row">
        <div id="csvimported_header">CSV files already imported to Solr:</div>
        <div id="csvimported">None</div>
    </div>

    <form id="importcsv_form">
        <div class="row" id="webdatasource_header">Import a CSV file to Solr:</div>

        <div id="webtab">
            <div class = "row">
                <label for="file"><span class = "red">*</span>File: </label>
                <input id="file" type = "text"/>
            </div>
            <div class = "row">
                <label for="is_on_server"><span class = "red">*</span>File is on server: </label>
                <input id="is_on_server" type = "checkbox" checked/>
            </div>
            <div class = "row">
                <label for="override_duplicate"><span class = "red">*</span>Override if file has already been imported: </label>
                <input id="override_duplicate" type = "checkbox"/>
            </div>
        </div>
        <div class="buttons">
            <a href="#" class="button small" id="import">Import</a>
        </div>
    </form>

    <script type="text/javascript">
        (function() {
            var Dom     = YAHOO.util.Dom,
                Event   = YAHOO.util.Event,
                Connect = YAHOO.util.Connect,
                Json    = YAHOO.lang.JSON;

            var csvFilesAlreadyImported = [];
            var url = window.location.href.split('/');
            var collectionName = url[url.indexOf("collection") + 1];
            Dom.get("collection_header").innerHTML = "Collection " + collectionName;

            Connect.asyncRequest('GET', '<c:url value="/field/csvimported" />' + "?collection=" + collectionName, {
                success: function (o) {
                    var result = o.responseText;
                    if (result != "") {
                        csvFilesAlreadyImported = result.split(";");
                    }

                    Dom.get("csvimported").innerHTML = csvFilesAlreadyImported.length == 0 ? "None" :
                            result.replace(/;/g, "<br>");
                },
                failure: function (e) {
                    alert("Could not connect to /field/importcsv to display imported files.");
                }
            });

            var checkFileExistsIfOverrideNotChecked = function(fileName) {
                fileName = (Dom.get("is_on_server").checked ? "server" : "localsystem") + ":" + fileName;

                for(var i = 0; i < csvFilesAlreadyImported.length; i++) {
                    if (csvFilesAlreadyImported[i] == fileName) {
                        return true;
                    }
                }
                return false;
            };

            Event.addListener("import", "click", function(e) {
                Event.stopEvent(e);

                var fileName = Dom.get("file").value;
                if (fileName == "") {
                    alert("Please specify a file name");
                    return;
                }

                if (!Dom.get("override_duplicate").checked) {
                    if (checkFileExistsIfOverrideNotChecked(fileName)) {
                        alert("This file " + fileName + " has already been imported. Check the override " +
                            "box if you want to import anyways.");
                        return;
                    }
                }

                var urlParams = "?file=" + fileName +
                                "&local=" + Dom.get("is_on_server").checked +
                                "&collection=" + collectionName;

                Connect.asyncRequest('GET', '<c:url value="/solr/update/csv" />' + urlParams, {
                    success: function (o) {
                        if (UI.util.checkXmlReturnValue(o)) {
                            window.location = '<c:url value="/collection/" />' + collectionName;
                        }
                    },
                    failure: function (e) {
                        alert("Could not connect to /solr/update/csv to import csv file.");
                    }
                });
            });
        })();
    </script>
</layout:main>
