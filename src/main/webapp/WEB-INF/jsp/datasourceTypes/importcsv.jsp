<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/datasource.css"/>" />

    <h2 id = "collection_header"></h2>

    <form id="importcsv_form">
        <div class="row" id="webdatasource_header">Import a CSV file to Solr:</div>

        <div id="webtab">
            <div class = "row">
                <label for="name"><span class = "red">*</span>Name: </label>
                <input id="name" type = "text"/>
            </div>
            <div class = "row">
                <label for="file"><span class = "red">*</span>File: </label>
                <input id="file" type = "text"/>
            </div>
            <div class = "row">
                <label for="is_local"><span class = "red">*</span>Local: </label>
                <input id="is_local" type = "checkbox" selected/>
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

            var url = window.location.href.split('/');
            var datasourceId = url[url.indexOf("datasource") + 1];
            var createNew = (datasourceId == -1);
            var collectionName = url[url.indexOf("collection") + 1];
            var urlParams = "?collection=" + collectionName + "&datasourceId=" + datasourceId;

            Dom.get("collection_header").innerHTML = "Collection " + collectionName;

            Event.addListener("create", "click", function(e) {
                Event.stopEvent(e);

                var urlParams = "?file=" + Dom.get("file").value + "&local=" + Dom.get("is_local").checked +
                        "&collection=" + collectionName;

                Connect.asyncRequest('GET', '<c:url value="/solr/update/csv" />' + urlParams, {
                    success: function (o) {
                        if (LWA.checkXmlReturnValue(o)) {
                            window.location = '<c:url value="/collection/" />' + collectionName;
                        }
                    },
                    failure: function (e) {
                        alert("Could not connect to /solr/update/csv to import csv file.");
                        console.log(e);
                    }
                });
            });


        })();
    </script>
</layout:main>