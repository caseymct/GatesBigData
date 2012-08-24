<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/datasource.css"/>" />
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/collection.css"/>" />

    <h2 id = "collection_name"></h2>

    <div>
        <div>Collection details:</div>
        <div id="collection_info"></div>
    </div>

    <div class="clearboth"></div>

    <div id = "edit_collection_div">
        <a href="#" class="button delete" id="delete_collection">Delete collection</a>
        <a href="#" class="button delete" id="empty_collection">Empty collection</a>
    </div>

    <div id="datasource_container">Datasources details: </div>
    <div id="results_table_container">
        <div class = "smallmargin" id="results_table"></div>

        <a href="#" class="button add smallmargin" id="add_new_datasource">Add datasource</a>
        <select id="new_datasource_type">
            <option value="web">Web Crawl</option>
            <option value="importcsv">Import CSV to Solr</option>
        </select>

        <div class="clearboth"></div>
    </div>

    <div class="clearboth"></div>

    <script type="text/javascript">
        (function() {
            var Dom       = YAHOO.util.Dom,
                Event     = YAHOO.util.Event,
                Connect   = YAHOO.util.Connect,
                Json      = YAHOO.lang.JSON,
                DataTable = YAHOO.widget.DataTable;

            var collectionName = window.location.href.split('/').pop();
            var urlParams = "?collection=" + collectionName;
            Dom.get("collection_name").innerHTML = "Collection: " + collectionName;

            Connect.asyncRequest('GET', '<c:url value="/collection/info" />' + urlParams, {
                success: function (o) {
                    var result = Json.parse(o.responseText);
                    LWA.ui.buildTable(result, "collection_info");
                },
                failure: function (e) {
                    alert("Could not delete");
                }
            });

            var dataSource = new YAHOO.util.XHRDataSource('<c:url value="/datasource/topleveldetails" />' + "?collection=" + collectionName);
            dataSource.responseSchema = {
                resultsList:'datasources',
                fields:[
                    {key:'name', parser:'text'},
                    {key:'type', parser:'text'},
                    {key:'url', parser:'text'},
                    {key:'crawl_state', parser:'text'},
                    {key:'docs', parser:'number'},
                    {key:'id', parser:'number'}
                ]
            };

            var contextPath = '<c:url value="/"/>';
            contextPath = contextPath != '/' ? contextPath : '';
            var formatCollectionName = function(elCell, oRecord, oColumn, oData) {
                elCell.innerHTML =  '<a href="' + contextPath + 'collection/' + collectionName +
                        "/datasource/" + oRecord.getData().id + '">' + oData + '</a>';
            };

            var columnDefs = [
                {key:'name', label:'Name', sortable:true, resizeable:true, formatter:formatCollectionName},
                {key:'type', label:'Type', sortable:true, resizeable:true},
                {key:'url', label:'Source', sortable:true, resizeable:true},
                {key:'crawl_state', label:'Crawl status', sortable:true},
                {key:'docs', label:'# Documents', resizeable:true, sortable:true}
            ];

            var dataTable = new DataTable("results_table", columnDefs, dataSource, {
                width:"100%",
                paginator : new YAHOO.widget.Paginator({
                    rowsPerPage: 25,
                    pageLinks:5
                })
            });
            dataTable.subscribe("rowClickEvent", dataTable.onEventSelectRow);

            Event.addListener("add_new_datasource", "click", function (e) {
                Event.stopEvent(e);
                var sel = Dom.get("new_datasource_type");
                var datasourceType = sel.options[sel.selectedIndex].value;

                window.location = '<c:url value="/collection/" />' + collectionName + "/type/" + datasourceType +
                    "/datasource/-1";
            });

            var deleteCollection = function() {
                Connect.asyncRequest('DELETE', '<c:url value="/collection/delete" />' + urlParams, {
                    success: function (o) {
                        if (o.responseText == "") {
                            window.location = '<c:url value="/" />';
                        } else {
                            LWA.ui.alertErrors(o);
                        }
                    },
                    failure: function (e) {
                        alert("Could not delete");
                    }
                });
            };

            var handleDeleteCollectionYes = function() {
                deleteCollection();
                this.hide();
            };

            var emptyCollection = function() {
                Connect.asyncRequest('DELETE', '<c:url value="/collection/empty" />' + urlParams, {
                    success: function (o) {
                        if (o.responseText == "") {
                            window.location.reload();
                        } else {
                            LWA.ui.alertErrors(o);
                        }
                    },
                    failure: function (e) {
                        alert("Could not delete");
                    }
                });
            };

            var handleEmptyCollectionYes = function() {
                emptyCollection();
                this.hide();
            };

            Event.addListener("delete_collection", "click", function (e) {
                Event.stopEvent(e);

                LWA.ui.confirmDelete.setBody("Are you sure you want to delete this collection?");
                LWA.ui.confirmDelete.cfg.queueProperty("buttons", [
                    { text: "Yes", handler: handleDeleteCollectionYes },
                    { text: "Cancel", handler: LWA.ui.confirmDeleteHandleNo, isDefault:true}
                ]);
                LWA.ui.confirmDelete.render(Dom.get("content"));
                LWA.ui.confirmDelete.show();
            });

            Event.addListener("empty_collection", "click", function (e) {
                Event.stopEvent(e);

                LWA.ui.confirmDelete.setBody("Are you sure you want to empty this collection?");
                LWA.ui.confirmDelete.cfg.queueProperty("buttons", [
                    { text: "Yes", handler: handleEmptyCollectionYes },
                    { text: "Cancel", handler: LWA.ui.confirmDeleteHandleNo, isDefault:true}
                ]);
                LWA.ui.confirmDelete.render(Dom.get("content"));
                LWA.ui.confirmDelete.show();
            });

        })();

    </script>


</layout:main>
