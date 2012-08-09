<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/datasource.css"/>" />
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/collection.css"/>" />

    <h2 id = "collection_name">Collection details</h2>

    <a href="#" class="button delete" id="delete_collection">Delete collection</a>


    <div id="results_table_container">
        <div><b>Epic datasources:</b></div>
        <div id="results_table"></div>

        <a href="#" class="button add" id="add_new_datasource">Add datasource</a>
        <select id="new_datasource_type">
            <option value="web">Web</option>
        </select>
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
            Dom.get("collection_name").innerHTML = collectionName + " collection details";

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
                console.log(datasourceType);
                window.location = '<c:url value="/collection/" />' + collectionName + "/type/" + datasourceType +
                    "/datasource/new";

            });


            var handleYes = function() {
                console.log("deleting!");
                Connect.asyncRequest('DELETE', '<c:url value="/collection/delete" />' + "&collection=" + collectionName, {
                    success: function (o) {
                        if (o.responseText == "") {
                            window.location = '<c:url value="/" />';
                        } else {
                            LWA.ui.alertErrors(o);
                        }
                    },
                    failure: function (e) {
                        alert("Could not delete");
                        console.log(e);
                    }
                });
                this.hide();
            };


            var myButtons = [
                { text: "Yes", handler: handleYes },
                { text: "Cancel", handler: LWA.ui.confirmDeleteHandleNo, isDefault:true}
            ];
            LWA.ui.confirmDelete.cfg.queueProperty("buttons", myButtons);
            LWA.ui.confirmDelete.render(YAHOO.util.Dom.get("content"));
            Event.addListener("delete_collection", "click", function (e) {
                Event.stopEvent(e);

                console.log("showing...");
                LWA.ui.confirmDelete.show();
            });

        })();

    </script>


</layout:main>