<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/datasource.css"/>" />

    <h2 id = "collection_name">Collection details</h2>
    <div id="results_table_container">
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

        })();

    </script>
    <!--<script type="text/javascript" src='<c:url value="../datasource/webTabView"/>'></script>-->

</layout:main>