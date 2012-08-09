<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/index.css"/>" />

    <h2>Collections</h2>

    <div id="results_table"></div>

    <form id="create_collection_form">
        <div id="create_new_collection_div">
            <h3 id="create_new_collection_header">Create new collection</h3>
            <div class="row">
                <label for="new_collection_name" id = "new_collection_name_label"><span class="red">*</span>Name: </label>
                <input id="new_collection_name" type="text"/>
            </div>
            <div class="buttons">
                <a href="#" class="button small" id="create">Create</a>
            </div>
            <div class = "row"></div>
        </div>
    </form>

    <script type="text/javascript">
    (function() {
        var Dom = YAHOO.util.Dom,
                Event = YAHOO.util.Event,
                Connect = YAHOO.util.Connect,
                Json = YAHOO.lang.JSON;

        var dataSource = new YAHOO.util.XHRDataSource('<c:url value="/collection/collectionOverview" />');
        dataSource.responseSchema = {
            resultsList:'collections',
            fields:[
                {key:'name', parser:'text'},
                {key:'docs', parser:'number'},
                {key:'size', parser:'text'},
                {key:'dataSources', parser:'number'},
                {key:'crawling', parser:'text'}
            ]
        };

        var contextPath = '<c:url value="/"/>';
        contextPath = contextPath != '/' ? contextPath : '';
        var formatCollectionName = function(elCell, oRecord, oColumn, oData) {
            elCell.innerHTML =  '<a href="' + contextPath + 'collection/' + oData + '">' + oData + '</a>';
        };

        var columnDefs = [
            {key:'name', label:'Name', sortable:true, resizeable:true, formatter:formatCollectionName},
            {key:'docs', label:'# Documents', resizeable:true, sortable:true},
            {key:'size', label:'Size', sortable:true, resizeable:true},
            {key:'dataSources', label:'# Data sources', sortable:true},
            {key:'crawling', label:'Crawl status', sortable:true}
        ];

        var dataTable = new YAHOO.widget.ScrollingDataTable('results_table', columnDefs, dataSource, {
            draggableColumns: true,
            width:"100%",
            paginator : new YAHOO.widget.Paginator({
                rowsPerPage: 25,
                pageLinks:5
            })
        });

        Event.addListener("create", "click", function(e) {
            Event.stopEvent(e);

            var newcollectioninfo =  { "collectionName" : Dom.get("new_collection_name").value };

            Connect.initHeader('Content-Type', 'application/json');
            Connect.setDefaultPostHeader('application/json');
            Connect.asyncRequest('POST', '<c:url value="/collection/create" />' , {
                success: function (o) {
                    if (!LWA.ui.alertErrors(o)) {
                        window.location.reload();
                    }
                },
                failure: function (e) {
                    alert("Could not create new collection");
                    console.log(e);
                }
            }, YAHOO.lang.JSON.stringify(newcollectioninfo));
        });

    })();
    </script>
</layout:main>