<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <h2>Collections</h2>

    <div id="results_table"></div>

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


    })();
    </script>
</layout:main>