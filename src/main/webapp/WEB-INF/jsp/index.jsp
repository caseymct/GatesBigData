<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/index.css"/>" />

    <h2>Collections</h2>

    <div id="tree_view"></div>

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
                Json = YAHOO.lang.JSON,
                TreeView = YAHOO.widget.TreeView,
                TextNode = YAHOO.widget.TextNode;

        var treeView = new YAHOO.widget.TreeView("tree_view");

        var buildTreeRecurse = function(doc, parentNode) {
            var isObject = Object.prototype.toString.call(doc).match("Object") != null;
            var nameNode = new TextNode(doc, parentNode, false);
            console.log("adding " + doc);

            if (!isObject) {
                nameNode.isLeaf = true;
            } else {
                debugger;
                var keys = Object.keys(doc);
                for(var j = 0; j < keys.length; j++) {
                    buildTreeRecurse(doc[keys[j]], nameNode);
                }
            }
        };

        var buildTree = function(docs) {
            var i, j, keys, root = treeView.getRoot();

            treeView.removeChildren(root);

            for(i = 0; i < docs.length; i++) {
                keys = Object.keys(docs[i]);
                for(j = 0; j < keys.length; j++) {
                    buildTreeRecurse(docs[i][keys[j]], root);
                }
            }
            treeView.render();
        };
        /*
        var dataSource = new YAHOO.util.XHRDataSource('<c:url value="/core/info" />');
        dataSource.responseSchema = {
            resultsList:'collections',
            fields:[
                {key:'name', parser:'text'},
                {key:'isDefaultCore', parser:'text'},
                {key:'instanceDir', parser:'text'},
                {key:'dataDir', parser:'text'},
                {key:'config', parser:'text'},
                {key:'schema', parser:'text'},
                {key:'startTime', parser:'date'},
                {key:'uptime', parser:'number'},
                {key:'dataSources', parser:'number'},
                {key:'crawling', parser:'text'}
            ]

            name: "collection1",
            isDefaultCore: "true",
            instanceDir: "solr/collection1/",
            dataDir: "solr/collection1/data/",
            config: "solrconfig.xml",
            schema: "schema.xml",
            startTime: "Mon Aug 27 16:39:09 MDT 2012",
            uptime: "82076746",
            index: {
                numDocs: "28",
                maxDoc: "28",
                version: "169",
                segmentCount: "1",
                current: "true",
                hasDeletions: "false",
                directory: "org.apache.lucene.store.NRTCachingDirectory:NRTCachingDirectory(org.apache.lucene.store.MMapDirectory@/projects/solr/solr4.0/example/solr/collection1/data/index lockFactory=org.apache.lucene.store.NativeFSLockFactory@6d3d7254; maxCacheMB=48.0 maxMergeSizeMB=4.0)",
                userData: {
                    commitTimeMSec: 1346101495813
                },
                lastModified: "Mon Aug 27 15:04:55 MDT 2012",
                sizeInBytes: "34866",
                size: "34.05 KB"
            }
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
        */
        Connect.asyncRequest('GET', '<c:url value="/core/info/all" />' , {
            success : function(o) {

                var result = Json.parse(o.responseText);
                buildTree(result);
            },
            failure : function (o) {
                alert("failed");
            }
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