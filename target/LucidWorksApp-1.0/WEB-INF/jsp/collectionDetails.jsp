<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <h2 id = "collection_name">Collection details</h2>
    <div id="results_table"></div>

    <form id="datasource_form">
        <div class="row">Create new datasource:</div>

        <div id="datasource_tabview" class="yui-navset">
            <ul class="yui-nav">
                <li class="selected"><a href="#webtab"><em>Web</em></a></li>
                <li><a href="#tab2"><em>Tab Two Label</em></a></li>
                <li><a href="#tab3"><em>Tab Three Label</em></a></li>
            </ul>
            <div class="yui-content">
                <div id="webtab">
                    <div class = "row">
                        <label for="datasource_name">Name: </label>
                        <input id="datasource_name" type = "text" value="epic"/>
                    </div>
                    <div class = "row">
                        <label for="datasource_url">Url: </label>
                        <input id="datasource_url" type = "text" value="http://epic.cs.colorado.edu"/>
                    </div>
                    <div class="buttons">
                        <a href="#" class="button small" id="create">Create</a>
                        <a href="#" class="button small" id="cancel">Cancel</a>
                    </div>
                    <div class = "row"></div>
                </div>
                <div id="tab2"><p>Tab Two Content</p></div>
                <div id="tab3"><p>Tab Three Content</p></div>
            </div>
        </div>
    </form>

    <script type="text/javascript">
        (function() {
            var Dom     = YAHOO.util.Dom,
                Event   = YAHOO.util.Event,
                Connect = YAHOO.util.Connect,
                Json    = YAHOO.lang.JSON,
                TabView = YAHOO.widget.TabView;

            var collectionName = window.location.href.split('/').pop();
            Dom.get("collection_name").innerHTML = collectionName + " collection details";

            Connect.asyncRequest('GET', '<c:url value="/datasource/topleveldetails" />' + "?collection=" + collectionName, {
                success : function(o) {
                    var result = Json.parse(o.responseText);
                },
                failure : function (o) {
                    alert("Could not retrieve datasource details");
                }
            });

            var dataSource = new YAHOO.util.XHRDataSource('<c:url value="/datasource/topleveldetails" />' + "?collection=" + collectionName);
            dataSource.responseSchema = {
                resultsList:'datasources',
                fields:[
                    {key:'name', parser:'text'},
                    {key:'crawl_state', parser:'text'},
                    {key:'docs', parser:'number'}
                ]
            };

            var contextPath = '<c:url value="/"/>';
            contextPath = contextPath != '/' ? contextPath : '';
            var formatCollectionName = function(elCell, oRecord, oColumn, oData) {
                elCell.innerHTML =  '<a href="' + contextPath + 'collection/' + oData + '">' + oData + '</a>';
            };

            var columnDefs = [
                {key:'name', label:'Name', sortable:true, resizeable:true, formatter:formatCollectionName},
                {key:'crawl_state', label:'Crawl status', sortable:true},
                {key:'docs', label:'# Documents', resizeable:true, sortable:true}
            ];

            var dataTable = new YAHOO.widget.ScrollingDataTable('results_table', columnDefs, dataSource, {
                draggableColumns: true,
                width:"100%",
                paginator : new YAHOO.widget.Paginator({
                    rowsPerPage: 25,
                    pageLinks:5
                })
            });

            var tabs = new TabView("datasource_tabview");

            Event.addListener("create", "click", function (e) {
                Event.stopEvent(e);

                var newdatasourceinfo = {
                    'collectionName' : collectionName,
                    'properties' : { "name" : Dom.get("datasource_name").value,
                                     "url" : Dom.get("datasource_url").value,
                                     "crawler" : "lucid.aperture",
                                     "crawl_depth" : 5,
                                     "type" : "web" }
                };

                console.log("Sending " + YAHOO.lang.JSON.stringify(newdatasourceinfo));

                Connect.initHeader('Content-Type', 'application/json');
                Connect.setDefaultPostHeader('application/json');
                Connect.asyncRequest('POST', '<c:url value="/datasource/create" />', {
                    success:function(o) {
                        alert("Success!");
                        window.location.reload();
                    },
                    failure:function(e) {
                        alert("Problem encountered adding data!" + e);
                    }
                }, YAHOO.lang.JSON.stringify(newdatasourceinfo));
            });


        })();

    </script>
    <!--<script type="text/javascript" src='<c:url value="../datasource/webTabView"/>'></script>-->

</layout:main>