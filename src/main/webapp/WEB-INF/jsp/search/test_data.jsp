<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />
    <!--<link rel="stylesheet" type="text/css" href="<c:url value="/static/css/jsdatepick/jsDatePick_ltr.min.css"/>" />

    <script type="text/javascript" src="<c:url value="/static/js/jsdatepick/jsDatePick.min.1.3.js"/>"></script>-->


    <h1 id="search_header">Search Core</h1>

    <form id = "search_form">
        <div id="search_tab" class="yui-navset">
            <ul class="yui-nav">
                <li class="selected tab_selected"><a href="#tab4"><em>General Query</em></a></li>
            </ul>
            <div class="yui-content">
                <div class = "search_tab_style row" id="search_generalquery_tab">
                    <div class="row" style="padding: 2px">
                        <textarea id="general_query_search_input">*:*</textarea>
                    </div>
                    <div class="row" style="padding: 2px; font-size: 10px">
                        Query terms take the form of '<i>Column name</i>:<i>Value</i>' e.g.<br>
                        Supplier.SupplierName:citibank*<br>
                        Supplier.SupplierName:citibank* AND CostCenter.CostCenterName:global*<br>
                        Multiple terms can be concatenated with AND or OR
                    </div>
                    <div class="clearboth"></div>
                </div>
            </div>
        </div>

        <div class="clearboth"></div>

        <div class="row" id = "sort_by_div">
            <div>
                <label id="sort_date_label">Order by: </label>
            </div>
            <div id = "sort_ascdesc_buttongroup" class = "yui-buttongroup search-button-style">
                <input id="sort_asc" type="radio" name="sorttype" value="asc" >
                <input id="sort_desc" type="radio" name="sorttype" value="desc" checked>
            </div>
            <div class="clearboth"></div>
        </div>

        <div id="constrain_by_date">
            <label for="date_begin">Constrain by <span id="date_constraint_text"></span>: </label>
            <input type="text" size="10" id="date_begin" value="*"/>
            <label for="date_end">to</label>
            <input type="text" size="10" id="date_end" value="*"/>
            <br>
            <span style="font-size:12px; padding-top:3px">Range for field <span id = "date_constraint_range"></span></span>
        </div>

        <div class="clearboth"></div>

        <div id="facet_options">
            <div class="clearboth"></div>
        </div>

        <div class="buttons" style="padding-bottom: 5px">
            <a href="#" class="button small" id="submit">Search</a>
            <a href="#" class="button small" id="reset">Reset query fields</a>
            <a href="#" class="button small" id="export">Export results</a>
        </div>
        <div class = "row"></div>
    </form>

    <div id="exportDialog" class="yui-pe-content">
        <div class="hd">Export File</div>
        <div class="bd">
            <label for="export_file_name">File Name:</label>
            <input type="text" id="export_file_name" value="test" style="width: 100%"/>
        </div>
    </div>

    <div>
        <div id="search_result_container" style="width: 58%">
            <div id = "show_query"></div>
            <div id = "num_found"></div>
            <div id = "search_results" ></div>
        </div>
        <div id = "preview_container"></div>
        <div class = "clearboth"></div>
    </div>

    <div id = "pag1"></div>

    <script type="text/javascript" src="<c:url value="/static/js/search.js" />"></script>

    <script type="text/javascript">
    (function() {
        var     Event   = YAHOO.util.Event,         Dom  = YAHOO.util.Dom,
                Connect = YAHOO.util.Connect,       Json = YAHOO.lang.JSON,
                TabView  = YAHOO.widget.TabView,    ScrollingDataTable = YAHOO.widget.ScrollingDataTable,
                LocalDataSource = YAHOO.util.LocalDataSource;

        var searchTab = new TabView('search_tab');

        var columnDefs = [
            {key:'title', label:'Title', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'author', label:'Author', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'last_modified', label:'Last Modified Date', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'creation_date', label:'Creation Date', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'application_name', label:'Application', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'company', label:'Company', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'last_author', label:'Last Author', sortable:true, formatter:SEARCH.ui.formatLink, width: SEARCH.ui.longStringWidth},
            {key:'content_type', label:'Content Type', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'thumbnail', label:'thumbnail', hidden: true},
            {key:'thumbnailType', label:'thumbnail', hidden: true}
        ];
        var dataSourceFields = [
            {key:'title', parser:'text'},
            {key:'author', parser:'text'},
            {key:'last_modified', parser:'text'},
            {key:'creation_date', parser:'text'},
            {key:'application_name', parser:'text'},
            {key:'company', parser:'text'},
            {key:'last_author', parser:'text'},
            {key:'content_type', parser:'text'},
            {key:'url', parser:'text'},
            {key:'HDFSKey', parser:'text'},
            {key:'HDFSSegment', parser:'text'}
        ];

        LWA.ui.initWait();
        SEARCH.ui.setSearchHeader("search_header");
        SEARCH.ui.initGeneralQuerySearchInput("general_query_search_input");
        SEARCH.ui.initPreviewContainer("preview_container");
        SEARCH.ui.initDatePickers("date_begin", "date_end", "date_constraint_text", "last_modified");
        SEARCH.ui.initDateRangeText('<c:url value="/core/field/daterange" />', "date_constraint_range");
        SEARCH.ui.initPaginator("pag1");
        SEARCH.ui.initSortOrderButtonGroup("sort_ascdesc_buttongroup");
        SEARCH.ui.initSelectData(columnDefs);
        SEARCH.ui.initSortBySelect("sort_date_label", 0);
        SEARCH.ui.initExportUrl('<c:url value="/export" />');
        SEARCH.ui.adjustContentContainerHeight();

        Connect.asyncRequest('GET', '<c:url value="/search/solrfacets" />' + "?core=" + SEARCH.ui.coreName, {
            success : SEARCH.ui.buildInitialFacetTree
        });

        Event.addListener("reset", "click", function (e) {
            Event.stopEvent(e);
            SEARCH.ui.generalQuerySearchInput.value = "*:*";
        });

        var buildSearchResultHtml = function(result) {
            var dataSource = new LocalDataSource(result.response, {
                    responseSchema : {
                        resultsList:'docs',
                        fields: dataSourceFields
                    }
            });

            var dataTable = new ScrollingDataTable('search_results', columnDefs, dataSource, { width:"100%" });

            dataTable.subscribe("sortedByChange", function(e) {
                var sortField = e.newValue.column.key;
                SEARCH.ui.dataTableSortedByChange(sortField);
                search();
            });

            dataTable.subscribe("cellClickEvent", function (e) {
                Event.stopEvent(e.event);
                SEARCH.ui.dataTableCellClickEvent(this.getRecord(e.target).getData(), '<c:url value="/core/document/viewtest" />');
            });

            dataTable.on('rowMouseoverEvent', function (e) {
                SEARCH.ui.dataTableCellMouseoverEvent(e, dataTable, "<c:url value="/static/images/loading.png" />",
                        '<c:url value="/document/thumbnail/get" />');
            });
        };

        var handlePagination = function (newState) {
            LWA.ui.showWait();

            var fq = SEARCH.util.getFilterQueryString();
            SEARCH.ui.updateSolrQueryDiv("show_query", fq);

            Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + SEARCH.util.constructSearchUrlParams(fq, newState.records[0]), {
                success: function(o) {
                    LWA.ui.hideWait();
                    buildSearchResultHtml(Json.parse(o.responseText));
                }
            });

            SEARCH.ui.pag.setState(newState);
        };
        SEARCH.ui.pag.subscribe('changeRequest', handlePagination);

        var search = function() {
            LWA.ui.showWait();
            var fq = SEARCH.util.getFilterQueryString();
            SEARCH.ui.updateSolrQueryDiv("show_query", fq);

            Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + SEARCH.util.constructSearchUrlParams(fq, 0), {
                success : function(o) {
                    LWA.ui.hideWait();

                    var result = Json.parse(o.responseText);
                    var numFound = result.response.numFound, facets = result.response.facets;

                    SEARCH.ui.updatePaginatorAfterSearch(numFound);

                    Dom.get("num_found").innerHTML = "Found " + numFound + " document" + ((numFound > 1) ? "s" : "");
                    SEARCH.ui.changeShowOverlayButtonVisibility(true);

                    buildSearchResultHtml(result);
                    SEARCH.ui.buildFacetTree(facets);
                },
                failure : function(o) {
                    alert("Could not connect.");
                }
            });
        };

        Event.addListener("submit", "click", function (e) {
            Event.stopEvent(e);
            search();
        });

    })();
    </script>
</layout:main>
