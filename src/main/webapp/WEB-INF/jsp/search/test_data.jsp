<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />
    <h1 id="search_header">Search Core</h1>

    <form id = "search_form">
        <div id="search_tab" class="yui-navset">
            <ul class="yui-nav">
                <li class="selected tab_selected"><a href="#tab1"><em>General Query</em></a></li>
            </ul>
            <div id="search_tab_content" class="yui-content">
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

        <div id="constrain_by_date"></div>

        <div id="insert_facets_after" class="clearboth"></div>

        <div class="buttons" style="padding-bottom: 5px">
            <a href="#" class="button small" id="submit">Search</a>
            <a href="#" class="button small" id="reset">Reset query fields</a>
            <!--<a href="#" class="button small" id="export">Export results</a>-->
        </div>
        <div class = "row"></div>
    </form>

    <!--
    <div id="exportDialog" class="yui-pe-content">
        <div class="hd">Export File</div>
        <div class="bd">
            <label for="export_file_name">File Name:</label>
            <input type="text" id="export_file_name" value="test" style="width: 100%"/>
        </div>
    </div>
    -->
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
    <script type="text/javascript" src="<c:url value="/static/js/querybuilder.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/datepick.js" />"></script>
    <script type="text/javascript" src="<c:url value="/static/js/facet.js" />"></script>

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

        var generalQueryConstraintDomEls = [ { key: "input", value: "general_query_search_input"},
            { key: "field", value: "general_query_field_constraint"}, { key: "phrase", value: "has_phrase" },
            { key: "exact", value: "is_exact"}, { key: "haswords", value: "has_words"},
            { key :"nowords", value: "does_not_have_words"}];

        UI.initWait();
        QUERYBUILDER.ui.buildQueryTabHTML("search_tab_content");
        QUERYBUILDER.ui.initPopulateFieldAutoCompleteUrl('<c:url value="/search/fields/all?core=" />' + SEARCH.ui.coreName);

        DATEPICK.ui.buildDatePickHTML("constrain_by_date");
        DATEPICK.ui.initDatePickerVars("last_modified",
                '<c:url value="/core/field/daterange" />' + "?core=" + SEARCH.ui.coreName + "&field=last_modified");

        FACET.ui.buildFacetHTML("insert_facets_after");
        FACET.ui.initFacetVars('<c:url value="/search/solrfacets" />' + "?core=" + SEARCH.ui.coreName);

        SEARCH.ui.setSearchHeader("search_header");
        SEARCH.ui.initQuerySearchInputs(["general_query_search_input"]);
        SEARCH.ui.initPreviewContainer("preview_container");
        SEARCH.ui.initPaginator("pag1");
        SEARCH.ui.initSearchTab("search_tab");
        SEARCH.ui.initSortOrderButtonGroup("sort_ascdesc_buttongroup");
        SEARCH.ui.initSelectData(columnDefs, "thumbnail");
        SEARCH.ui.initSortBySelect("sort_date_label", 0);
        SEARCH.ui.initExportUrl('<c:url value="/core/document/export" />');

        FACET.ui.buildInitialFacetTree();

        Event.addListener("reset", "click", function (e) {
            Event.stopEvent(e);
            SEARCH.ui.resetQuerySearchInputs();
            FACET.ui.buildInitialFacetTree();
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

        function getFilterQueryString() {
            return FACET.util.getFacetFilterQueryString() + DATEPICK.util.getDateConstraintFilterQueryString();
        }

        var handlePagination = function (newState) {
            UI.showWait();

            var fq = getFilterQueryString();
            SEARCH.ui.updateSolrQueryDiv("show_query", fq);
            SEARCH.ui.urlSearchParams = SEARCH.util.constructUrlSearchParams(fq, newState.reocrds[0]);
            Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + SEARCH.ui.urlSearchParams, {
                success: function(o) {
                    UI.hideWait();
                    buildSearchResultHtml(Json.parse(o.responseText));
                }
            });

            SEARCH.ui.pag.setState(newState);
        };
        SEARCH.ui.pag.subscribe('changeRequest', handlePagination);

        var search = function() {
            UI.showWait();
            var fq = getFilterQueryString();
            SEARCH.ui.urlSearchParams = SEARCH.util.constructUrlSearchParams(fq, 0);
            SEARCH.ui.updateSolrQueryDiv("show_query", fq);

            Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + SEARCH.ui.urlSearchParams, {
                success : function(o) {
                    UI.hideWait();

                    var result = Json.parse(o.responseText);
                    var facets = result.response.facets;
                    SEARCH.ui.updateNumFound(result.response.numFound);
                    SEARCH.ui.updatePaginatorAfterSearch(SEARCH.ui.numFound);

                    buildSearchResultHtml(result);
                    if (SEARCH.ui.numFound > 0) {
                        FACET.ui.buildFacetTree(facets);
                    }
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
