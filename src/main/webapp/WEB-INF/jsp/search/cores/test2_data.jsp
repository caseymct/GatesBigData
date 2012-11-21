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
            <a href="#" class="button small" id="export">Export results</a>
        </div>
        <div class = "row"></div>
    </form>




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
            var Event = YAHOO.util.Event;

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

            var datePickerUrl = '<c:url value="/core/field/daterange" />' + "?core=" + SEARCH.ui.coreName + "&field=last_modified",
                    queryBuilderAutoCompleteUrl = '<c:url value="/search/fields/all?core=" />' + SEARCH.ui.coreName,
                    facetUrl = '<c:url value="/search/solrfacets" />' + "?core=" + SEARCH.ui.coreName,
                    exportUrl = '<c:url value="/export?type=zip&file=export.zip" />',
                    viewDocUrl = '<c:url value="/core/document/viewtest" />',
                    loadingImgUrl = '<c:url value="/static/images/loading.png" />',
                    thumbnailUrl = '<c:url value="/document/thumbnail/get" />',
                    searchUrl = '<c:url value="/search/solrquery" />';

            UI.initWait();
            QUERYBUILDER.ui.buildQueryTabHTML("search_tab_content");
            QUERYBUILDER.ui.initPopulateFieldAutoCompleteUrl(queryBuilderAutoCompleteUrl);

            DATEPICK.ui.buildDatePickHTML("constrain_by_date");
            DATEPICK.ui.initDatePickerVars("last_modified", datePickerUrl);

            FACET.ui.buildFacetHTML("insert_facets_after");
            FACET.ui.initFacetVars(facetUrl);

            SEARCH.ui.initUrls({ exportUrl : exportUrl, viewDocUrl : viewDocUrl, loadingImgUrl : loadingImgUrl,
                thumbnailUrl : thumbnailUrl, searchUrl : searchUrl });
            SEARCH.ui.initHTML({ searchTabElName : "search_tab",        sortOrderButtonGroupElName : "sort_ascdesc_buttongroup",
                previewContainerElName : "preview_container",         sortBySelectElName : "sort_date_label",
                selectDataColumnDefs : columnDefs ,              selectDataRegexIgnore : "thumbnail",
                exportFileName : "export_file_name",             paginatorElName : "pag1",
                dataSourceFields : dataSourceFields,            searchHeaderElName : "search_header",
                searchInputElNames : ["general_query_search_input"] });

            FACET.ui.buildInitialFacetTree();

            Event.addListener("reset", "click", function (e) {
                Event.stopEvent(e);
                SEARCH.ui.resetQuerySearchInputs();
                FACET.ui.buildInitialFacetTree();
            });

            Event.addListener("submit", "click", function (e) {
                Event.stopEvent(e);
                var fq = FACET.util.getFacetFilterQueryString() + DATEPICK.util.getDateConstraintFilterQueryString();
                if (SEARCH.ui.search(fq) > 0) {
                    FACET.ui.buildFacetTree(SEARCH.ui.facetsFromLastSearch);
                }
            });

        })();
    </script>
</layout:main>
