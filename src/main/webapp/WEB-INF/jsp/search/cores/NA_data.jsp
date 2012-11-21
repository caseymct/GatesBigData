<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ page pageEncoding="UTF-8" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />

    <h1 id="search_header">Search Core</h1>

    <form id = "search_form">
        <div id="search_tab" class="yui-navset">
            <ul class="yui-nav">
                <li class="selected tab_selected"><a href="#tab1"><em>Search</em></a></li>
                <li><a href="#tab2"><em>Construct General Query</em></a></li>
            </ul>
            <div id="search_tab_content" class="yui-content">
                <div class="search_tab_style row" id="autocomplete_tab">
                    <div id="autocomplete_div">
                        <input type="text" id="autocomplete_input"/>
                        <div id="autocomplete_container"></div>
                    </div>
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

        <div id="constrain_by_date"></div>

        <div id="insert_facets_after" class="clearboth"></div>

        <div class="buttons" style="padding-bottom: 5px">
            <a href="#" class="button small" id="submit">Search</a>
            <a href="#" class="button small" id="reset">Reset</a>
            <a href="#" class="button small" id="export">Export results</a>
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
        var Event         = YAHOO.util.Event,
            XHRDataSource = YAHOO.util.XHRDataSource,
            AutoComplete  = YAHOO.widget.AutoComplete;

        var columnDefs = [
            {key:'PaidDate', label:'Paid Date', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.shortStringWidth },
            {key:'InvoiceDate', label:'Invoice Date', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.shortStringWidth },
            {key:'Account.AccountName', label:'Account Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'AccountCompanyCode', label:'Company Code', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'User.UserName', label:'User Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'CompanySite.SiteName', label:'Site Name', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'Supplier.SupplierName', label:'Supplier Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'CostCenter.CostCenterName', label:'Cost Center Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'CostCenterId', label:'Cost Center ID', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'InvoiceId', label:'Invoice ID', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'thumbnail', label:'thumbnail', hidden: true},
            {key:'thumbnailType', label:'thumbnailType', hidden: true}
        ];

        var dataSourceFields = [
            {key:'Account.AccountName', parser:'text'},
            {key:'AccountCompanyCode', parser:'text'},
            {key:'User.UserName', parser:'text'},
            {key:'Amount', parser:'number'},
            {key:'CompanySite.SiteName', parser:'text'},
            {key:'CostCenter.CostCenterName', parser:'text'},
            {key:'Supplier.SupplierName', parser:'text'},
            {key:'CostCenterId', parser:'text'},
            {key:'InvoiceId', parser:'number'},
            {key:'PaidDate', parser:'text'},
            {key:'InvoiceDate', parser:'text'},
            {key:'url', parser:'text'},
            {key:'HDFSKey', parser:'text'},
            {key:'HDFSSegment', parser:'text'}
        ];

        var datePickerUrl = '<c:url value="/core/field/daterange" />' + "?core=" + SEARCH.ui.coreName + "&field=InvoiceDate",
            queryBuilderAutoCompleteUrl = '<c:url value="/search/fields/all?core=" />' + SEARCH.ui.coreName,
            facetUrl = '<c:url value="/search/solrfacets" />' + "?core=" + SEARCH.ui.coreName,
            exportUrl = '<c:url value="/core/document/export" />',
            viewDocUrl = '<c:url value="/core/document/viewtest" />',
            loadingImgUrl = '<c:url value="/static/images/loading.png" />',
            thumbnailUrl = '<c:url value="/document/thumbnail/get" />',
            searchUrl = '<c:url value="/search/solrquery" />',
            suggestUrl = '<c:url value="/search/suggest" />';

        UI.initWait();
        QUERYBUILDER.ui.buildQueryTabHTML("search_tab_content");
        QUERYBUILDER.ui.initPopulateFieldAutoCompleteUrl(queryBuilderAutoCompleteUrl);

        DATEPICK.ui.buildDatePickHTML("constrain_by_date");
        DATEPICK.ui.initDatePickerVars("InvoiceDate", datePickerUrl);

        FACET.ui.buildFacetHTML("insert_facets_after");
        FACET.ui.initFacetVars(facetUrl);

        SEARCH.ui.initUrls({ exportUrl : exportUrl, viewDocUrl : viewDocUrl, loadingImgUrl : loadingImgUrl,
                             thumbnailUrl : thumbnailUrl, searchUrl : searchUrl, suggestUrl : suggestUrl });
        SEARCH.ui.initHTML({ searchTabElName : "search_tab",        sortOrderButtonGroupElName : "sort_ascdesc_buttongroup",
                      previewContainerElName : "preview_container",         sortBySelectElName : "sort_date_label",
                        selectDataColumnDefs : columnDefs ,              selectDataRegexIgnore : "thumbnail",
                              exportFileName : "export_file_name",             paginatorElName : "pag1",
                            dataSourceFields : dataSourceFields,            searchHeaderElName : "search_header",
                          searchInputElNames : ["autocomplete_input", "general_query_search_input"] });

        FACET.ui.buildInitialFacetTree();

        /* Autocomplete code */
        var ds = new XHRDataSource('<c:url value="/search/suggest" />' );
        ds.responseType = XHRDataSource.TYPE_JSON;
        ds.responseSchema = { resultsList : "suggestions" };

        var itemSelectHandler = function(s, args) {
            var inputEl = Dom.get(args[0].getInputEl());
            var sel = args[2][0].match("<b>(.*)</b> <i>(.*)</i>.*");
            inputEl.value = sel[2] + ":\"" + sel[1] + "\"";
            search();
        };

        var ac = new AutoComplete("autocomplete_input", "autocomplete_container", ds);
        ac.generateRequest = function() {
            return '?n=5&core=' + SEARCH.ui.coreName + '&userinput=' + this.getInputEl().value.encodeForRequest();
        };
        ac.itemSelectEvent.subscribe(itemSelectHandler);

        Event.addListener("autocomplete_input", "keyup", function(e) {
            if (this.value == "") {
                FACET.ui.buildInitialFacetTree();
            }
        });

        Event.addListener("reset", "click", function (e) {
            Event.stopEvent(e);
            SEARCH.ui.resetQuerySearchInputs();
            FACET.ui.buildInitialFacetTree();
        });

        function search() {
            var fq = FACET.util.getFacetFilterQueryString() + DATEPICK.util.getDateConstraintFilterQueryString();
            if (SEARCH.ui.search(fq) > 0) {
                FACET.ui.buildFacetTree(SEARCH.ui.facetsFromLastSearch);
            }
        }

        Event.addListener("submit", "click", function (e) {
            Event.stopEvent(e);
            search();
        });

    })();
    </script>
</layout:main>
