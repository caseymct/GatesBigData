<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />

    <h1 id="search_header">Search Core</h1>

    <form id = "search_form">
        <div id="search_tab" class="yui-navset">
            <ul class="yui-nav">
                <li class="selected tab_selected"><a href="#tab1"><em>Supplier</em></a></li>
                <li><a href="#tab2"><em>Company</em></a></li>
                <li><a href="#tab3"><em>User</em></a></li>
                <li><a href="#tab4"><em>General Query</em></a></li>
            </ul>
            <div class="yui-content">
                <div class="search_tab_style row" id="supplier_name_tab">
                    <label for="supplier_name_input">Supplier Name:</label>
                    <div id="supplier_name_autocomplete">
                        <input type="text" id="supplier_name_input"/>
                        <div id="supplier_name_autocomplete_container"></div>
                    </div>
                </div>
                <div class="search_tab_style row" id="search_company_tab">
                    <div class="row">
                        <label for="company_name_input">Company Site Name: </label>
                        <div id="company_name_autocomplete">
                            <input type="text" id="company_name_input" />
                            <div id="company_name_autocomplete_container"></div>
                        </div>
                    </div>
                    <div class="row">
                        <label for="account_name_input">Account Name: </label>
                        <div id="account_name_autocomplete">
                            <textarea id="account_name_input"></textarea>
                            <div id="account_name_autocomplete_container"></div>
                        </div>
                    </div>
                    <div class="row">
                        <label for="cost_center_name_input">Cost Center Name: </label>
                        <div id="cost_center_name_autocomplete">
                            <input type="text" id="cost_center_name_input" />
                            <div id="cost_center_name_autocomplete_container"></div>
                        </div>
                    </div>
                    <div class="row"></div>
                </div>
                <div class="search_tab_style row" id="user_name_tab">
                    <label for="user_name_input">User Name:</label>
                    <div id="user_name_autocomplete">
                        <input type="text" id="user_name_input"/>
                        <div id="user_name_autocomplete_container"></div>
                    </div>
                </div>
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
        </div>

        <div class="clearboth"></div>

        <div id="facet_options">
            <div class="clearboth"></div>
        </div>

        <div class="buttons" style="padding-bottom: 5px">
            <a href="#" class="button small" id="submit">Search</a>
            <a href="#" class="button small" id="reset">Reset</a>
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
        <div id="search_result_container" >
            <div id = "show_query"></div>
            <div id = "num_found"></div>
            <div id = "search_results" ></div>
        </div>

        <div id="overlay" style="visibility:hidden">
            <div id="tree_view" class="bd"></div>
        </div>

        <div class = "clearboth"></div>
    </div>

    <div id = "pag1"></div>

    <script type="text/javascript" src="<c:url value="/static/js/search.js" />"></script>

    <script type="text/javascript">
    (function() {
        var     Event   = YAHOO.util.Event,   AutoComplete = YAHOO.widget.AutoComplete,
                Dom  = YAHOO.util.Dom,        ScrollingDataTable = YAHOO.widget.ScrollingDataTable,
                Connect = YAHOO.util.Connect, LocalDataSource = YAHOO.util.LocalDataSource,
                Json = YAHOO.lang.JSON,       XHRDataSource = YAHOO.util.XHRDataSource;

        var inputElNames = [
            "account_name_input", "company_name_input", "supplier_name_input", "cost_center_name_input", "user_name_input"
        ];

        var columnDefs = [
            {key:'PaidDate', label:'Paid Date', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.shortStringWidth },
            {key:'Account.AccountName', label:'Account Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'AccountCompanyCode', label:'Company Code', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'User.UserName', label:'User Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'CompanySite.SiteName', label:'Site Name', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'Supplier.SupplierName', label:'Supplier Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'CostCenter.CostCenterName', label:'Cost Center Name', sortable:true, formatter:SEARCH.ui.formatLink, width:SEARCH.ui.longStringWidth},
            {key:'CostCenterId', label:'Cost Center ID', sortable:true, formatter:SEARCH.ui.formatLink},
            {key:'InvoiceId', label:'Invoice ID', sortable:true, formatter:SEARCH.ui.formatLink}
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
            {key:'url', parser:'text'},
            {key:'HDFSKey', parser:'text'},
            {key:'HDFSSegment', parser:'text'}
        ];

        LWA.ui.initWait();
        SEARCH.ui.setSearchHeader("search_header");
        SEARCH.ui.initGeneralQuerySearchInput("general_query_search_input");
        SEARCH.ui.initPaginator("pag1");
        SEARCH.ui.initSearchTab("search_tab");
        SEARCH.ui.initSortOrderButtonGroup("sort_ascdesc_buttongroup");
        SEARCH.ui.initSelectData(columnDefs);
        SEARCH.ui.initSortBySelect("sort_date_label", 0);
        SEARCH.ui.initExportUrl('<c:url value="/export" />');

        Connect.asyncRequest('GET', '<c:url value="/search/solrfacets" />' + "?core=" + SEARCH.ui.coreName, {
            success : SEARCH.ui.buildInitialFacetTree
        });

        /*var updateMenuOnTabChange = function(e) {
            switch (this.get("activeTab").get("label")) {
                case "User":
                    SEARCH.ui.sortBySelect.set("selectedMenuItem", SEARCH.ui.sortBySelect.getMenu().getItem(2));
                    break;
                case "Supplier":
                    SEARCH.ui.sortBySelect.set("selectedMenuItem", SEARCH.ui.sortBySelect.getMenu().getItem(4));
                    break;
                case "Company":
                    SEARCH.ui.sortBySelect.set("selectedMenuItem", SEARCH.ui.sortBySelect.getMenu().getItem(0));
                    break;
            }
        };
        SEARCH.ui.searchTab.on("activeTabChange", updateMenuOnTabChange);   */

        /* Autocomplete code */
        var ds = new XHRDataSource('<c:url value="/search/suggest" />' );
        ds.responseType = XHRDataSource.TYPE_JSON;
        ds.responseSchema = { resultsList : "suggestions" };

        var acRequestName = ["account", "companysite", "supplier", "costcenter", "user"];
        var itemSelectHandler = function(s, args) {
            var inputEl = Dom.get(args[0].getInputEl()), sel = args[2][0];
            inputEl.value = sel.substring(0, sel.lastIndexOf("(") - 1);
            if (inputEl.id.match(/user|supplier/) != null) {
                search();
            }
        };

        for(var i = 0; i < acRequestName.length; i++) {
            var acContainer = inputElNames[i].replace("_input", "_autocomplete_container");
            var ac = new AutoComplete(inputElNames[i], acContainer, ds);
            ac.generateRequest = (function(n) {
                return function() {
                    return '?core=' + SEARCH.ui.coreName + '&f=' + acRequestName[n] +
                            '&userinput=' + SEARCH.util.encodeForRequest(this.getInputEl().value);
                };
            })(i);
            ac.itemSelectEvent.subscribe(itemSelectHandler);
        }


        var buildSearchResultHtml = function(result) {
            var dataSource = new LocalDataSource(result.response);
            dataSource.responseSchema = {
                resultsList:'docs',
                fields: dataSourceFields
            };

            var dataTable = new ScrollingDataTable('search_results', columnDefs, dataSource, { width:"100%" });

            dataTable.subscribe("sortedByChange", function(e) {
                var sortField = e.newValue.column.key;
                SEARCH.ui.dataTableSortedByChange(sortField);
                search();
            });

            dataTable.subscribe("cellClickEvent", function (e) {
                Event.stopEvent(e.event);
                SEARCH.ui.dataTableCellClickEvent(this.getRecord(e.target).getData(), '<c:url value="/core/document/view" />');
            });

            dataTable.on('cellMouseoverEvent', SEARCH.ui.showTooltip);
            dataTable.on('cellMouseoutEvent', SEARCH.ui.hideTooltip);
        };


        var getFilterQueryString = function() {
               var company = Dom.get("company_name_input").value,
                   account = Dom.get("account_name_input").value,
                costCenter = Dom.get("cost_center_name_input").value,
                  supplier = Dom.get("supplier_name_input").value,
                  username = Dom.get("user_name_input").value,
                     fqStr = SEARCH.util.getFilterQueryString();

            switch (SEARCH.ui.getSearchTabActiveIndex()) {
                case 0:
                    fqStr += (supplier != "") ? "%2BSupplier.SupplierName:(\"" + SEARCH.util.encodeForRequest(supplier) + "\")" : "";
                    break;
                case 1:
                    fqStr += (company != "") ? "%2BCompanySite.SiteName:(\"" + SEARCH.util.encodeForRequest(company) + "\")" : "";
                    fqStr += (account != "") ? "%2BAccount.AccountName:(\"" + SEARCH.util.encodeForRequest(account) + "\")" : "";
                    fqStr += (costCenter != "") ? "%2BCostCenter.CostCenterName:(\"" + SEARCH.util.encodeForRequest(costCenter) + "\")" : "";
                    break;
                case 2:
                    fqStr += (username != "") ? "%2BUser.UserName:(\"" + SEARCH.util.encodeForRequest(username) + "\")" : "";
                    break;
                default:
                    break;
            }
            return fqStr;
        };

        Event.addListener("reset", "click", function (e) {
            Event.stopEvent(e);
            SEARCH.ui.resetGeneralQuerySearchInput();
            for (i = 0; i < inputElNames.length; i++) {
                Dom.get(inputElNames[i]).value = "";
            }
        });

        var handlePagination = function (newState) {
            LWA.ui.showWait();
            var fq = getFilterQueryString();
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
            var fq = getFilterQueryString();
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
