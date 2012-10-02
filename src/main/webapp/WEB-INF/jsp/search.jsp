<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />

    <h1>Search</h1>

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
                            <!--<input type="text" id="account_name_input" />-->
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

        <!--
        <div class="row" id = "collection_name">
            <label id ="collection_name_label" for="core_name_select">Select a collection: </label>
            <select id ="core_name_select"></select>
        </div>
        -->
        <div class="clearboth"></div>
        <div class="row" id = "sort_by_div">

            <div id = "sort_ascdesc_buttongroup" class = "yui-buttongroup search-button-style">
                <label for="sort_asc" id="sort_asc_label">Sort by: </label>
                <input id="sort_asc" type="radio" name="sorttype" value="asc" >
                <input id="sort_desc" type="radio" name="sorttype" value="desc" checked>
            </div>

            <div id = "sort_type_buttongroup" class = "yui-buttongroup search-button-style" style="margin-left: 20px">
                <label for="sort_date" id="sort_date_label">Order by: </label>
                <input id="sort_date" type="radio" name="sorttype" value="date" checked>
                <input id="sort_relevance" type="radio" name="sorttype" value="score">
                <input id="sort_random" type="radio" name="sorttype" value="random">
            </div>
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
        <div id="search_result_container" >
            <div id = "show_query"></div>
            <div id = "num_found"></div>
            <div id = "search_results" ></div>
        </div>
        <div id="facet_container">
            <a class = "button down-big-overlay" id = "show_overlay">Narrow your search</a>
            <div id="overlay" style="visibility:hidden">
                <div id="tree_view" class="bd"></div>
            </div>
        </div>
        <div class = "clearboth"></div>
    </div>

    <div id = "pag1"></div>


    <script type="text/javascript">
    (function() {

        var Dom      = YAHOO.util.Dom,                    Event = YAHOO.util.Event,
            Connect  = YAHOO.util.Connect,         AutoComplete = YAHOO.widget.AutoComplete,
            Button   = YAHOO.widget.Button,         ButtonGroup = YAHOO.widget.ButtonGroup,
            TreeView = YAHOO.widget.TreeView,      SimpleDialog = YAHOO.widget.SimpleDialog,
            TabView  = YAHOO.widget.TabView, ScrollingDataTable = YAHOO.widget.ScrollingDataTable,
            Overlay  = YAHOO.widget.Overlay,          Paginator = YAHOO.widget.Paginator,
            Tooltip  = YAHOO.widget.Tooltip,      XHRDataSource = YAHOO.util.XHRDataSource,
            TextNode = YAHOO.widget.TextNode, Json = YAHOO.lang.JSON;

        LWA.ui.initWait();

        /* Autocomplete code */
        var ds = new XHRDataSource('<c:url value="/search/suggest" />');
        ds.responseType = XHRDataSource.TYPE_JSON;
        ds.responseSchema = { resultsList : "suggestions" };

        var acInputs = ["account_name_input", "company_name_input", "supplier_name_input", "cost_center_name_input", "user_name_input"];
        var acContainers = ["account_name_autocomplete_container", "company_name_autocomplete_container", "supplier_name_autocomplete_container",
                            "cost_center_name_autocomplete_container", "user_name_autocomplete_container"];
        var acRequestName = ["account", "companysite", "supplier", "costcenter", "user"];

        for(var i = 0; i < acRequestName.length; i++) {
            var ac = new AutoComplete(acInputs[i], acContainers[i], ds);
            ac.generateRequest = (function(n) {
                return function() {
                    return '?f=' + acRequestName[n] + '&userinput=' + encodeURIComponent(this.getInputEl().value);
                };
            })(i);
        }

        var searchTab = new TabView('search_tab');
        var treeView  = new TreeView("tree_view");
        var pag = new Paginator( {rowsPerPage : 25, containers : [ "pag1" ] });
        var tt = new Tooltip("tool_tip");
        var generalquery_tabindex = 3;

        // Sort buttons code
        var sortType = "date", ascType = "asc",
            sortTypeButtonGroup = new ButtonGroup("sort_type_buttongroup"),
            ascTypeButtonGroup = new ButtonGroup("sort_ascdesc_buttongroup");
        ascTypeButtonGroup.check(0);
        sortTypeButtonGroup.on("checkedButtonChange", function (o) { sortType = o.newValue.get("value"); });
        ascTypeButtonGroup.on("checkedButtonChange", function (o) { ascType = o.newValue.get("value"); });

        // Overlay code
        var narrowSearchOverlay = new Overlay("overlay", {
            context: ["show_overlay","tl","bl", ["beforeShow", "windowResize"]],
            visible: false,
            width:"400px"
        });
        narrowSearchOverlay.render(document.body);

        var showOverlay = function() {
            narrowSearchOverlay.show();
            Dom.removeClass("show_overlay", "button down-big-overlay");
            Dom.addClass("show_overlay", "button up-big-overlay");
        };
        var hideOverlay = function() {
            narrowSearchOverlay.hide();
            Dom.addClass("show_overlay", "button down-big-overlay");
            Dom.removeClass("show_overlay", "button up-big-overlay");
        };
        Event.addListener("show_overlay", "click", function(e) {
            if (Dom.hasClass("show_overlay", "button down-big-overlay")) {
                showOverlay();
            } else {
                hideOverlay();
            }
        }, narrowSearchOverlay, true);

        /* Export dialog code */
        var exportFile = function(type, dlg) {
            window.open('<c:url value="/export" />' + "?type=" + type + "&file=" + Dom.get("export_file_name").value);
            window.focus();
            dlg.hide();
        };
        var handleCSVExport  = function() { exportFile("csv", this); };
        var handleJSONExport = function() { exportFile("json", this); };
        var hideDlg          = function() { this.hide() };

        var exportDlg = new SimpleDialog("exportDialog", {
            width: "20em", fixedcenter: true,  modal: true, visible: false,
            buttons : [ { text: "CSV", handler: handleCSVExport, isDefault:true },
                        { text: "JSON", handler: handleJSONExport },
                        { text: "Cancel", handler: hideDlg }]
        });
        exportDlg.render(document.body);

        Event.addListener("export", "click", function (e) {
            Event.stopEvent(e);
            exportDlg.show();
        });

        // Get collection names for search select box
        /*
        Connect.asyncRequest('GET', '<c:url value="/core/corenames" />', {
            success : function(o) {
                var i, result = Json.parse(o.responseText);

                for(i = 0; i < result.names.length; i++) {
                    LWA.ui.createDomElement("option", Dom.get("core_name_select"),
                      [ { key: "text", value: result.names[i]}, { key: "value", value: result.names[i]}]);
                }
            },
            failure : function(o) {
                alert("Could not retrieve collection names.");
            }
        });  */

        var buildFacetTree = function(facets) {
            var i, j, nameNode, valueNode, root = treeView.getRoot();

            treeView.removeChildren(root);

            for(i = 0; i < facets.length; i++) {
                var parent = root;
                if (facets[i].name.indexOf(".") > 0) {
                    var facetNameArray = facets[i].name.split(".");
                    var facetParentName = facetNameArray[0], facetChildName = facetNameArray[1];
                    var parentNode = treeView.getNodeByProperty("label", facetParentName);
                    if (parentNode == null) {
                        parentNode = new TextNode(facetParentName, root, false);
                    }
                    parent = parentNode;
                    facets[i].name = facetChildName;
                }

                if (facets[i].values.length > 0 &&
                        !(facets[i].values.length == 1 && facets[i].values[0].match(/^\s\([0-9]+\)$/))) {
                    nameNode = new TextNode(facets[i].name, parent, false);

                    for(j = 0; j < facets[i].values.length; j++) {
                        valueNode = new TextNode(facets[i].values[j], nameNode, false);
                        valueNode.isLeaf = true;
                    }
                }
            }
            treeView.render();
            treeView.subscribe("clickEvent", function(e) {
                Event.stopEvent(e);
                if (treeView.getRoot() == e.node.parent) {
                    return;
                }

                var node = e.node;
                var anchorText = (!(node.parent.parent instanceof YAHOO.widget.RootNode) ? node.parent.parent.label + "." : "")
                                    + node.parent.label + " : " + node.label.substring(0, node.label.lastIndexOf("(") - 1);

                if (Dom.inDocument("treeNode" + node.index) == false) {
                    var anchor = LWA.ui.createDomElement("a", Dom.get("facet_options"), [
                        { key : "class", value: "button delete" },
                        { key : "id", value : "treeNode" + node.index },
                        { key : "style", value: "margin: 2px" }]);
                    anchor.appendChild(document.createTextNode(anchorText));

                    Event.addListener("treeNode" + node.index, "click", function(e) {
                        LWA.ui.removeElement("treeNode" + node.index);
                    });

                    narrowSearchOverlay.align("tl", "bl");
                }
            });
        };

        Connect.asyncRequest('GET', '<c:url value="/search/solrfacets?core=collection1" />', {
            success : function(o) {
                var result = Json.parse(o.responseText);
                Dom.setStyle("show_overlay", "visibility", "visible");
                showOverlay();
                buildFacetTree(result.response.facets);
            }
        });

        var viewFullRecordFormatter = function(el, oRecord, oColumn, oData) {
            el.innerHTML = '<a href="#" class="button expand" />';
        };
        var formatLink = function(el, record, column, data) {
            if (data == undefined) {
                data = "<i>No value</i>";
            } if (data.length > 20) {
                data = data.substring(0, 20) + " ...";
            }
            el.innerHTML = '<a href="#">' + data + '</a>';
        };

        var formatDate = function(el, record, column, data) {
            data = (data == undefined) ? "<i>No value</i>" : YAHOO.util.Date.format(data, {format: "%I:%M:%S %p %Y-%m-%d %Z"});
            el.innerHTML = '<a href="#">' + data + '</a>';
        };

        var buildSearchResultHtml = function(result) {
            var dataSource = new YAHOO.util.LocalDataSource(result.response);
            dataSource.responseSchema = {
                resultsList:'docs',
                fields:[
                    {key:'Account.AccountName', parser:'text'},
                    {key:'AccountCompanyCode', parser:'text'},
                    {key:'User.UserName', parser:'text'},
                    {key:'Amount', parser:'number'},
                    {key:'CompanySite.SiteName', parser:'text'},
                    {key:'CostCenter.CostCenterName', parser:'text'},
                    {key:'CostCenterId', parser:'text'},
                    {key:'InvoiceId', parser:'number'},
                    {key:'PaidDate', parser:'text'},
                    {key:'content_type', parser:'text'},
                    {key:'timestamp', parser:'date'},
                    {key:'url', parser:'text'},
                    {key:'HDFSKey', parser:'text'},
                    {key:'HDFSSegment', parser:'text'}
                ]
            };

            var columnDefs = [
                {key:'url', label:'', formatter: viewFullRecordFormatter },
                {key:'Account.AccountName', label:'Account Name', sortable:true, formatter:formatLink, width:210},
                {key:'AccountCompanyCode', label:'Company Code', sortable:true, formatter:formatLink},
                {key:'User.UserName', label:'User Name', sortable:true, formatter:formatLink, width:210},
                {key:'CompanySite.SiteName', label:'Site Name', sortable:true, formatter:formatLink},
                {key:'CostCenter.CostCenterName', label:'Cost Center Name', sortable:true, formatter:formatLink, width:210},
                {key:'CostCenterId', label:'Cost Center ID', sortable:true, formatter:formatLink},
                {key:'InvoiceId', label:'Invoice ID', sortable:true, formatter:formatLink},
                {key:'PaidDate', label:'Paid Date', sortable:true, formatter:formatLink, width:80},
                {key:'timestamp', label:'Timestamp', sortable:true, formatter:formatDate, width:200},
                {key:'content_type', label:'Content type', sortable:true, formatter:formatLink}
            ];

            var dataTable = new ScrollingDataTable('search_results', columnDefs, dataSource, {
                draggableColumns: true, width:"100%" });

            dataTable.subscribe("cellClickEvent", function (args) {
                Event.stopEvent(args.event);
                var target = args.target,
                    record = this.getRecord(target),
                    column = this.getColumn(target),
                    data   = record.getData();

                switch (column.key) {
                    case "url" :
                        var urlParams = "?core=collection1&segment=" + data.HDFSSegment + "&file=" + data.HDFSKey + "&view=preview";
                        window.open('<c:url value="/core/document/view" />' + urlParams, "_blank");
                        break;
                    default:
                        searchTab.set('activeIndex', generalquery_tabindex);

                        var currentValue = Dom.get("general_query_search_input").value;
                        var newSearchKey = column.getField();
                        var newSearchVal = "\"" + data[newSearchKey] + "\"";
                        var newValue = currentValue;

                        if (currentValue == "*:*" || currentValue == "") {
                            newValue = newSearchKey + ":(" + newSearchVal + ")";

                        } else if (currentValue.indexOf(newSearchVal) == -1 ) {
                            var keyIdx = currentValue.indexOf(newSearchKey);
                            if (keyIdx >= 0) {
                                var sliceIdx = keyIdx + newSearchKey.length + 2;
                                newValue = currentValue.slice(0, sliceIdx) + newSearchVal + " OR " + currentValue.slice(sliceIdx);
                            } else {
                                newValue = currentValue + " AND " + newSearchKey + ":(" + newSearchVal + ")";
                            }
                        }
                        Dom.get("general_query_search_input").value = newValue;
                        break;
                }
            });

            dataTable.on('cellMouseoverEvent', function (oArgs) {
                var target = oArgs.target;
                var column = this.getColumn(target), record = this.getRecord(target);
                var idx = target.innerText.indexOf(" ...");
                if (idx != -1 && idx === target.innerText.length - 5) {
                    tt.setBody(record.getData()[column.getField()]);
                    tt.cfg.setProperty('xy', [oArgs.event.pageX, oArgs.event.pageY]);
                    tt.show();
                }
            });

            dataTable.on('cellMouseoutEvent', function (oArgs) {
                tt.hide();
            });
        };

        var encodeForRequest = function(s) { return s.replace(/&amp;/g, "%26"); };

        var getFilterQueryString = function() {
            var company = Dom.get("company_name_input").value,
                account = Dom.get("account_name_input").value,
                costCenter = Dom.get("cost_center_name_input").value,
                supplier = Dom.get("supplier_name_input").value,
                username = Dom.get("user_name_input").value;

            var facetOptions = {}, domFacets = Dom.get("facet_options").children;
            for (var i = 0; i < domFacets.length; i++) {
                if (!Dom.hasClass(domFacets[i], "clearboth")) {
                    var t = domFacets[i].innerHTML.split(" : ");
                    var o = "\"" + t[1] + "\"";
                    facetOptions[t[0]] = facetOptions.hasOwnProperty(t[0]) ? facetOptions[t[0]] + " " + o: o;
                }
            }

            var fqStr = "";
            if (Object.keys(facetOptions).length > 0) {
                for (var key in facetOptions) {
                    fqStr += "%2B" + key + ":(" + encodeURIComponent(facetOptions[key]) + ")";
                }
            }

            switch (searchTab.get('activeIndex')) {
                case 0:
                    fqStr += (supplier != "") ? "%2BSupplier.SupplierName:(\"" + encodeForRequest(supplier) + "\")" : "";
                    break;
                case 1:
                    fqStr += (company != "") ? "%2BCompanySite.SiteName:(\"" + encodeForRequest(company) + "\")" : "";
                    fqStr += (account != "") ? "%2BAccount.AccountName:(\"" + encodeForRequest(account) + "\")" : "";
                    fqStr += (costCenter != "") ? "%2BCostCenter.CostCenterName:(\"" + encodeForRequest(costCenter) + "\")" : "";
                    break;
                case 2:
                    fqStr += (username != "") ? "%2BUser.UserName:(\"" + encodeForRequest(username) + "\")" : "";
                    break;
                default:
                    break;
            }
            return fqStr;
        };

        Event.addListener("reset", "click", function (e) {
            Event.stopEvent(e);
            Dom.get("general_query_search_input").value = "*:*";
            Dom.get("company_name_input").value = "";
            Dom.get("account_name_input").value = "";
            Dom.get("cost_center_name_input").value = "";
            Dom.get("user_name_input").value = "";
            Dom.get("supplier_name_input").value = "";
        });



        Event.addListener("submit", "click", function (e) {
            Event.stopEvent(e);
            LWA.ui.showWait();

            var queryTerms = Dom.get("general_query_search_input").value,
                coreName = "collection1",
                //coreName = Dom.get("core_name_select").value,
                fq = getFilterQueryString();


            var urlParams = "?query=" + queryTerms + "&core=" + coreName + "&sort=" + sortType + "&order=" + ascType;
            if (fq != "") {
                urlParams += "&fq=" + fq;
            }
            Dom.get("show_query").innerHTML = "Submitting to solr with<br>q: " + queryTerms + "<br>fq: " + decodeURIComponent(fq);

            var handlePagination = function (newState) {
                LWA.ui.showWait();

                Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + urlParams + "&start=" + newState.records[0], {
                    success: function(o) {
                        LWA.ui.hideWait();

                        var result = Json.parse(o.responseText);
                        buildSearchResultHtml(result);
                    }
                });

                pag.setState(newState);
            };

            pag.subscribe('changeRequest', handlePagination);

            Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + urlParams, {
                success : function(o) {
                    LWA.ui.hideWait();

                    var result = Json.parse(o.responseText);
                    var numFound = result.response.numFound, facets = result.response.facets;

                    pag.set('totalRecords', numFound);
                    pag.render();

                    Dom.get("num_found").innerHTML = "Found " + numFound + " document" + ((numFound > 1) ? "s" : "");
                    Dom.setStyle("show_overlay", "visibility", "visible");

                    buildSearchResultHtml(result);
                    buildFacetTree(facets);
                },
                failure : function(o) {
                    alert("Could not connect.");
                }
            });
        });


    })();
    </script>
</layout:main>
