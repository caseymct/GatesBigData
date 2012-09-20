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
                <li><a href="#tab3"><em>General Query</em></a></li>
            </ul>
            <div class="yui-content">
                <div class="search_tab_style row" id="supplier_name_tab">
                    <label for="supplier_name_input">Supplier Name:</label>
                    <input type="text" id="supplier_name_input"/>
                </div>
                <div class="search_tab_style row" id="search_company_tab" class="search_tab_style">
                    <div class="row">
                        <label for="company_name_input">Company Site Name: </label>
                        <input type="text" id="company_name_input" />
                    </div>
                    <div class="row">
                        <label for="account_name_input">Account Name: </label>
                        <input type="text" id="account_name_input" />
                    </div>
                    <div class="row">
                        <label for="cost_center_name_input">Cost Center Name: </label>
                        <input type="text" id="cost_center_name_input" />
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
            <a href="#" class="button small" id="submit">Submit</a>
            <a href="#" class="button small" id="reset">Reset query fields</a>
        </div>
        <div class = "row"></div>
    </form>

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

        var Dom = YAHOO.util.Dom,
            Event = YAHOO.util.Event,
            Connect = YAHOO.util.Connect,
            Json = YAHOO.lang.JSON,
            Button = YAHOO.widget.Button,
            ButtonGroup = YAHOO.widget.ButtonGroup,
            TreeView = YAHOO.widget.TreeView,
            TextNode = YAHOO.widget.TextNode,
            TabView = YAHOO.widget.TabView,
            Overlay = YAHOO.widget.Overlay;

        var searchTab = new TabView('search_tab');
        var treeView = new TreeView("tree_view");

        // Sort buttons code
        var sortType = "date", ascType = "asc",
            sortTypeButtonGroup = new ButtonGroup("sort_type_buttongroup"),
            ascTypeButtonGroup = new ButtonGroup("sort_ascdesc_buttongroup");
        ascTypeButtonGroup.check(0);
        sortTypeButtonGroup.on("checkedButtonChange", function (o) { sortType = o.newValue.get("value"); });
        ascTypeButtonGroup.on("checkedButtonChange", function (o) { ascType = o.newValue.get("value"); });

        // Overlay code
        var narrowSearchOverlay = new YAHOO.widget.Overlay("overlay", {
            context: ["show_overlay","tl","bl", ["beforeShow", "windowResize"]],
            visible:false,
            width:"400px"
        });
        narrowSearchOverlay.render(document.body);

        Event.addListener("show_overlay", "click", function(e) {
            if (Dom.hasClass("show_overlay", "button down-big-overlay")) {
                narrowSearchOverlay.show();
                Dom.removeClass("show_overlay", "button down-big-overlay");
                Dom.addClass("show_overlay", "button up-big-overlay");
            } else {
                narrowSearchOverlay.hide();
                Dom.addClass("show_overlay", "button down-big-overlay");
                Dom.removeClass("show_overlay", "button up-big-overlay");
            }
        }, narrowSearchOverlay, true);

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

        /* Sort keys so that title and author come first */
        var sortKeys = function(unsortedkeys) {
            var sortedkeys = [];
            var strings = ["title", "author", "creator", "url"];

            for(var i = 0; i < strings.length; i++) {
                var index = unsortedkeys.indexOf(strings[i]);
                if (index != -1) {
                    sortedkeys.push(strings[i]);
                    unsortedkeys.splice(index, 1);
                }
            }

            return sortedkeys.concat(unsortedkeys.sort());
        };

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

        var buildSearchResultHtml = function(result) {
            var docs = result.response.docs,
                highlighting = result.highlighting,
                searchResults = Dom.get("search_results"),
                i, j, containerDiv, childDiv, innerContainerDiv, titleAnchor, titleAnchorText, anchor;

            LWA.ui.removeDivChildNodes("search_results");

            for(i = 0; i < docs.length; i++) {
                containerDiv = LWA.ui.createDomElement("div", searchResults, [
                    { key : "class", value : "search-result-div" } ]);

                var sortedkeys = sortKeys(Object.keys(docs[i]));
                for(j = 0; j < sortedkeys.length; j++) {
                    var key = sortedkeys[j];
                    if (key.indexOf("HDFS") == -1) {
                        var value = (docs[i][key] == "") ? "<i>No value</i>" : LWA.util.stripBrackets(docs[i][key]);
                        innerContainerDiv = LWA.ui.createDomElement("div", containerDiv, []);

                        /*if (key == "title" && docs[i].hasOwnProperty("id")) {
                            childDiv = LWA.ui.createDomElement("div", containerDiv, []);
                            titleAnchor = LWA.ui.createDomElement("a", childDiv, [
                                { key : "class", value: "search-result-header" },
                                { key : "id",    value: "titleanchor" + i },
                                { key : "style", value: "color: steelblue" }, { key : "target", value : "_blank" }]);
                            setHrefAttribute(titleAnchor, docs[i]);
                            titleAnchor.appendChild(document.createTextNode(LWA.util.stripBrackets(docs[i].title)));

                        } else { }*/

                        LWA.ui.createDomElement("div", innerContainerDiv, [
                            { key : "text",  value : key },
                            { key : "class", value : "search-result-inner-container-label" } ]);

                        LWA.ui.createDomElement("div", innerContainerDiv, [
                            { key : "text",  value : "<a href=\"#\">" + value + "</a>"},
                            { key : "id",    value : key + "_" + i },
                            { key : "class", value : "search-result-inner-container" } ]);

                        Event.addListener(key + "_" + i, "click", function(e) {
                            Event.stopEvent(e);
                            searchTab.set('activeIndex', 2);

                            var currentValue = Dom.get("general_query_search_input").value;
                            var newSearchKey = this.id.replace(/_[0-9]+$/, "");
                            var newSearchVal = "\"" + this.innerHTML.replace("<a href=\"#\">", "").replace("</a>", "") + "\"";
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
                        });

                        if (j == sortedkeys.length - 1) {
                            anchor = LWA.ui.createDomElement("a", innerContainerDiv, [
                                { key : "class", value: "search-result-button button add" },
                                { key : "style", value: "padding-right: 3px"},
                                { key : "id",    value: "expand_" + i } ]);
                            anchor.setAttribute("href", "hdfs://" + docs[i]["HDFSSegment"] + ";" + docs[i]["HDFSKey"]);

                            Event.addListener("expand_" + i, "click", function(e) {
                                Event.stopEvent(e);
                                var href = this.href.substring("hdfs://".length).split(";");
                                window.open('<c:url value="/core/" />' +
                                        "collection1/document/view?segment=" + href[0] + "&file=" + href[1], "_blank");
                            });
                        }
                    }
                }

                LWA.ui.createDomElement("div", containerDiv, [ { key: "style", value : "clear:both" } ]);
            }
        };

        var encodeForRequest = function(s) { return s.replace(/&amp;/g, "%26"); };

        var getFilterQueryString = function() {
            var company = Dom.get("company_name_input").value,
                account = Dom.get("account_name_input").value,
                costCenter = Dom.get("cost_center_name_input").value,
                supplier = Dom.get("supplier_name_input").value;

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
            Dom.get("supplier_name_input").value = "";
        });

        Event.addListener("submit", "click", function (e) {
            Event.stopEvent(e);


            var queryTerms = Dom.get("general_query_search_input").value,
                coreName = "collection1",
                //coreName = Dom.get("core_name_select").value,
                fq = getFilterQueryString();


            var urlParams = "?query=" + queryTerms + "&core=" + coreName + "&sort=" + sortType + "&order=" + ascType;
            if (fq != "") {
                urlParams += "&fq=" + fq;
            }
            Dom.get("show_query").innerHTML = "Submitting to solr with<br>q: " + queryTerms + "<br>fq: " + decodeURIComponent(fq);

            var pag = new YAHOO.widget.Paginator( {rowsPerPage : 10, containers : [ "pag1" ] });

            var handlePagination = function (newState) {
                Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + urlParams + "&start=" + newState.records[0], {
                    success: function(o) {
                        var result = Json.parse(o.responseText);
                        buildSearchResultHtml(result);
                    }
                });

                pag.setState(newState);
            };

            pag.subscribe('changeRequest', handlePagination);

            Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + urlParams, {
                success : function(o) {

                    var result = Json.parse(o.responseText);
                    var numFound = result.response.numFound,
                        facets = result.response.facets;

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
