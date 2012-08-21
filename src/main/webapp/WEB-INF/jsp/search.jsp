<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />

    <h1>Search</h1>

    <form id = "search_form">
        <div class = "row" id="search_div">
            <label for="search_input">Query terms: </label>
            <textarea id="search_input">*:*</textarea>
        </div>

        <div class="row" id = "collection_name">
            <label id ="collection_name_label" for="collection_name_select">Select a collection: </label>
            <select id = "collection_name_select"></select>
        </div>

        <div class="row" id = "sort_by_div">
            <label for="sort_type_buttongroup" id="sort_type_buttongroup_label">Sort by: </label>
            <div id = "sort_ascdesc_buttongroup" class = "yui-buttongroup" style="float:right">
                <input id="sort_asc" type="radio" name="sorttype" value="asc" >
                <input id="sort_desc" type="radio" name="sorttype" value="desc" checked>
            </div>

            <label for="sort_ascdesc_buttongroup" id ="sort_ascdesc_buttongroup_label">Order by: </label>
            <div id = "sort_type_buttongroup" class = "yui-buttongroup">
                <input id="sort_date" type="radio" name="sorttype" value="date" checked>
                <input id="sort_relevance" type="radio" name="sorttype" value="score">
                <input id="sort_random" type="radio" name="sorttype" value="random">
            </div>
        </div>

        <div class="clearboth"></div>

        <div id="facet_options">
            <div class="clearboth"></div>
        </div>

        <div class="buttons">
            <a href="#" class="button small" id="submit">Submit</a>
            <a href="#" class="button small" id="reset">Reset query</a>
        </div>
        <div class = "row"></div>
    </form>

    <div>
        <div id="search_result_container" >
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
            Overlay = YAHOO.widget.Overlay;

        var treeView = new YAHOO.widget.TreeView("tree_view");

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
        Connect.asyncRequest('GET', '<c:url value="/collection/collectionNames" />', {
            success : function(o) {
                var i, result = Json.parse(o.responseText);

                for(i = 0; i < result.names.length; i++) {
                    LWA.ui.createDomElement("option", Dom.get("collection_name_select"),
                      [ { key: "text", value: result.names[i]}, { key: "value", value: result.names[i]}]);
                }
            },
            failure : function(o) {
                alert("Could not retrieve collection names.");
            }
        });

        var setHrefAttribute = function(a, doc) {
            a.setAttribute("href", doc.id.match(/http*/) ? doc.id : "file:///" + doc.id);
        };

        var getHighlightedText = function(index, highlighting, queryTerms) {
            if (highlighting.hasOwnProperty(index)) {
                var body = highlighting[index].body[0];
                body = body.replace(/^\s+|\s+$/g, '').replace(/\n/g, "<br>");
                return body;
            }
            return "";
        };

        /* Sort keys so that title and author come first */
        var sortKeys = function(unsortedkeys) {
            var sortedkeys = [];

            var titleindex = unsortedkeys.indexOf("title");
            if (titleindex!= -1) {
                sortedkeys.push("title");
                unsortedkeys.splice(titleindex, 1);
            }

            var authorindex = unsortedkeys.indexOf("author");
            if (authorindex != -1) {
                sortedkeys.push("author");
                unsortedkeys.splice(authorindex, 1);
            }

            var creatorindex = unsortedkeys.indexOf("creator");
            if (creatorindex != -1) {
                sortedkeys.push("creator");
                unsortedkeys.splice(creatorindex, 1);
            }
            return sortedkeys.concat(unsortedkeys.sort());
        };

        var buildFacetTree = function(facets) {
            var i, j, nameNode, valueNode, root = treeView.getRoot();

            treeView.removeChildren(root);

            for(i = 0; i < facets.length; i++) {
                if (facets[i].values.length > 0 && facets[i].name != "id") {
                    nameNode = new TextNode(facets[i].name, root, false);

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
                var anchorText = node.parent.label + " : " + node.label.substring(0, node.label.lastIndexOf("(") - 1);

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
                i, j, containerDiv, childDiv, innerContainerDiv, titleAnchor, titleAnchorText;

            LWA.ui.removeDivChildNodes("search_results");

            for(i = 0; i < docs.length; i++) {
                containerDiv = LWA.ui.createDomElement("div", searchResults, [
                    { key : "class", value : "search-result-div" } ]);

                var sortedkeys = sortKeys(Object.keys(docs[i]));

                for(j = 0; j < sortedkeys.length; j++) {
                    var key = sortedkeys[j];
                    innerContainerDiv = LWA.ui.createDomElement("div", containerDiv, []);

                    if (key == "title" && docs[i].hasOwnProperty("id")) {
                        childDiv = LWA.ui.createDomElement("div", containerDiv, []);
                        titleAnchor = LWA.ui.createDomElement("a", childDiv, [
                            { key : "class", value: "search-result-header" },
                            { key : "id", value : "titleanchor" + i },
                            { key : "style", value: "color: steelblue" }, { key : "target", value : "_blank" }]);
                        setHrefAttribute(titleAnchor, docs[i]);
                        titleAnchor.appendChild(document.createTextNode(LWA.util.stripBrackets(docs[i].title)));

                    } else {
                        LWA.ui.createDomElement("div", innerContainerDiv, [
                            { key : "text", value : key },
                            { key : "class", value : "search-result-inner-container-label" } ]);

                        LWA.ui.createDomElement("div", innerContainerDiv, [
                            { key : "text", value : "<a href=\"#\">" + LWA.util.stripBrackets(docs[i][key]) + "</a>"},
                            { key : "id", value : key + "_" + i },
                            { key: "class", value : "search-result-inner-container" } ]);

                        Event.addListener(key + "_" + i, "click", function(e) {
                            Event.stopEvent(e);
                            var currentValue = Dom.get("search_input").value;
                            Dom.get("search_input").value = ((currentValue == "*:*") ? "" : currentValue) + "+" +
                                    this.id.replace(/_[0-9]+$/, "") + ":\"" +
                                    this.innerHTML.replace("<a href=\"#\">", "").replace("</a>", "") + "\"";
                        });
                    }
                }

                LWA.ui.createDomElement("div", containerDiv, [ { key: "style", value : "clear:both" } ]);
            }
        };

        var getFilterQueryString = function() {
            var facetOptions = {}, domFacets = Dom.get("facet_options").children;
            for (var i = 0; i < domFacets.length; i++) {
                if (!Dom.hasClass(domFacets[i], "clearboth")) {
                    var t = domFacets[i].innerHTML.split(" : ");
                    facetOptions[t[0]] = facetOptions.hasOwnProperty(t[0]) ? facetOptions[t[0]] + " OR " + t[1] : t[1];
                }
            }

            var fqStr = "";
            if (Object.keys(facetOptions).length > 0) {
                for (var key in facetOptions) {
                    fqStr += "%2B" + key + ":(" + facetOptions[key] + ")";
                }
            }
            return fqStr;
        };

        Event.addListener("reset", "click", function (e) {
            Event.stopEvent(e);
            Dom.get("search_input").value = "*:*";
        });

        Event.addListener("submit", "click", function (e) {
            Event.stopEvent(e);

            var queryTerms = Dom.get("search_input").value,
                collectionName = Dom.get("collection_name_select").value,
                fq = getFilterQueryString();

            var urlParams = "?query=" + queryTerms + "&collection=" + collectionName + "&sort=" + sortType + "&order=" + ascType;
            if (fq != "") {
                urlParams += "&fq=" + fq;
            }

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

            //http://localhost:8080/LucidWorksApp/search/query?query=disaster&collection=epic

            Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + urlParams, {
                success : function(o) {

                    var result = Json.parse(o.responseText);
                    var numFound = result.response.numFound,
                        facets = result.response.facets;

                    pag.set('totalRecords', numFound);
                    pag.render();

                    Dom.get("num_found").innerHTML = (numFound == 1) ? "Found 1 document" : "Found " + numFound + " documents";
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
