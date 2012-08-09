<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/search.css"/>" />

    <h1>Search</h1>

    <form id = "search_form">
        <div class = "row" id="search_div">
            <label for="search_input">Query terms: </label>
            <input id="search_input" type = "text" value="*"/>
        </div>

        <div class="row" id = "collection_name">
            <label id ="collection_name_label" for="collection_name_select">Select a collection: </label>
            <select id = "collection_name_select"></select>
        </div>

        <div class="row" id = "sort_by_div">
            <label for="sort_type_buttongroup">Sort by: </label>
            <div id = "sort_type_buttongroup" class = "yui-buttongroup" style="float:right">
                <input id="sort_date" type="radio" name="sorttype" value="lastModified" checked>
                <input id="sort_relevance" type="radio" name="sorttype" value="score">
                <input id="sort_random" type="radio" name="sorttype" value="random">
            </div>
        </div>

        <div class="row" id= "filter_by_datasource_div">
            <label for="filter_by_datasource">Filter by datasource:</label>
            <select id = "filter_by_datasource"></select>
        </div>

        <div class="buttons">
            <a href="#" class="button small" id="submit">Submit</a>
            <a href="#" class="button small" id="cancel">Cancel</a>
        </div>
        <div class = "row"></div>
    </form>

    <div id = "num_found" class = "row"></div>
    <div id = "search_results"></div>
    <div id = "pag1"></div>

    <script type="text/javascript">
    (function() {

 //       http://localhost:8989/collections/epic/search?filter[data_source_name][]=EPIC&q=disaster
 //       http://localhost:8989/collections/epic/search?filter[data_source_name][]=Xfile+test&q=disaster
 //       http://localhost:8989/collections/epic/search?filter[author_display][]=Leysia+Palen&q=disaster
 //       http://localhost:8989/collections/epic/search?filter[author_display][]=Leysia+Palen&filter[data_source_name][]=EPIC&q=disaster

        var Dom = YAHOO.util.Dom,
            Event = YAHOO.util.Event,
            Connect = YAHOO.util.Connect,
            Json = YAHOO.lang.JSON,
            Button = YAHOO.widget.Button,
            ButtonGroup = YAHOO.widget.ButtonGroup;

        var sortType = "lastModified", sortTypeButtonGroup = new ButtonGroup("sort_type_buttongroup");
        var changeSortType = function (o) { sortType = o.newValue.get("value"); };
        sortTypeButtonGroup.on("checkedButtonChange", changeSortType);

        Connect.asyncRequest('GET', '<c:url value="/collection/collectionNames" />', {
            success : function(o) {
                var result = Json.parse(o.responseText);

                for(var i = 0; i < result.names.length; i++) {
                    var el = document.createElement('option');
                    el.text = result.names[i];
                    el.value = result.names[i];
                    Dom.get("collection_name_select").add(el);
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
                            { key : "text", value : LWA.util.stripBrackets(docs[i][key]) },
                            { key : "id", value : key + i },
                            { key: "class", value : "search-result-inner-container" } ]);
                    }
                }
                /*
                 LWA.ui.createDomElement("div", innerContainerDiv, [
                 { key : "text", value : getHighlightedText(docs[i].id, highlighting) },
                 { key : "id", value : "highlighting" + i },
                 { key: "class", value : "search-result-inner-container" } ]);
                 */
                LWA.ui.createDomElement("div", containerDiv, [ { key: "style", value : "clear:both" } ]);
            }
        };

        Event.addListener("submit", "click", function (e) {
            Event.stopEvent(e);

            var queryTerms = Dom.get("search_input").value,
                collectionName = Dom.get("collection_name_select").value;

            var urlParams = "?query=" + queryTerms + "&collection=" + collectionName + "&sort=" + sortType;

            var pag = new YAHOO.widget.Paginator( {rowsPerPage : 10, containers : [ "pag1" ] });

            var handlePagination = function (newState) {

                Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + urlParams + "&start=" + newState.records[0], {
                    success: function(o) {
                        var result = Json.parse(o.responseText);
                        buildSearchResultHtml(result);
                        /*
                        var i, docs = result.response.docs;

                        for(i = 0; i < docs.length; i++) {
                            for(var key in docs[i]) {
                                if (key == "title" && docs[i].hasOwnProperty("id")) {
                                    setHrefAttribute(Dom.get("titleanchor" + i), docs[i]);
                                } else {
                                    console.log(key + i);
                                    Dom.get(key + i).innerHTML = stripBrackets(docs[i][key]);
                                }
                            }
                        }  */
                    }
                });

                pag.setState(newState);
            };

            pag.subscribe('changeRequest', handlePagination);

            //http://localhost:8080/LucidWorksApp/search/query?query=disaster&collection=epic
            Connect.asyncRequest('GET', '<c:url value="/search/solrquery" />' + urlParams, {
                success : function(o) {

                    var result = Json.parse(o.responseText);
                    var numFound = result.response.numFound;
                    pag.set('totalRecords', numFound);
                    pag.render();

                    Dom.get("num_found").innerHTML = (numFound == 1) ? "Found 1 document" : "Found " + numFound + " documents";

                    buildSearchResultHtml(result);
                },
                failure : function(o) {
                    alert("Could not connect.");
                }
            });
        });


    })();
    </script>
</layout:main>