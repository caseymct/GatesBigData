var SEARCH = {};
SEARCH.ui = {};
SEARCH.util = {};

(function() {

    var Event = YAHOO.util.Event,           Tooltip = YAHOO.widget.Tooltip,
          Dom = YAHOO.util.Dom,              Button = YAHOO.widget.Button,
         Json = YAHOO.lang.JSON,          Paginator = YAHOO.widget.Paginator,
      TabView = YAHOO.widget.TabView,   ButtonGroup = YAHOO.widget.ButtonGroup,
      Connect = YAHOO.util.Connect,         LocalDS = YAHOO.util.LocalDataSource,
      ScrollingDataTable = YAHOO.widget.ScrollingDataTable;

    SEARCH.ui.longStringWidth       = 210;
    SEARCH.ui.shortStringWidth      = 80;
    SEARCH.ui.urlSearchParams       = "";
    SEARCH.ui.facetsFromLastSearch  = null;

    var colStringMaxChars      = 20,
        colDateStringMaxChars  = 30;

    var searchInputEls       = [],
        queryDefaultValue    = "*:*",
        dataSourceFields     = [],
        tooltip              = null,
        thumbnailData        = {},
        rowsPerPage          = 10,
        initialSelectIndex   = 0;

    var loadingImgUrl        = "",
        thumbnailUrl         = "",
        viewDocUrl           = "",
        searchUrl            = "";

    var previewDataName                 = "preview_image",

        submitButtonElId                = "submit",
        resetButtonElId                 = "reset",
        exportButtonElId                = "export",

        showQueryElName                 = "show_query",
        showQueryEl                     = null,

        paginatorElName                 = "pag",
        paginator                       = null,

        searchResultContainerElName     = "search_result_container",

        numFoundElName                  = "num_found",
        numFoundEl                      = null,
        numFound                        = 0,

        searchResultsElName             = "search_results",
        searchResultsEl                 = null,

        previewContainerElName          = "preview_container",
        previewContainerEl              = null,

        searchTabElName                 = "",
        searchTab                       = null,
        searchHeaderElName              = "",

        sortByDivElName                 = "sort_by_div",
        sortBySelectElName              = "sort_date_label",
        sortBySelect                    = null,

        selectData                      = [],
        selectDataColumnDefs            = [],
        selectDataRegexIgnore           = "",

        sortOrderButtonGroupElName      = "sort_ascdesc_buttongroup",
        sortOrderButtonGroup            = null,

        insertSearchResultsAfterElName  = "",
        insertSortButtonsAfterElName    = "",
        insertSearchButtonsAfterElName  = "",

        buildSearchResultHtmlFn         = buildSearchResultDataTableHtml,
        submitFn                        = null,
        resetFn                         = null,
        getFilterQueryFn                = null,
        formatSearchResultFn            = function(a) { return a;};

    /* Set core name and header for this search file */
    var url = window.location.href.split('/');
    SEARCH.ui.coreName = url[url.length - 1];

    SEARCH.ui.init = function(names) {
        dataSourceFields      = names['dataSourceFields'];
        selectDataColumnDefs  = names['selectDataColumnDefs'];
        selectDataRegexIgnore = names['selectDataRegexIgnore'];
        searchTabElName       = names['searchTabElName'];
        searchHeaderElName    = names['searchHeaderElName'];
        searchInputEls        = names['searchInputEls'];

        insertSearchResultsAfterElName = names['insertSearchResultsAfterElName'];
        insertSortButtonsAfterElName   = names['insertSortButtonsAfterElName'];
        insertSearchButtonsAfterElName = names['insertSearchButtonsAfterElName'];

        submitFn             = names['submitFn'];
        resetFn              = names['resetFn'];
        getFilterQueryFn     = names['getFilterQueryFn'];
        formatSearchResultFn = UI.util.returnIfDefined(formatSearchResultFn, names['formatSearchResultFn']);

        var urls = names['urls'];
        viewDocUrl    = urls['viewDocUrl'];
        loadingImgUrl = urls['loadingImgUrl'];
        thumbnailUrl  = urls['thumbnailUrl'];
        searchUrl     = urls['searchUrl'];

        initialSelectIndex = UI.util.returnIfDefined(initialSelectIndex, names['initialSelectIndex']);        
        if (names['tooltipElName'] !== undefined) {
            initToolTip(names['tooltipElName']);
        }
        
        if (names['dataType'] == "unstructured") {
            buildSearchResultHtmlFn = buildSearchResultList;
        }

        buildHTML();
    };

    function buildHTML() {
        // sort order buttongroup code
        var sortButtonsInsertAfterEl = Dom.get(insertSortButtonsAfterElName);
        var el = UI.insertDomElementAfter('div', sortButtonsInsertAfterEl, { id: sortByDivElName }, { class: "row" });
        var div = UI.addDomElementChild('div', el);
        UI.addDomElementChild('label', div, { id: sortBySelectElName, innerHTML: "Order by: " });
        div = UI.addDomElementChild('div', el, { id: sortOrderButtonGroupElName }, { class: "yui-buttongroup search-button-style" });
        UI.addDomElementChild('input', div, { type: "radio", name: "sorttype", value: "asc", id: "sort_asc"});
        UI.addDomElementChild('input', div, { type: "radio", name: "sorttype", value: "desc", id: "sort_desc", checked: "checked"});
        UI.addDomElementChild('div', el, null, { class: "clearboth" });

        // Search result HTML
        var searchResultsInsertAfterEl = Dom.get(insertSearchResultsAfterElName);
        el = UI.insertDomElementAfter('div', searchResultsInsertAfterEl);
        div = UI.addDomElementChild('div', el, { id : searchResultContainerElName }, { width: "56%"} );
        UI.addDomElementChild('div', div, { id : showQueryElName });
        UI.addDomElementChild('div', div, { id : numFoundElName });
        UI.addDomElementChild('div', div, { id : searchResultsElName });
        UI.addDomElementChild('div', el,  { id : previewContainerElName });
        UI.addDomElementChild('div', el, null, { class: "clearboth" });
        UI.insertDomElementAfter('div', searchResultsInsertAfterEl, { id: paginatorElName });

        var searchButtonsInsertAfterEl = Dom.get(insertSearchButtonsAfterElName);
        el = UI.insertDomElementAfter('div', searchButtonsInsertAfterEl, null, { class: "buttons", "padding-bottom" : "5px" });
        UI.addDomElementChild('a', el, { id: submitButtonElId, innerHTML: "Search", href: "#"}, { class : "button small"});
        UI.addDomElementChild('a', el, { id: resetButtonElId,  innerHTML: "Reset",  href: "#"}, { class : "button small"});
        UI.addDomElementChild('a', el, { id: exportButtonElId, innerHTML: "Export", href: "#"}, { class : "button small"});
        UI.insertDomElementAfter('div', el, null, { class: "row" });

        previewContainerEl = Dom.get(previewContainerElName);
        numFoundEl         = Dom.get(numFoundElName);
        showQueryEl        = Dom.get(showQueryElName);
        searchResultsEl    = Dom.get(searchResultsElName);

        setSearchHeader();
        initSearchTab();
        initSelectData(selectDataRegexIgnore);
        initSortBySelect();
        initControlButtonListeners();
        initPaginator();
        initSortOrderButtonGroup();
    }

    SEARCH.ui.getExportButtonElId = function() {
        return exportButtonElId;
    };

    SEARCH.ui.resetQuerySearchInputs = function() {
        for(var i = 0; i < searchInputEls.length; i++) {
            searchInputEls[i].value = queryDefaultValue;
        }
    };

    /* Formatters for the search results table */
    SEARCH.ui.formatLink = function(el, record, column, data) {
        var max = (column.label.match(/ Date/) != null) ? colDateStringMaxChars : colStringMaxChars;
        if (data == undefined) {
            data = "<i>No value</i>";
        } else if (data.length > max) {
            data = data.substring(0, max) + " ...";
        }
        el.innerHTML = '<a href="#">' + data + '</a>';
    };

    SEARCH.ui.formatDate = function(el, record, column, data) {
        data = (data == undefined) ? "<i>No value</i>" : YAHOO.util.Date.format(data, {format: "%I:%M:%S %p %Y-%m-%d %Z"});
        el.innerHTML = '<a href="#">' + data + '</a>';
    };

    SEARCH.ui.search = function() {
        UI.showWait();

        var fq = getFilterQueryFn();
        var queryTerms = getQueryTerms();
        SEARCH.ui.urlSearchParams = constructUrlSearchParams(queryTerms, fq, 0);
        updateSolrQueryDiv(queryTerms, fq);
        clearPreviewContainerImage();

        Connect.asyncRequest('GET', searchUrl + SEARCH.ui.urlSearchParams, {
            success : function(o) {
                UI.hideWait();

                var result = Json.parse(o.responseText);
                SEARCH.ui.facetsFromLastSearch = result.facets;

                updateNumFound(result.response.numFound, 1);
                updatePaginatorAfterSearch();
                buildSearchResultHtmlFn(result);

                return numFound;
            },
            failure : function(o) {
                alert("Could not connect.");
            }
        });
        return -1;
    };

    SEARCH.ui.getUrlSearchParams = function() {
        return (SEARCH.ui.urlSearchParams == "") ? "" : SEARCH.ui.urlSearchParams + "&numfound=" + numFound;
    };

    function initControlButtonListeners() {
        Event.addListener(resetButtonElId, "click", function (e) {
            Event.stopEvent(e);
            resetFn();
        });

        Event.addListener(submitButtonElId, "click", function (e) {
            Event.stopEvent(e);
            submitFn();
        });
    }

    function initSearchTab() {
        searchTab = new TabView(searchTabElName);
    }

    function initToolTip(tooltipElName) {
        tooltip = new Tooltip(tooltipElName, { zIndex : 20 });
    }
    
    function initSortOrderButtonGroup() {
        sortOrderButtonGroup = new ButtonGroup(sortOrderButtonGroupElName);
        sortOrderButtonGroup.check(1);
    }

    function initPaginator() {
        paginator = new Paginator( { rowsPerPage : rowsPerPage, containers : [ paginatorElName ] });
        paginator.subscribe('changeRequest', handlePagination);
    }

    function setSearchHeader() {
        Dom.get(searchHeaderElName).innerHTML = "Search Core: " + SEARCH.ui.coreName;
    }

    function updateNumFound (n, start) {
        numFound = n;
        var s = "Found " + numFound + " document" + ((numFound != 1) ? "s" : "");
        if (numFound > 0) {
            s += ", showing items " + start + " through " + Math.min(start + rowsPerPage - 1, numFound);
        }
        numFoundEl.innerHTML = s;
    }

    /* Paginator code */
    function updatePaginatorAfterSearch() {
        paginator.set('totalRecords', numFound);
        paginator.render();
        paginator.setStartIndex(0);
    }

    /* Preview container code */
    function getPreviewHtmlTitleString(title) {
        var titleString = (title == undefined) ? "<b>Search result preview</b>" : "Document: <b>" + title + "</b>";
        return "<span style='margin-left: 5px; font-size: 10px'>" + titleString +
                "<a style='float: right' class='button zoom_out' id='previewzoomout'></a>" +
                "<a style='float: right' class='button zoom_in' id='previewzoomin'></a>" +
                "</span><hr>";
    }

    function setPreviewContainerImageData(title, dataStr) {
        var htmlStr = getPreviewHtmlTitleString(title);
        htmlStr += "<img src='" + dataStr + "' height='400px' width='400px' id='" + previewDataName + "'>";
        previewContainerEl.innerHTML = htmlStr;

        Dom.setStyle(previewDataName, 'zoom', 2);
        Event.addListener('previewzoomin', 'click', function(e) {
            Dom.setStyle(previewDataName, 'zoom', parseInt(Dom.getStyle(previewDataName, 'zoom')) + 1);
        });

        Event.addListener('previewzoomout', 'click', function(e) {
            var zoom = parseInt(Dom.getStyle(previewDataName, 'zoom'));
            Dom.setStyle(previewDataName, 'zoom', (zoom > 1) ? zoom - 1 : zoom);
        });
    }

    function setPreviewContainerTextData(title, dataStr) {
        var htmlStr = getPreviewHtmlTitleString(title);
        htmlStr += "<div id='" + previewDataName + "'>" + UI.util.jsonSyntaxHighlight(dataStr) + "</div>";
        previewContainerEl.innerHTML = htmlStr;

        Dom.setStyle(previewDataName, 'font-size', '10px');
        Event.addListener('previewzoomin', 'click', function(e) {
            var px = UI.util.getNPixels(Dom.getStyle(previewDataName, 'font-size'));
            Dom.setStyle(previewDataName, 'font-size', (px + 1) + 'px');
        });

        Event.addListener('previewzoomout', 'click', function(e) {
            var px = UI.util.getNPixels(Dom.getStyle(previewDataName, 'font-size'));
            Dom.setStyle(previewDataName, 'font-size', (px - 1) + 'px');
        });
    }

    function setPreviewContainerData(title, dataStr, dataType, i) {
        Dom.setStyle(previewContainerEl, "border", "1px solid gray");
        Dom.setStyle(previewContainerEl, "background-image", "none");

        if (dataType == "image") {
            Dom.setStyle(previewContainerEl, "margin-top", (65 + 92*i) + "px");
            setPreviewContainerImageData(title, dataStr);
        } else {
            setPreviewContainerTextData(title, dataStr);
        }
    }

    function setPreviewContainerLoadingImage(img) {
        previewContainerEl.innerHTML = "";
        Dom.setStyle(previewContainerEl, "background-image", "url(\"" + img + "\")");
        Dom.setStyle(previewContainerEl, "background-position", "50% 10%");
        Dom.setStyle(previewContainerEl, "background-repeat", "no-repeat");
    }

    function clearPreviewContainerImage() {
        Dom.setStyle(previewContainerEl, "border", "none");
        previewContainerEl.innerHTML = "";
    }

    /* Search tab */
    function getSearchTabActiveIndex() {
        return searchTab.get('activeIndex');
    }

    function setSearchTabToGeneralQuery() {
        searchTab.set('activeIndex', searchTab.get("tabs").length - 1);
    }

    function getQueryTerms() {
        var val = searchInputEls[getSearchTabActiveIndex()].value;
        return (searchTab == null || val == "") ? queryDefaultValue : val.encodeForRequest();
    }

    /* Sort order button code */
    function getSortOrder() {
        return UI.getButtonGroupCheckedButtonValue(sortOrderButtonGroup);
    }

    /* Tooltip code */
    function showTooltip(target, record, column, x, y) {
        //var target = oArgs.target;
        //var column = this.getColumn(target), record = this.getRecord(target);
        var s = target.innerText.replace(/\n$/, '');
        if (s.substring(s.length - 3) == "...") {
            SEARCH.ui.tooltip.setBody(record.getData()[column.getField()]);
            SEARCH.ui.tooltip.cfg.setProperty('xy', [x, y]);
            SEARCH.ui.tooltip.show();
        }
    }

    function hideTooltip() {
        tooltip.hide();
    }

    /* Field select code */
    function initSelectData(regexIgnore) {
        for(var i = 0; i < selectDataColumnDefs.length; i++) {
            var text = selectDataColumnDefs[i].key;
            if (regexIgnore == undefined || !text.match(regexIgnore)) {
                var split = text.split(".");
                if (split.length == 2) {
                    text += ".facet";
                }
                selectData.push({ text: split[split.length - 1], value: text });
            }
        }
    }

    function initSortBySelect() {
        sortBySelect = new Button({
            id          : "sortByMenu",
            name        : "sortByMenu",
            type        : "menu",
            lazyloadmenu: false,
            menu        : selectData,
            container   : sortBySelectElName
        });

        var sortBySelectMenu = sortBySelect.getMenu();
        sortBySelectMenu.subscribe("render", function (type, args, button) {
            button.set("selectedMenuItem", this.getItem(initialSelectIndex));
        }, sortBySelect);

        sortBySelect.on("selectedMenuItemChange", function (event) {
            this.set("label", ("<em class=\"yui-button-label\">" + event.newValue.cfg.getProperty("text") + "</em>"));
        });
    }

    function handlePagination(newState) {
        UI.showWait();
        var newStart = newState.records[0], newStartStr = "&start=" + newStart;
        var match = SEARCH.ui.urlSearchParams.match(/(&start=[0-9]+)/);

        SEARCH.ui.urlSearchParams = (match == null) ? SEARCH.ui.urlSearchParams + newStartStr :
                                     SEARCH.ui.urlSearchParams.replace(match[1], newStartStr);
        updateNumFound(numFound, newStart);
        Connect.asyncRequest('GET', searchUrl + SEARCH.ui.urlSearchParams, {
            success: function(o) {
                UI.hideWait();
                buildSearchResultHtmlFn(Json.parse(o.responseText));
            }
        });

        paginator.setState(newState);
    }

    function updateSolrQueryDiv (queryTerms, fq) {
        showQueryEl.innerHTML = "Submitting to solr with<br>q: " + queryTerms + "<br>fq: " + decodeURIComponent(fq);
    }

    function constructUrlSearchParams(queryTerms, fq, start) {
        var sortType = sortBySelect.get("selectedMenuItem").value,
            coreName = SEARCH.ui.coreName,
            sortOrder = getSortOrder();

        var urlParams = "?query=" + queryTerms + "&core=" + coreName + "&sort=" + sortType + "&order=" + sortOrder;
        if (fq != "") {
            urlParams += "&fq=" + fq;
        }
        if (start != undefined && start != "") {
            urlParams += "&start=" + start;
        }

        return urlParams;
    }

    /* Data table event handlers */
    function dataTableCellMouseoverEvent(e, dataTable) {
        var target = e.target;
        var record = dataTable.getRecord(target),
            data = record.getData();

        var urlParams = "?core=" + SEARCH.ui.coreName + "&id=" + UI.util.returnEmptyIfUndefined(data.id);
        if (data.HDFSSegment != undefined) {
            urlParams += "&segment=" + data.HDFSSegment;
        }

        var callback = {
            success: function(o) {
                var response = Json.parse(o.responseText);
                this.argument.table.updateCell(record, "thumbnail", response.Contents);
                this.argument.table.updateCell(record, "thumbnailType", response.contentType);
                setPreviewContainerData(record.getData().title, response.Contents, response.contentType);
            },
            argument: { table: dataTable, record: record}
        };

        if (data.thumbnail == undefined) {
            setPreviewContainerLoadingImage(loadingImgUrl);
            Connect.asyncRequest('GET', thumbnailUrl + urlParams, callback);
        } else {
            setPreviewContainerData(data.title, data.thumbnail, data.thumbnailType);
        }
    }


    function buildSearchResultDataTableHtml(result) {
        var dataSource = new LocalDS(formatSearchResultFn(result.response), {
            responseSchema : {
                resultsList:'docs',
                fields: dataSourceFields
            }
        });

        var dataTable = new ScrollingDataTable('search_results', selectDataColumnDefs, dataSource, { width:"100%" });

        dataTable.subscribe("sortedByChange", function(e) {
            var sortField = e.newValue.column.key;
            var index = -1;
            for(var i = 0; i < selectData.length; i++) {
                if (selectData[i].value.replace(/\.facet$/, "") == sortField) {
                    index = i;
                    break;
                }
            }
            sortBySelect.set("selectedMenuItem", sortBySelect.getMenu().getItem(index));
            paginator.setStartIndex(0);
            SEARCH.ui.search();
        });

        dataTable.subscribe("cellClickEvent", function (e) {
            Event.stopEvent(e.event);
            var data = this.getRecord(e.target).getData();

            var urlParams = "?core=" + SEARCH.ui.coreName + "&id=" + data.id + "&view=preview";
            if (data['HDFSSegment'] != undefined) {
                urlParams += "&segment=" + data['HDFSSegment'];
            }
            window.open(viewDocUrl + urlParams, "_blank");
        });

        dataTable.on('rowMouseoverEvent', function (e) {
            dataTableCellMouseoverEvent(e, dataTable, loadingImgUrl, thumbnailUrl);
        });
    }

    function buildSearchResultList(result) {
        searchResultsEl.innerHTML = "";

        var docs = result.response.docs, i = 0, highlight = result.highlighting;

        docs.forEach(function(doc) {
            var titleHref = viewDocUrl + "?core=" + SEARCH.ui.coreName + "&segment=" +
                            doc['HDFSSegment'] + "&id=" + doc['id'] + "&view=preview";
            var titleId = "link_" + i, searchResultDivId = "searchResult_" + i;
            i = i + 1;

            var div   = UI.addDomElementChild("div", searchResultsEl, { id: searchResultDivId }, { class: "search_result_div" });
            var title = UI.addDomElementChild("div", div);
            UI.addDomElementChild("a", title, { id: titleId, href: titleHref, innerHTML: doc.title }, { color: "blue" });
            UI.addDomElementChild("div", div, { innerHTML: "Author: <b>" + doc.author + "</b>"}, { class: "search_subheading" });
            UI.addDomElementChild("div", div, { innerHTML: "Content type: <b>" + doc.content_type + "</b>"}, { class: "search_subheading" });
            UI.addDomElementChild("div", div, { innerHTML: "Created On: <i>" + doc.creation_date + "</i>, " +
                                                           "Last Modified: <i>" + doc.last_modified + "</i>"}, { class: "search_subheading"});

            var highlightObj = highlight[doc.HDFSKey];
            Object.keys(highlightObj).forEach(function(key) {
                UI.addDomElementChild("div", div, { innerHTML: "Field <b>" + key + "</b>"}, { class: "search_subheading", color: "darkgreen"});
                highlightObj[key].forEach(function(s) {
                    UI.addDomElementChild("div", div, { innerHTML: "... " + s + "... "}, { class: "search_result"});
                })
            });

            UI.addDomElementChild("div", div, {}, {class: "clearboth"});

            Event.addListener(titleId, "click", function(e) {
                Event.stopEvent(e);
                window.open(this.href, "_blank");
            });

            Event.addListener(searchResultDivId, "mouseover", function(e) {
                var idx   = this.id.match("searchResult_([0-9]+)")[1];
                var href  = Dom.get("link_" + idx).href;
                var file  = href.match("id=([^&]+)[&|$]")[1], segment = href.match("segment=([^&]+)[&|$]")[1];
                var title = file.substring(file.lastIndexOf("/") + 1);

                if (thumbnailData[file] == undefined) {
                    setPreviewContainerLoadingImage(loadingImgUrl);
                    var urlParams = "?core=" + SEARCH.ui.coreName + "&segment=" + segment + "&id=" + file;
                    Connect.asyncRequest('GET', thumbnailUrl + urlParams, {
                        success: function(o) {
                            var response = Json.parse(o.responseText);
                            thumbnailData[file] = { "contents" : response.Contents, "contentType" : response.contentType };
                            setPreviewContainerData(title, response.Contents, response.contentType, idx);
                        }
                    });

                } else {
                    setPreviewContainerData(title, thumbnailData[file].contents, thumbnailData[file].contentType, idx);
                }
            });
        });
    }

})();