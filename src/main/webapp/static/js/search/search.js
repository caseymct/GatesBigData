var SEARCH = {};
SEARCH.ui = {};
SEARCH.util = {};

(function() {

    /* Set core name and header for this search file */
    var url = window.location.href.split('/'), lastUrlComponent = url[url.length - 1],
        idx = lastUrlComponent.indexOf('?');
    SEARCH.ui.coreName = (idx == -1) ? lastUrlComponent : lastUrlComponent.substring(0, idx);

    SEARCH.ui.longStringWidth  = 210;
    SEARCH.ui.shortStringWidth = 80;

    var Event   = YAHOO.util.Event,     Tooltip     = YAHOO.widget.Tooltip,
        Dom     = YAHOO.util.Dom,       DataTable   = YAHOO.widget.ScrollingDataTable,
        Json    = YAHOO.lang.JSON,      Paginator   = YAHOO.widget.Paginator,
        TabView = YAHOO.widget.TabView, ButtonGroup = YAHOO.widget.ButtonGroup,
        Connect = YAHOO.util.Connect,   LocalDS     = YAHOO.util.LocalDataSource,
        Button  = YAHOO.widget.Button,  Panel       = YAHOO.widget.Panel;

    var colStringMaxChars     = 20,                  colDateStringMaxChars       = 30,
        rowsPerPage           = 10,                  queryDefaultValue           = '*:*',

        // button IDs
        submitButtonElId      = 'submit',            resetButtonElId             = 'reset',
        exportButtonElId      = 'export',            analyzeButtonElId           = 'analyze',

        // other DOM IDs
		previewDataId         = 'preview_image',     showQueryElId               = 'show_query',
        paginatorElId         = 'pag',               searchResultContainerElId   = 'search_result_container',
        numFoundElId          = 'num_found',         sortOrderButtonGroupElId    = 'sort_ascdesc_buttongroup',
        sortByDivElId         = 'sort_by_div',       searchResultsElId           = 'search_results',
        previewContainerElId  = 'preview_container', sortBySelectElId            = 'sort_date_label',
        showQueryElIdBody     = 'show_query_text',   showQueryButtonElId         = 'show_query_button',
        showQueryCloseElId    = 'show_query_close',

        // solr
        solrResponseKey       = 'response',          solrResponseFacetKey        = 'facet_counts',
        solrResponseDocsKey   = 'docs',              solrResponseNumFoundKey     = 'num_found',
        solrDocIdFieldName    = 'id',                solrResponseHighlightingKey = 'highlighting',
        solrDocContentsKey    = 'content',           solrDocContentTypeKey       = 'content_type',
        thumbnailKey          = 'thumbnail',         thumbnailTypeKey            = 'thumbnail_type',
        solrDocTxtContentType = 'text/html',         solrDocImageContentType     = 'image/png',
                                                     solrDocJSONContentType      = 'application/json',
        //request
        numFoundRequestParam  = 'numfound',

        //preview
        noPreviewText         = "No preview available",

        // CSS variables
        searchResultSubheadingCSSClass  = 'search_subheading',      searchResultElWidthStructured   = '56%',
        searchResultDivCSSClass         = 'search_result_div',      searchResultElWidthUnstructured = '100%',   
        yuiButtonLabelCSSClass          = 'yui-button-label',       previewDivCSSClass              = 'preview_result_div';

    var searchInputEls                  = [],       dataSourceFields                = [],
        thumbnailData                   = {},       initialSelectIndex              = 0,
        facetsFromLastSearch            = null,     docsFromLastSearch              = null,
        numFound                        = 0,        urlSearchParams                 = "",

        // URLs
        loadingImgUrl = "", thumbnailUrl  = "", analyzeUrl = "",
        viewDocUrl    = "", searchUrl     = "",

        // set in init
        unstructuredData                = false,    displayName                     = "",
        searchTabElId                   = "",       searchHeaderElName              = "",
        insertSearchResultsAfterElName  = "",       insertSortButtonsAfterElName    = "",
        insertSearchButtonsAfterElName  = "",       selectData                      = [],
        selectDataColumnDefs            = [],       selectDataRegexIgnore           = "",
        searchResultContainerElWidth    = "",       showQueryPanel                  = null,

        // widgets
        paginator                       = null,     currPreviewContainerEl          = null,
        searchTab                       = null,     sortBySelect                    = null,
        sortOrderButtonGroup            = null,     tooltip                         = null,

        // functions
        buildSearchResultHtmlFn         = buildSearchResultDataTableHtml,
        submitFn                        = null,
        resetFn                         = null,
        getFilterQueryFn                = null,
        updateFacetFn                   = null,
        formatSearchResultFn            = function(a) { return a;};


    SEARCH.ui.init = function(names) {
        displayName           = names[UI.DISPLAY_NAME_KEY];
        dataSourceFields      = names[UI.SEARCH.DATA_SOURCE_FIELDS_KEY];
        selectDataColumnDefs  = names[UI.SEARCH.SELECT_DATA_COLUMN_DEFS_KEY];
        selectDataRegexIgnore = names[UI.SEARCH.SELECT_DATA_REGEX_IGNORE_KEY];
        searchTabElId         = names[UI.SEARCH.SEARCH_TAB_EL_NAME_KEY];
        searchHeaderElName    = names[UI.SEARCH.SEARCH_HEADER_EL_NAME_KEY];
        searchInputEls        = names[UI.SEARCH.SEARCH_INPUT_ELS_KEY];

        insertSearchResultsAfterElName = names[UI.SEARCH.INSERT_SEARCH_RESULTS_AFTER_EL_NAME_KEY];
        insertSortButtonsAfterElName   = names[UI.SEARCH.INSERT_SORT_BUTTONS_AFTER_EL_NAME_KEY];
        insertSearchButtonsAfterElName = names[UI.SEARCH.INSERT_SEARCH_BUTTONS_AFTER_EL_NAME_KEY];

        submitFn             = names[UI.SEARCH.SUBMIT_FN_KEY];
        resetFn              = names[UI.SEARCH.RESET_FN_KEY];
        getFilterQueryFn     = names[UI.SEARCH.GET_FILTER_QUERY_FN_KEY];
        updateFacetFn        = names[UI.FACET.UPDATE_FACET_FN];
        formatSearchResultFn = UI.util.returnIfDefined(formatSearchResultFn, names[UI.SEARCH.FORMAT_SEARCH_RESULT_FN_KEY]);

        var urls      = names[UI.URLS_KEY];
        viewDocUrl    = urls[UI.VIEW_DOC_URL_KEY];
        loadingImgUrl = urls[UI.LOADING_IMG_URL_KEY];
        thumbnailUrl  = urls[UI.THUMBNAIL_URL_KEY];
        searchUrl     = urls[UI.SEARCH_URL_KEY];
        analyzeUrl    = urls[UI.ANALYZE_URL_KEY];

        initialSelectIndex = UI.util.returnIfDefined(initialSelectIndex, names[UI.SEARCH.INITIAL_SELECT_INDEX_KEY]);
        if (names[UI.SEARCH.TOOLTIP_EL_NAME_KEY] !== undefined) {
            initToolTip(names[UI.SEARCH.TOOLTIP_EL_NAME_KEY]);
        }

        unstructuredData = (names[UI.DATA_TYPE_KEY] == UI.DATA_TYPE_UNSTRUCTURED);
        searchResultContainerElWidth = unstructuredData ? searchResultElWidthUnstructured : searchResultElWidthStructured;
        if (unstructuredData) {
            buildSearchResultHtmlFn = buildSearchResultList;
        }

        buildHTML();
        searchIfInitialQuerySpecified();
    };

    function buildHTML() {
        // sort order buttongroup code
        var sortButtonsInsertAfterEl = Dom.get(insertSortButtonsAfterElName);
        var el = UI.insertDomElementAfter('div', sortButtonsInsertAfterEl, { id: sortByDivElId }, { "class" :  "row" });
        var div = UI.addDomElementChild('div', el);
        UI.addDomElementChild('label', div, { id: sortBySelectElId, innerHTML: "Order by: " });
        div = UI.addDomElementChild('div', el, { id: sortOrderButtonGroupElId }, { "class" :  "yui-buttongroup search-button-style" });
        UI.addDomElementChild('input', div, { type: "radio", name: "sorttype", value: "asc", id: "sort_asc"});
        UI.addDomElementChild('input', div, { type: "radio", name: "sorttype", value: "desc", id: "sort_desc", checked: "checked"});
        UI.addDomElementChild('div', el, null, { "class" :  "clearboth" });

        // Search result HTML
        var searchResultsInsertAfterEl = Dom.get(insertSearchResultsAfterElName);
        el = UI.insertDomElementAfter('div', searchResultsInsertAfterEl);
        div = UI.addDomElementChild('div', el, { id : searchResultContainerElId }, { width: searchResultContainerElWidth } );

        UI.addDomElementChild('div', div, { id : numFoundElId });
        UI.addDomElementChild('div', div, { id : searchResultsElId });
        UI.addDomElementChild('div', el,  { id : previewContainerElId });
        UI.addClearBothDiv(el);

        UI.insertDomElementAfter('div', searchResultsInsertAfterEl, { id: paginatorElId });

        // Show solr query
        UI.insertDomElementAfter('a', searchResultsInsertAfterEl, { id : showQueryButtonElId, innerHTML: "Show Solr Query" },
            { 'class' : 'button small' } );
        var showQueryEl = UI.addDomElementChild('div', Dom.get("collections_tab_content"), { id : showQueryElId });
        UI.addDomElementChild('div', showQueryEl, { innerHTML: "Query submitted to Solr"}, { "class" : "hd" } );
        UI.addDomElementChild('div', showQueryEl, { id : showQueryElIdBody }, { "class" : "bd" } );

        // Build buttons
        var searchButtonsInsertAfterEl = Dom.get(insertSearchButtonsAfterElName);
        el = UI.insertDomElementAfter('div', searchButtonsInsertAfterEl, null, { "class" :  "buttons", "padding-bottom" : "5px" });
        UI.addDomElementChild('a', el, { id: submitButtonElId,  innerHTML: "Search",  href: "#"}, { "class" :  "button small"});
        UI.addDomElementChild('a', el, { id: resetButtonElId,   innerHTML: "Reset",   href: "#"}, { "class" :  "button small"});
        UI.addDomElementChild('a', el, { id: exportButtonElId,  innerHTML: "Export",  href: "#"}, { "class" :  "button small"});
        UI.addDomElementChild('a', el, { id: analyzeButtonElId, innerHTML: "Analyze", href: "#"}, { "class" :  "button small"});

        UI.insertDomElementAfter('div', el, null, { "class" :  "row" });

        currPreviewContainerEl = Dom.get(previewContainerElId);

        setSearchHeader();
        initSearchTab();
        initSelectData(selectDataRegexIgnore);
        initSortBySelect();
        initControlButtonListeners();
        initPaginator();
        initSortOrderButtonGroup();
        initShowQueryPanel();
    }

    function searchIfInitialQuerySpecified() {
        var initialQuery = YAHOO.deconcept.util.getRequestParameter(UI.INITIAL_QUERY_KEY);
        if (initialQuery == '') return;

        Event.onContentReady(UI.FACET.FACET_OPTIONS_DIV_EL_NAME, function() {
            searchInputEls[getSearchTabActiveIndex()].value = decodeURIComponent(initialQuery);
            SEARCH.ui.search();
        });
    }

    SEARCH.ui.getUrlSearchParams = function() {
        return urlSearchParams;
    };

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

    function updateDocsFromLastSearch(docs) {
        var i, docId, doc;
        docsFromLastSearch = {};
        for(i = 0; i < docs.length; i++) {
            doc = docs[i];
            docId = doc.id;
            delete doc.id;
            delete doc.url;
            docsFromLastSearch[docId] = doc;
        }
    }

    var searchSuccessCallback = function(o) {
        UI.hideWait();
        var result = Json.parse(o.responseText);
        facetsFromLastSearch = result[solrResponseKey][solrResponseFacetKey];
        buildSearchResultHtmlFn(result);

        updateSolrQueryDiv(decodeURIComponent(result.response.q));
        updateDocsFromLastSearch(result[solrResponseKey][solrResponseDocsKey]);
        updateNumFound(result[solrResponseKey][solrResponseNumFoundKey], 1);
        updatePaginatorAfterSearch();

        if (numFound > 0 && updateFacetFn != null) {
            updateFacetFn(facetsFromLastSearch);
        }
    };

    var searchFailureCallback = function() {
        alert("Could not connect to Solr");
    };

    SEARCH.ui.search = function() {
        UI.showWait();

        var fq = getFilterQueryFn();
        var queryTerms = getQueryTerms();
        urlSearchParams = constructUrlSearchParams(queryTerms, fq, 0);

        clearPreviewContainerImage();

        Connect.asyncRequest('GET', searchUrl + urlSearchParams, {
            success : searchSuccessCallback,
            failure : searchFailureCallback
        });
    };

    SEARCH.ui.getUrlSearchParams = function() {
        return (urlSearchParams == "") ? "" : urlSearchParams + "&" + numFoundRequestParam + "=" + numFound;
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

        Event.addListener(analyzeButtonElId, "click", function (e) {
            Event.stopEvent(e);
            var url = analyzeUrl + (unstructuredData ? "/wordtree" + SEARCH.ui.getUrlSearchParams() :
                                                       SEARCH.ui.getUrlSearchParams() + '&structured=true');
            window.open(url, "_blank");
        });
    }

    function initShowQueryPanel() {
        showQueryPanel = new Panel(showQueryElId, { width : "500px", visible : false, constraintoviewport: true } );
        showQueryPanel.render();

        Event.addListener(showQueryButtonElId, "click", function(o) { showQueryPanel.show(); });
    }

    function initSearchTab() {
        searchTab = new TabView(searchTabElId);
    }

    function initToolTip(tooltipElName) {
        tooltip = new Tooltip(tooltipElName, { zIndex : 20 });
    }
    
    function initSortOrderButtonGroup() {
        sortOrderButtonGroup = new ButtonGroup(sortOrderButtonGroupElId);
        sortOrderButtonGroup.check(1);
    }

    function initPaginator() {
        paginator = new Paginator( { rowsPerPage : rowsPerPage, containers : [ paginatorElId ] });
        paginator.subscribe('changeRequest', handlePagination);
    }

    function setSearchHeader() {
        Dom.get(searchHeaderElName).innerHTML = "Search " + displayName;
    }

    function updateNumFound (n, start) {
        numFound = n;
        var s = "Found " + numFound + " document" + ((numFound != 1) ? "s" : "");
        if (numFound > 0) {
            s += ", showing items " + start + " through " + Math.min(start + rowsPerPage - 1, numFound);
        }
        Dom.get(numFoundElId).innerHTML = s;
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
        htmlStr += "<img src='" + dataStr + "' height='400px' width='400px' id='" + previewDataId + "'>";
        currPreviewContainerEl.innerHTML = htmlStr;

        Dom.setStyle(previewDataId, 'zoom', 2);
        Event.addListener('previewzoomin', 'click', function(e) {
            Dom.setStyle(previewDataId, 'zoom', parseInt(Dom.getStyle(previewDataId, 'zoom')) + 1);
        });

        Event.addListener('previewzoomout', 'click', function(e) {
            var zoom = parseInt(Dom.getStyle(previewDataId, 'zoom'));
            Dom.setStyle(previewDataId, 'zoom', (zoom > 1) ? zoom - 1 : zoom);
        });
    }

    function setPreviewContainerTextData(title, dataStr) {
        var htmlStr = getPreviewHtmlTitleString(title);
        var dispStr = (dataStr != noPreviewText) ? UI.util.jsonSyntaxHighlight(dataStr) :
            "<span style='font-weight: bold; font-size: 12px; margin-left: 10px'>" + dataStr + "</span>";
        htmlStr += "<div id='" + previewDataId + "'>" + dispStr + "</div>";
        currPreviewContainerEl.innerHTML = htmlStr;

        Dom.setStyle(previewDataId, 'font-size', '10px');
        Event.addListener('previewzoomin', 'click', function(e) {
            var px = UI.util.getNPixels(Dom.getStyle(previewDataId, 'font-size'));
            Dom.setStyle(previewDataId, 'font-size', (px + 1) + 'px');
        });

        Event.addListener('previewzoomout', 'click', function(e) {
            var px = UI.util.getNPixels(Dom.getStyle(previewDataId, 'font-size'));
            Dom.setStyle(previewDataId, 'font-size', (px - 1) + 'px');
        });
    }

    function setPreviewContainerData(title, dataStr, dataType) {
        Dom.setStyle(currPreviewContainerEl, "border", "1px solid gray");
        Dom.setStyle(currPreviewContainerEl, "background-image", "none");

        if (dataStr == "" || dataStr == undefined) {
            dataType = "text";
            dataStr  = noPreviewText;
        }

        if (dataType == solrDocImageContentType) {
            //console.log(arguments.callee.caller.toString());
            setPreviewContainerImageData(title, dataStr);
        } else {
            setPreviewContainerTextData(title, dataStr);
        }
    }

    function setPreviewContainerLoadingImage() {
        currPreviewContainerEl.innerHTML = "";
        Dom.setStyle(currPreviewContainerEl, "background-image", "url(\"" + loadingImgUrl + "\")");
        Dom.setStyle(currPreviewContainerEl, "background-position", "50% 10%");
        Dom.setStyle(currPreviewContainerEl, "background-repeat", "no-repeat");
    }

    function clearPreviewContainerImage() {
        Dom.setStyle(currPreviewContainerEl, "border", "none");
        currPreviewContainerEl.innerHTML = "";
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
            container   : sortBySelectElId
        });

        var sortBySelectMenu = sortBySelect.getMenu();
        sortBySelectMenu.subscribe("render", function (type, args, button) {
            button.set("selectedMenuItem", this.getItem(initialSelectIndex));
        }, sortBySelect);

        sortBySelect.on("selectedMenuItemChange", function (e) {
            var newVal = e.newValue.cfg.getProperty('text');
            this.set('label', ('<em class="' + yuiButtonLabelCSSClass + '">' + newVal + '</em>'));

            var oldVal = e.prevValue;
            if (docsFromLastSearch != null && oldVal != null && oldVal.cfg.getProperty('text') != newVal) {
                SEARCH.ui.search();
            }
        });
    }

    function handlePagination(newState) {
        UI.showWait();
        var newStart = newState.records[0], newStartStr = "&start=" + newStart;
        updateNumFound(numFound, newStart);

        var match = urlSearchParams.match(/(&start=[0-9]+)/);
        urlSearchParams = (match == null) ? urlSearchParams + newStartStr : urlSearchParams.replace(match[1], newStartStr);

        Connect.asyncRequest('GET', searchUrl + urlSearchParams, {
            success: function(o) {
                UI.hideWait();

                var result = Json.parse(o.responseText);
                buildSearchResultHtmlFn(result);
                updateDocsFromLastSearch(result[solrResponseKey][solrResponseDocsKey]);
            }
        });

        paginator.setState(newState);
    }

    function updateSolrQueryDiv (params) {
        var s = [], paramArgs = params.split("&");
        for(var i = 0; i < paramArgs.length; i++) {
            if (paramArgs[i].match(/^(facet|fl|hl)/) == null) {
                s.push(paramArgs[i] + '<br>');
            }
        }
        Dom.setStyle(Dom.get(showQueryButtonElId), "visibility", "visible");
        Dom.get(showQueryElIdBody).innerHTML = s.join('&');
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
              data = record.getData(),
            dataId = UI.util.returnEmptyIfUndefined(data.id);

        if (data[thumbnailKey] == undefined) {
            dataTable.updateCell(record, thumbnailKey, docsFromLastSearch[dataId]);
            dataTable.updateCell(record, thumbnailTypeKey, solrDocJSONContentType);
        }

        setPreviewContainerData(data.title, data[thumbnailKey], data[thumbnailTypeKey]);
    }


    function buildSearchResultDataTableHtml(result) {
        var dataSource = new LocalDS(formatSearchResultFn(result.response), {
            responseSchema : {
                resultsList:'docs',
                fields: dataSourceFields
            }
        });

        var dataTable = new DataTable(searchResultsElId, selectDataColumnDefs, dataSource, { width:"100%" });

        /*dataTable.subscribe("sortedByChange", function(e) {
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
        }); */

        dataTable.subscribe("cellClickEvent", function (e) {
            Event.stopEvent(e.event);
            var data = this.getRecord(e.target).getData();

            var urlParams = "?core=" + SEARCH.ui.coreName + "&id=" + data.id + "&view=preview";
            window.open(viewDocUrl + urlParams, "_blank");
        });

        dataTable.on('rowMouseoverEvent', function (e) {
            dataTableCellMouseoverEvent(e, dataTable, loadingImgUrl, thumbnailUrl);
        });
    }

    function getIndexFromSearchResultDivId(id)  { return id.match("searchResult([0-9]+)")[1]; }
    function constructSearchResultDivId(i)      { return "searchResult" + i; }
    function constructPreviewDivId(i)           { return "preview" + i; }
    function constructLinkDivId(i)              { return "link" + i; }

    function searchResultListMouseEnter(e) {
        clearPreviewContainerImage();

        var idx     = getIndexFromSearchResultDivId(this.id);
        var href    = Dom.get(constructLinkDivId(idx)).href;
        var file    = href.match("id=([^&]+)(&|$)")[1];
        var segment = href.match("segment=([^&]+)(&|$)")[1];
        var title   = file.substring(file.lastIndexOf("/") + 1);

        currPreviewContainerEl = Dom.get(constructPreviewDivId(idx));

        if (thumbnailData[file] == undefined) {
            setPreviewContainerLoadingImage();
            var urlParams = "?core=" + SEARCH.ui.coreName + "&segment=" + segment + "&id=" + file;

            Connect.asyncRequest('GET', thumbnailUrl + urlParams, {
                success: function(o) {
                    var response = Json.parse(o.responseText);
                    thumbnailData[file] = {};
                    thumbnailData[file][solrDocContentsKey]    = response[solrDocContentsKey];
                    thumbnailData[file][solrDocContentTypeKey] = response[solrDocContentTypeKey];

                    setPreviewContainerData(title, response[solrDocContentsKey], response[solrDocContentTypeKey]);
                }
            });

        } else {
            setPreviewContainerData(title, thumbnailData[file][solrDocContentsKey], thumbnailData[file][solrDocContentTypeKey]);
        }
    }

    function buildSearchResultList(result) {
        var searchResultsEl = Dom.get(searchResultsElId);
        searchResultsEl.innerHTML = "";

        var docs      = result[solrResponseKey][solrResponseDocsKey],
            highlight = result[solrResponseKey][solrResponseHighlightingKey];

        for(var i = 0; i < docs.length; i++) {
            var doc               = docs[i],
                titleHref         = viewDocUrl + "?view=preview&core=" + SEARCH.ui.coreName + "&id=" + doc[solrDocIdFieldName],
                titleId           = constructLinkDivId(i),
                searchResultDivId = constructSearchResultDivId(i),
                previewDivId      = constructPreviewDivId(i);

            var pdiv   = UI.addDomElementChild("div", searchResultsEl);

            var div   = UI.addDomElementChild("div", pdiv, { id: searchResultDivId },
                { "class" :  searchResultDivCSSClass, width: "56%", float: "left" });
            var title = UI.addDomElementChild("div", div);
            UI.addDomElementChild("a", title, { id: titleId, href: titleHref, innerHTML: doc['title'] }, { color: "blue" });
            UI.addDomElementChild("div", div, { innerHTML: "Author: <b>" + doc['author'] + "</b>"},
                { "class" :  searchResultSubheadingCSSClass });
            UI.addDomElementChild("div", div, { innerHTML: "Content type: <b>" + doc[solrDocContentTypeKey] + "</b>"},
                { "class" :  searchResultSubheadingCSSClass });
            UI.addDomElementChild("div", div, { innerHTML: "Created On: <i>" + doc['creation_date'] + "</i>, " +
                    "Last Modified: <i>" + doc['last_modified'] + "</i>"},
                { "class" :  searchResultSubheadingCSSClass});

            var highlightObj = highlight[doc[solrDocIdFieldName]];
            Object.keys(highlightObj).forEach(function(key) {
                UI.addDomElementChild("div", div, { innerHTML: "Field <b>" + key + "</b>"},
                    { "class" :  searchResultSubheadingCSSClass, color: "darkgreen"});
                highlightObj[key].forEach(function(s) {
                    UI.addDomElementChild("div", div, { innerHTML: "... " + s + "... "}, { "class" : "search_result"});
                })
            });

            UI.addDomElementChild("div", pdiv, { id: previewDivId }, { "class" :  previewDivCSSClass, float: "left" });
            UI.addClearBothDiv(pdiv);

            Event.addListener(titleId, "click", function(e) {
                Event.stopEvent(e);
                window.open(this.href, "_blank");
            });

            Event.addListener(searchResultDivId, "mouseenter", searchResultListMouseEnter);
        }
    }

})();