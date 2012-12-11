var DATA_TABVIEW = {};

(function() {
    var Dom = YAHOO.util.Dom, TabView = YAHOO.widget.TabView;

    var contentElName       = "content",
        contentEl           = Dom.get(contentElName),

        collectionsTabElId              = "collections_tab",
        collectionsTabElCSSClass        = "yui-navset",
        collectionsTabListElId          = "collections_tab_list",
        collectionsTabListCSSClass      = "yui-nav",
        collectionsTabContentElId       = "collections_tab_content",
        collectionsTabContentCSSClass   = "yui-content",
        selectedCSSClass                = "selected",
        tabUnselectedCSSClass           = "collections_tab_unselected",
        tabSelectedCSSClass             = "collections_tab_selected",

        searchHeaderElName          = "search_header",
        searchFormElName            = "search_form",
        searchTabElName             = "search_tab",
        searchTabElCSSClass         = "yui-navset",
        searchTabListElName         = "search_tab_list",
        searchTabListElCSSClass     = "yui-nav",
        searchTabContentElName      = "search_tab_content",
        searchTabContentElCSSClass  = "yui-content",
        clearbothCSSClass           = "clearboth",

        insertSortButtonsAfterElName   = "insert_sortbuttons_after",
        insertFacetsAfterElName        = "insert_facets_after",
        insertSearchButtonsAfterElName = "insert_searchbuttons_after",
        constrainByDateElName          = "constrain_by_date",

        dataCoreNames = [],
        urls                    = {},
        buildCoreTabHtmlFn      = buildCoreTabHTML,

        collectionsTab                  = null;

    DATA_TABVIEW.init = function(names) {
        urls = names['urls'];
        dataCoreNames = names['coreNames'];

        buildCoreTabHtmlFn = UI.util.returnIfDefined(buildCoreTabHtmlFn, names['buildCoreTabHtmlFn']);

        buildHTML(names['selectedCore']);

        SEARCHAUTOCOMPLETE.ui.init({
            url : urls['suggestUrl'],
            searchFn : search,
            acKeyupFn : acKeyup,
            tabListElName : searchTabListElName,
            tabContentElName : searchTabContentElName
        });

        QUERYBUILDER.ui.init({
            url : urls['queryBuilderAutoCompleteUrl'],
            tabListElName : searchTabListElName,
            tabContentElName: searchTabContentElName
        });

        SEARCH.ui.init({
            selectDataColumnDefs : names['selectDataColumnDefs'],
            dataSourceFields : names['dataSourceFields'],
            selectDataRegexIgnore : UI.util.specifyReturnValueIfUndefined(names['selectDataRegexIgnore'], 'thumbnail'),
            submitFn : search,
            resetFn : reset,
            formatSearchResultFn : names['formatSearchResultFn'],
            dataType: UI.util.specifyReturnValueIfUndefined(names['dataType'], 'structured'),
            getFilterQueryFn : getFilterQueryString,
            urls : urls,
            searchTabElName : searchTabElName,
            searchHeaderElName : searchHeaderElName,
            searchInputEls : getSearchInputEls(),
            insertSearchResultsAfterElName : searchFormElName,
            insertSortButtonsAfterElName : insertSortButtonsAfterElName,
            insertSearchButtonsAfterElName : insertSearchButtonsAfterElName
        });

        FACET.ui.init({
            facetTreeUrl : urls['facetUrl'],
            insertFacetHtmlAfterElName : insertFacetsAfterElName
        });

        DATEPICK.ui.init({
            dateField : names['dateField'],
            dateRangeUrl : urls['datePickerUrl'],
            datePickElName: constrainByDateElName });

        EXPORT.ui.init({
            exportUrl : urls['exportUrl'],
            openSeparateExportPage : UI.util.specifyReturnValueIfUndefined(names['openSeparateExportPage'], true),
            exportButtonElId : SEARCH.ui.getExportButtonElId(),
            getExportUrlParamsFn : SEARCH.ui.getUrlSearchParams
        });
    };

    function exportUrlParams() {
        var p = SEARCH.ui.getUrlSearchParams();
        return (p == "") ? p : p + "&type=csv";
    }

    function acKeyup() {
        if (this.value == "") {
            FACET.ui.buildInitialFacetTree();
        }
    }

    function reset() {
        SEARCH.ui.resetQuerySearchInputs();
        FACET.ui.buildInitialFacetTree();
    }

    function getFilterQueryString() {
        return FACET.util.getFacetFilterQueryString() + DATEPICK.util.getDateConstraintFilterQueryString();
    }

    function search() {
        if (SEARCH.ui.search() > 0) {
            FACET.ui.buildFacetTree(SEARCH.ui.facetsFromLastSearch);
        }
    }

    function getSearchInputEls() {
        return [Dom.get(SEARCHAUTOCOMPLETE.ui.getAutocompleteInputElName()),
                Dom.get(QUERYBUILDER.ui.getGeneralQuerySearchInputElName())];
    }

    function buildHTML(selectedCore) {
        var el = UI.addDomElementChild('div', contentEl, { id: collectionsTabElId },
            { class : collectionsTabElCSSClass, background: "white", border: "none"} );
        var ul = UI.addDomElementChild('ul', el, { id : collectionsTabListElId },
            { class : collectionsTabListCSSClass, border: "solid #777", "border-width": "0 0 2px" });

        dataCoreNames.forEach(function(coreName) {
            var li = UI.addDomElementChild('li', ul);
            var tabColor = "#AAA";
            if (coreName == selectedCore) {
                Dom.addClass(li, selectedCSSClass);
                tabColor = "#777";
            }

            UI.addDomElementChild('a', li, { innerHTML: "<em style='border:none'>" + coreName + "</em>", href : "#"},
                { border: "1px solid " + tabColor, "border-radius" : "4px 4px 0 0", background: tabColor });
        });
        UI.addDomElementChild('div', el, { id : collectionsTabContentElId },
            { class: collectionsTabContentCSSClass, border : "1px groove whitesmoke", background : "white" });

        collectionsTab = new TabView(collectionsTabElId);

        function handleClick(e) {
            window.location = urls['searchBaseUrl'] + this.get("label");
        }

        for(var i = 0; i < dataCoreNames.length; i++) {
            collectionsTab.getTab(i).addListener('click', handleClick);
        }

        buildCoreTabHtmlFn(collectionsTabContentElId);
    }

    function buildCoreTabHTML() {
        var tabContentEl = Dom.get(collectionsTabContentElId);
        var el = UI.addDomElementChild('div', tabContentEl);
        UI.addDomElementChild('h2', el, { id: searchHeaderElName });
        var form = UI.addDomElementChild('form', el, { id : searchFormElName });
        var div = UI.addDomElementChild('div', form, { id : searchTabElName }, { class : searchTabElCSSClass });
        UI.addDomElementChild('ul', div, { id : searchTabListElName }, { class : searchTabListElCSSClass });
        UI.addDomElementChild('div', div, { id : searchTabContentElName }, { class : searchTabContentElCSSClass });

        UI.addDomElementChild('div', form, { id : insertSortButtonsAfterElName }, { class : clearbothCSSClass });
        UI.addDomElementChild('div', form, { id : constrainByDateElName });
        UI.addDomElementChild('div', form, { id : insertFacetsAfterElName }, { class : clearbothCSSClass });
        UI.addDomElementChild('div', form, { id : insertSearchButtonsAfterElName }, { class : clearbothCSSClass });
    }

})();