var DATA_TABVIEW = {};

(function() {
    var Dom = YAHOO.util.Dom, TabView = YAHOO.widget.TabView;

    var collectionsTab                  = null,
        collectionsTabElId              = "collections_tab",
        collectionsTabElCSSClass        = "yui-navset",
        collectionsTabListElId          = "collections_tab_list",
        collectionsTabListCSSClass      = "yui-nav",
        collectionsTabContentElId       = "collections_tab_content",
        collectionsTabContentCSSClass   = "yui-content",
        selectedCSSClass                = "selected",
        tabUnselectedCSSClass           = "collections_tab_unselected",
        tabSelectedCSSClass             = "collections_tab_selected",

        searchHeaderElName              = "search_header",
        searchFormElName                = "search_form",
        searchTabElName                 = "search_tab",
        searchTabElCSSClass             = "yui-navset",
        searchTabListElName             = "search_tab_list",
        searchTabListElCSSClass         = "yui-nav",
        searchTabContentElName          = "search_tab_content",
        searchTabContentElCSSClass      = "yui-content",
        clearbothCSSClass               = "clearboth",

        insertSortButtonsAfterElName    = "insert_sortbuttons_after",
        insertFacetsAfterElName         = "insert_facets_after",
        insertSearchButtonsAfterElName  = "insert_searchbuttons_after",
        constrainByDateElName           = "constrain_by_date",

        displayNames                    = [],
        coreNames                       = [],
        selectedCore                    = "",
        urls                            = {},
        buildCoreTabHtmlFn              = buildCoreTabHTML;

    DATA_TABVIEW.init = function(vars) {
        urls         = vars[UI.URLS_KEY];
        displayNames = vars[UI.TAB_DISPLAY_NAMES_KEY];
        coreNames    = vars[UI.CORE_NAMES_KEY];
        selectedCore = vars[UI.SELECTED_CORE_KEY];

        buildCoreTabHtmlFn = UI.util.returnIfDefined(buildCoreTabHtmlFn, vars[UI.BUILD_CORE_TAB_HTML_FN_KEY]);

        buildHTML();

        var searchAutoCompleteParams = {};
        searchAutoCompleteParams[UI.SUGGEST_URL_KEY]         = urls[UI.SUGGEST_URL_KEY];
        searchAutoCompleteParams[UI.SEARCH.SEARCH_FN_KEY]    = search;
        searchAutoCompleteParams[UI.SEARCH.ACKEYUP_FN_KEY]   = acKeyup;
        searchAutoCompleteParams[UI.TAB_LIST_EL_NAME_KEY]    = searchTabListElName;
        searchAutoCompleteParams[UI.TAB_CONTENT_EL_NAME_KEY] = searchTabContentElName;
        SEARCHAUTOCOMPLETE.ui.init(searchAutoCompleteParams);

        var queryBuilderParams = {};
        queryBuilderParams[UI.QUERY_BUILDER_AC_URL_KEY] = urls[UI.QUERY_BUILDER_AC_URL_KEY];
        queryBuilderParams[UI.TAB_LIST_EL_NAME_KEY]     = searchTabListElName;
        queryBuilderParams[UI.TAB_CONTENT_EL_NAME_KEY]  = searchTabContentElName;
        QUERYBUILDER.ui.init(queryBuilderParams);

        var searchParams = {};
        searchParams[UI.SEARCH.SELECT_DATA_COLUMN_DEFS_KEY]  = vars[UI.SEARCH.SELECT_DATA_COLUMN_DEFS_KEY];
        searchParams[UI.SEARCH.DATA_SOURCE_FIELDS_KEY]       = vars[UI.SEARCH.DATA_SOURCE_FIELDS_KEY];
        searchParams[UI.SEARCH.SELECT_DATA_REGEX_IGNORE_KEY] = UI.util.specifyReturnValueIfUndefined(vars[UI.SEARCH.SELECT_DATA_REGEX_IGNORE_KEY], UI.THUMBNAIL_KEY);
        searchParams[UI.SEARCH.SUBMIT_FN_KEY]                = search;
        searchParams[UI.SEARCH.RESET_FN_KEY]                 = reset;
        searchParams[UI.SEARCH.FORMAT_SEARCH_RESULT_FN_KEY]  = vars[UI.SEARCH.FORMAT_SEARCH_RESULT_FN_KEY];
        searchParams[UI.DATA_TYPE_KEY]                       = UI.util.specifyReturnValueIfUndefined(vars[UI.DATA_TYPE_KEY], UI.DATA_TYPE_STRUCTURED);
        searchParams[UI.SEARCH.GET_FILTER_QUERY_FN_KEY]      = getFilterQueryString;
        searchParams[UI.URLS_KEY]                            = urls;
        searchParams[UI.SEARCH.SEARCH_TAB_EL_NAME_KEY]       = searchTabElName;
        searchParams[UI.SEARCH.SEARCH_HEADER_EL_NAME_KEY]    = searchHeaderElName;
        searchParams[UI.SEARCH.SEARCH_INPUT_ELS_KEY]         = getSearchInputEls();
        searchParams[UI.DISPLAY_NAME_KEY]                    = getDisplayNameFromCoreName(selectedCore);
        searchParams[UI.FACET.UPDATE_FACET_FN]               = FACET.ui.buildFacetTree;
        searchParams[UI.SEARCH.INSERT_SEARCH_RESULTS_AFTER_EL_NAME_KEY] = searchFormElName;
        searchParams[UI.SEARCH.INSERT_SORT_BUTTONS_AFTER_EL_NAME_KEY]   = insertSortButtonsAfterElName;
        searchParams[UI.SEARCH.INSERT_SEARCH_BUTTONS_AFTER_EL_NAME_KEY] = insertSearchButtonsAfterElName;

        SEARCH.ui.init(searchParams);

        var facetParams = {};
        facetParams[UI.FACET_URL_KEY]                             = urls[UI.FACET_URL_KEY];
        facetParams[UI.FACET.INSERT_FACET_HTML_AFTER_EL_NAME_KEY] = insertFacetsAfterElName;
        FACET.ui.init(facetParams);

        var datePickParams = {};
        datePickParams[UI.DATEPICK.DATE_FIELD_KEY]        = vars[UI.DATEPICK.DATE_FIELD_KEY];
        datePickParams[UI.DATE_PICKER_URL_KEY]            = urls[UI.DATE_PICKER_URL_KEY];
        datePickParams[UI.DATEPICK.DATE_PICK_EL_NAME_KEY] = constrainByDateElName;
        DATEPICK.ui.init(datePickParams);

        var exportParams = {};
        exportParams[UI.EXPORT_URL_KEY]                       = UI.util.specifyReturnValueIfUndefined(urls[UI.EXPORT_URL_KEY], urls[UI.JSP_EXPORT_URL_KEY]);
        exportParams[UI.EXPORT.OPEN_SEPARATE_EXPORT_PAGE_KEY] = UI.util.specifyReturnValueIfUndefined(vars[UI.EXPORT.OPEN_SEPARATE_EXPORT_PAGE_KEY], true);
        exportParams[UI.EXPORT.EXPORT_BUTTON_EL_ID_KEY]       = SEARCH.ui.getExportButtonElId();
        exportParams[UI.SEARCH.GET_SEARCH_REQ_PARAMS_FN_KEY]  = SEARCH.ui.getUrlSearchParams;
        exportParams[UI.EXPORT.HTML_SIBLING_NAME_KEY]         = searchFormElName;
        EXPORT.ui.init(exportParams);
    };

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
        return UI.getFilterOptionsQueryString(UI.FACET.FACET_OPTIONS_DIV_EL_NAME,
                                              UI.combineFacetValues, UI.formatFacetString) +
               DATEPICK.util.getDateConstraintFilterQueryString();
    }

    function search() {
        SEARCH.ui.search();
    }

    function getSearchInputEls() {
        return [Dom.get(SEARCHAUTOCOMPLETE.ui.getAutocompleteInputElName()),
                Dom.get(QUERYBUILDER.ui.getGeneralQuerySearchInputElName())];
    }

    function buildHTML() {
        var el = UI.addDomElementChild('div', Dom.get(UI.CONTENT_EL_NAME), { id: collectionsTabElId },
            { "class" :  collectionsTabElCSSClass, background: "white", border: "none"} );
        var ul = UI.addDomElementChild('ul', el, { id : collectionsTabListElId },
            { "class" :  collectionsTabListCSSClass, border: "solid #777", "border-width": "0 0 2px" });

        for(i = 0; i < coreNames.length; i++) {
            var li = UI.addDomElementChild('li', ul);
            var tabColor = "#AAA";
            if (coreNames[i] == selectedCore) {
                Dom.addClass(li, selectedCSSClass);
                tabColor = "#777";
            }

            UI.addDomElementChild('a', li,
                { innerHTML: "<em style='border:none'>" + displayNames[i] + "</em>", href : "#"},
                { border: "1px solid " + tabColor, "border-radius" : "4px 4px 0 0", background: tabColor });
        }

        UI.addDomElementChild('div', el, { id : collectionsTabContentElId },
            { "class" :  collectionsTabContentCSSClass, border : "1px groove whitesmoke", background : "white" });

        collectionsTab = new TabView(collectionsTabElId);

        function handleClick(e) {
            window.location = urls[UI.SEARCH_BASE_URL_KEY] + getCoreNameFromDisplayName(this.get('label'));
        }

        for(var i = 0; i < coreNames.length; i++) {
            collectionsTab.getTab(i).addListener('click', handleClick);
        }

        buildCoreTabHtmlFn(collectionsTabContentElId);
    }

    function getCoreNameFromDisplayName(displayName) {
        return coreNames[displayNames.indexOf(displayName)];
    }

    function getDisplayNameFromCoreName(coreName) {
        return displayNames[coreNames.indexOf(coreName)];
    }

    function buildCoreTabHTML() {
        var tabContentEl = Dom.get(collectionsTabContentElId);
        var el = UI.addDomElementChild('div', tabContentEl);
        UI.addDomElementChild('h2', el, { id: searchHeaderElName });
        var form = UI.addDomElementChild('form', el, { id : searchFormElName });
        var div = UI.addDomElementChild('div', form, { id : searchTabElName }, { "class" :  searchTabElCSSClass });
        UI.addDomElementChild('ul', div, { id : searchTabListElName }, { "class" :  searchTabListElCSSClass });
        UI.addDomElementChild('div', div, { id : searchTabContentElName }, { "class" :  searchTabContentElCSSClass });

        UI.addDomElementChild('div', form, { id : insertSortButtonsAfterElName }, { "class" :  clearbothCSSClass });
        UI.addDomElementChild('div', form, { id : constrainByDateElName });
        UI.addDomElementChild('div', form, { id : insertFacetsAfterElName }, { "class" :  clearbothCSSClass });
        UI.addDomElementChild('div', form, { id : insertSearchButtonsAfterElName }, { "class" :  clearbothCSSClass });
    }

})();