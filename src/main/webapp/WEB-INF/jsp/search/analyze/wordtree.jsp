<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page pageEncoding="UTF-8" %>
<%@ page contentType="text/html" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/wordtree.css"/>" />

    <script type="text/javascript" src="<c:url value="/static/js/jquery/jquery.js"/>" ></script>
    <script type="text/javascript" src="<c:url value="/static/js/wordtree/raphael.js"/>" ></script>
    <script type="text/javascript" src="<c:url value="/static/js/wordtree/word-tree.js"/>" ></script>
    <script type="text/javascript" src="<c:url value="/static/js/wordtree/word-tree-layout.js"/>" ></script>

    <script type="text/javascript">
    (function() {
        var Dom  = YAHOO.util.Dom,    Connect = YAHOO.util.Connect,
            Json = YAHOO.lang.JSON,     Event = YAHOO.util.Event;

        var infoElId             = "info",             contentEl            = Dom.get(UI.CONTENT_EL_NAME),
            graphContentElId     = "graphcontent",     queryInfoElId        = 'query_info',
            searchLabelCSSClass  = "search_label_div", searchHdrLblCSSClass = 'search_header_label',
            onlySelLabelCSSClass = "only_sel_label",   searchImgCSSClass    = 'search_img',
            searchLblCtrCSSClass = 'search_label_container_div',
            justFirstPage        = false;

        var requestParams       = UI.util.getRequestParameters(),
            structured          = requestParams[UI.STRUCTURED_DATA_EL_ID_KEY],
            coreName            = requestParams['core'],
            analysisField       = requestParams['analysisfield'],
            requestFields       = ['fl', 'query', 'core', 'analysisfield', 'fq'],
            extraFields         = [ { key : 'hl', value : !structured }, { key : 'rows', value : requestParams['numfound'] }],
            urlRequestParams    = UI.util.constructRequestString(requestParams, requestFields, extraFields);

        var baseUrl             = '<c:url value="/" />',
            searchImg           = baseUrl + 'static/images/wordtree/search.gif',
            analyzeAllUrl       = baseUrl + 'analyze/snippets/all' + urlRequestParams,
            analyzeFirstPageUrl = baseUrl + 'analyze/snippets/firstpage' + urlRequestParams,
            viewDocUrl          = baseUrl + 'core/document/' + (structured ? 'view' : 'prizmview') + '?view=preview&core=' + coreName;

        var PREFIX_KEY              = 'prefix',             SUFFIX_KEY              = 'suffix',
            ALT_QUERIES_KEY         = 'alternate_queries',  MV_BTN_EL_TEXT          = '_mvbtn_',
            GRAPH_EL_TEXT           = '_graph',             SEARCH_CLICK_EL_TEXT    = '_search_click',
            SEARCH_INPUT_EL_TEXT    = '_search_input',      SEARCH_RESULTS_EL_TEXT  = '_search_results',
            ONLY_SEL_INPUT_EL_TEXT  = '_search_onlysel',    RECENTER_CLICK_EL_TEXT  = '_recenter',
            MOVE_INCR               = 50,                   PAPER_HEIGHT            = 500,
            PAPER_WIDTH             = 2000,                 DIRS                    = ['left', 'right', 'up', 'down'];

        buildHtml();

        function getSvgGraphElName(field)           { return field + GRAPH_EL_TEXT; }
        function getRecenterClickElName(field)      { return field + RECENTER_CLICK_EL_TEXT; }
        function getSearchClickElName(field)        { return field + SEARCH_CLICK_EL_TEXT; }
        function getSearchInputElName(field)        { return field + SEARCH_INPUT_EL_TEXT; }
        function getSearchOnlySelInputElName(field) { return field + ONLY_SEL_INPUT_EL_TEXT; }
        function getSearchResultsInputElName(field) { return field + SEARCH_RESULTS_EL_TEXT; }
        function getSearchInputValue(field)         { return Dom.get(getSearchInputElName(field)).value; }
        function getSearchOnlySelValue(field)       { return Dom.get(getSearchOnlySelInputElName(field)).checked; }

        function getMoveButtonElName(field, dir)    { return field + MV_BTN_EL_TEXT + dir; }
        function getFieldAndDirFromMoveButtonElName(elName) {
            var f = elName.split(MV_BTN_EL_TEXT);
            return { field : f[0], dir : f[1] };
        }

        function getFieldFromSearchClickElName(el)   {  return el.substring(0, el.indexOf(SEARCH_CLICK_EL_TEXT)); }
        function getFieldFromRecenterClickElName(el) {  return el.substring(0, el.indexOf(RECENTER_CLICK_EL_TEXT)); }
        function getFieldFromSearchResultsClickElName(el) {  return el.substring(0, el.indexOf(SEARCH_RESULTS_EL_TEXT)); }

        function getTransform(fieldName, dir) {
            switch (dir) {
                case "left"  : return { x : -1*MOVE_INCR, y : 0 };
                case "right" : return { x : MOVE_INCR, y : 0 };
                case "up"    : return { x : 0, y : -1*MOVE_INCR };
                case "down"  : return { x : 0, y : MOVE_INCR };
            }
            return { x : 0, y : 0 };
        }

        function getHlStrings(s, isPost) {
            var key = 'hl.simple.' + (isPost ? 'post' : 'pre');
            var orig = s.match(eval('/"' + key + '":"(.*?)",/'))[1];
            orig = orig.replace(/\//g, "\\/");
            return [orig, orig.replace(/"/g, "'")];
        }

        function fixHlStrings(s) {
            var pre  = getHlStrings(rawSnippetData, false);
            var post = getHlStrings(rawSnippetData, true);
            return s.replace(eval('/' + pre[0] + '/g'), pre[1]).replace(eval('/' + post[0] + '/g'), post[1]);
        }

        if (!structured) {
            var hl = {};
            hl["hl"] = fixHlStrings(rawSnippetData);

            // send the event
            Connect.initHeader('Content-Type', 'application/json');
            Connect.setDefaultPostHeader('application/json');
            Connect.asyncRequest('POST', analyzeFirstPageUrl, {
                success: function(o) {
                    var response = Json.parse(o.responseText);
                    buildGraphicResponse(response);
                }
            }, Json.stringify(hl));
        } else {
            Connect.asyncRequest('GET', analyzeAllUrl, {
                success: function(o) {
                    var response = Json.parse(o.responseText);
                    buildGraphicResponse(response);
                }
            });
        }

        function buildGraphicResponse(response) {
            var fields = Object.keys(response);

            for(var i = 0; i < fields.length; i++) {
                var fieldName = fields[i], graphElName = getSvgGraphElName(fieldName);
                buildResponseHtml(fieldName);

                Event.addListener(getSearchClickElName(fieldName), 'click', function(e) {
                    Event.stopEvent(e);
                    var fieldName = getFieldFromSearchClickElName(this.id);
                    var searchResults = WORDTREE.search(getSearchInputValue(fieldName), getSearchOnlySelValue(fieldName), 0);
                    var select = Dom.get(getSearchResultsInputElName(fieldName));
                    select.options.length = 0;

                    function addToSelect(nodes, treeDir) {
                        for(var i = 0; i < nodes.length; i++) {
                            select.options[select.options.length] = new Option(nodes[i].dsc, treeDir + '_' + nodes[i].id);
                        }
                    }
                    addToSelect(searchResults['ltreeNodes'], 'L');
                    addToSelect(searchResults['rtreeNodes'], 'R');
                });

                Event.addListener(getSearchResultsInputElName(fieldName), 'change', function(e) {
                    var val = this.options[this.selectedIndex].value.match(/(R|L)_(\d+)/);
                    WORDTREE.selectNode(val[1], val[2], 0);
                    WORDTREE.recenterOnSelected(getSvgGraphElName(getFieldFromSearchResultsClickElName(this.id)));
                });

                Event.addListener(getRecenterClickElName(fieldName), 'click', function(e) {
                    Event.stopEvent(e);
                    WORDTREE.recenterOnSelected(getSvgGraphElName(getFieldFromRecenterClickElName(this.id)));
                });

                var paper = Raphael(graphElName, PAPER_WIDTH, PAPER_HEIGHT);
                WORDTREE.makeWordTrees({
                    paper         : paper,
                    graphElHeight : parseInt(Dom.getStyle(graphElName, "height")),
                    graphElName   : graphElName,
                    viewDocUrl    : viewDocUrl,
                    prefixes      : response[fieldName][PREFIX_KEY],
                    suffixes      : response[fieldName][SUFFIX_KEY],
                    altQueries    : response[fieldName][ALT_QUERIES_KEY]
                });

                formatSvgBorder(fieldName);
            }
        }

        function formatSvgBorder(fieldName) {
            var svg = getSvg(getSvgGraphElName(fieldName));
            Dom.setStyle(svg, "border", "1px solid gray");
            Dom.setStyle(svg, "border-radius", "5px");
        }

        function getSvg(parentNodeName) {
            var parentNode = Dom.get(parentNodeName);
            var children = Dom.getChildrenBy(parentNode, function(node) { return node.tagName == "svg"; });
            return children[0];
        }

        function buildResponseHtml(field) {
            var graphContentEl = Dom.get(graphContentElId);
            var parentDiv = UI.addDomElementChild('div', graphContentEl, { id : field + "_div" }, { "padding-top" : "30px"});
            var headerDiv = UI.addDomElementChild('div', parentDiv, { id : field + "_hdr" });

            var r = UI.addDomElementChild('div', headerDiv, {}, { 'class' : searchLblCtrCSSClass } );
            UI.addDomElementChild('div', r, { innerHTML: "Sentence results from field " + field }, { "class" : searchHdrLblCSSClass });

            r = UI.addDomElementChild('div', headerDiv, {}, { 'class' : searchLblCtrCSSClass } );
            UI.addDomElementChild('a', r, { innerHTML: 'Recenter on selected', id : field + '_recenter' }, { "class" : searchHdrLblCSSClass });

            var searchDiv = UI.addDomElementChild('div', headerDiv, {}, { "class" : searchLabelCSSClass } );
            var searchLabelDiv = UI.addDomElementChild('div', searchDiv, {}, { "class" : searchLabelCSSClass } );
            var label = UI.addDomElementChild('label', searchLabelDiv, { innerHTML : 'Search '});
            UI.addDomElementChild('input', label, { id : getSearchInputElName(field), value : '', size: '10', maxlength : '100'});
            var searchLink = UI.addDomElementChild('a', searchLabelDiv, { id : getSearchClickElName(field), href: '#'});
            UI.addDomElementChild('img', searchLink, { src : searchImg, border : 0 }, { 'class' : searchImgCSSClass });

            var searchResultsDiv = UI.addDomElementChild('div', headerDiv, {}, { "class" : searchLabelCSSClass } );
            var searchResultsLabelDiv = UI.addDomElementChild('div', searchResultsDiv, {}, { "class" : searchLabelCSSClass } );
            UI.addDomElementChild('label', searchResultsLabelDiv, { innerHTML : 'Search Results '});
            UI.addDomElementChild('select', searchResultsLabelDiv, { id : getSearchResultsInputElName(field) }, { float : 'left'});

            var onlySelLabel = UI.addDomElementChild('label', searchDiv, { innerHTML : 'Search only children of selected: '},
                    { 'class' : onlySelLabelCSSClass });
            var onlySelInput = UI.addDomElementChild('input', onlySelLabel, { id : getSearchOnlySelInputElName(field), type : 'checkbox' },
                    { 'vertical-align' : 'middle' });

            UI.addClearBothDiv(headerDiv);
            UI.addDomElementChild('div', parentDiv, { id : getSvgGraphElName(field) });
        }

        function buildHtml() {
            var infoEl = UI.addDomElementChild('div', contentEl, { id : infoElId });
            var div = UI.addDomElementChild('div', infoEl, { id : queryInfoElId });
            var keys = Object.keys(requestParams);
            for(var i = 0; i < keys.length; i++) {
                var midDiv = UI.addDomElementChild('div', div, {}, { 'class' : searchLblCtrCSSClass });
                UI.addDomElementChild('span', midDiv, { innerHTML : keys[i] });
                UI.addDomElementChild('div',  midDiv, { innerHTML : requestParams[keys[i]] });
                UI.addClearBothDiv(midDiv);
            }
            UI.addClearBothDiv(infoEl);

            UI.addDomElementChild('div', contentEl, { id : graphContentElId });
            UI.addClearBothDiv(contentEl);
        }

    })();
</script>
</layout:main>
