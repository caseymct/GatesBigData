<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page pageEncoding="UTF-8" %>
<%@ page contentType="text/html" %>

<layout:main>

    <p class = 'hidden_element' id = 'snippetDataId'>${param['snippetData']}</p>
    <p class = 'hidden_element' id = 'queryDataId'>${param['queryData']}</p>
    <p class = 'hidden_element' id = 'viewDocUrlDataId'>${param['viewDocUrlData']}</p>

    <div id = "info"></div>
    <div id = "graphcontent"></div>

    <div class="clearboth"></div>

    <script src="<c:url value="/static/js/jquery/jquery.js"/>" type="text/javascript"></script>
    <script src="<c:url value="/static/js/wordtree/raphael.js"/>" type="text/javascript"></script>
    <script src="<c:url value="/static/js/wordtree/word-tree.js"/>" type="text/javascript"></script>
    <script src="<c:url value="/static/js/wordtree/word-tree-layout.js"/>" type="text/javascript"></script>

    <script type="text/javascript">
    (function() {
        var Dom  = YAHOO.util.Dom,      Connect = YAHOO.util.Connect,
            Json = YAHOO.lang.JSON,     Event = YAHOO.util.Event;

        var searchImg = '<c:url value="/static/images/wordtree/search.gif" />';
        Dom.setStyle(Dom.get(UI.CONTENT_EL_NAME), "width", "3000px");

        var rawQueryData     = Dom.get('queryDataId').innerHTML,
            rawSnippetData   = Dom.get('snippetDataId').innerHTML,
            viewDocUrl       = Dom.get('viewDocUrlDataId').innerHTML,
            infoEl           = Dom.get('info'),
            graphContentEl   = Dom.get('graphcontent');

        var PREFIX_KEY              = 'prefix',
            SUFFIX_KEY              = 'suffix',
            ALT_QUERIES_KEY         = 'alternate_queries',
            MV_BTN_EL_TEXT          = '_mvbtn_',
            GRAPH_EL_TEXT           = '_graph',
            SEARCH_CLICK_EL_TEXT    = '_search_click',
            SEARCH_INPUT_EL_TEXT    = '_search_input',
            SEARCH_RESULTS_EL_TEXT  = '_search_results',
            ONLY_SEL_INPUT_EL_TEXT  = '_search_onlysel',
            RECENTER_CLICK_EL_TEXT  = '_recenter',
            DIRS                    = ['left', 'right', 'up', 'down'],
            MOVE_INCR               = 50,
            PAPER_HEIGHT            = 500,
            PAPER_WIDTH             = 2000;

        updateInfoDiv();

        function getSvgGraphElName(field)           { return field + GRAPH_EL_TEXT; }
        function getRecenterClickElName(field)      { return field + RECENTER_CLICK_EL_TEXT; }
        function getSearchClickElName(field)        { return field + SEARCH_CLICK_EL_TEXT; }
        function getSearchInputElName(field)        { return field + SEARCH_INPUT_EL_TEXT; }
        function getSearchOnlySelInputElName(field) { return field + ONLY_SEL_INPUT_EL_TEXT; }
        function getSearchResultsInputElName(field) { return field + SEARCH_RESULTS_EL_TEXT; }
        function getSearchInputValue(field)         { return Dom.get(getSearchInputElName(field)).value; }
        function getSearchOnlySelValue(field)       { return Dom.get(getSearchOnlySelInputElName(field)).value == 'on'; }

        function getMoveButtonElName(field, dir)    { return field + MV_BTN_EL_TEXT + dir; }
        function getFieldAndDirFromMoveButtonElName(elName) {
            var f = elName.split(MV_BTN_EL_TEXT);
            return { field : f[0], dir : f[1] };
        }

        function getFieldFromSearchClickElName(el)   {  return el.substring(0, el.indexOf(SEARCH_CLICK_EL_TEXT)); }
        function getFieldFromRecenterClickElName(el) {  return el.substring(0, el.indexOf(RECENTER_CLICK_EL_TEXT)); }

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

        var hl = {};
        hl["hl"] = fixHlStrings(rawSnippetData);

        // send the event
        Connect.initHeader('Content-Type', 'application/json');
        Connect.setDefaultPostHeader('application/json');
        Connect.asyncRequest('POST', '<c:url value="/analyze/snippets" />', {
            success: function(o) {
                var response = Json.parse(o.responseText);
                buildGraphicResponse(response);
            }
        }, Json.stringify(hl));


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

                    function addToSelect(select, nodes) {
                        for(var i = 0; i < nodes.length; i++) {
                            select.options[select.options.length] = new Option(nodes[i].dsc, nodes[i].id);
                        }
                    }
                    addToSelect(select, searchResults['ltreeNodes']);
                    addToSelect(select, searchResults['rtreeNodes']);
                });

                Event.addListener()
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
            var parentDiv = UI.addDomElementChild('div', graphContentEl, { id : field + "_div" }, { "padding-top" : "30px"});
            var headerDiv = UI.addDomElementChild('div', parentDiv, { id : field + "_hdr" });

            var resultsLabelDiv = UI.addDomElementChild('div', headerDiv, {}, { float : 'left', width: '100%'} );
            UI.addDomElementChild('div', resultsLabelDiv, { innerHTML: "Sentence results from field " + field },
                    { float : "left", "font-weight" : "bold", "padding-right" : "10px"});

            var recenterDiv = UI.addDomElementChild('div', headerDiv, {}, { float : 'left', width: '100%'} );
            UI.addDomElementChild('a', recenterDiv, { innerHTML: 'Recenter on selected', id : field + '_recenter' },
                    { float : "left", "font-weight" : "bold", "padding-right" : "10px"});

            var searchDiv = UI.addDomElementChild('div', headerDiv, {}, { float : 'left', width: '350px'} );
            var searchLabelDiv = UI.addDomElementChild('div', searchDiv, {}, { float : 'left', width: '350px'} );
            var label = UI.addDomElementChild('label', searchLabelDiv, { innerHTML : 'Search '});
            var searchInput = UI.addDomElementChild('input', label,
                    { id : getSearchInputElName(field), value : '', size: '10', maxlength : '100'});
            var searchLink = UI.addDomElementChild('a', searchLabelDiv,
                    { id : getSearchClickElName(field), href: '#'});
            UI.addDomElementChild('img', searchLink, { src : searchImg, border : 0 },
                    { 'vertical-align' : 'middle', height : '20px', width : '20px'});

            var searchResultsDiv = UI.addDomElementChild('div', headerDiv, {}, { float : 'left', width: '350px'} );
            var searchResultsLabelDiv = UI.addDomElementChild('div', searchResultsDiv, {}, { float : 'left', width: '350px'} );
            UI.addDomElementChild('label', searchResultsLabelDiv, { innerHTML : 'Search Results '});
            UI.addDomElementChild('select', searchResultsLabelDiv, { id : getSearchResultsInputElName(field) }, { float : 'left'});

            var onlySelLabel = UI.addDomElementChild('label', searchDiv, { innerHTML : 'Search only children of selected: '},
                    { 'vertical-align' : 'middle', float: 'left', padding: '3px 0px 10px 0px'} );
            var onlySelInput = UI.addDomElementChild('input', onlySelLabel,
                    { id : getSearchOnlySelInputElName(field), type : 'checkbox' });


            UI.addClearBothDiv(headerDiv);

            UI.addDomElementChild('div', parentDiv, { id : getSvgGraphElName(field) });
        }

        function decodedRawQueryData() {
            return decodeURIComponent(rawQueryData).replace(/&amp;/g, "&").replace(/^\?/, '').split("&");
        }

        function updateInfoDiv() {
            var decoded = decodedRawQueryData();
            var div = UI.addDomElementChild('div', infoEl, { id : 'queryInfo'});
            for(var i = 0; i < decoded.length; i++) {
                var midDiv = UI.addDomElementChild('div', div);
                var split = decoded[i].split("=");
                UI.addDomElementChild('div', midDiv, { innerHTML : split[0] },
                        { float : "left", "text-align" : "right", width : "100px", "padding-right" : "10px", "font-weight" : "bold"} );

                UI.addDomElementChild('div', midDiv, { innerHTML : split[1]}, { float : "left"} );
                UI.addClearBothDiv(midDiv);
            }
        }

    })();
</script>
</layout:main>
