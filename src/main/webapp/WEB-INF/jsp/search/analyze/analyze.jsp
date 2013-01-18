<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page pageEncoding="UTF-8" %>
<%@ page contentType="text/html" %>

<layout:main>

    <p class = 'hidden_element' id = 'snippetDataId'>${param['snippetData']}</p>
    <p class = 'hidden_element' id = 'queryDataId'>${param['queryData']}</p>

    <div id = "info"></div>
    <div id = "graphcontent"></div>

    <div class="clearboth"></div>

    <script src="<c:url value="/static/js/d3/d3.v3.min.js" />" type="text/javascript"></script>
    <script src="<c:url value="/static/js/jquery/jquery.js"/>" type="text/javascript"></script>
    <script src="<c:url value="/static/js/wordtree/wordtree.js"/>" type="text/javascript"></script>
    <script src="<c:url value="/static/js/wordtree/raphael.js"/>" type="text/javascript"></script>
    <script src="<c:url value="/static/js/wordtree/word-tree-layout.js"/>" type="text/javascript"></script>
    <script src="<c:url value="/static/js/wordtree/eco-tree-layout.js"/>" type="text/javascript"></script>

    <script type="text/javascript">
    (function() {
        var Dom  = YAHOO.util.Dom,      Connect = YAHOO.util.Connect,
            Json = YAHOO.lang.JSON,     Event = YAHOO.util.Event;

        var content = Dom.get(UI.CONTENT_EL_NAME);
        Dom.setStyle(content, "width", "4000px");

        var queryElId            = 'queryDataId',
            queryDataEl          = Dom.get(queryElId),
            rawQueryData         = queryDataEl.innerHTML,

            snippetDataElId      = 'snippetDataId',
            snippetDataEl        = Dom.get(snippetDataElId),
            rawSnippetData       = snippetDataEl.innerHTML,

            infoElId             = 'info',
            infoEl               = Dom.get(infoElId),
            graphContentElId     = 'graphcontent',
            graphContentEl       = Dom.get(graphContentElId),
            moveIncr             = 50,
            directions           = ['left', 'right', 'up', 'down'],
            paperMoveIncrs       = {},
            svgStyle             = { left : 'left', right : 'left', up : 'top', down : 'top' },
            svgIncrs             = { left : -1*moveIncr, right : moveIncr, down : moveIncr, up: -1*moveIncr};

        updateInfoDiv();

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

        function buildArrayOfArrays(resp) {
            var result = [];
            for(var i = 0; i < resp.length; i++) {
                result.push(resp[i].split(","));
            }
            return result;
        }
        var hl = {};
        hl["hl"] = fixHlStrings(rawSnippetData);

        // send the event
        Connect.initHeader('Content-Type', 'application/json');
        Connect.setDefaultPostHeader('application/json');
        Connect.asyncRequest('POST', '<c:url value="/analyze/snippets" />', {
            success: function(o) {
                var response = Json.parse(o.responseText);
                debugger;
                buildGraphicResponse(response);
            }
        }, Json.stringify(hl));

        function getSvgGraphElName(field) { return field + "_graph"; }
        function getMoveButtonElName(field, dir) { return field + "_MVBTN_" + dir; }
        function getFieldAndDirFromMoveButtonElName(elName) { return elName.split("_MVBTN_"); }
        function getTransform(fieldName, dir) {
            if (dir == "left") {
                paperMoveIncrs[fieldName].x -= 1;
            } else if (dir == "right") {
                paperMoveIncrs[fieldName].x += 1;
            } else if (dir == "down") {
                paperMoveIncrs[fieldName].y += 1;
            } else if (dir == "up") {
                paperMoveIncrs[fieldName].y -= 1;
            }
            return "t" + (paperMoveIncrs[fieldName].x) * moveIncr + "," + (paperMoveIncrs[fieldName].y * moveIncr);
        }

        function buildGraphicResponse(response) {
            var fields = Object.keys(response);

            for(var i = 0; i < fields.length; i++) {
                var fieldName = fields[i], fieldsObj = response[fieldName];
                paperMoveIncrs[fieldName] = { x : 0, y : 0};

                var lefts = buildArrayOfArrays(fieldsObj['left']);
                var rights = buildArrayOfArrays(fieldsObj['right']);

                buildResponseHtml(fieldName);
                var paper = createTree(getSvgGraphElName(fieldName), fieldsObj['queries'].join(",\n"), lefts, rights);
                formatSvgBorder(fieldName);
                registerListeners(paper, fieldName);
            }
            console.log(paperMoveIncrs);
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

            var buttonsDiv = UI.addDomElementChild('div', headerDiv, { id : field + "_btns" }, { float: "left"});
            for(var i = 0; i < directions.length; i++) {
                var dir = directions[i];
                UI.addDomElementChild('a', headerDiv, { id : getMoveButtonElName(field, dir) },
                        { "class" : "button move move_" + dir });
            }

            UI.addDomElementChild('div', headerDiv, { innerHTML: "Sentence results from field " + field },
                    { float : "left", "font-weight" : "bold", "padding-right" : "10px"});
            UI.addClearBothDiv(headerDiv);

            UI.addDomElementChild('div', parentDiv, { id : getSvgGraphElName(field) });
        }

        function registerListeners(paper, field) {
            for(var i = 0; i < directions.length; i++) {
                var dir = directions[i];

                Event.addListener(getMoveButtonElName(field, dir), 'click', function(e) {
                    Event.stopEvent(e);
                    var f = getFieldAndDirFromMoveButtonElName(this.id);
                    var field = f[0], dir = f[1];
                    var xformStr = getTransform(field, dir);
                    function xform(el) { el.transform(xformStr); }
                    paper.forEach(xform);
                });
            }
        }

        function createTree(elName, context, lefts, rights) {
            for(var i = 0; i < lefts.length; i++){
                lefts[i] = lefts[i].reverse();
            }

            var w = 500, //2500,
                h = 1000, //1000,
                detail = 100 /* % */;
            var paper = Raphael(elName, h, w);
            makeWordTree(rights, context, detail, elName, 2500, h, WordTree.RO_LEFT, paper);
            var wordTree = makeWordTree(lefts, context, detail, elName, 2500, h, WordTree.RO_RIGHT, paper);

            /*var children = wordTree.root.nodeChildren;
            for(i = 0; i < children.length; i++) {
                collapse(children[i], "gates", WordTree.RO_RIGHT);
            }
            var children = wordTree.root.nodeChildren[0].nodeChildren;
            //debugger;
            for(i = 0; i < children.length; i++) {
               // wordTree.collapseNode(children[i].id, false);
                //wordTree.setNodeColors(children[i].id, "red", "blue", true);
            }
            wordTree.UpdateTree();  */
            return paper;
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
